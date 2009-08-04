/*
Advanced Computing Center for Research and Education Proprietary License
Version 1.0 (April 2006)

Copyright (c) 2006, Advanced Computing Center for Research and Education,
 Vanderbilt University, All rights reserved.

This Work is the sole and exclusive property of the Advanced Computing Center
for Research and Education department at Vanderbilt University.  No right to
disclose or otherwise disseminate any of the information contained herein is
granted by virtue of your possession of this software except in accordance with
the terms and conditions of a separate License Agreement entered into with
Vanderbilt University.

THE AUTHOR OR COPYRIGHT HOLDERS PROVIDES THE "WORK" ON AN "AS IS" BASIS,
WITHOUT WARRANTY OF ANY KIND, EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT
LIMITED TO THE WARRANTIES OF MERCHANTABILITY, TITLE, FITNESS FOR A PARTICULAR
PURPOSE, AND NON-INFRINGEMENT. IN NO EVENT SHALL THE AUTHORS OR COPYRIGHT
HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER LIABILITY, WHETHER IN AN
ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM, OUT OF OR IN CONNECTION
WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE SOFTWARE.

Vanderbilt University
Advanced Computing Center for Research and Education
230 Appleton Place
Nashville, TN 37203
http://www.accre.vanderbilt.edu
*/ 

#include <glib.h>
#include <unistd.h>
#include <string.h>
#include <stdlib.h>
#include <stdio.h>
#include <assert.h>
#include <time.h>
#include <signal.h>
#include <sys/types.h>
#include <unistd.h>
#include "ibp_server.h"
#include "debug.h"
#include "log.h"
#include "security_log.h"
#include "dns_cache.h"
#include "lock_alloc.h"
#include "ceph/config.h"  //This is from ceph

int upgrade_resource_v120(Resource_t *res, GKeyFile *keyfile, char *group, DB_env_t *dbenv);

//***** This is just used in the parallel mounting of the resources ****
typedef struct {
  pthread_t thread_id;
  Resource_t *res;
  DB_env_t *dbenv;
  GKeyFile *keyfile;
  char *group;
} pMount_t;


//*****************************************************************************
// parallel_upgrade_resource - Mounts a resource in a separate thread
//*****************************************************************************

void *parallel_upgrade_resource(void *data) {
   pMount_t *pm = (pMount_t *)data;

   int err = upgrade_resource_v120(pm->res, pm->keyfile, pm->group, pm->dbenv);

   if (err != 0) {
     log_printf(0, "parallel_upgrade_resource:  Error mounting resource!!!!!\n");
     exit(-10);
   }

   pthread_exit(NULL);
}

//*****************************************************************************
//  parse_config - Parses the config file(fname) and initializes the config
//                 data structure (cfg).
//*****************************************************************************

int parse_config(GKeyFile *keyfile, Config_t *cfg) {
//  GKeyFileFlags flags;
//  GError *error = NULL;
  Server_t *server;
  char *str;
  int val;
  pMount_t *pm, *pmarray;

  // *** Initialize the data structure to default values ***
  server = &(cfg->server);
server->iface = NULL;
server->n_iface = 0;
//  assert((server->hostname = (char *)malloc(512)) != NULL);  server->hostname[511] = '\0';
//  gethostname(server->hostname, 511);
//  server->port = IBP_PORT;
  server->max_threads = 64;
  server->max_pending = 16;
  server->min_idle = 60;
  server->stats_size = 5000;
  set_net_timeout(&(server->timeout), 0, 1000*1000);  //**Wait 1sec
  server->logfile = "ibp.log";
  server->log_overwrite = 0;
  server->log_level = 0;
  server->log_maxsize = 100 * 1024 * 1024;
  server->debug_level = 0;
  server->timestamp_interval = 60;
  server->password = DEFAULT_PASSWORD;
  server->lazy_allocate = 0;
  cfg->dbenv_loc = "/tmp/ibp_dbenv";
  cfg->db_mem = 256;
  cfg->tmpdir = "/etc/ibp";
  cfg->force_resource_rebuild = 0;

  // *** Parse the Server settings ***
//  str = g_key_file_get_string(keyfile, "server", "address", NULL);
//  if (str != NULL) { free(server->hostname); server->hostname = str; }
//  val = g_key_file_get_integer(keyfile, "server", "port", NULL);
//  if (val > 0) server->port = val;
  val = g_key_file_get_integer(keyfile, "server", "threads", NULL);
  if (val > 0) server->max_threads = val;
  val = g_key_file_get_integer(keyfile, "server", "max_pending", NULL);
  if (val > 0) server->max_pending = val;
  val = g_key_file_get_integer(keyfile, "server", "min_idle", NULL);
  if (val > 0) server->min_idle = val;
  val = g_key_file_get_integer(keyfile, "server", "max_network_wait_ms", NULL);
  if (val > 0) {
     int sec = val/1000;
     int us = val - 1000*sec;
     us = us * 1000;  //** Convert from ms->us
     set_net_timeout(&(server->timeout), sec, us);  //**Convert it from ms->us
  }
  str = g_key_file_get_string(keyfile, "server", "log_file", NULL);
  if (str != NULL) server->logfile = str;
  val = g_key_file_get_integer(keyfile, "server", "log_level", NULL);
  if (val > 0) server->log_level = val;
  val = g_key_file_get_integer(keyfile, "server", "log_maxsize", NULL);
  if (val > 0) server->log_maxsize = val * 1024 * 1024;
  val = g_key_file_get_integer(keyfile, "server", "debug_level", NULL);
  if (val > 0) server->debug_level = val;
  cfg->tmpdir = g_key_file_get_string(keyfile, "server", "tmpdir", NULL);
  str = g_key_file_get_string(keyfile, "server", "db_env_loc", NULL);
  if (str != NULL) cfg->dbenv_loc = str;
  val = g_key_file_get_integer(keyfile, "server", "db_mem", NULL);
  if (val > 0) cfg->db_mem = val;

  //*** Do some initial config of the log and debugging info ***
  open_log(cfg->server.logfile);            
  set_log_level(cfg->server.log_level);
  set_debug_level(cfg->server.debug_level);
  set_log_maxsize(cfg->server.log_maxsize);

  // *** Now iterate through each resource which is assumed to be all groups beginning with "resource" ***      
  cfg->dbenv = create_db_env(cfg->dbenv_loc, cfg->db_mem, cfg->force_resource_rebuild);
  gsize ngroups, i;
  char **group = g_key_file_get_groups(keyfile, &ngroups);
  cfg->n_resources = 0;
  assert((cfg->res = (Resource_t *)malloc(sizeof(Resource_t)*(ngroups-1))) != NULL);
  assert((pmarray = (pMount_t *)malloc(sizeof(pMount_t)*(ngroups-1))) != NULL);
  for (i=0; i<ngroups; i++) {
      if (strncmp("resource", group[i], 8) == 0) {
         pm = &(pmarray[cfg->n_resources]);
         pm->res = &(cfg->res[cfg->n_resources]);
         pm->keyfile = keyfile;
         pm->group = group[i];
         pm->dbenv = cfg->dbenv;
  
         pthread_create(&(pm->thread_id), NULL, parallel_upgrade_resource, (void *)pm);

         cfg->n_resources++;
      }
  }  

  //** Wait for all the threads to join **
  void *dummy;
  for (i=0; i<cfg->n_resources; i++) {
     pthread_join(pmarray[i].thread_id, &dummy);
  }

  free(pmarray);

  if (cfg->n_resources < 0) {
     printf("parse_config:  No resources defined!!!!\n");
     abort();
  }

  return(0);
}

//*****************************************************************************
//*****************************************************************************
//*****************************************************************************

int main(int argc, const char **argv)
{

  vector<const char*> args;

  Config_t config;
  char *config_file;

  global_config = &config;   //** Make the global point to what's loaded
  memset(global_config, 0, sizeof(Config_t));  //** init the data

  if (argc < 2) {
     printf("ibp_upgrade12 config_file\n\n");
     return(0);
  }

  config_file = (char *)argv[1];
 
  argv_to_vec(argc, argv, args);
  parse_config_options(args);     // These are for EBOFS 


  //*** Open the config file *****
  printf("Config file: %s\n\n", config_file);

  GKeyFile *keyfile;
  GKeyFileFlags flags;
  GError *error = NULL;

  keyfile = g_key_file_new();
  flags = G_KEY_FILE_NONE;

  /* Load the GKeyFile from disk or return. */
  if (!g_key_file_load_from_file (keyfile, config_file, flags, &error)) {
    g_error (error->message);
    return(-1);
  }

  
  //** Parse the global options.  This also performs the upgrade***
  parse_config(keyfile, &config);

  g_key_file_free(keyfile);   //Free the keyfile context

  log_printf(0, "main: Completed shutdown. Exiting\n");
}
