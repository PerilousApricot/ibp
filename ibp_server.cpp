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
#include "activity_log.h"
#include "ceph/config.h"  //This is from ceph

//***** This is just used in the parallel mounting of the resources ****
typedef struct {
  pthread_t thread_id;
  Resource_t *res;
  DB_env_t *dbenv;
  GKeyFile *keyfile;
  char *group;
  int force_resource_rebuild;
} pMount_t;

//*****************************************************************************
// parallel_mount_resource - Mounts a resource in a separate thread
//*****************************************************************************

void *parallel_mount_resource(void *data) {
   pMount_t *pm = (pMount_t *)data;

   int err = mount_resource(pm->res, pm->keyfile, pm->group, pm->dbenv, 
        pm->force_resource_rebuild, global_config->server.lazy_allocate, 
        global_config->truncate_expiration);

   if (err != 0) {
     log_printf(0, "parallel_mount_resource:  Error mounting resource!!!!!\n");
     exit(-10);
   }

   pthread_exit(NULL);
}

//*****************************************************************************
// log_preamble - Print the initial log file output
//*****************************************************************************

void log_preamble(Config_t *cfg)
{
  time_t t = get_starttime();
  
  log_printf(0, "\n");
  log_printf(0, "*****************************************************************\n");
  log_printf(0, "Starting ibp_server on %s", ctime(&t));
  log_printf(0, "*****************************************************************\n");
  log_printf(0, "\n");

  log_printf(0, "*********************Printing configuration file **********************\n\n");
   
  print_config(log_fd(), cfg);  

  log_printf(0, "*****************************************************************\n\n");
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
  int val, k;
  gsize n;
  pMount_t *pm, *pmarray;

  // *** Initialize the data structure to default values ***
  server = &(cfg->server);
  server->max_threads = 64;
  server->max_pending = 16;
  server->min_idle = 60;
  server->stats_size = 5000;
  set_net_timeout(&(server->timeout), 1, 0);  //**Wait 1sec
  server->timeout_secs = 1;
  server->logfile = "ibp.log";
  server->log_overwrite = 0;
  server->log_level = 0;
  server->log_maxsize = 100 * 1024 * 1024;
  server->debug_level = 0;
  server->enable_timestamps = 1;
  server->timestamp_interval = 60;
  server->password = DEFAULT_PASSWORD;
  server->lazy_allocate = 1;
  server->alog_name = "ibp_activity.log";
  server->alog_max_size = 100*1024*1024;
  server->alog_max_history = 1;
  server->alog_host = NULL;
  server->alog_port = 0;
  cfg->dbenv_loc = "/tmp/ibp_dbenv";
  cfg->db_mem = 256;
  cfg->tmpdir = "/etc/ibp";
  cfg->force_resource_rebuild = 0;
  cfg->truncate_expiration = 0;

  // *** Parse the Server settings ***
  char **list = g_key_file_get_string_list(keyfile, "server", "interfaces",  &n, NULL);
  interface_t *iface;
  if (list == NULL) {
     server->iface = (interface_t *)malloc(sizeof(interface_t));
     assert(server->iface != NULL);
     server->n_iface = 1;
     iface = server->iface;
     assert((iface->hostname = (char *)malloc(512)) != NULL);  iface->hostname[511] = '\0';
     gethostname(iface->hostname, 511);
     iface->port = IBP_PORT;
  } else {  //** Cycle through the hosts
     server->iface = (interface_t *)malloc(sizeof(interface_t)*n);
     assert(server->iface != NULL);
     server->n_iface = n;
     char *bstate;
     int fin;
     for (k=0; k<n; k++) {
        iface = &(server->iface[k]);
        iface->hostname = string_token(list[k], ":", &bstate, &fin);
        if (sscanf(string_token(NULL, " ", &bstate, &fin), "%d", &(iface->port)) != 1) {
           iface->port = IBP_PORT;
        }     
     }
  }

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
     server->timeout_secs = sec;
     set_net_timeout(&(server->timeout), sec, us);  //**Convert it from ms->us
  }
  val = g_key_file_get_integer(keyfile, "server", "stats_size", NULL);
  if (val > 0) server->stats_size = val;
  val = g_key_file_get_integer(keyfile, "server", "timestamp_interval", NULL);
  if (val > 0) server->timestamp_interval = val;
  str = g_key_file_get_string(keyfile, "server", "password", NULL);
  if (str != NULL) server->password = str;
  str = g_key_file_get_string(keyfile, "server", "log_file", NULL);
  if (str != NULL) server->logfile = str;
  val = g_key_file_get_integer(keyfile, "server", "log_level", NULL);
  if (val > 0) server->log_level = val;
  val = g_key_file_get_integer(keyfile, "server", "enable_timestamps", NULL);
  if (val > 0) server->enable_timestamps = val;
  val = g_key_file_get_integer(keyfile, "server", "log_maxsize", NULL);
  if (val > 0) server->log_maxsize = val * 1024 * 1024;
  val = g_key_file_get_integer(keyfile, "server", "debug_level", NULL);
  if (val > 0) server->debug_level = val;
  cfg->tmpdir = g_key_file_get_string(keyfile, "server", "tmpdir", NULL);
  str = g_key_file_get_string(keyfile, "server", "db_env_loc", NULL);
  if (str != NULL) cfg->dbenv_loc = str;
  val = g_key_file_get_integer(keyfile, "server", "db_mem", NULL);
  if (val > 0) cfg->db_mem = val;
  val = g_key_file_get_integer(keyfile, "server", "lazy_allocate", NULL);
  if (val > 0) server->lazy_allocate = val;

  str = g_key_file_get_string(keyfile, "server", "activity_file", NULL);
  if (str != NULL) server->alog_name = str;
  val = g_key_file_get_integer(keyfile, "server", "activity_maxsize", NULL);
  if (val > 0) {
     server->alog_max_size = val * 1024 * 1024;
  } else if (val == -1) {
     server->alog_max_size = -1;
  }

  str = g_key_file_get_string(keyfile, "server", "activity_host", NULL);
  if (str != NULL) server->alog_host = str;
  val = g_key_file_get_integer(keyfile, "server", "activity_max_history", NULL);
  if (val > 0) server->alog_max_history = val;
  val = g_key_file_get_integer(keyfile, "server", "activity_port", NULL);
  if (val > 0) server->alog_port = val;

  val = g_key_file_get_integer(keyfile, "server", "force_resource_rebuild", NULL);
  if (val > 0) cfg->force_resource_rebuild = val;
  val = g_key_file_get_integer(keyfile, "server", "truncate_duration", NULL);
  if (val > 0) cfg->truncate_expiration = val;

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
         pm->res->rl_index = cfg->n_resources;
         pm->keyfile = keyfile;
         pm->group = group[i];
         pm->dbenv = cfg->dbenv;
         pm->force_resource_rebuild = cfg->force_resource_rebuild;
  
         pthread_create(&(pm->thread_id), NULL, parallel_mount_resource, (void *)pm);
//         mount_resource(&(cfg->res[cfg->n_resources]), keyfile, group[i], cfg->dbenv, cfg->force_resource_rebuild);

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

void signal_shutdown(int sig)
{
  time_t t = time(NULL);

  log_printf(0, "Shutdown requested on %s\n", ctime(&t));

  pthread_mutex_lock(&shutdown_lock);
  shutdown_now = 1;
  pthread_mutex_unlock(&shutdown_lock);

  signal_taskmgr();
  wakeup_network(global_network);

  return;
}

//*****************************************************************************
// shutdown - Shuts down everything
//*****************************************************************************

int shutdown(Config_t *cfg)
{
  int err;
  int i;

  //** Close all the resources **
  for (i=0; i< cfg->n_resources; i++) {
    if ((err = umount_resource(&(cfg->res[i]))) != 0) {
       char tmp[128];
       log_printf(0, "ibp_server: Error closing Resource %s!  Err=%d\n",rid2str(&(cfg->res[i].rid), tmp, sizeof(tmp)), err);
    }
  }  

  //** Now clsoe the DB environment **
  if ((err = close_db_env(cfg->dbenv)) != 0) {
     log_printf(0, "ibp_server: Error closing DB envirnment!  Err=%d\n", err);
  }

  return(0);
}

//*****************************************************************************
// configure_signals - Configures the signals
//*****************************************************************************

void configure_signals()
{
  sigset_t    sset;

  //***Attach the signal handler for shutdown
  if (signal(SIGQUIT, signal_shutdown) == SIG_ERR) {
     log_printf(0, "Error installing shutdown signal handler!\n");
  }     
  if (signal(SIGPIPE, SIG_IGN) == SIG_ERR) {
     log_printf(0, "Error installing shutdown signal handler!\n");
  }     

  //** Want everyone to ignore SIGPIPE messages
  sigemptyset(&sset);
  sigaddset(&sset,SIGPIPE);
  pthread_sigmask(SIG_UNBLOCK,&sset,NULL);
}

//*****************************************************************************
//*****************************************************************************
//*****************************************************************************

int main(int argc, const char **argv)
{

  vector<const char*> args;

  Config_t config;
  char *config_file;
  int i;

  global_config = &config;   //** Make the global point to what's loaded
  memset(global_config, 0, sizeof(Config_t));  //** init the data
  global_network = NULL;

  if (argc < 2) {
     printf("ibp_server [-d] config_file\n\n");
     printf("-d          - Run as a daemon\n");
     printf("config_file - Configuration file\n");
     return(0);
  }

  int astart = 1;
  int daemon = 0;
  if (strcmp(argv[astart], "-d") == 0) {
     daemon = 1;
     argv[astart] = "";
     astart++;
  } 

  config_file = (char *)argv[astart];
 
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

  
  //** Parse the global options first ***
  parse_config(keyfile, &config);

  init_thread_slots(2*config.server.max_threads);  //** Make pigeon holes

  dns_cache_init(1000);
  init_subnet_list(config.server.iface[0].hostname);  

  //*** Install the commands: loads Vectable info and parses config options only ****
  install_commands(keyfile);

  g_key_file_free(keyfile);   //Free the keyfile context

  set_starttime();

  log_preamble(&config);

  configure_signals();   //** Setup the signal handlers

  //*** Set up the shutdown variables  
  pthread_mutex_init(&shutdown_lock, NULL);
  pthread_mutex_unlock(&shutdown_lock);
  shutdown_now = 0;

  //*** Make the searchable version of the resources ***
  config.rl = create_resource_list(config.res, config.n_resources);
//  log_printf(0, "Looking up resource 2 and printing info.....\n")
//  print_resource(resource_lookup(config.rl, "2"), log_fd());

  init_stats(config.server.stats_size);
  lock_alloc_init();

  //***Launch as a daemon if needed***
  if (args.size() == 2) {    //*** Launch as a daemon ***
     if ((strcmp(config.server.logfile, "stdout") == 0) || 
         (strcmp(config.server.logfile, "stderr") == 0)) {
        log_printf(0, "Can't launch as a daemom because log_file is either stdout or stderr\n");  
        log_printf(0, "Running in normal mode\n");  
     } else if (fork() == 0) {    //** This is the daemon
        log_printf(0, "Running as a daemon.\n");
        flush_log();
        fclose(stdin);     //** Need to close all the std* devices **
        fclose(stdout);
        fclose(stderr);

        char fname[1024];
        fname[1023] = '\0';
        snprintf(fname, 1023, "%s.stdout", config.server.logfile);
        assert((stdout = fopen(fname, "w")) != NULL);
        snprintf(fname, 1023, "%s.stderr", config.server.logfile);
        assert((stderr = fopen(fname, "w")) != NULL);
//        stdout = stderr = log_fd();  //** and reassign them to the log device         
printf("ibp_server.c: STDOUT=STDERR=LOG_FD() dnoes not work!!!!!!!!!!!!!!!!!!!!!!!!\n");
     } else {           //** Parent exits
        exit(0);         
     }    
  }

//  test_alloc();   //** Used for testing allocation speed only

  //*** Initialize all command data structures.  This is mainly 3rd party commands ***
  initialize_commands();

  //** Launch the garbage collection threads
  for (i=0; i<config.n_resources; i++) launch_resource_cleanup_thread(&(config.res[i]));
  //*** Start the activity log ***
  alog_open();

  server_loop(&config);     //***** Main processing loop ******

  //*** Shutdown the activity log ***
  alog_close();

  //*** Destroy all the 3rd party structures ***
  destroy_commands();

  lock_alloc_destroy();

  destroy_thread_slots();

  shutdown(&config);

  free_resource_list(config.rl);

  free_stats();

  log_printf(0, "main: Completed shutdown. Exiting\n");
//  close_log();
//  close_debug();
}
