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

#ifndef _IBP_SERVER_H_
#define _IBP_SERVER_H_

#include <sys/types.h>
#include <pthread.h>
#include <glib.h>
#include "resource_list.h"
#include "network.h"
#include "ibp_task.h"
#include "fmttypes.h"
#include "resource.h"
#include "subnet.h"
#include "string_token.h"
#include "phoebus.h"

#define IBP_ST_STATS  4   //** Return stats
#define IBP_ST_VERSION 5  //** Return the version string

#define DEFAULT_PASSWORD "IBPch@ngem3"
#define IBP_PORT 6714

//** PRivate internal commands
#define INTERNAL_EXPIRE_LIST 97
#define INTERNAL_DATE_FREE 98
#define INTERNAL_GET_ALLOC 99
#define INTERNAL_SEND 100

//*** Internal constant to represent the key is the osd_id
#define INTERNAL_ID 4  

typedef struct {  //** bind ports
  char *hostname;
  int port;
} interface_t;

typedef struct {    //*** forms the fn table for a depot command
   int command;                             //** Command value
   int used;                                //** Determines if the command is used
   char name[64];                           //** Command name
   subnet_list_t *subnet;                   //** Valid subnets for command execution
   char **acl;                              //** String version of the ACLs
   void (*load_config)(GKeyFile *keyfile);  //** Used to load the config file
   void (*init)(void);                      //** Perform final initialization. Called after depot init
   void (*destroy)(void);                   //** Cleanup.  Called before depot destroy
   void (*print)(FILE *fd);                 //** Print command cofnig to fd
   int (*read)(ibp_task_t *task, char **bstate);  //** Reads the command from the socket
   int (*execute)(ibp_task_t *task);       //** The actual command
}  command_t;

#define COMMAND_TABLE_MAX 100   //** Size of static command table

typedef struct {       // Structure containg the overall server config
   interface_t *iface;   //Interfaces listening on
   int n_iface;          //Number of bound interfaces
//   char *hostname;       //Hostname to bind to
//   int port;             //Port to listen on
   int max_threads;      //Max number of threads for pool
   int max_pending;      //Max pending connections
   int timestamp_interval;  //Log timestamp interval in sec
   int stats_size;       //Max size of statistics to keep
   Net_timeout_t timeout;  //Max waiting time on a network connection
   int timeout_secs;        //Same as above just in simple seconds and not a struct
   int min_idle;         //Min time of inactivity for a connection before it's *possibly* considered dead
   int lazy_allocate;    //If 1 don't create the physical file just make the DB entry
   const char *logfile;        //Log file
   int log_maxsize;      //Max size of logs to keep before rolling over
   int  log_level;       //Log level to control output
   int  log_overwrite;   //overwrite log file instead of append(default)
   int  debug_level;     //Debug level if compiled with debugging turned on
   int  enable_timestamps;     //Enables access timestamp logging
   const char *debugfile;      //Debug output file (default stdout)
   int   alog_max_size;      //Max size for the activity log
   const char *alog_name;      //Activity file (default ibp_activity.log)
   const char *alog_host;      //** Host to send alog info
   int   alog_max_history;     //** How many alog files to keep before dropping them
   int   alog_port;            //** alog host's port to use
   const char *password;        //Depot Password for status change commands
   char **default_acl;     //Default command ACLs
} Server_t;

typedef struct {      //Main config structure
   Server_t server;     // Server config
   int n_resources;     // Number of resources
   const char *tmpdir;        // Location of Temproray and log files
   const char *dbenv_loc;     // Location of DB enviroment
   int  db_mem;      // DB envirment memory usage in MB
   DB_env_t  *dbenv;     // Container for DB environment
   int  force_resource_rebuild; // Force rebuilding of all resources
   int  truncate_expiration;    // Force existing allocs duration to be the RID max.  Only used in rebuild!
   Resource_t *res;     // Array of resources
   Resource_list_t *rl; // Searchable list of resources
   command_t command[COMMAND_TABLE_MAX+1];  //** List of commands
} Config_t;


typedef struct {     //** Thrread data strcuture
  pthread_t thread;
  pthread_attr_t attr;
  NetStream_t *ns;
} Thread_task_t;


//**************Global control Variables*****************
extern pthread_mutex_t shutdown_lock;
extern int shutdown_now;
extern Config_t *global_config;
extern ibp_task_t *global_task;
extern Network_t *global_network;

//************** Function definitions *******************

//*** Function in install_commands.c ***
void install_commands(GKeyFile *kf);

//*** functions in commands.c ***
void generate_command_acl(char *peer_name, int *acl);
void add_command(int cmd, const char *cmd_keyword, GKeyFile *kf,
   void (*load_config)(GKeyFile *keyfile), 
   void (*init)(void),
   void (*destroy)(void),
   void (*print)(FILE *fd),
   int (*read)(ibp_task_t *task, char **bstate),
   int (*execute)(ibp_task_t *task) );
void print_command_config(FILE *fd);
void initialize_commands();
void destroy_commands();

//*** Functions in server_lib.c ****
void print_config(FILE *fd, Config_t *cfg);
void set_starttime();
time_t get_starttime();
void print_uptime(char *str, int n);
void lock_task(ibp_task_t *task);
void unlock_task(ibp_task_t *task);
void server_loop(Config_t *config);
Net_timeout_t *convert_epoch_time2net(Net_timeout_t *tm, int epoch_secs);
int send_cmd_result(ibp_task_t *task, int status);
int get_command_timeout(ibp_task_t *task, char **bstate);
int read_command(ibp_task_t *task);
Cmd_state_t *new_command();
void free_command(Cmd_state_t *cmd);
void *worker_task(void *arg);
void server_loop(Config_t *config);
void signal_shutdown(int sig);
void close_client_connection(ibp_task_t *task);
int request_task_close();
int currently_running_tasks();
void release_task(Thread_task_t *t);
void wait_all_tasks();
void signal_taskmgr();

//*** Functions in parse_commands.c ***
int read_rename(ibp_task_t *task, char **bstate);
int read_allocate(ibp_task_t *task, char **bstate);
int read_merge_allocate(ibp_task_t *task, char **bstate);
int read_alias_allocate(ibp_task_t *task, char **bstate);
int read_status(ibp_task_t *task, char **bstate);
int read_manage(ibp_task_t *task, char **bstate);
int read_write(ibp_task_t *task, char **bstate);
int read_read(ibp_task_t *task, char **bstate);
int read_internal_get_alloc(ibp_task_t *task, char **bstate);
int read_internal_date_free(ibp_task_t *task, char **bstate);
int read_internal_expire_list(ibp_task_t *task, char **bstate);

//*** Functions in handle_commands.c ***
int handle_allocate(ibp_task_t *task);
int handle_merge(ibp_task_t *task);
int handle_alias_allocate(ibp_task_t *task);
int handle_rename(ibp_task_t *task);
int handle_status(ibp_task_t *task);
int handle_manage(ibp_task_t *task);
int handle_write(ibp_task_t *task);
int handle_read(ibp_task_t *task);
int handle_copy(ibp_task_t *task);
int handle_transfer(ibp_task_t *task, osd_id_t rpid, NetStream_t *ns, const char *key, const char *typekey);
int handle_internal_get_alloc(ibp_task_t *task);
int handle_internal_date_free(ibp_task_t *task);
int handle_internal_expire_list(ibp_task_t *task);

//*** Functions in buffer_transfer.c ***
int read_from_disk(ibp_task_t *task, Allocation_t *a, off_t *left, Resource_t *res);
int write_to_disk(ibp_task_t *task, Allocation_t *a, off_t *left, Resource_t *res);
void stream_transfer(ibp_task_t *rtask, ibp_task_t *wtask, Allocation_t *a, int *r_status, int *w_status);
int single_buffer_transfer(ibp_task_t *mytask, Task_que_t *tq);

//*** Functions in transfer_stats.c ***
void init_stats(int n);
void free_stats();
void clear_stat(Transfer_stat_t *s);
void get_transfer_stats(uint64_t *rbytes, uint64_t *wbytes, uint64_t *copybytes);
void add_stat(Transfer_stat_t *s);
int send_stats(NetStream_t *ns, time_t start_time, Net_timeout_t dt);

//*** Functions in test_alloc.c ***
void test_alloc();

//*** Functions in thread_slots.c ****
void release_thread_slot(int slot);
int reserve_thread_slot();
void destroy_thread_slots();
void init_thread_slots(int size);

#endif

