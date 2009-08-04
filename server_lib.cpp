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

//*****************************************************************
//*****************************************************************

#include <string.h>
#include <signal.h>
#include <sys/socket.h>
#include "ibp_server.h"
#include "log.h"
#include "debug.h"
#include "security_log.h"
#include "allocation.h"
#include "resource.h"
#include "network.h"
#include "ibp_task.h"
#include "ibp_protocol.h"
#include "activity_log.h"
#include "net_sock.h"

Network_t *global_network;

time_t depot_start_time;   //**Depot start time

uint64_t task_count;
pthread_mutex_t task_count_lock = PTHREAD_MUTEX_INITIALIZER;

typedef struct {
  int max_threads;       //** Max allowed threads
  int curr_threads;      //** Current running threads
  int request_thread;    //** Request for a new thread
  pthread_mutex_t lock;  //** Lock for accessing the taskmgr data
  pthread_cond_t  cond; 
  Stack_t *completed;
} Taskmgr_t;

Taskmgr_t taskmgr;  //** Global used by the task rountines

//*****************************************************************
//  convert_epoch_time2net - Des what is says:)
//*****************************************************************

Net_timeout_t *convert_epoch_time2net(Net_timeout_t *tm, int epoch_secs) 
{
   int dt = epoch_secs - time(NULL); 
   if (dt < 0) dt = 5;  //** Even if it's timed out give it a little time
   return(set_net_timeout(tm, dt, 0));
}     

//*****************************************************************
// send_cmd_result - Sends the command result back
//*****************************************************************

int send_cmd_result(ibp_task_t *task, int status)
{
   NetStream_t *ns = task->ns;
   char result[100];
   Net_timeout_t dt;
   int nbytes, nstr;

   snprintf(result, sizeof(result), "%d \n", status);   
   log_printf(10, "send_cmd_result(tid=" LU " ns=%d): %s", task->tid, ns->id, result);
   convert_epoch_time2net(&dt, task->cmd_timeout);
   nstr = strlen(result);
   nbytes = write_netstream(ns, result, nstr, dt);
   if (nbytes != nstr) {
      log_printf(10, "send_cmd_result: Sent partial command!  sent %d bytes but should be %d\n", nbytes, nstr);
   }

   alog_append_cmd_result(task->myid, status);

   return(nbytes);
}

//*****************************************************************
//  lock_task - Lock the task mutex
//*****************************************************************

void lock_task(ibp_task_t *task)
{
  log_printf(15, "lock_task: ns=%d\n", task->ns->id);
  pthread_mutex_lock(&(task->lock));
}

//*****************************************************************
//  unlock_task - Lock the task mutex
//*****************************************************************

void unlock_task(ibp_task_t *task)
{
  log_printf(15, "unlock_task: ns=%d\n", task->ns->id);
  pthread_mutex_unlock(&(task->lock));
}


//*****************************************************************
// shutdown_request - Returns 1 if a shutdown request has been made
//*****************************************************************

int shutdown_request()
{
  int state;

   pthread_mutex_lock(&shutdown_lock);
   state = shutdown_now;
   pthread_mutex_unlock(&shutdown_lock);

   return(state);
}

//*****************************************************************
// init_starttime - Stores the depot start time
//*****************************************************************

void set_starttime()
{
  depot_start_time = time(NULL);
}

//*****************************************************************
//  get_starttime - Returns the depot start time
//*****************************************************************

time_t get_starttime()
{
  return(depot_start_time);
}

//*****************************************************************
//  print_uptime - Stores in the provided string the depot uptime
//*****************************************************************

void print_uptime(char *str, int n)
{
   char up_str[256], time_str[256];
   int days, hours, min, sec, d;

   time_t dt = get_starttime();
   ctime_r(&dt, time_str);
   time_str[strlen(time_str)-1] = '\0';  //**Strip the CR

   dt = time(NULL) - get_starttime();
   hours = dt / 3600;
   days = hours / 24;
   hours = hours - days * 24;

   d = dt % 3600; min = d / 60;
   sec = d - min * 60;

   snprintf(up_str, sizeof(up_str), "%d:%d:%d:%d", days, hours, min, sec);

   snprintf(str, n, "Depot start time: %s\nUptime(d:h:m:s): %s\n", time_str, up_str);
  
   return;
}

//*****************************************************************************
//  print_config - Prints the config file to the stream;
//*****************************************************************************

void print_config(FILE *fd, Config_t *cfg) {
  Server_t *server;
  int d;

  server = &(cfg->server);

  //Print the server settings first
  fprintf(fd, "[server]\n");
  fprintf(fd, "address = %s\n", server->hostname);
  fprintf(fd, "port = %d\n", server->port);
  fprintf(fd, "threads = %d\n", server->max_threads);
  fprintf(fd, "max_pending = %d\n", server->max_pending);
//  fprintf(fd, "max_connections = %d\n", server->max_connections);
  fprintf(fd, "min_idle = %d\n", server->min_idle);
  d = server->timeout.tv_usec / 1000 + server->timeout.tv_sec * 1000; 
  fprintf(fd, "max_network_wait_ms = %d\n", d);
  fprintf(fd, "password = %s\n", server->password);  
  fprintf(fd, "stats_size = %d\n", server->stats_size);
  fprintf(fd, "lazy_allocate = %d\n", server->lazy_allocate);
  fprintf(fd, "db_env_loc = %s\n", cfg->dbenv_loc);
  fprintf(fd, "db_mem = %d\n", cfg->db_mem);
  fprintf(fd, "log_file = %s\n", server->logfile);
  fprintf(fd, "log_level = %d\n", server->log_level);
  d = server->log_maxsize / 1024 / 1024;
  fprintf(fd, "log_maxsize = %d\n", d);
  fprintf(fd, "debug_level = %d\n", server->debug_level);
  fprintf(fd, "enable_timestamps = %d\n", server->enable_timestamps);
  fprintf(fd, "timestamp_interval = %d\n", server->timestamp_interval);
  fprintf(fd, "activity_file = %s\n", server->alog_name);
  d = server->alog_max_size / 1024 / 1024;
  fprintf(fd, "activity_maxsize = %d\n", server->alog_max_size);
  fprintf(fd, "\n");
  fprintf(fd, "force_resource_rebuild = %d\n", cfg->force_resource_rebuild);
  fprintf(fd, "truncate_duration = %d\n", cfg->truncate_expiration);
  fprintf(fd, "\n");

  //Cycle through each resource
  fprintf(fd, "# Total Resources : %d\n\n", cfg->n_resources);
  int i;
  for (i=0; i< cfg->n_resources; i++) {
      print_resource(&(cfg->res[i]), fd);
  }  

  print_command_config(fd);  //** Print the rest of the command info
}

//*****************************************************************
// read_command - Reads a command from the stream
//*****************************************************************

int read_command(ibp_task_t *task)
{
   NetStream_t *ns = task->ns;
   Cmd_state_t *cmd = &(task->cmd);
    
   int bufsize = 10*1024; 
   char buffer[bufsize];
   char *bstate;
   int  nbytes, status, offset, count;
   int err, fin;
   time_t endtime;
   command_t *mycmd;

//   pthread_mutex_unlock(&(task->lock));

   log_printf(10, "read_command: ns=%d initial tid=" LU " START--------------------------\n", task->ns->id, task->tid);

   cmd->state = CMD_STATE_CMD;
   task->child = NULL;
   task->parent = NULL;

   clear_stat(&(task->stat));
   memset(task->stat.address, 0, sizeof(task->stat.address));
   if (task->stat.address != 0) {
     strncpy(task->stat.address, task->ns->peer_address, sizeof(task->stat.address));
   }

   Net_timeout_t dt;
//   int wt = global_config->server.min_idle/3;
   set_net_timeout(&dt, 1, 0);
   offset = 0;
   count = 0;
   endtime = time(NULL) + 10; //** Wait a max of 10 sec
   do {
     nbytes = readline_netstream_raw(ns, &(buffer[offset]), sizeof(buffer) - offset, dt, &status);
     count++;
     offset = offset + nbytes;
   } while ((offset > 0) && (status == 0) && (time(NULL) <= endtime));
   nbytes = offset;

   log_printf(10, "read_command: ns=%d tid=" LU " Command: %s\n", task->ns->id, task->tid, buffer);
   log_printf(10, "read_command: ns=%d nbytes:=  %d\n", task->ns->id, nbytes);
   flush_log();

   if (nbytes == -2) {  //** Not enough buffer space to flag an internal error
      send_cmd_result(task, IBP_E_INTERNAL);
      cmd->state = CMD_STATE_FINISHED;
      cmd->command = IBP_NOP;
      return(-1);
   } else if (nbytes == -1) {
     return(-1);
   } else if (nbytes == 0) {
     if (status == -1) {
        return(-1);
     } else {
        return(1);
     }
   }

   //** Looks like we have actual data so inc the tid **
   pthread_mutex_lock(&task_count_lock);
   task->tid = task_count;
   task_count++;
   if (task_count > 1000000000) task_count = 0;
   pthread_mutex_unlock(&task_count_lock);

   cmd->version = -1;  cmd->command = -1;
   sscanf(string_token(buffer, " ", &bstate, &fin), "%d", &(cmd->version));
   log_printf(10, "read_command: version=%d\n", cmd->version); flush_log(); 
   sscanf(string_token(NULL, " ", &bstate, &fin), "%d", &(cmd->command));

   log_printf(10, "read_command: ns=%d version = %d tid=" LU "* Command = %d\n", task->ns->id, cmd->version, task->tid, cmd->command);
flush_log();

   if (cmd->version == -1) {
      send_cmd_result(task, IBP_E_BAD_FORMAT);
      cmd->state = CMD_STATE_FINISHED;
      return(-1);
  }   

   //** Rest of arguments depends on the command **
   err = 0;
   if ((cmd->command<0) || (cmd->command > COMMAND_TABLE_MAX)) {
      log_printf(10, "read_command:  Unknown command! ns=%d\n", task->ns->id);
      send_cmd_result(task, IBP_E_BAD_FORMAT);
      cmd->state = CMD_STATE_FINISHED;
      err = -1;
   } else {
      mycmd = &(global_config->command[cmd->command]);
      if (mycmd->read != NULL) err = mycmd->read(task, &bstate); 

      if (task->command_acl[cmd->command] == 0) {  //** Not allowed so err out
         log_printf(10, "read_command:  Can't execute command due to ACL restriction! ns=%d cmd=%d\n", task->ns->id, cmd->command);
         send_cmd_result(task, IBP_E_UNKNOWN_FUNCTION);
         cmd->state = CMD_STATE_FINISHED;
         err = -1;
      }
   }

  log_printf(10, "read_command: end of routine ns=%d tid=" LU " err = %d\n", task->ns->id, task->tid, err);

  return(err);
 
}

//*****************************************************************
// handle_command - Main worker thread to handle an IBP commands
//*****************************************************************

int handle_task(ibp_task_t *task)
{
   NetStream_t *ns = task->ns;
   Cmd_state_t *cmd = &(task->cmd);
   uint64_t mytid = task->tid;
   command_t *mycmd;
   int err = 0;

   cmd->state = CMD_STATE_CMD;
   
   log_code( time_t tt = time(NULL); )
   log_printf(10, "handle_task: ns=%d ***START*** tid=" LU " Got a connection at " TT "\n", ns->id, task->tid, time(NULL));

   //**** Start processing the command *****
   err = 0;
   if ((cmd->command<0) || (cmd->command > COMMAND_TABLE_MAX)) {
      log_printf(10, "handle_command:  Unknown command! ns=%d\n", task->ns->id);
      send_cmd_result(task, IBP_E_BAD_FORMAT);
      cmd->state = CMD_STATE_FINISHED;
      err = -1;
   } else {
      mycmd = &(global_config->command[cmd->command]);
      if (mycmd->execute != NULL) err = mycmd->execute(task);    
   }

   log_printf(10, "handle_task: tid=" LU " After handle_command " TT " ns=%d\n", task->tid, time(NULL),task->ns->id);


  log_code( tt = time(NULL); )
   log_printf(10, "handle_task: ns=%d err=%d ***END*** tid=" LU " Completed processing at " TT "\n", ns->id, err, mytid, tt);

   return(err);
}

//*****************************************************************
// worker_task - Handles the task thread
//*****************************************************************

void *worker_task(void *arg)
{
   Thread_task_t *th = (Thread_task_t *)arg;
   ibp_task_t task;
   int closed;
   int status;
   int myid;
   int ncommands;
   time_t last_used;

   log_printf(10, "worker_task: ns=%d ***START*** Got a connection at " TT "\n", th->ns->id, time(NULL));


   task.tid = 0;
   task.ns = th->ns;
   task.net = global_network;
   closed = 0;
   ncommands = 0;
   last_used = time(NULL);

   //** Store the address for use in the time stamps
   task.ipadd.atype = AF_INET;
   ipdecstr2address(task.ns->peer_address, task.ipadd.ip);

   generate_command_acl(task.ns->peer_address, task.command_acl);

   myid = reserve_thread_slot();
   task.myid = myid;
   log_printf(0, "worker_task: open_thread: myid=%d ns=%d\n", myid, ns_getid(task.ns));

   alog_append_thread_open(myid, task.ns->id, task.ipadd.atype, task.ipadd.ip);

   
   while ((shutdown_request() == 0) && (closed == 0)) {
      status = read_command(&task);
      if (status == 0) {
          ncommands++;
          closed = handle_task(&task);
          last_used = time(NULL);
      } else if (status == -1) {
          closed = 1;
      }

      if (request_task_close() == 1) closed = 1; 
      if ((time(NULL) - last_used) > global_config->server.min_idle) closed = 1; 
   }

   alog_append_thread_close(myid, ncommands);

   release_thread_slot(myid);

   close_netstream(task.ns);
   release_task(th);

   log_printf(10, "worker_task: ns=%d myid=%d ***END*** exiting at " TT "\n", th->ns->id, myid, time(NULL));

   pthread_join(th->thread, NULL); 

   return(NULL);
}

//*****************************************************************
// close_client_connection - Closes a connection CLEANLY
//*****************************************************************

void close_client_connection(ibp_task_t *task)
{
  ibp_task_t *parent, *child;

  log_printf(15, "close_client_connection: Start of routine.  ns=%d\n", task->ns->id);

  lock_tc(task); 
  lock_task(task);
  
  clear_stat(&(task->stat));
  if ((task->cmd.state == CMD_STATE_FINISHED) || (task->cmd.state == CMD_STATE_NONE)) {
     close_netstream(task->ns);            
  } else {
     switch (task->cmd.command) {
        case IBP_WRITE:
        case IBP_STORE:
        case IBP_LOAD:
             log_printf(15, "close_client_connection: Closing IBP_WRITE/STORE/LOAD connection.  ns=%d\n", task->ns->id);
             if (task->cmd.command == IBP_LOAD) {
                if (task->cmd.cargs.read.a.type != IBP_BYTEARRAY) {
                   task_droplockreq(task->tc, task, task->cmd.cargs.read.cid, TASK_READ);
                }
             } else {
                if (task->cmd.cargs.write.a.type != IBP_BYTEARRAY) {
                   task_droplockreq(task->tc, task, task->cmd.cargs.write.cid, TASK_WRITE);
                }
             }
 
             task->cmd.state = CMD_STATE_FINISHED;
             close_netstream(task->ns);
             break;
        case IBP_SEND:
        case INTERNAL_SEND:
             if (task->cmd.command == IBP_SEND) {
                log_printf(15, "close_client_connection: Closing IBP_SEND connection.  ns=%d\n", task->ns->id);
                parent = task;
                child = task->child;
             } else {
             log_printf(15, "close_client_connection: Closing INTERNAL_SEND connection.  ns=%d\n", task->ns->id);
                parent = task->parent;
                child = task;
             }

             if (parent) {
                clear_stat(&(parent->stat));
                close_netstream(parent->ns);
             }
             if (child) {
                if (child->cmd.cargs.read.a.type != IBP_BYTEARRAY) {
                   task_droplockreq(child->tc, child, child->cmd.cargs.read.cid, TASK_READ);
                }
                child->cmd.state = CMD_STATE_FINISHED;
                close_netstream(child->ns);            
             }
             break;
     }

  }

  unlock_tc(task); 
  unlock_task(task);  
}

//*****************************************************************
//  currently_running_tasks - Returns the number of tasks currently 
//         running
//*****************************************************************

int currently_running_tasks()
{
  int n;

  pthread_mutex_lock(&(taskmgr.lock));
  n = taskmgr.curr_threads;
  pthread_mutex_unlock(&(taskmgr.lock));

  return(n);  
}

//*****************************************************************
// spawn_new_task - Spawns a new task
//*****************************************************************

void spawn_new_task(NetStream_t *ns)
{
  Thread_task_t *t = (Thread_task_t *)malloc(sizeof(Thread_task_t));

  t->ns = ns;

  pthread_attr_init(&(t->attr));

//** if needed set the default stack size **
//  size_t stacksize;
//  pthread_attr_getstacksize(&(t->attr), &stacksize);
//  log_printf(0, "spawn_new_task: default stacksize=" ST "\n", stacksize);

  pthread_create(&(t->thread), NULL, worker_task, (void *)t);

  pthread_mutex_lock(&(taskmgr.lock));  
  taskmgr.curr_threads++;
  pthread_mutex_unlock(&(taskmgr.lock));  
}

//*****************************************************************
// release_task - Releases the current task back for respawning
//*****************************************************************

void release_task(Thread_task_t *t)
{
  pthread_mutex_lock(&(taskmgr.lock));

  taskmgr.curr_threads--;
  push(taskmgr.completed, t);
  
  pthread_mutex_unlock(&(taskmgr.lock));
  
}

//*****************************************************************
// request_task_close - checks to see if the task should be closed
//*****************************************************************

int request_task_close()
{
  int result;

  result = 0;

  pthread_mutex_lock(&(taskmgr.lock));

  if (taskmgr.request_thread == 1) {
//     result = (taskmgr.curr_threads>=taskmgr.max_threads) ? 1 : 0;
result = 1;
  }
  if (result == 1) pthread_cond_signal(&(taskmgr.cond));

  log_printf(15, "request_task_close: result=%d ncurr=%d\n", result, taskmgr.curr_threads);

  pthread_mutex_unlock(&(taskmgr.lock));

  return(result);
}

//*****************************************************************
// join_completed - Does a join on any completed task/threads
//*****************************************************************

void join_completed()
{
  Thread_task_t *t;
  void *ptr;

  pthread_mutex_lock(&(taskmgr.lock));

  while ((t=(Thread_task_t *)pop(taskmgr.completed)) != NULL) {
     pthread_join(t->thread, &ptr);
     destroy_netstream(t->ns);
     free(t);
  }
  
  pthread_mutex_unlock(&(taskmgr.lock));
}

//*****************************************************************
// signal_taskmgr - Signals the taskmgr to wakeup if needed
//*****************************************************************

void signal_taskmgr()
{
  pthread_mutex_lock(&(taskmgr.lock));
  pthread_cond_signal(&(taskmgr.cond));
  pthread_mutex_unlock(&(taskmgr.lock));
}

//*****************************************************************
// wait_for_free_task - Waits until a free task slot is available
//*****************************************************************

void wait_for_free_task()
{
  pthread_mutex_lock(&(taskmgr.lock));
 
  if (taskmgr.curr_threads >= taskmgr.max_threads) {
     taskmgr.request_thread = 1;
     log_printf(15, "wait_for_free_task: Before cond_wait time=" TT "\n", time(NULL));
     pthread_cond_wait(&(taskmgr.cond), &(taskmgr.lock));
     log_printf(15, "wait_for_free_task: After cond_wait time=" TT "\n", time(NULL));
     taskmgr.request_thread = 0;
  }

  pthread_mutex_unlock(&(taskmgr.lock));
}

//*****************************************************************
// wait_all_tasks - Waits until all tasks have completed
//*****************************************************************

void wait_all_tasks()
{
  struct timespec t;
  pthread_mutex_lock(&(taskmgr.lock));
 
  while (taskmgr.curr_threads > 0) {
    t.tv_sec = time(NULL) + 1;    //wait for at least a second
    t.tv_nsec = 0;
    taskmgr.request_thread = 1;
    pthread_cond_timedwait(&(taskmgr.cond), &(taskmgr.lock), &t);
  }   

  pthread_mutex_unlock(&(taskmgr.lock));
}

//*****************************************************************
// init_tasks  -Inititalizes the task management
//*****************************************************************

void init_tasks()
{
  taskmgr.max_threads = global_config->server.max_threads;
  taskmgr.curr_threads = 0;
  taskmgr.request_thread = 0;
  taskmgr.completed = new_stack();

  pthread_mutex_init(&(taskmgr.lock), NULL);
  pthread_cond_init(&(taskmgr.cond), NULL);  
}

//*****************************************************************
// close_tasks - Closes the task management
//*****************************************************************

void close_tasks()
{
  wait_all_tasks();

  join_completed();

  free_stack(taskmgr.completed, 0);

  pthread_mutex_destroy(&(taskmgr.lock));
  pthread_cond_destroy(&(taskmgr.cond));
}

//*****************************************************************
// server_loop - Main processing loop
//*****************************************************************

void server_loop(Config_t *config)
{
  Network_t *network;  
  NetStream_t *ns, *bns;
  time_t tt;
  time_t print_time;
  char current_time[128];

  print_time = time(NULL);

  log_printf(10, "server_loop: Start.....\n");

  task_count = 0;

  //*** Init the networking ***
  network = network_init();

  if (network == NULL) return;
  global_network = network;

  bns = new_netstream();
  ns_config_sock(bns, -1, 0);
  if (bind_server_port(network, bns, config->server.hostname, config->server.port, config->server.max_pending) != 0) {
     shutdown_now =1;  //** Trigger a shutdown
  }

//  if ((tc = create_task_coord()) == NULL) goto ns_bail;

  init_tasks();

  //*** Main processing loop ***
  while (shutdown_request() == 0) {
     tt = time(NULL);
     log_printf(10, "server_loop: Waiting for a connection time= " TT "\n", tt);
     if (wait_for_connection(network, config->server.timeout_secs) > 0) { // ** got a new connection
        log_printf(10, "server_loop: Got a connection request or timed out!  time=" TT "\n", time(NULL));
        wait_for_free_task(); 
        ns = new_netstream();
        if (accept_pending_connection(network, ns) == 0) {
           spawn_new_task(ns);
        } else {
           destroy_netstream(ns);
        }
     }
        
     if (time(NULL) > print_time) {   //** Print the time stamp
        print_time = time(NULL);
        ctime_r(&print_time, current_time);
        log_printf(0, "MARK: " TT " ------> %s", print_time, current_time);
        print_time += config->server.timestamp_interval;
     }

     join_completed();   //** reap any completed threads
  }

  log_printf(15, "Exited server_loop\n"); flush_log();

  network_close(network);  //** Stop accepting connections

  close_tasks();

  log_printf(15, "before network_close\n"); flush_log();

  //*** Shutdown the networking ***
  network_destroy(network);
  destroy_netstream(bns);
  
//  if ((tc != NULL) free_task_coord(tc);
  
  log_printf(10, "server_loop: Stop.....\n");

  return;
}

