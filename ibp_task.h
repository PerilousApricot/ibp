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

//************************************************************************
//************************************************************************


#ifndef __IBP_TASK_H_
#define __IBP_TASK_H_

#include <pthread.h>
#include "network.h"
#include "allocation.h"
#include "task_coordinator.h"
#include "resource.h"
#include "transfer_stats.h"

#define COMMAND_TABLE_MAX 100   //** Size of static command table

#define CMD_STATE_NONE      0   //** No command currently being processed
#define CMD_STATE_CMD       1   //** Still getting input for the command itself
#define CMD_STATE_WRITE     2   //** Sending data to client
#define CMD_STATE_READ      3   //** Reading data from the client
#define CMD_STATE_READWRITE 4   //** Performing both read and write operations simultaneously
#define CMD_STATE_RESULT    5   //** Sending the result to the client
#define CMD_STATE_WAITING   6   //** Command is in a holding pattern. Used for FIFO/BUFFER/CIRQ transfers
#define CMD_STATE_FINISHED  7   //** Command completed
#define CMD_STATE_NEW       8   //** New command
#define CMD_STATE_PARENT    9   //** This command has a 2ndary command which handles the parent connection
#define CMD_STATE_CLOSED   10   //** Command's network connection is already closed

#define PARENT_RETURN      1000 //** Used in worker_task to signify the return value is fro ma parent task so ignore ns

typedef struct {  // date_free args
  RID_t rid;
  uint64_t size;
  char    crid[128];        //** Character version of the RID for querying
} Cmd_internal_date_free_t;

typedef struct {  // expire_log args
  RID_t rid;
  time_t start_time;
  int direction;
  int mode;
  int max_rec;
  char    crid[128];        //** Character version of the RID for querying
} Cmd_internal_expire_log_t;


typedef struct {          //**Allocate args
   RID_t     rid;            //RID to use (0=don't care)
   Allocation_t a;           //Allocation being created
} Cmd_allocate_t;

#define PASSLEN  32
typedef struct {          //** Status Args
  RID_t rid;               //** RID for querying
  char    crid[128];        //** Character version of the RID for querying
  Rsize_t new_size[2];     //** New sizes
  int   start_time;         //** Start time used by status call
  int   subcmd;            //** Subcommand
  long int new_duration;      //** New max duration for allocation
  char password[PASSLEN];  //** Password  
} Cmd_status_t;

typedef struct {
  RID_t   rid;             //** RID for querying
  char    crid[128];       //** Character version of the RID for querying
  char    cid[64];         //** Character version of the ID for querying
  Cap_t   cap;             //** Manage cap of original allocation
  off_t   offset;          //** Offset into original allocation 
  off_t   len;             //** Length in original alloc if offset=len=0 then full range is given  
  uint32_t expiration;       //** Duration of proxy allocation
} Cmd_proxy_alloc_t;

typedef struct {
  RID_t rid;               //** RID for querying
  char    crid[128];       //** Character version of the RID for querying
  char    cid[64];        //** Character version of the ID for querying
  Cap_t   cap;             //** Key
  Cap_t   master_cap;      //** Master manage key for PROXY_MANAGE
  Rsize_t new_size;        //** New size
  off_t offset;
  int   subcmd;            //** Subcommand
  int   captype;           //** Capability type
  int  new_reliability;    //** New reliability
  long int new_duration;   //** New max duration for allocation
  Allocation_t a;          //** Allocation for command
} Cmd_manage_t;

typedef struct {
  int      sending;        //** Write state
  RID_t rid;               //** RID for querying
  osd_id_t  id;            //** Object id
  char    crid[128];        //** Character version of the RID for querying
  char    cid[64];         //** Character version of the ID for querying
  Resource_t *r;           //** Resource being used
  Task_que_t *tq;          //** Task que struct
  Cap_t   cap;             //** Key
  off_t   offset;          //** Offset into allocation to start writing
  off_t   len;             //** Length of write
  off_t   pos;             //** Current buf pos
  off_t   left;            //** Bytes left to copy
  Allocation_t a;          //** Allocation for command
} Cmd_write_t;

typedef struct {
  int      recving;        //** read state
  int      retry;          //** Used only for IBP_copy commands
  RID_t rid;               //** RID for querying
  osd_id_t  id;            //** Object id
  char    crid[128];       //** Character version of the RID for querying
  char    cid[64];         //** Character version of the ID for querying
  char    path[4096];      //** command path for phoebus transfers
  Resource_t *r;           //** Resource being used
  Task_que_t *tq;          //** Task que struct
  Cap_t   cap;             //** Key
  off_t   offset;          //** Offset into allocation to start writing
  off_t   len;             //** Length of write
  off_t   pos;             //** Current buf pos
  off_t   left;            //** Bytes left to copy
  int     valid_conn;      //** Determines if I need to make a new depot connection
  time_t remote_sto;       //** REmote commands server timeout
  time_t remote_cto;       //** Remote commands client timeout
  char   remote_wcap[1024];//** Remote Write cap for IBP_*copy commands
  Allocation_t a;          //** Allocation for command
} Cmd_read_t;

typedef struct {
  RID_t rid;               //** RID for querying
  osd_id_t  id;            //** Object id
  char    crid[128];        //** Character version of the RID for querying
  char    cid[64];         //** Character version of the ID for querying
  Resource_t *r;           //** Resource being used 
  int     key_type;
  Cap_t   cap; 
  int64_t   offset;          //** Offset into allocation to start reading
  uint64_t  len;             //** Length of read
} Cmd_internal_get_alloc_t;

typedef union {            //** Union of command args
    Cmd_allocate_t allocate;
    Cmd_status_t   status;
    Cmd_manage_t   manage;
    Cmd_write_t    write;
    Cmd_read_t     read;
    Cmd_proxy_alloc_t  proxy_alloc;
    Cmd_internal_get_alloc_t get_alloc;
    Cmd_internal_date_free_t date_free;
    Cmd_internal_expire_log_t expire_log;
} Cmd_args_t;

typedef struct {           //** Stores the state of the command
  int   state;             //** Internal command state or phase
  int   command;           //** Command being processed
  int   version;           //** Command version
  Cmd_args_t cargs;         //** Command args
} Cmd_state_t;

typedef struct ibp_task_struct {            //** Data structure sent to the job_pool thread
  NetStream_t *ns;          //** Stream to use
  Network_t   *net;         //** Network connection is managed by
  Task_coord_t *tc;         //** Global task coordinator
  Allocation_address_t  ipadd; //** Used for updating allocations
  Cmd_state_t cmd;
  int command_acl[COMMAND_TABLE_MAX+1];  //** ACL's for commands based on ns
  uint64_t tid;            //** Unique task id used for debugging purposes only
  int      myid;           //** Thread id
  Net_timeout_t  timeout;      //** Max wait time for select() calls
  time_t      cmd_timeout;  //** Timeout for the command in seconds since the epoch
  pthread_mutex_t lock;     //** Task lock
  struct ibp_task_struct *parent;
  struct ibp_task_struct *child;
  int         dpinuse;      //** Used in IBP_copy commands
  int         submitted;      //** USed to control access to the task
  Transfer_stat_t  stat;     //** Command stats
} ibp_task_t;

void lock_tc(ibp_task_t *task);
void unlock_tc(ibp_task_t*task);
Task_coord_t *create_task_coord();
void free_task_coord(Task_coord_t *tc);
Task_que_t *task_lock(Task_coord_t *tc, ibp_task_t *task, Resource_t *r, char *cid, int op, 
       int offset, int len, int tryagain, Allocation_t *alloc, int *lock);
void task_unlock(Task_coord_t *tc, Task_que_t *tq, int get_qlock);
void task_droplockreq(Task_coord_t *tc, ibp_task_t *task, char *cid, int op);
 
#endif

