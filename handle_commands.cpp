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
#include <limits.h>
#include <time.h>
#include <pthread.h>
#include "dns_cache.h"
#include "ibp_ClientLib.h"
#include "ibp_server.h"
#include "log.h"
#include "debug.h"
#include "security_log.h"
#include "allocation.h"
#include "resource.h"
#include "db_resource.h"
#include "network.h"
#include "net_sock.h"
#include "net_phoebus.h"
#include "ibp_task.h"
#include "ibp_protocol.h"
#include "task_coordinator.h"
#include "server_version.h"
#include "lock_alloc.h"
#include "cap_timestamp.h"
#include "activity_log.h"
#include "fmttypes.h"

//*****************************************************************
// handle_allocate - Processes the allocate command
//
// Results:
//    status readCap writeCap manageCap
//*****************************************************************

int handle_allocate(ibp_task_t *task)
{
   int d, err;
   unsigned int ui;
   Resource_t *res;

   char token[4096];
   Allocation_t a, ma; 
   Cmd_state_t *cmd = &(task->cmd);

   debug_printf(1, "handle_allocate: Starting to process command\n");

   Allocation_t *alloc = &(cmd->cargs.allocate.a);
   RID_t *rid = &(cmd->cargs.allocate.rid);

   err = 0;

   if (is_empty_rid(rid)) { //** Pick a random resource to use
       get_random(&ui, sizeof(ui));
       double r = (1.0 * ui) / (UINT_MAX + 1.0);
       d = global_config->n_resources * r;
       *rid = global_config->res[d].rid;
      
       log_printf(10, "handle_allocate: Picking random resource %s\n", rid2str(rid, token, sizeof(token)));
   }

      //** Check the resource **
   res = resource_lookup(global_config->rl, rid2str(rid, token, sizeof(token)));
   if (res == NULL) {    //**Can't find the resource
      log_printf(1, "handle_allocate: Invalid resource: %s\n", rid2str(rid, token, sizeof(token)));
      alog_append_ibp_allocate(task->myid, -1, alloc->max_size, alloc->type, alloc->reliability, alloc->expiration);
      send_cmd_result(task, IBP_E_INVALID_RID);
      return(-1);
   }   

   //** Check the duration **
   if (alloc->expiration == INT_MAX) {
     alloc->expiration = res->max_duration + time(NULL);     
   }

   if (cmd->command == IBP_SPLIT_ALLOCATE) {
      //** Get the Master allocation ***
      if ((err = get_allocation_by_cap_resource(res, MANAGE_CAP, &(cmd->cargs.allocate.master_cap), &ma)) != 0) {
         log_printf(10, "handle_split_allocate: Invalid master cap: %s rid=%s\n", cmd->cargs.allocate.master_cap.v, res->name);
         send_cmd_result(task, IBP_E_CAP_NOT_FOUND);
         return(-1);
      }
      
      alog_append_ibp_split_allocate(task->myid, res->rl_index, ma.id, alloc->max_size, alloc->type, alloc->reliability, alloc->expiration);

      lock_osd_id(ma.id);  //** Redo the get allocation again with the lock enabled ***
      if ((err = get_allocation_by_cap_resource(res, MANAGE_CAP, &(cmd->cargs.allocate.master_cap), &ma)) != 0) {
         log_printf(10, "handle_split_allocate: Invalid master cap: %s rid=%s\n", cmd->cargs.allocate.master_cap.v, res->name);
         unlock_osd_id(ma.id);
         send_cmd_result(task, IBP_E_CAP_NOT_FOUND);
         return(-1);
      }
   } else {
      alog_append_ibp_allocate(task->myid, res->rl_index, alloc->max_size, alloc->type, alloc->reliability, alloc->expiration);
   }

   d = alloc->expiration - time(NULL);
   debug_printf(10, "handle_allocate:  expiration: %u (%d sec from now)\n", alloc->expiration, d); flush_debug();

   if (res->max_duration < d) {
       log_printf(1, "handle_allocate: Duration(%d sec) exceeds that for RID %s of %d sec\n", d, res->name, res->max_duration);
       if (cmd->command == IBP_SPLIT_ALLOCATE) unlock_osd_id(ma.id);
       send_cmd_result(task, IBP_E_LONG_DURATION);
       return(-1);
   }

   //** Perform the allocation **
   d = (global_config->server.lazy_allocate == 1) ? 0 : 1;
   if (cmd->command == IBP_SPLIT_ALLOCATE) {
      if ((d = split_allocation_resource(res, &ma, &a, alloc->max_size, alloc->type, alloc->reliability, alloc->expiration, 0, d)) != 0) {
         log_printf(1, "handle_allocate: split_allocation_resource failed on RID %s!  Error=%d\n", res->name, d);
         unlock_osd_id(ma.id);  //** Now we can unlock the master
         send_cmd_result(task, IBP_E_WOULD_EXCEED_LIMIT);
         return(-1);
      }

      //** Update the master's manage timestamp
      update_manage_history(res, ma.id, ma.is_alias, &(task->ipadd), cmd->command, 0, a.reliability, a.expiration, a.max_size, 0);

      unlock_osd_id(ma.id);  //** Now we can unlock the master
   } else {
      if ((d = create_allocation_resource(res, &a, alloc->max_size, alloc->type, alloc->reliability, alloc->expiration, 0, d)) != 0) {
         log_printf(1, "handle_allocate: create_allocation_resource failed on RID %s!  Error=%d\n", res->name, d);
         send_cmd_result(task, IBP_E_WOULD_EXCEED_LIMIT);
         return(-1);
      }
  }

   if (a.type != IBP_BYTEARRAY) {  //** Set the size to be 0 for all the Queue types
     a.size = 0;
     a.w_pos = 0;
     a.r_pos = 0;
   }

   //** Store the creation timestamp **
   set_alloc_timestamp(&(a.creation_ts), &(task->ipadd));
//log_printf(15, "handle_allocate: create time =" TT " ns=%d\n", a.creation_ts.time, ns_getid(task->ns));
   err = modify_allocation_resource(res, a.id, &a);
   if (err != 0) {
      log_printf(0, "handle_allocate:  Error with modify_allocation_resource for new queue allocation!  err=%d, type=%d\n", err, a.type); 
   }

   //** Send the result back **
   ns_monitor_t *nm = ns_get_monitor(task->ns);
   snprintf(token, sizeof(token), "%d ibp://%s:%d/%s#%s/3862277/READ "
       "ibp://%s:%d/%s#%s/3862277/WRITE "
       "ibp://%s:%d/%s#%s/3862277/MANAGE \n",
       IBP_OK,
       nm_get_host(nm), nm_get_port(nm), res->name, a.caps[READ_CAP].v,
       nm_get_host(nm), nm_get_port(nm), res->name, a.caps[WRITE_CAP].v,
       nm_get_host(nm), nm_get_port(nm), res->name, a.caps[MANAGE_CAP].v);

   Net_timeout_t dt;
   convert_epoch_time2net(&dt, task->cmd_timeout);   

   debug_code(time_t tt=time(NULL);)
   debug_printf(1, "handle_allocate: before sending result time: %s\n", ctime(&tt));
   err = write_netstream_block(task->ns, task->cmd_timeout, token, strlen(token));

   alog_append_osd_id(task->myid, a.id);

   debug_printf(1, "handle_allocate: Allocation: %s", token);

   debug_code(
      if (debug_level() > 5) print_allocation_resource(res, log_fd(), &a);
   )

//Allocation_history_t h1;
//get_history_table(res, a.id, &h1);
//log_printf(0, "handle_allocate history: r=%s id=" LU " h.id=" LU " write_slot=%d\n", res->name, a.id, h1.id, h1.write_slot);

   return(err);
}

//*****************************************************************
//  handle_merge - Merges 2 allocations
//*****************************************************************

int handle_merge(ibp_task_t *task)
{
   int err;
   Resource_t *r;
   Allocation_t ma, ca; 
   Cmd_state_t *cmd = &(task->cmd);
   Cmd_merge_t *op = &(cmd->cargs.merge);

   debug_printf(1, "handle_merge: Starting to process command\n");

      //** Check the resource **
   r = resource_lookup(global_config->rl, op->crid);
   if (r == NULL) {    //**Can't find the resource
      log_printf(1, "handle_merge: Invalid resource: %s\n", op->crid);
      alog_append_ibp_merge(task->myid, 0, 0, -1);
      send_cmd_result(task, IBP_E_INVALID_RID);
      return(-1);
   }   

   //** Get the master allocation ***
  if ((err = get_allocation_by_cap_resource(r, MANAGE_CAP, &(op->mkey), &ma)) != 0) {
     log_printf(10, "handle_merge: Invalid mcap: %s rid=%s\n", op->mkey.v, r->name);
     alog_append_ibp_merge(task->myid, 0, 0, r->rl_index);
     send_cmd_result(task, IBP_E_CAP_NOT_FOUND);
     return(-1);
  }

  //** and the child allocation
  if ((err = get_allocation_by_cap_resource(r, MANAGE_CAP, &(op->ckey), &ca)) != 0) {
     log_printf(10, "handle_merge: Invalid childcap: %s rid=%s\n", op->ckey.v, r->name);
     alog_append_ibp_merge(task->myid, ma.id, 0, r->rl_index);
     send_cmd_result(task, IBP_E_CAP_NOT_FOUND);
     return(-1);
  }

  alog_append_ibp_merge(task->myid, ma.id, ca.id, r->rl_index);

  //** Now do the same thing with locks enabled
  lock_osd_id_pair(ma.id, ca.id);

   //** Get the master allocation ***
  if ((err = get_allocation_by_cap_resource(r, MANAGE_CAP, &(op->mkey), &ma)) != 0) {
     log_printf(10, "handle_merge: Invalid mcap: %s rid=%s\n", op->mkey.v, r->name);
     alog_append_ibp_merge(task->myid, 0, 0, r->rl_index);
     unlock_osd_id_pair(ma.id, ca.id);
     send_cmd_result(task, IBP_E_CAP_NOT_FOUND);
     return(-1);
  }

  //** and the child allocation
  if ((err = get_allocation_by_cap_resource(r, MANAGE_CAP, &(op->ckey), &ca)) != 0) {
     log_printf(10, "handle_merge: Invalid childcap: %s rid=%s\n", op->ckey.v, r->name);
     alog_append_ibp_merge(task->myid, ma.id, 0, r->rl_index);
     unlock_osd_id_pair(ma.id, ca.id);
     send_cmd_result(task, IBP_E_CAP_NOT_FOUND);
     return(-1);
  }

   //** Update the manage timestamp
   update_manage_history(r, ca.id, ca.is_alias, &(task->ipadd), cmd->command, 0, 0, 0, ca.max_size, 0);

   //** Check the child ref count
   if ((ca.read_refcount == 1) && (ca.write_refcount == 0)) {
      //** merge the allocation **
      if ((err = merge_allocation_resource(r, &ma, &ca)) != 0) {
         log_printf(1, "handle_merge: merge_allocation_resource failed on RID %s!  Error=%d\n", r->name, err);
         unlock_osd_id_pair(ma.id, ca.id);   
         send_cmd_result(task, IBP_E_GENERIC);
         return(-1);
      }
   } else {
      log_printf(15, "handle_merge: Bad child refcount! RID=%s  ca.read=%d ca.write=%d\n", r->name, ca.read_refcount, ca.write_refcount);
      unlock_osd_id_pair(ma.id, ca.id);   
      send_cmd_result(task, IBP_E_GENERIC);
      return(-1);
   }

   unlock_osd_id_pair(ma.id, ca.id);   

   send_cmd_result(task, IBP_OK);

   debug_printf(1, "handle_merge: completed\n");
   return(0);
}

//*****************************************************************
// handle_rename - Processes the allocation rename command
//
// Results:
//    status readCap writeCap manageCap
//*****************************************************************

int handle_rename(ibp_task_t *task)
{
   int d, err;
   Resource_t *res;

   char token[4096];
   Allocation_t a; 
   Cmd_state_t *cmd = &(task->cmd);
   Cmd_manage_t *manage = &(cmd->cargs.manage);

   debug_printf(1, "handle_rename: Starting to process command\n");

   err = 0;

      //** Check the resource **
   res = resource_lookup(global_config->rl, rid2str(&(manage->rid), token, sizeof(token)));
   if (res == NULL) {    //**Can't find the resource
      log_printf(1, "handle_allocate: Invalid resource: %s\n", rid2str(&(manage->rid), token, sizeof(token)));
      alog_append_ibp_rename(task->myid, -1, 0);
      send_cmd_result(task, IBP_E_INVALID_RID);
      return(-1);
   }   

   //** Get the allocation ***
  if ((err = get_allocation_by_cap_resource(res, MANAGE_CAP, &(manage->cap), &a)) != 0) {
     log_printf(10, "handle_rename: Invalid cap: %s rid=%s\n", manage->cap.v, res->name);
     alog_append_ibp_rename(task->myid, -1, 0);
     send_cmd_result(task, IBP_E_CAP_NOT_FOUND);
     return(-1);
  }

  alog_append_ibp_rename(task->myid, res->rl_index, a.id);

  lock_osd_id(a.id);

   //** Get the allocation again with the lock enabled ***
  if ((err = get_allocation_by_cap_resource(res, MANAGE_CAP, &(manage->cap), &a)) != 0) {
     log_printf(10, "handle_rename: Invalid cap: %s rid=%s\n", manage->cap.v, res->name);
     send_cmd_result(task, IBP_E_CAP_NOT_FOUND);
     unlock_osd_id(a.id);
     return(-1);
  }

   //** Update the manage timestamp
   update_manage_history(res, a.id, a.is_alias, &(task->ipadd), cmd->command, 0, a.reliability, a.expiration, a.max_size, 0);

   //** Rename the allocation **
   if ((d = rename_allocation_resource(res, &a)) != 0) {
      log_printf(1, "handle_rename: rename_allocation_resource failed on RID %s!  Error=%d\n", res->name, d);
      send_cmd_result(task, IBP_E_GENERIC);
      unlock_osd_id(a.id);
      return(-1);
   }

   unlock_osd_id(a.id);

   //** Send the result back **
   //** Send the result back **
   ns_monitor_t *nm = ns_get_monitor(task->ns);
   snprintf(token, sizeof(token), "%d ibp://%s:%d/%s#%s/3862277/READ "
       "ibp://%s:%d/%s#%s/3862277/WRITE "
       "ibp://%s:%d/%s#%s/3862277/MANAGE \n",
       IBP_OK,
       nm_get_host(nm), nm_get_port(nm), res->name, a.caps[READ_CAP].v,
       nm_get_host(nm), nm_get_port(nm), res->name, a.caps[WRITE_CAP].v,
       nm_get_host(nm), nm_get_port(nm), res->name, a.caps[MANAGE_CAP].v);

   debug_code(time_t tt=time(NULL);)
   debug_printf(1, "handle_rename: before sending result time: %s\n", ctime(&tt));
   err = write_netstream_block(task->ns, task->cmd_timeout, token, strlen(token));

   alog_append_osd_id(task->myid, a.id);

   debug_printf(1, "handle_rename: Allocation: %s", token);

   debug_code(
      if (debug_level() > 5) print_allocation_resource(res, log_fd(), &a);
   )
   return(err);
}

//*****************************************************************
// handle_internal_get_alloc - Processes the raw internal get allocation
//
// Results:
//    status ndatabytes\n
//    ..raw allocation..
//*****************************************************************

int handle_internal_get_alloc(ibp_task_t *task)
{
   int bufsize = 1024*1024;
   char buffer[bufsize];
   int err;
   uint64_t nbytes;
   Resource_t *res;
   
   char token[4096];
   Allocation_t a; 
   Allocation_history_t h;
   Cmd_state_t *cmd = &(task->cmd);
   Cmd_internal_get_alloc_t *arg = &(cmd->cargs.get_alloc);

   debug_printf(1, "handle_internal_get_alloc: Starting to process command\n");

   err = 0;

      //** Check the resource **
   res = resource_lookup(global_config->rl, rid2str(&(arg->rid), token, sizeof(token)));
   if (res == NULL) {    //**Can't find the resource
      log_printf(1, "handle_internal_get_alloc: Invalid resource: %s\n", rid2str(&(arg->rid), token, sizeof(token)));
      alog_append_internal_get_alloc(task->myid, -1, 0);
      send_cmd_result(task, IBP_E_INVALID_RID);
      return(-1);
   }   

   err = -1;
   a.id = 0;
   switch(arg->key_type) {
     case IBP_READCAP:
         err = get_allocation_by_cap_resource(res, READ_CAP, &(arg->cap), &a);
         break;
     case IBP_WRITECAP:
         err = get_allocation_by_cap_resource(res, WRITE_CAP, &(arg->cap), &a);
         break;
     case IBP_MANAGECAP:
         err = get_allocation_by_cap_resource(res, MANAGE_CAP, &(arg->cap), &a);
         break;
     case INTERNAL_ID:
         err = get_allocation_resource(res, arg->id, &a);
         break;
   }

   alog_append_internal_get_alloc(task->myid, res->rl_index, a.id);

   if (err != 0) {
     log_printf(10, "handle_internal_get_alloc: Invalid cap/id: rid=%s ns=%d\n",res->name, ns_getid(task->ns));
     send_cmd_result(task, IBP_E_CAP_NOT_FOUND);
     return(-1);
   }

   err = get_history_table(res, arg->id, &h);

   if (err != 0) {
     log_printf(10, "handle_internal_get_alloc: Cant read the history! rid=%s ns=%d\n",res->name, ns_getid(task->ns));
     send_cmd_result(task, IBP_E_CAP_NOT_FOUND);
     return(-1);
   }


   nbytes = 0;
   if (arg->offset > -1) {
      if (arg->len == 0) {
         nbytes = a.size - arg->offset;
      } else if (arg->offset > a.size) {
         nbytes = 0;
      } else if ((arg->len + arg->offset) > a.size) {
         nbytes = a.size - arg->offset;
      } else {
         nbytes = arg->len;
      }
   }

   //*** Send back the results ***
   sprintf(buffer, "%d " LU " \n",IBP_OK, nbytes); 
   err = write_netstream_block(task->ns, task->cmd_timeout, buffer, strlen(buffer));

   //** Send the allocation
   err = write_netstream_block(task->ns, task->cmd_timeout, (char *)&a, sizeof(a));

   //** ...and the history
   err = write_netstream_block(task->ns, task->cmd_timeout, (char *)&h, sizeof(h));

   //** Send back the data if needed **
   if (arg->offset > -1) {
      ibp_task_t rtask;
      Cmd_read_t *rcmd = &(rtask.cmd.cargs.read);
      rcmd->a = a;
      rcmd->r = res;
      a.r_pos = arg->offset;
      rcmd->left = nbytes;
      rcmd->len = nbytes;
      rcmd->offset = arg->offset;
      rtask.ns = task->ns;
      err = read_from_disk(&rtask, &a, &(rcmd->left), rcmd->r);
   }

   debug_printf(1, "handle_internal_get_alloc: Allocation:\n");

   debug_code(
      if (debug_level() > 5) print_allocation_resource(res, log_fd(), &a);
   )
   debug_printf(1, "handle_internal_get_alloc: completed\n");

   return(err);
}


//*****************************************************************
// handle_alias_allocate - Generates a alias allocation 
//
// Results:
//    status readCap writeCap manageCap
//*****************************************************************

int handle_alias_allocate(ibp_task_t *task)
{
   int d, err;
   Resource_t *res;

   char token[4096];
   Allocation_t a, alias_alloc; 
   Cmd_state_t *cmd = &(task->cmd);
   Cmd_alias_alloc_t *pa = &(cmd->cargs.alias_alloc);

   debug_printf(1, "handle_alias_allocate: Starting to process command\n");

   err = 0;

      //** Check the resource **
   res = resource_lookup(global_config->rl, rid2str(&(pa->rid), token, sizeof(token)));
   if (res == NULL) {    //**Can't find the resource
      log_printf(1, "handle_alias_allocate: Invalid resource: %s\n", rid2str(&(pa->rid), token, sizeof(token)));
      alog_append_alias_alloc(task->myid, -1, 0, 0, 0, 0);
      send_cmd_result(task, IBP_E_INVALID_RID);
      return(-1);
   }   

   //** Get the original allocation ***
   if ((err = get_allocation_by_cap_resource(res, MANAGE_CAP, &(pa->cap), &a)) != 0) {
     log_printf(10, "handle_alias_allocate: Invalid cap: %s rid=%s\n", pa->cap.v, res->name);
     alog_append_alias_alloc(task->myid, -1, 0, 0, 0, 0);
     send_cmd_result(task, IBP_E_CAP_NOT_FOUND);
     return(-1);
   }

   lock_osd_id(a.id);

   //** Get the original allocation again with the lock ***
   if ((err = get_allocation_by_cap_resource(res, MANAGE_CAP, &(pa->cap), &a)) != 0) {
     log_printf(10, "handle_alias_allocate: Invalid cap: %s rid=%s\n", pa->cap.v, res->name);
     alog_append_alias_alloc(task->myid, -1, 0, 0, 0, 0);
     send_cmd_result(task, IBP_E_CAP_NOT_FOUND);
     unlock_osd_id(a.id);
     return(-1);
   }

   //** Validate the range and duration **
//   log_printf(1, "handle_alias_alloc:  pa->duration= %u\n", pa->expiration);
   if (pa->expiration == 0) pa->expiration = a.expiration;

   alog_append_alias_alloc(task->myid, res->rl_index, a.id, pa->offset, pa->len, pa->expiration);

   if (pa->expiration > a.expiration) {
      log_printf(1, "handle_alias_alloc: ALIAS duration > actual allocation! alias= %u alloc = %u\n", pa->expiration, a.expiration);

      send_cmd_result(task, IBP_E_LONG_DURATION);
      unlock_osd_id(a.id);
      return(-1);
   }

   if ((pa->len + pa->offset) > a.max_size) {  
      uint64_t epos = pa->len + pa->offset;
      log_printf(1, "handle_alias_alloc: ALIAS range > actual allocation! alias= " LU " alloc = " LU "\n", epos, a.max_size);
      send_cmd_result(task, IBP_E_WOULD_EXCEED_LIMIT);
      unlock_osd_id(a.id);
      return(-1);
   }

   //*** Create the alias ***
   if ((d =  create_allocation_resource(res, &alias_alloc, 0, a.type, a.reliability, pa->expiration, 1, 0)) != 0) {
      log_printf(1, "handle_alias_alloc: create_allocation_resource failed on RID %s!  Error=%d\n", res->name, d);
      send_cmd_result(task, IBP_E_GENERIC);
      unlock_osd_id(a.id);
      return(-1);
   }

   //** Store the creation timestamp **
   set_alloc_timestamp(&(alias_alloc.creation_ts), &(task->ipadd));

   //*** Specifify the allocation as a alias ***
   alias_alloc.alias_id = a.id;
   alias_alloc.alias_offset = pa->offset;
   alias_alloc.alias_size = pa->len;

   //** and store it back in the DB only **
   if ((d = modify_allocation_resource(res, alias_alloc.alias_id, &alias_alloc)) != 0) {
      log_printf(1, "handle_alias_allocate: modify_allocation_resource failed on RID %s!  Error=%d\n", res->name, d);
      send_cmd_result(task, IBP_E_GENERIC);
      unlock_osd_id(a.id);
      return(-1);
   }

   //** Update the parent timestamp
   update_manage_history(res, a.id, a.is_alias, &(task->ipadd), IBP_ALIAS_ALLOCATE, 0, alias_alloc.alias_offset, alias_alloc.expiration, alias_alloc.alias_size, alias_alloc.id);

   unlock_osd_id(a.id);

   //** Send the result back **
   ns_monitor_t *nm = ns_get_monitor(task->ns);
   snprintf(token, sizeof(token), "%d ibp://%s:%d/%s#%s/3862277/READ "
       "ibp://%s:%d/%s#%s/3862277/WRITE "
       "ibp://%s:%d/%s#%s/3862277/MANAGE \n",
       IBP_OK,
       nm_get_host(nm), nm_get_port(nm), res->name, alias_alloc.caps[READ_CAP].v,
       nm_get_host(nm), nm_get_port(nm), res->name, alias_alloc.caps[WRITE_CAP].v,
       nm_get_host(nm), nm_get_port(nm), res->name, alias_alloc.caps[MANAGE_CAP].v);

   debug_code(time_t tt=time(NULL);)
   debug_printf(1, "handle_alias_allocate: before sending result time: %s\n", ctime(&tt));
   err = write_netstream_block(task->ns, task->cmd_timeout, token, strlen(token));

   alog_append_osd_id(task->myid, alias_alloc.id);

   debug_printf(1, "handle_alias_allocate: Allocation: %s", token);

   debug_code(
      if (debug_level() > 5) print_allocation_resource(res, log_fd(), &alias_alloc);
   )
   return(err);
}


//*****************************************************************
// handle_status - Processes a status request.
//
//  IBP_ST_INQ
//    v1.3
//      status nbytes \n
//      hard_max_mb hard_used_mb soft_max_mb soft_used_mb max_duration \n
//    v1.4
//      status nbytes \n
//      VS:1.4 DT:dm_type RID:rid RT:rtype CT:ct_bytes ST:st_bytes UT:ut_bytes UH:uh_bytes SH:sh_bytes
//      CH:ch_bytes AT:at_bytes AH:ah_bytes DT:max_duration RE \n
//
//  IBP_ST_CHANGE 
//      status \n
//
//  IBP_ST_RES 
//      status RID1 RID2 ... RIDn \n
//
//*****************************************************************

int handle_status(ibp_task_t *task)
{
  Cmd_state_t *cmd = &(task->cmd); 
  Cmd_status_t *status = &(cmd->cargs.status);

  debug_printf(1, "handle_status: Starting to process command\n");


  if (status->subcmd == IBP_ST_VERSION) {
     alog_append_status_version(task->myid);     

     char buffer[2048];
     char result[4096];
     uint64_t total, total_used, total_diff, total_free, rbytes, wbytes, cbytes;
     uint64_t r_total, r_diff, r_free, r_used, r_alloc, r_alias, total_alloc, total_alias;
     double r_total_gb, r_diff_gb, r_free_gb, r_used_gb;
     Net_timeout_t dt;
     Resource_t *r;
     int i;
     set_net_timeout(&dt, 1, 0);
     char *version = server_version();

     result[0] = '\0'; result[sizeof(result)-1] = '\0';
     buffer[0] = '\0'; buffer[sizeof(buffer)-1] = '\0';
     sprintf(result, "%d\n", IBP_OK);

     strncat(result, version, sizeof(result)-1 - strlen(result));

     //** Add the uptime data  **
     print_uptime(buffer, sizeof(buffer));
     strncat(result, buffer, sizeof(result)-1 - strlen(result));

     //** Add some stats **
     snprintf(buffer, sizeof(buffer)-1, "Total Commands: " LU "  Connections: %d\n", task->tid, 
           network_counter(global_network));
     strncat(result, buffer, sizeof(result)-1 - strlen(result));
     snprintf(buffer, sizeof(buffer)-1, "Active Threads: %d\n", currently_running_tasks());
     strncat(result, buffer, sizeof(result)-1 - strlen(result));
     get_transfer_stats(&rbytes, &wbytes, &cbytes);
     total = rbytes+wbytes;
     r_total_gb = total / (1024.0*1024.0*1024.0);
     r_used_gb = rbytes / (1024.0*1024.0*1024.0);
     r_free_gb = wbytes / (1024.0*1024.0*1024.0);
     snprintf(buffer, sizeof(buffer)-1, "Depot Transfer Stats --  Read: " LU " b (%.2lf GB) Write: " LU " b (%.2lf GB) Total: " LU " b (%.2lf GB)\n", 
          rbytes, r_used_gb, wbytes, r_free_gb, total, r_total_gb);
     strncat(result, buffer, sizeof(result)-1 - strlen(result)); 

     r_total_gb = cbytes / (1024.0*1024.0*1024.0);
     snprintf(buffer, sizeof(buffer)-1, "Depot-Depot copies: " LU " b (%.2lf GB)\n", cbytes, r_total_gb);
     strncat(result, buffer, sizeof(result)-1 - strlen(result)); 

     total = 0; total_used = 0; total_free = 0; total_diff = 0; total_alloc = 0; total_alias = 0;
     for (i=0; i< global_config->n_resources; i++) {
         r = &(global_config->res[i]);

         r_free = resource_allocable(r, 0);  

         pthread_mutex_lock(&(r->mutex));
         r_total = r->max_size[ALLOC_TOTAL];
         r_used = r->used_space[ALLOC_HARD] + r->used_space[ALLOC_SOFT];
         r_alloc = r->n_allocs;
         r_alias = r->n_alias;
         pthread_mutex_unlock(&(r->mutex));

         if (r_total > r_used) {
            r_diff = r_total - r_used;
         } else {
            r_diff = 0;
         }
         total = total + r_total;
         total_used = total_used + r_used;
         total_diff = total_diff + r_diff; 
         total_free = total_free + r_free;
         total_alloc = total_alloc + r_alloc;
         total_alias = total_alias + r_alias;

         r_total_gb = r_total / (1024.0*1024.0*1024.0);
         r_used_gb = r_used / (1024.0*1024.0*1024.0);
         r_diff_gb = r_diff / (1024.0*1024.0*1024.0);
         r_free_gb = r_free / (1024.0*1024.0*1024.0);
         snprintf(buffer, sizeof(buffer)-1, "RID: %s Max: " LU " b (%.2lf GB) Used: " LU " b (%.2lf GB) Diff: " LU " b (%.2lf GB) Free: " LU " b (%.2lf GB) Allocations: " LU " (" LU " alias)\n", 
             r->name, r_total, r_total_gb, r_used, r_used_gb, r_diff, r_diff_gb, r_free, r_free_gb, r_alloc, r_alias);
         strncat(result, buffer, sizeof(result)-1 - strlen(result)); 
     }

     r_total_gb = total / (1024.0*1024.0*1024.0);
     r_used_gb = total_used / (1024.0*1024.0*1024.0);
     r_diff_gb = total_diff / (1024.0*1024.0*1024.0);
     r_free_gb = total_free / (1024.0*1024.0*1024.0);
     snprintf(buffer, sizeof(buffer)-1, "Total resources: %d  Max: " LU " b (%.2lf GB) Used: " LU " b (%.2lf GB) Diff: " LU " b (%.2lf GB) Free: " LU " b (%.2lf GB) Allocations: " LU " (" LU " alias)\n", 
             global_config->n_resources, total, r_total_gb, total_used, r_used_gb, total_diff, r_diff_gb, total_free, r_free_gb, total_alloc, total_alias);
     strncat(result, buffer, sizeof(result)-1 - strlen(result)); 

     snprintf(buffer,sizeof(result)-1 - strlen(result), "\n");
     snprintf(buffer, sizeof(result)-1 - strlen(result), "END\n");
     strncat(result, buffer, sizeof(result) - 1 - strlen(result));
     i = strlen(result);
     write_netstream_block(task->ns, task->cmd_timeout, result, i);

     alog_append_cmd_result(task->myid, IBP_OK);
     
     return(0);
  }

  if (strcmp(global_config->server.password, status->password) != 0) {
    log_printf(10, "handle_status:  Invalid password: %s\n", status->password); 
    send_cmd_result(task, IBP_E_WRONG_PASSWD);
    return(-1);    
  }

  if (status->subcmd == IBP_ST_RES) {
     alog_append_status_res(task->myid);     

     char buffer[2048];
     char result[2048];
     int i;
     Resource_t *r = global_config->res;
     
     snprintf(result, sizeof(result), "%d ", IBP_OK);
     for (i=0; i< global_config->n_resources; i++) {
         snprintf(buffer, sizeof(buffer), "%s ", r[i].name);
         strncat(result, buffer, sizeof(result) -1 - strlen(result)); 
     }

     sprintf(buffer, "\n");
     strncat(result, buffer, sizeof(result) -1 - strlen(result));

     log_printf(10, "handle_status: Sending resource list: %s\n", result);
     write_netstream_block(task->ns, task->cmd_timeout, result, strlen(result));

     alog_append_cmd_result(task->myid, IBP_OK);
  } else if (status->subcmd == IBP_ST_STATS) {  //** Send the depot stats
     alog_append_status_stats(task->myid, status->start_time);
     Net_timeout_t dt;
     convert_epoch_time2net(&dt, task->cmd_timeout);   
     log_printf(10, "handle_status: Sending stats\n");
     send_stats(task->ns, status->start_time, dt);
     alog_append_cmd_result(task->myid, IBP_OK);
  } else if (status->subcmd == IBP_ST_INQ) {
     char buffer[2048]; 
     char result[32];
     int n, nres;

     Resource_t *r = resource_lookup(global_config->rl, status->crid);
     if (r == NULL) {
        log_printf(10, "handle_status:  Invalid RID :%s\n",status->crid); 
        alog_append_status_inq(task->myid, -1);
        send_cmd_result(task, IBP_E_INVALID_RID);
        return(-1);
     }

     alog_append_status_inq(task->myid, r->rl_index);

     uint64_t totalconfigured;
     uint64_t totalused;
     uint64_t soft_alloc, hard_alloc, total_alloc;

     total_alloc = resource_allocable(r, 0); 

     pthread_mutex_lock(&(r->mutex));
     totalconfigured = r->max_size[ALLOC_TOTAL];
     totalused = r->used_space[ALLOC_HARD] + r->used_space[ALLOC_SOFT];
     if (totalused > totalconfigured) {
        soft_alloc = 0;
        hard_alloc = 0;
     } else {
        soft_alloc = r->max_size[ALLOC_SOFT] - r->used_space[ALLOC_SOFT];
        if (soft_alloc > total_alloc) soft_alloc = total_alloc;
        hard_alloc = r->max_size[ALLOC_HARD] - r->used_space[ALLOC_HARD];
        if (hard_alloc > total_alloc) hard_alloc = total_alloc;
     }

     if (cmd->version == IBPv040) {
                                            //***  1       2       3     4      5       6       7       8        9       10      11     12     13   13
        n = snprintf(buffer, sizeof(buffer), "%s:1.4:1.0 %s:%d  %s:%s %s:%d %s:" LU " %s:" LU " %s:" LU " %s:" LU " %s:" LU " %s:" LU " %s:" LU " %s:" LU " %s:%d %s \n", 
           ST_VERSION,                                   //** 1
           ST_DATAMOVERTYPE, DM_TCP,                     //** 2
           ST_RESOURCEID, r->name,                       //** 3
           ST_RESOURCETYPE, RS_DISK,                     //** 4
           ST_CONFIG_TOTAL_SZ, totalconfigured,          //** 5
           ST_SERVED_TOTAL_SZ, totalconfigured,          //** 6
           ST_USED_TOTAL_SZ, totalused,                  //** 7
           ST_USED_HARD_SZ, r->used_space[ALLOC_HARD],   //** 8 
           ST_SERVED_HARD_SZ, r->max_size[ALLOC_HARD],   //** 9
           ST_CONFIG_HARD_SZ, r->max_size[ALLOC_HARD],   //** 10
           ST_ALLOC_TOTAL_SZ, total_alloc,                //** 11
           ST_ALLOC_HARD_SZ, hard_alloc,                 //** 12
           ST_DURATION, r->max_duration,                 //** 13
           ST_RS_END);                                   //** 14
     } else {
       uint64_t soft, soft_used, hard, hard_used;
       soft = r->max_size[ALLOC_SOFT] >> 20;    soft_used = r->used_space[ALLOC_SOFT] >> 20;
       hard = r->max_size[ALLOC_HARD] >> 20;    hard_used = r->used_space[ALLOC_HARD] >> 20;
        
       n = snprintf(buffer, sizeof(buffer), "" LU " " LU " " LU " " LU " %d \n",
              hard, hard_used, soft, soft_used, r->max_duration);
     }

     pthread_mutex_unlock(&(r->mutex));

     nres = snprintf(result, sizeof(result), "%d %d \n", IBP_OK, n);
     if (cmd->version != IBPv031) write_netstream_block(task->ns, task->cmd_timeout, result, nres);
     write_netstream_block(task->ns, task->cmd_timeout, buffer, n);

     alog_append_cmd_result(task->myid, IBP_OK);

     log_printf(10, "handle_status: Succesfully processed IBP_ST_INQ on RID %s\n", r->name);
     log_printf(10, "handle_status: depot info: %s\n", buffer);
     return(0);
  } else if (status->subcmd == IBP_ST_CHANGE) {
//==================IBP_ST_CHANGE NOT ALLOWED==============================
log_printf(10, "handle_status:  IBP_ST_CHANGE!!!! rid=%s ns=%d  IGNORING!!\n",status->crid, task->ns->id); 
alog_append_status_change(task->myid);
send_cmd_result(task, IBP_E_INVALID_CMD);
close_netstream(task->ns);
return(-1);


     Resource_t *r = resource_lookup(global_config->rl, status->crid);
     if (r == NULL) {
        log_printf(10, "handle_status:  Invalid RID :%s\n",status->crid); 
        send_cmd_result(task, IBP_E_INVALID_RID);
        return(-1);
     }

     log_printf(10, "handle_status: Requested change on RID %s hard:" LU " soft:" LU " expiration:%ld\n",
           r->name, status->new_size[ALLOC_HARD], status->new_size[ALLOC_SOFT], status->new_duration);

     pthread_mutex_lock(&(r->mutex));
     if (r->used_space[ALLOC_HARD] < status->new_size[ALLOC_HARD]) r->max_size[ALLOC_HARD] = status->new_size[ALLOC_HARD];
     if (r->used_space[ALLOC_SOFT] < status->new_size[ALLOC_SOFT]) r->max_size[ALLOC_SOFT] = status->new_size[ALLOC_SOFT];
     if (status->new_duration < 0) status->new_duration = INT_MAX;
     r->max_duration = status->new_duration;
     pthread_mutex_unlock(&(r->mutex));

     log_printf(10, "handle_status: Succesfully processed IBP_ST_CHANGE on RID %s hard:" LU " soft:" LU " expiration:%d\n", 
           r->name, r->max_size[ALLOC_HARD], r->max_size[ALLOC_SOFT], r->max_duration);
     cmd->state = CMD_STATE_FINISHED;
     send_cmd_result(task, IBP_OK);
     return(0);
  } else {                
     log_printf(10, "handle_status:  Invalid sub command :%d\n",status->subcmd); 
     send_cmd_result(task, IBP_E_INVALID_CMD);
     return(0);
  }

  debug_printf(1, "handle_status: Successfully processed command\n");

  return(0);
}

//*****************************************************************
// handle_manage - Processes the manage allocation command
//
//  IBP_INCR | IBP_DECR | IBP_CHNG
//     status \n
//
//  IBP_PROBE (for an IBP_MANAGE command)
//     status read_refcnt write_refcnt curr_size max_size time_remaining \n

//  IBP_PROBE (for an IBP_ALIAS_MANAGE command)
//     status read_refcnt write_refcnt offset len time_remaining \n
//
//  NOTE: From my unsderstanding this command is flawed since any only READ counts
//    are used to delete an allocation!!  Both READ and WRITE counts
//    should be used.
//*****************************************************************

int handle_manage(ibp_task_t *task)
{
  Cmd_state_t *cmd = &(task->cmd); 
  Cmd_manage_t *manage = &(cmd->cargs.manage);
  Allocation_t *a = &(manage->a);
  Allocation_t ma;
  osd_id_t id, pid;
  int lock, err, is_alias;
  uint64_t alias_offset, alias_len;

  if (cmd->command == IBP_MANAGE) {
     debug_printf(1, "handle_manage: Starting to process IBP_MANAGE command ns=%d\n", ns_getid(task->ns));
  } else {
     debug_printf(1, "handle_manage: Starting to process IBP_ALIAS_MANAGE command ns=%d\n", ns_getid(task->ns));
  }

  Resource_t *r = resource_lookup(global_config->rl, manage->crid);
  if (r == NULL) {
     log_printf(10, "handle_manage:  Invalid RID :%s\n",manage->crid); 
     alog_append_manage_bad(task->myid, cmd->command, manage->subcmd);
     send_cmd_result(task, IBP_E_INVALID_RID);
     return(-1);
  }

  if ((err = get_allocation_by_cap_resource(r, MANAGE_CAP, &(manage->cap), a)) != 0) {
     log_printf(10, "handle_manage: Invalid cap: %s rid=%s\n", manage->cap.v, r->name);
     alog_append_manage_bad(task->myid, cmd->command, manage->subcmd);
     send_cmd_result(task, IBP_E_CAP_NOT_FOUND);
     return(-1);
  }


  pid = a->id;

  is_alias = a->is_alias;
  alias_offset = 0;
  alias_len = a->max_size;
  if ((a->is_alias == 1) && (cmd->command == IBP_MANAGE)) {  //** This is a alias cap so load the master and invoke restrictions
     is_alias = 1;
     id = a->alias_id;
     alias_offset = a->alias_offset;
     alias_len =  a->alias_size;

     log_printf(10, "handle_manage: ns=%d got a alias cap! loading id " LU "\n", ns_getid(task->ns), id);
     
     if ((err = get_allocation_resource(r, id, a)) != 0) {
        log_printf(10, "handle_manage: Invalid alias id: " LU " rid=%s\n", id, r->name);
        alog_append_manage_bad(task->myid, cmd->command, manage->subcmd);
        send_cmd_result(task, IBP_E_CAP_NOT_FOUND);
        return(-1);
     }     

     if (alias_len == 0) alias_len = a->max_size - alias_offset;

     //** Can only isue a probe on a alias allocation through ibp_manage
     if (manage->subcmd != IBP_PROBE) { 
        log_printf(10, "handle_manage: Invalid subcmd for alias id: " LU " rid=%s\n", id, r->name);
        alog_append_manage_bad(task->myid, cmd->command, manage->subcmd);
        send_cmd_result(task, IBP_E_INVALID_CMD);
        return(-1);
     }     
  }

  //** Verify the master allocation if needed.  ***
  if ((is_alias == 1) && (cmd->command == IBP_ALIAS_MANAGE) && (manage->subcmd != IBP_PROBE)) {
     if ((err = get_allocation_resource(r, a->alias_id, &ma)) != 0) {
        log_printf(10, "handle_manage: Invalid alias id: " LU " rid=%s\n", a->alias_id, r->name);
        alog_append_manage_bad(task->myid, cmd->command, manage->subcmd);
        send_cmd_result(task, IBP_E_CAP_NOT_FOUND);
        return(-1);
     }     

     if (strcmp(ma.caps[MANAGE_CAP].v, manage->master_cap.v) != 0) {
        log_printf(10, "handle_manage: Master cap doesn't match with alias read: %s actual: %s  rid=%s ns=%d\n", 
             manage->master_cap.v, ma.caps[MANAGE_CAP].v, r->name, ns_getid(task->ns));
        alog_append_manage_bad(task->myid, cmd->command, manage->subcmd);
        send_cmd_result(task, IBP_E_INVALID_MANAGE_CAP);
        return(-1);
     }
  }

  lock_osd_id(a->id);  //** Lock it so we don't get race updates

  //** Re-read the data with the lock enabled
  id = a->id;     
  if ((err = get_allocation_resource(r, id, a)) != 0) {
     log_printf(10, "handle_manage: Error reading id after lock_osd_id  id: " LU " rid=%s\n", id, r->name);
     alog_append_manage_bad(task->myid, cmd->command, manage->subcmd);
     send_cmd_result(task, IBP_E_CAP_NOT_FOUND);
     return(-1);
  }     

  Task_que_t *tq = NULL;
  lock = TASK_LOCK_GRANTED;
  snprintf(manage->cid,sizeof(manage->cid), "" LU "", a->id);

  if (lock == TASK_LOCK_GRANTED) {
     int dir = -1;
     switch (manage->subcmd) {
        case IBP_INCR:
           dir = 1;
        case IBP_DECR:
           alog_append_manage_incdec(task->myid, cmd->command, manage->subcmd, r->rl_index, pid, id, manage->captype);
           err = IBP_OK;
           if (manage->captype == READ_CAP) {
              a->read_refcount = a->read_refcount + dir;
              if (a->read_refcount < 0) a->read_refcount = 0;
           } else if (manage->captype == WRITE_CAP) {
              a->write_refcount = a->write_refcount + dir;
              if (a->write_refcount < 0) a->write_refcount = 0;
           } else {
             log_printf(0, "handle_manage: Invalid captype foe IBP_INCR/DECR!\n");
             err = IBP_E_WRONG_CAP_FORMAT;
           }

           if (tq != NULL) {
              task_unlock(task->tc, tq, 1);
              tq = NULL;
           } else if ((a->read_refcount > 0) || (a->write_refcount > 0)) {   
              if ((err = modify_allocation_resource(r, a->id, a)) == 0) err = IBP_OK;              
           }

           //** Check if we need to remove the allocation **
           if ((a->read_refcount == 0) && (a->write_refcount == 0)) {   
              remove_allocation_resource(r, a);
           }
           send_cmd_result(task, err);
           break;
        case IBP_CHNG:
           if (is_alias) {  //** If this is an IBP_ALIAS_MANAGE call we have different fields
             a->alias_offset = manage->offset;
             a->alias_size = manage->new_size;
             
             if (manage->new_duration == INT_MAX) manage->new_duration = time(NULL) + r->max_duration;
             if (manage->new_duration > (time(NULL)+r->max_duration)) {
                err = 1;
                log_printf(10, "handle_manage: Duration >max_duration  id: " LU " rid=%s\n", id, r->name);               
             }

             if (manage->new_duration == 0) { //** Inherit duration from master allocation
                if ((err = get_allocation_resource(r, a->id, &ma)) != 0) {
                  log_printf(10, "handle_manage: Invalid alias id: " LU " rid=%s\n", id, r->name);
                  manage->new_duration = a->expiration;
                } else {
                  manage->new_duration = ma.expiration;
                }
             }
             a->expiration = manage->new_duration; 

             alog_append_alias_manage_change(task->myid, r->rl_index, a->id, a->alias_offset, a->alias_size, a->expiration);

           } else {
             a->max_size = manage->new_size;
             a->reliability = manage->new_reliability;
             if (manage->new_duration == INT_MAX) manage->new_duration = time(NULL) + r->max_duration;
             if (manage->new_duration > (time(NULL)+r->max_duration)) {
                err = 1;
                log_printf(10, "handle_manage: Duration >max_duration  id: " LU " rid=%s\n", id, r->name);               
             }
             a->expiration = manage->new_duration;       

             alog_append_manage_change(task->myid, r->rl_index, a->id, a->max_size, a->reliability, a->expiration);
           }

           if (tq != NULL) {
              task_unlock(task->tc, tq, 1);
              tq = NULL;
           } else {
              //** Update the manage timestamp
              if (is_alias) {
                 update_manage_history(r, a->id, a->is_alias, &(task->ipadd), cmd->command, manage->subcmd, a->alias_offset, a->expiration, a->alias_size, pid);
              } else {
                 update_manage_history(r, a->id, a->is_alias, &(task->ipadd), cmd->command, manage->subcmd, a->reliability, a->expiration, a->max_size, pid);
              }
              err = modify_allocation_resource(r, a->id, a);
           }

           if (err == 0) {
              send_cmd_result(task, IBP_OK);
           } else {
              send_cmd_result(task, IBP_E_WOULD_EXCEED_POLICY);
           }
           break;
        case IBP_PROBE:
           char buf[1024];
           int rel = IBP_SOFT;
           uint64_t psize, pmax_size;

           if (a->reliability == ALLOC_HARD) rel = IBP_HARD;

           if (cmd->command == IBP_MANAGE) {
              alog_append_manage_probe(task->myid, r->rl_index, id);
              log_printf(15, "handle_manage: poffset=" LU " plen=" LU " a->size=" LU " a->max_size=" LU " ns=%d\n", 
                      alias_offset, alias_len, a->size, a->max_size, ns_getid(task->ns));
              pmax_size = a->max_size - alias_offset;
              if (pmax_size > alias_len) pmax_size = alias_len;
              psize = a->size - alias_offset;
              if (psize > alias_len) psize = alias_len;

              snprintf(buf, sizeof(buf)-1, "%d %d %d " LU " " LU " %ld %d %d \n",
                  IBP_OK, a->read_refcount, a->write_refcount, psize, pmax_size, a->expiration - time(NULL),
                  rel, a->type);
           } else { //** IBP_ALIAS_MANAGE
              alog_append_alias_manage_probe(task->myid, r->rl_index, pid, id);
              snprintf(buf, sizeof(buf)-1, "%d %d %d " LU " " LU " %ld \n",
                  IBP_OK, a->read_refcount, a->write_refcount, a->alias_offset, a->alias_size, a->expiration - time(NULL));

           }

           //** Update the manage timestamp
           update_manage_history(r, a->id, a->is_alias, &(task->ipadd), cmd->command, manage->subcmd, a->reliability, a->expiration, a->max_size, pid);
           err = modify_allocation_resource(r, a->id, a);
           if (err != 0) {
              log_printf(0, "handle_manage/probe:  Error with modify_allocation_resource for new queue allocation!  err=%d\n", err); 
           }

           log_printf(10, "handle_manage: probe results = %s\n",buf);
           write_netstream_block(task->ns, task->cmd_timeout, buf, strlen(buf));

           alog_append_cmd_result(task->myid, IBP_OK);

           if (tq != NULL) {
              task_unlock(task->tc, tq, 1);
           }
           break;
     }
  } else {
    log_printf(10, "handle_manage:  Denied lock:(\n");
    alog_append_manage_bad(task->myid, cmd->command, manage->subcmd);
    send_cmd_result(task, IBP_E_INTERNAL);
    cmd->state = CMD_STATE_FINISHED;
  }

  cmd->state = CMD_STATE_FINISHED;
  log_printf(10, "handle_manage: Sucessfully processed manage command\n");

  unlock_osd_id(a->id); 

  if (a->type != IBP_BYTEARRAY) unlock_tc(task);

  return(0);
}

//*****************************************************************
// handle_write  - HAndles the IBP_STORE and IBP_WRITE commands
//
// Returns
//    status \n
//
//*****************************************************************

int handle_write(ibp_task_t *task)
{
  Cmd_state_t *cmd = &(task->cmd); 
  Cmd_write_t *w = &(cmd->cargs.write);
  Allocation_t *a = &(w->a);
  Allocation_t pa; 
  Allocation_t a_final;
  osd_id_t id, pid, aid, apid;
  uint64_t asize_original;
  int err;
  int is_alias;
  size_t alias_offset, alias_len, alias_end;
  int append_mode = 0;

  err = 0;
  
  debug_printf(1, "handle_write: Starting to process command tid=" LU " ns=%d\n", task->tid, task->ns->id);

  task->stat.start = time(NULL);
  task->stat.dir = DIR_IN;
  task->stat.id = task->tid;

  w->r = resource_lookup(global_config->rl, w->crid);
  if (w->r == NULL) {
     log_printf(10, "handle_write:  Invalid RID :%s  tid=" LU "\n",w->crid, task->tid); 
     alog_append_write(task->myid, cmd->command, -1, 0, 0, w->offset, w->len);
     send_cmd_result(task, IBP_E_INVALID_RID);
     return(-1);
  }

  if ((err = get_allocation_by_cap_resource(w->r, WRITE_CAP, &(w->cap), a)) != 0) {
     log_printf(10, "handle_write: Invalid cap: %s for resource = %s  tid=" LU "\n", w->cap.v, w->r->name,task->tid);
     alog_append_write(task->myid, cmd->command, w->r->rl_index, 0, 0, w->offset, w->len);
     send_cmd_result(task, IBP_E_CAP_NOT_FOUND);
     return(-1);
  }

  pid = a->id;
  id = 0;
  
  is_alias = 0;
  alias_offset = 0;
  alias_len = a->max_size;
  if (a->is_alias == 1) {  //** This is a alias allocation so load the actual one
     pa = *a;
     id = a->alias_id;

     apid = pid; aid = id;

     alias_offset = a->alias_offset;
     alias_len =  a->alias_size;

     if ((err = get_allocation_resource(w->r, id, a)) != 0) {
        alog_append_write(task->myid, cmd->command, w->r->rl_index, pid, id, w->offset, w->len);
        log_printf(10, "handle_write: Invalid alias_id: " LU " for resource = %s  tid=" LU " ns=%d\n", id, w->r->name,task->tid, ns_getid(task->ns));
        send_cmd_result(task, IBP_E_CAP_NOT_FOUND);
        return(-1);
     }

     if (alias_len == 0) alias_len = a->max_size - alias_offset;
  } else {
    apid = 0; aid = pid;
  }

  asize_original = a->size;

  //** Validate the writing range **
  if (w->offset == -1)  { //Wants to append data
     w->offset = a->size;
     append_mode = 1;
  }

  alog_append_write(task->myid, cmd->command, w->r->rl_index, apid, aid, w->offset, w->len);

  //** Can only append if the alias allocation is for the whole allocation
  if ((append_mode == 1) && (alias_offset != 0) && (alias_len != 0)) {
     log_printf(10, "handle_write: Attempt to append to an allocation with a alias cap without full access! cap: %s r = %s off=" LU " len=" LU "  tid=" LU "\n", w->cap.v, w->r->name, w->offset, w->len, task->tid);
     send_cmd_result(task, IBP_E_WOULD_EXCEED_LIMIT);
     return(-1);

  }

  w->offset = w->offset + alias_offset;
  if (((w->offset+ w->len) > a->max_size) && (a->type == IBP_BYTEARRAY)) {
     log_printf(10, "handle_write: Attempt to write beyond end of allocation! cap: %s r = %s off=" LU " len=" LU "  tid=" LU "\n", w->cap.v, w->r->name, w->offset, w->len, task->tid);
     send_cmd_result(task, IBP_E_WOULD_EXCEED_LIMIT);
     return(-1);
  }

  //** check if we are inside the alias bounds
  alias_end = alias_offset + alias_len;
  if (((w->offset+ w->len) > alias_end) && (a->type == IBP_BYTEARRAY)) {
     log_printf(10, "handle_write: Attempt to write beyond end of alias range! cap: %s r = %s off=" LU " len=" LU " poff = " ST " plen= " ST " tid=" LU "\n", 
              w->cap.v, w->r->name, w->offset, w->len, alias_offset, alias_len, task->tid);
     send_cmd_result(task, IBP_E_WOULD_EXCEED_LIMIT);
     return(-1);
  }

  w->left = w->len;
  if (a->type == IBP_BYTEARRAY) {
     a->w_pos = w->offset;
  }        

  send_cmd_result(task, IBP_OK); //** Notify the client that everything is Ok and start sending data

  if (a->type == IBP_BYTEARRAY) {
     err = 0;
     while (err == 0) {
        log_printf(20, "handle_write: a->w_pos=" LU " w->left=" LU " ns=%d\n", a->w_pos, w->left, task->ns->id);
        err = write_to_disk(task, a, &(w->left), w->r);
        if (err == 0) {
           if (time(NULL) > task->cmd_timeout) {
              log_printf(15, "handle_write: EXPIRED command!  ctime=" TT " to=" TT "\n", time(NULL), task->cmd_timeout);
              err = -1;
           }
        }
     }
  }

  add_stat(&(task->stat));

  int bufsize = 128;
  char buffer[bufsize];

  if (err == -1) {  //** Dead connection
     log_printf(10, "handle_write:  Disk error occured! ns=%d\n", task->ns->id);
     close_netstream(task->ns);
  } else if (err == 1) {  //** Finished command
     //** Update the amount of data written if needed
     lock_osd_id(a->id);

     //** Update the write timestamp
     update_write_history(w->r, a->id, a->is_alias, &(task->ipadd), w->offset, w->len, pid);

     err = get_allocation_resource(w->r, a->id, &a_final);
     log_printf(15, "handle_write: ns=%d a->size=" LU " db_size=" LU "\n", task->ns->id, a->size, a_final.size);
     if (err == 0) {
        if (a->size > a_final.size ) {
           a_final.size = a->size;
           if (a->type == IBP_BYTEARRAY) a->size = a->w_pos; 

           err = modify_allocation_resource(w->r, a->id, &a_final);
           if (err != 0) {
              log_printf(10, "handle_write:  ns=%d ERROR with modify_allocation_resource(%s, " LU ", a)=%d\n", task->ns->id, w->crid, a->id, err);
              err = IBP_E_INTERNAL;
           }
        }
     } else {
        log_printf(10, "handle_write: ns=%d error with final get_allocation_resource(%s, " LU ", a)=%d\n", task->ns->id,
             w->r->name, a->id, err);
        if (err != 0) err = IBP_E_INTERNAL;
     }
     unlock_osd_id(a->id);

     if (err != 0) {
        snprintf(buffer, bufsize-1, "%d " LU " \n", err, w->len);
     } else {
        snprintf(buffer, bufsize-1, "%d " LU " \n", IBP_OK, w->len);
     }
 
     log_printf(10, "handle_write:  ns=%d Sending result: %s\n", task->ns->id, buffer);
     write_netstream_block(task->ns, task->cmd_timeout, buffer, strlen(buffer));
  } 

  log_printf(10, "handle_write: Exiting write tid=" LU "\n", task->tid);
  return(err);
}

//*****************************************************************
//  handle_read  - Handles the IBP_LOAD command
//
//  Returns
//    status nbytes \n
//    ...raw data stream...
//
//*****************************************************************

int handle_read(ibp_task_t *task)
{
  Cmd_state_t *cmd = &(task->cmd); 
  Cmd_read_t *r = &(cmd->cargs.read);
  Allocation_t *a = &(r->a);
  int err;
  osd_id_t id, pid;
  int is_alias;
  size_t alias_offset, alias_len, alias_end;

  err = 0;

  debug_printf(1, "handle_read: Starting to process command ns=%d\n", task->ns->id);

  task->stat.start = time(NULL);
  task->stat.dir = DIR_OUT;
  task->stat.id = task->tid;

  r->r = resource_lookup(global_config->rl, r->crid);
  if (r->r == NULL) {
     alog_append_read(task->myid, -1, 0, 0, r->offset, r->len);
     log_printf(10, "handle_read:  Invalid RID :%s\n",r->crid); 
     send_cmd_result(task, IBP_E_INVALID_RID);
     return(0);
  }

  if ((err = get_allocation_by_cap_resource(r->r, READ_CAP, &(r->cap), a)) != 0) {
     log_printf(10, "handle_read: Invalid cap: %s for resource = %s\n", r->cap.v, r->r->name);
     alog_append_read(task->myid, r->r->rl_index, 0, 0, r->offset, r->len);
     send_cmd_result(task, IBP_E_CAP_NOT_FOUND);
     return(0);
  }

  pid = a->id;
  is_alias = 0;
  alias_offset = 0;
  alias_len = a->max_size;
log_printf(10, "handle_read: id: " LU " is_alias=%d cmd offset=" OT " ns=%d\n", a->id, a->is_alias, r->offset, ns_getid(task->ns));

  if (a->is_alias == 1) {  //** This is a alias allocation so load the actual one
     id = a->alias_id;
     alias_offset = a->alias_offset;
     alias_len =  a->alias_size;
log_printf(10, "handle_read: mid: " LU " offset=" ST " len=" ST " ns=%d\n", id, alias_offset, alias_len, ns_getid(task->ns));

     if ((err = get_allocation_resource(r->r, id, a)) != 0) {
        log_printf(10, "handle_read: Invalid alias_id: " LU " for resource = %s  tid=" LU " ns=%d\n", id, r->r->name,task->tid, ns_getid(task->ns));
        alog_append_read(task->myid, r->r->rl_index, pid, id, r->offset, r->len);
        send_cmd_result(task, IBP_E_CAP_NOT_FOUND);
        return(-1);
     }

     if (alias_len == 0) alias_len = a->max_size - alias_offset;

     alog_append_read(task->myid, r->r->rl_index, pid, id, r->offset, r->len);
  } else {
    alog_append_read(task->myid, r->r->rl_index, 0, pid, r->offset, r->len);
  }

  //** Validate the reading range **
  r->offset = r->offset + alias_offset;

  if (((r->offset+ r->len) > a->max_size) && (a->type ==IBP_BYTEARRAY)) {
     log_printf(10, "handle_read: Attempt to read beyond end of allocation! cap: %s r = %s off=" LU " len=" LU "\n", r->cap.v, r->r->name, r->offset, r->len);
     send_cmd_result(task, IBP_E_WOULD_EXCEED_LIMIT);
     return(0);
  }

  //** check if we are inside the alias bounds
  alias_end = alias_offset + alias_len;
  if (((r->offset+ r->len) > alias_end) && (a->type == IBP_BYTEARRAY)) {
     log_printf(10, "handle_read: Attempt to write beyond end of alias range! cap: %s r = %s off=" LU " len=" LU " poff = " ST " plen= " ST " tid=" LU "\n", 
              r->cap.v, r->r->name, r->offset, r->len, alias_offset, alias_len, task->tid);
     send_cmd_result(task, IBP_E_WOULD_EXCEED_LIMIT);
     return(-1);
  }

  //*** and make sure there is data ***
  if (((r->offset+ r->len) > a->size) && (a->type ==IBP_BYTEARRAY)) {
     log_printf(10, "handle_read: Not enough data! cap: %s r = %s off=" LU " alen=" LU " curr_size=" LU "\n", 
           r->cap.v, r->r->name, r->offset, r->len, a->size);
     send_cmd_result(task, IBP_E_WOULD_EXCEED_LIMIT);
     return(0);
  }


  r->left = r->len;
  if (a->type == IBP_BYTEARRAY) {
     r->pos = r->offset;
     a->r_pos = r->offset;
  }

  char buffer[1024];
  snprintf(buffer, sizeof(buffer)-1, "%d " LU " \n", IBP_OK, r->len);
  log_printf(15, "handle_read: response=%s\n", buffer);
  write_netstream_block(task->ns, task->cmd_timeout, buffer, strlen(buffer));
  

  if (a->type == IBP_BYTEARRAY) {
     err = 0;
     while (err == 0) {
        log_printf(20, "handle_read: a->r_pos=" LU " r->left=" LU " ns=%d\n", a->r_pos, r->left, task->ns->id);
        err = read_from_disk(task, a, &(r->left), r->r);
        if (err == 0) {
           if (time(NULL) > task->cmd_timeout) {
              log_printf(15, "handle_read: EXPIRED command!  ctime=" TT " to=" TT "\n", time(NULL), task->cmd_timeout);
              err = -1;
           }
        }
     }

  }

  //** Update the read timestamp
  lock_osd_id(a->id);
  update_read_history(r->r, a->id, a->is_alias, &(task->ipadd), r->offset, r->len, pid);
  unlock_osd_id(a->id);

  add_stat(&(task->stat));

  alog_append_cmd_result(task->myid, IBP_OK);

  if (err == -1) {  //** Dead connection
     log_printf(10, "handle_read:  Dead connection!\n");
     close_netstream(task->ns);
  } else {
     err = 0;
  }

  log_printf(10, "handle_read: Exiting read\n");
  return(err);
}

//*****************************************************************
// same_depot_copy - Makes a same depot-depot copy
//     if rem_offset < 0 then the data is appended
//*****************************************************************

int same_depot_copy(ibp_task_t *task, char *rem_cap, int rem_offset, osd_id_t rpid)
{
   Cmd_state_t *cmd = &(task->cmd); 
   Cmd_read_t *r = &(cmd->cargs.read);
   Allocation_t *a = &(r->a);
   int alias_end, cmode;
   int err, finished;
   Resource_t *rem_r, *src_r, *dest_r;
   Allocation_t rem_a;
   Allocation_t *src_a, *dest_a;
   osd_id_t wpid, did;
   char dcrid[128]; 
   char *bstate;
   RID_t drid;
   Cap_t dcap;

   log_printf(10, "same_depot_copy: Starting to process command ns=%d rem_cap=%s\n", task->ns->id, rem_cap);
   alias_end = -1;
   
   //** Get the RID and key...... the format is RID#key
   char *tmp;
   dcrid[sizeof(dcrid)-1] = '\0'; 
   tmp = string_token(rem_cap, "#", &bstate, &finished);
   if (str2rid(tmp, &(drid)) != 0) {
      log_printf(1, "same_depot_copy: Bad RID: %s\n", tmp);
      send_cmd_result(task, IBP_E_INVALID_RID);                
      cmd->state = CMD_STATE_FINISHED;
      return(0);
   }
   rid2str(&(drid), dcrid, sizeof(dcrid));

   //** Get the write key
   dcap.v[sizeof(dcap.v)-1] = '\0';
   strncpy(dcap.v, string_token(NULL, " ", &bstate, &finished), sizeof(dcap.v)-1);
   debug_printf(10, "same_depot_copy: remote_cap=%s\n", dcap.v);

   debug_printf(10, "same_depot_copy: RID=%s\n", dcrid);

   rem_r = resource_lookup(global_config->rl, dcrid);
   if (rem_r == NULL) {
      log_printf(10, "same_depot_copy:  Invalid RID :%s\n",dcrid); 
      send_cmd_result(task, IBP_E_INVALID_RID);
      return(0);
   }

   cmode = WRITE_CAP;
   if (cmd->command == IBP_PULL) cmode = READ_CAP;

   if ((err = get_allocation_by_cap_resource(rem_r, cmode, &(dcap), &rem_a)) != 0) {
      log_printf(10, "same_depot_copy: Invalid destcap: %s for resource = %s\n", dcap.v, rem_r->name);
      send_cmd_result(task, IBP_E_CAP_NOT_FOUND);
      return(0);
   }

   wpid = rem_a.id;
   did = rem_a.id;

   //*** ALIAS cap so load the master **
   if (rem_a.is_alias) {
      if ((rem_a.alias_offset > 0) && (rem_offset < 0)) {  //** This is an append but the alias can't do it
         log_printf(10, "same_depot_copy: destcap: %s on resource = %s is a alias with offset=" LU "\n", dcap.v, rem_r->name, rem_a.alias_offset);
         send_cmd_result(task, IBP_E_CAP_ACCESS_DENIED);
         return(0);
      }

      alias_end = rem_a.alias_offset + rem_a.alias_size;
      did = rem_a.alias_id;
   }

   lock_osd_id(did);

   //*** Now get the true allocation ***
   if ((err = get_allocation_resource(rem_r, did, &rem_a)) != 0) {
      log_printf(10, "same_depot_copy: Invalid destcap: %s for resource = %s\n", dcap.v, rem_r->name);
      send_cmd_result(task, IBP_E_CAP_NOT_FOUND);
      unlock_osd_id(did);
      return(0);
   }

   if (rem_offset < 0) rem_offset = rem_a.size;  //** This is an append copy

   if (rem_a.is_alias) {
      rem_offset = rem_offset + rem_a.alias_offset;  //** Tweak the offset based on the alias bounds

      //** Validate the alias writing range **
      if (((rem_offset + r->len) > alias_end) && (rem_a.type ==IBP_BYTEARRAY)) {
         log_printf(10, "same_depot_copy: Attempt to write beyond end of alias allocation! cap: %s r = %s off=%d len=" LU "\n", 
             dcap.v, rem_r->name, rem_offset, r->len);
         send_cmd_result(task, IBP_E_WOULD_EXCEED_LIMIT);
         unlock_osd_id(did);
         return(0);
      }

   }

   //** Validate the writing/reading range **
   if (((rem_offset + r->len) > rem_a.max_size) && (rem_a.type ==IBP_BYTEARRAY)) {
      log_printf(10, "same_depot_copy: Attempt to write beyond end of allocation! cap: %s r = %s off=%d len=" LU "\n", 
          dcap.v, rem_r->name, rem_offset, r->len);
      send_cmd_result(task, IBP_E_WOULD_EXCEED_LIMIT);
      unlock_osd_id(did);
      return(0);
   }

   unlock_osd_id(did);

   //*** Now do the copy ***
   const int bufsize = 1048576;
   char buffer[bufsize];
   int soff, doff, nbytes, i, nleft, src_off, dest_off;
   osd_id_t spid, dpid; 
   nleft = r->len;
   if (r->transfer_dir == IBP_PULL) {
      src_a = &rem_a;   src_r = rem_r;  soff = rem_offset;  spid = wpid;
      dest_a = a;       dest_r = r->r;  doff = r->offset;   dpid = rpid;
   } else {
      src_a = a;        src_r = r->r;   soff = r->offset;   spid = rpid;
      dest_a = &rem_a;  dest_r = rem_r; doff = rem_offset;  dpid = wpid;
   }
   src_off = soff; dest_off = doff;

log_printf(15, "same_depot_Copy: rem_offset=%d r->offset=" OT " ns=%d\n", rem_offset, r->offset, ns_getid(task->ns));
   for (i=0; i<r->len; i=i+bufsize) {
     nbytes = (nleft > bufsize) ? bufsize : nleft;
     nleft = nleft - bufsize;
     err = read_allocation_with_id(src_r, src_a->id, soff, nbytes, buffer);
     if (err != 0) {
        char tmp[128];
        log_printf(0, "same_depot_copy: Error with read_allocation(%s, " LU ", %d, %d, buffer) = %d\n",
             rid2str(&(src_r->rid), tmp, sizeof(tmp)), src_a->id, soff, nbytes, err); 
        send_cmd_result(task, IBP_E_FILE_READ);
        cmd->state = CMD_STATE_FINISHED;
        unlock_osd_id(did);
        return(0);
     }

     err = write_allocation_with_id(dest_r, dest_a->id, doff, nbytes, buffer);
     if (err != 0) {
        char tmp[128];
        log_printf(0, "same_depot_copy: Error with write_allocation(%s, " LU ", %d, %d, buffer) = %d  tid=" LU "\n",
                rid2str(&(dest_r->rid), tmp, sizeof(tmp)), dest_a->id, doff, nbytes, err, task->tid); 
        send_cmd_result(task, IBP_E_FILE_WRITE);
        unlock_osd_id(did);
        return(0);
     }

     soff = soff + nbytes;
     doff = doff + nbytes;
   } 

   char result[512];
   result[511] = '\0';

   //** update the dest write history 
   update_write_history(dest_r, dest_a->id, dest_a->is_alias, &(task->ipadd), dest_off, r->len, dpid);

   //** Update the amount of data written if needed
   lock_osd_id(dest_a->id);
   err = get_allocation_resource(dest_r, dest_a->id, &rem_a);
   log_printf(15, "same_depot_copy: ns=%d dest->size=" LU " doff=%d\n", task->ns->id, rem_a.size, doff);
   if (err == 0) {
      if (doff > rem_a.size) {
         rem_a.size = doff;
         rem_a.w_pos = doff;
         err = modify_allocation_resource(dest_r, dest_a->id, &rem_a);
         if (err != 0) {
            log_printf(10, "same_depot_copy:  ns=%d ERROR with modify_allocation_resource(%s, " LU ", a)=%d for dest\n", task->ns->id,
                  dcrid, rem_a.id, err);
            err = IBP_E_INTERNAL;
         }
      }
   } else {
     log_printf(10, "same_depot_copy: ns=%d error with final get_allocation_resource(%s, " LU ", a)=%d for dest\n", task->ns->id,
           dcrid, rem_a.id, err);
     err = IBP_E_INTERNAL;
   }

   unlock_osd_id(dest_a->id);

   //** Now update the timestamp for the read allocation
   update_read_history(src_r, src_a->id, src_a->is_alias, &(task->ipadd), src_off, r->len, spid);

   if (err != 0) {
      snprintf(result, 511, "%d " LU " \n", err, r->len);
   } else {
      snprintf(result, 511, "%d " LU " \n", IBP_OK, r->len);
   }

   log_printf(10, "same_depot_copy: ns=%d Completed successfully.  Sending result: %s", task->ns->id, result); 
   write_netstream_block(task->ns, task->cmd_timeout, result, strlen(result));
   log_printf(15, "handle_copysend: END pns=%d cns=%d---same_depot_copy-------------------------\n", task->ns->id, task->ns->id);

   return(0);
}

//*****************************************************************
//  handle_copy  - Handles the IBP_SEND command
//
//  Returns
//    results from child command - status nbytes
//
//*****************************************************************

int handle_copy(ibp_task_t *task)
{
  Cmd_state_t *cmd = &(task->cmd); 
  Cmd_read_t *r = &(cmd->cargs.read);
  Allocation_t *a = &(r->a);
  char key[sizeof(r->remote_cap)], typekey[sizeof(r->remote_cap)];
  char addr[16];
  int err, cmode, i;
  osd_id_t id, rpid, aid, apid;
  int is_alias;
  size_t alias_offset, alias_len, alias_end;
  phoebus_t ppath;

  memset(addr, 0, sizeof(addr));

  err = 0;

  log_printf(10, "handle_copy: Starting to process command ns=%d cmd=%d\n", task->ns->id, cmd->command);

  task->stat.start = time(NULL);
  task->stat.dir = DEPOT_COPY_OUT;
  task->stat.id = task->tid;

  r->r = resource_lookup(global_config->rl, r->crid);
  if (r->r == NULL) {
     log_printf(10, "handle_read:  Invalid RID :%s\n",r->crid); 
     alog_append_dd_copy(cmd->command, task->myid, -1, 0, 0, r->len, 0, 0, r->write_mode, r->ctype, 
           r->path, 0, AF_INET, addr, r->remote_cap, "");
     send_cmd_result(task, IBP_E_INVALID_RID);
     return(0);
  }

  cmode = READ_CAP;
  if (cmd->command == IBP_PULL) cmode = WRITE_CAP;

  if ((err = get_allocation_by_cap_resource(r->r, cmode, &(r->cap), a)) != 0) {
     log_printf(10, "handle_copy: Invalid cap: %s for resource = %s\n", r->cap.v, r->r->name);
     alog_append_dd_copy(cmd->command, task->myid, r->r->rl_index, 0, 0, r->len, 0, 0, r->write_mode, 
          r->ctype, r->path, 0, AF_INET, addr, r->remote_cap, "");
     send_cmd_result(task, IBP_E_CAP_NOT_FOUND);
     return(0);
  }

  rpid = a->id;
  aid = a->id;
  apid = 0;

  is_alias = 0;
  alias_offset = 0;
  alias_len = a->max_size;
  if (a->is_alias == 1) {  //** This is a alias allocation so load the actual one
     id = a->alias_id;
     aid = id;
     apid = a->id;

     alias_offset = a->alias_offset;
     alias_len =  a->alias_size;

     if ((err = get_allocation_resource(r->r, id, a)) != 0) {
        log_printf(10, "handle_copy: Invalid alias_id: " LU " for resource = %s  tid=" LU " ns=%d\n", id, r->r->name,task->tid, ns_getid(task->ns));
        send_cmd_result(task, IBP_E_CAP_NOT_FOUND);
        return(-1);
     }

     if (alias_len == 0) alias_len = a->max_size - alias_offset;
  }

  //** Validate the reading or writing range for the local allocation **
  if ((cmd->command == IBP_PULL) && (r->write_mode == 1)) { 
     r->offset = a->size;  //** PULL with append
  } else {
     r->offset = r->offset + alias_offset;
  }
  if (((r->offset + r->len) > a->max_size) && (a->type ==IBP_BYTEARRAY)) {
     log_printf(10, "handle_copy: Attempt to read beyond end of allocation! cap: %s r = %s off=" LU " len=" LU "\n", r->cap.v, r->r->name, r->offset, r->len);
     alog_append_dd_copy(cmd->command, task->myid, r->r->rl_index, apid, aid, r->len, r->offset, 
           r->remote_offset, r->write_mode, r->ctype, r->path, 0, AF_INET, addr, r->remote_cap, "");
     send_cmd_result(task, IBP_E_WOULD_EXCEED_LIMIT);
     return(0);
  }

  //** check if we are inside the alias bounds
  alias_end = alias_offset + alias_len;
  if (((r->offset + r->len) > alias_end) && (a->type == IBP_BYTEARRAY)) {
     log_printf(10, "handle_copy: Attempt to write beyond end of alias range! cap: %s r = %s off=" LU " len=" LU " poff = " ST " plen= " ST " tid=" LU "\n", 
              r->cap.v, r->r->name, r->offset, r->len, alias_offset, alias_len, task->tid);
     alog_append_dd_copy(cmd->command, task->myid, r->r->rl_index, apid, aid, r->len, r->offset, 
           r->remote_offset, r->write_mode, r->ctype, r->path, 0, AF_INET, addr, r->remote_cap, "");
     send_cmd_result(task, IBP_E_WOULD_EXCEED_LIMIT);
     return(-1);
  }


  //*** and make sure there is data for PUSH commands***
  if (r->transfer_dir == IBP_PUSH) {
     if (((r->offset + r->len) > a->size) && (a->type ==IBP_BYTEARRAY)) {
        log_printf(10, "handle_copy: Not enough data! cap: %s r = %s off=" LU " alen=" LU " curr_size=" LU "\n", 
              r->cap.v, r->r->name, r->offset, r->len, a->size);
        alog_append_dd_copy(cmd->command, task->myid, r->r->rl_index, apid, aid, r->len, 
              r->offset, r->remote_offset, r->write_mode, r->ctype, r->path, 0, AF_INET, addr, r->remote_cap, "");
        send_cmd_result(task, IBP_E_WOULD_EXCEED_LIMIT);
        return(0);
     }
  }

  //*** Parse the remote cap for the host/port
  int fin, rport;
  char *bstate;
  char *rhost;
  char *temp = strdup(r->remote_cap);
  rhost = string_token(temp, "/", &bstate, &fin); //** gets the ibp:/
  rhost = string_token(NULL, ":", &bstate, &fin); //** This should be the host name
  rhost = &(rhost[1]);  //** Skip the extra "/"
  sscanf(string_token(NULL, "/", &bstate, &fin), "%d", &rport);  
  strncpy(key, string_token(NULL, "/", &bstate, &fin), sizeof(key)-1); key[sizeof(key)-1] = '\0';
  strncpy(typekey, string_token(NULL, "/", &bstate, &fin), sizeof(typekey)-1); typekey[sizeof(typekey)-1] = '\0';

  lookup_host(rhost, addr);
  alog_append_dd_copy(cmd->command, task->myid, r->r->rl_index, apid, aid, r->len, 
       r->offset, r->remote_offset, r->write_mode, r->ctype, r->path, rport, AF_INET, addr, key, typekey);
    
  log_printf(15, "handle_copy: rhost=%s rport=%d cap=%s key=%s typekey=%s\n", rhost, rport, r->remote_cap, key, typekey);

  //** check if it's a copy to myself
  for (i=0; i<global_config->server.n_iface; i++) {
     if ((strcmp(rhost, global_config->server.iface[i].hostname) == 0) && (rport == global_config->server.iface[i].port)) {
        err = same_depot_copy(task, key, r->remote_offset, rpid);
        free(temp);
        return(err);
     }
  }
  
  //** Make the connection
  Net_timeout_t tm;
  NetStream_t *ns = new_netstream();
  int to = task->cmd_timeout - time(NULL) - 5;
  if (to > 20) to = 20;  //** If no connection in 20 sec die anyway
  if (to < 0 ) to = 1;
  set_net_timeout(&tm, to, 0);
  ppath.p_count = 0;

  if (r->ctype  == IBP_PHOEBUS) {
     if (r->path[0] == '\0') {
        log_printf(5, "handle_copy: using default phoebus path to %s:%d\n", rhost, rport);
        ns_config_phoebus(ns, NULL, 0);
     } else {
        log_printf(5, "handle_copy: using phoebus path r->path= %s to %s:%d\n", r->path, rhost, rport);
        phoebus_path_set(&ppath, r->path);
        ns_config_phoebus(ns, &ppath, 0);
     }
  } else {
     ns_config_sock(ns, -1, 0);
  }
  err = net_connect(ns, rhost, rport, tm);
  if (err != 0) {
     log_printf(5, "handle_copy: net_connect returned an error err=%d to host %s:%d\n",err, rhost, rport);
     send_cmd_result(task, IBP_E_CONNECTION);
     if (ppath.p_count != 0) phoebus_path_destroy(&ppath);
     destroy_netstream(ns);
     return(0);
  }

  //** Send the data
  err = handle_transfer(task, rpid, ns, key, typekey);

  log_printf(10, "handle_copy: End of routine Remote host:%s:%d ns=%d\n", rhost, rport, task->ns->id);

  destroy_netstream(ns);
  if (ppath.p_count != 0) phoebus_path_destroy(&ppath);

  free(temp);
  return(err);
}

//*****************************************************************
//  handle_transfer  - Handles the actual transfer
//*****************************************************************

int handle_transfer(ibp_task_t *task, osd_id_t rpid, NetStream_t *ns, const char *key, const char *typekey)
{
  Cmd_state_t *cmd = &(task->cmd); 
  Cmd_read_t *r = &(cmd->cargs.read);
  Allocation_t *a = &(r->a);
  Allocation_t a_final;
  char *bstate;
  int err, myid;
  int fin;
  long long unsigned int llu;
  char write_cmd[1024];

  myid = task->ns->id;

  log_printf(10, "handle_transfer: Starting to process command ns=%d\n", task->ns->id);

  int rto = r->remote_sto;
  int rlen = r->len;
  switch (r->transfer_dir) {
    case IBP_PUSH:
       if (r->write_mode == 1) {
          snprintf(write_cmd, 1024, "%d %d %s %s %d %d \n", IBPv040, IBP_STORE, key, typekey, rlen, rto);
       } else {
          snprintf(write_cmd, 1024, "%d %d %s %s " OT " %d %d \n", IBPv040, IBP_WRITE, key, typekey, r->remote_offset, rlen, rto);
       }
       break;
    case IBP_PULL:
       snprintf(write_cmd, 1024, "%d %d %s %s " OT " %d %d \n", IBPv040, IBP_LOAD, key, typekey, r->remote_offset, rlen, rto);
       break;
  }
  log_printf(15, "handle_send: ns=%d sending command: %s", task->ns->id, write_cmd);
  write_cmd[1023] = '\0';

  err = write_netstream_block(ns, task->cmd_timeout, write_cmd, strlen(write_cmd));
  if (err != NS_OK) {
     log_printf(10, "handle_send: Error sending command! write_netstream_block=%d  cmd=%s\n", err, write_cmd);
     send_cmd_result(task, IBP_E_CONNECTION);
     return(0);
  }

log_printf(15, "handle_send: ns=%d AAAAAAAAAAAAAAAAAAAA\n", task->ns->id);
  Net_timeout_t dt2;
  set_net_timeout(&dt2, rto, 0);
  err = readline_netstream(ns, write_cmd, sizeof(write_cmd), dt2);
  if (err == -1) { //** Dead connection
     log_printf(10, "handle_send:  Failed receving command response rns=%d\n", ns_getid(ns));
     close_netstream(ns);
     send_cmd_result(task, IBP_E_CONNECTION);
     return(0);
  } else if (write_cmd[strlen(write_cmd)-1] == '\n') {  //** Got a complete line
     log_printf(15, "handle_send: ns=%d BBBBBBBBBBBBB response=%s\n", task->ns->id, write_cmd);
     err = 0;
     sscanf(string_token(write_cmd, " ", &bstate, &fin), "%d", &err);
     if (err != IBP_OK) {  //** Got an error 
        log_printf(10, "handle_transfer:  Response had an error remote_ns=%d  Error=%d\n", ns_getid(ns), err);
        send_cmd_result(task, err);
        close_netstream(ns);
        return(0);
     }
     if (r->transfer_dir == IBP_PULL) { //** Check the nbytes
        sscanf(string_token(NULL, " ", &bstate, &fin), "%llu", &llu);
        if (llu != r->len) {  //** Not enoughpbytes
           log_printf(10, "handle_transfer:  Not enough bytes! remote_ns=%d  nbytes=%llu\n", ns_getid(ns), llu);
           send_cmd_result(task, IBP_E_WOULD_EXCEED_LIMIT);
           close_netstream(ns);
           return(0);
        }
     }
  }     

  r->left = r->len;

  log_printf(15, "handle_send: ns=%d rns=%dCCCCCCCCCCCCCCCCCCCCCC\n", ns_getid(task->ns), ns_getid(ns));

  //** For the send copy the remote ns into the task
  NetStream_t *pns = task->ns;
  task->ns = ns;
  if (r->transfer_dir == IBP_PUSH) {
     a->r_pos = r->offset;
     err = read_from_disk(task, a, &(r->left), r->r);
  } else {
     a->w_pos = r->offset;
     err = write_to_disk(task, a, &(r->left), r->r);
  }
  while (err == 0) {
     if (r->transfer_dir == IBP_PUSH) {
        err = read_from_disk(task, a, &(r->left), r->r);
     } else {
        err = write_to_disk(task, a, &(r->left), r->r);
     }
     if (err == 0) {
        if (time(NULL) > (task->cmd_timeout - 1)) err = -1;
     }
  }
  task->ns = pns;  //** Put it back

  add_stat(&(task->stat));

  if (err == -1) {  //** Dead connection
     log_printf(10, "handle_send:  Dead connection\n");
     send_cmd_result(task, IBP_E_CONNECTION);
     close_netstream(ns);
  } else if (err == 1) {  //** Finished command
     char result[512];
     Net_timeout_t dt;
     set_net_timeout(&dt, 1, 0);

     err = 0;

     //** Handle my return result from IBP_STORE/WRITE
     result[0] = '\0';
     readline_netstream(ns, result, sizeof(result), dt);
     result[511] = '\0';
     log_printf(10, "handle_send: ns=%d server returned: %s", task->ns->id, result);    

     //** Send parents result back **
     snprintf(result, 511, "%d " LU " \n", IBP_OK, r->len);
     result[511] = '\0';
     log_printf(10, "handle_send: ns=%d Completed successfully.  Sending result: %s", task->ns->id, result); 
     write_netstream_block(task->ns, task->cmd_timeout, result, strlen(result));
     log_printf(15, "handle_copysend: END ns=%d rns=%d----------------------------\n", ns_getid(task->ns), ns_getid(ns));

     //** Update the read timestamp
     lock_osd_id(a->id);
     if (r->transfer_dir == IBP_PUSH) {
        update_read_history(r->r, a->id, a->is_alias, &(task->ipadd), r->offset, r->len, rpid);
     } else {     //** And also the size if needed
        update_write_history(r->r, a->id, a->is_alias, &(task->ipadd), r->offset, r->len, rpid);

        err = get_allocation_resource(r->r, a->id, &a_final);
        log_printf(15, "handle_transfer: ns=%d a->size=" LU " db_size=" LU "\n", task->ns->id, a->size, a_final.size);
        if (err == 0) {
           if (a->size > a_final.size ) {
              a_final.size = a->size;
              if (a->type == IBP_BYTEARRAY) a->size = a->w_pos; 

              err = modify_allocation_resource(r->r, a->id, &a_final);
              if (err != 0) {
                 log_printf(10, "handle_write:  ns=%d ERROR with modify_allocation_resource(%s, " LU ", a)=%d\n",
                       task->ns->id, r->crid, a->id, err);
                 err = IBP_E_INTERNAL;
              }
           }
        } else {
           log_printf(10, "handle_write: ns=%d error with final get_allocation_resource(%s, " LU ", a)=%d\n", task->ns->id,
                r->r->name, a->id, err);
           if (err != 0) err = IBP_E_INTERNAL;
        }
     }
     unlock_osd_id(a->id);
   } else {  //** Error!!!
     log_printf(0, "handle_send:  Invalid result from read_from_disk!!!! err=%d\n", err);
   }

   log_printf(10, "handle_send: Completed. ns=%d err=%d\n", myid, err);

   return(err);
}


//*****************************************************************
//  handle_internal_date_free  - Handles the internal command for determining
//      when xxx amount of space becomes free using eith hard or soft allocations
//
//*****************************************************************

int handle_internal_date_free(ibp_task_t *task)
{
  Cmd_state_t *cmd = &(task->cmd); 
  Cmd_internal_date_free_t *arg = &(cmd->cargs.date_free);
  int a_count, p_count, err;
  uint64_t total_bytes, bytes, bytes_used;
  time_t curr_time;
  Allocation_t a;
  walk_expire_iterator_t *wei;
  Resource_t *r;
  char text[512];

  log_printf(5, "handle_internal_date_free: Start of routine.  ns=%d size= " LU "\n",ns_getid(task->ns), arg->size);

  r = resource_lookup(global_config->rl, arg->crid);
  if (r == NULL) {
     log_printf(10, "handle_internal_date_free:  Invalid RID :%s\n",arg->crid); 
     alog_append_internal_date_free(task->myid, -1, arg->size);
     send_cmd_result(task, IBP_E_INVALID_RID);
     return(0);
  }

  alog_append_internal_date_free(task->myid, r->rl_index, arg->size);

  //*** Send back the results ***
  send_cmd_result(task, IBP_OK);
  
  wei = walk_expire_iterator_begin(r);
  
  a_count = p_count = 0;
  bytes = bytes_used = total_bytes = 0;
  curr_time = 0;
  
  err = get_next_walk_expire_iterator(wei, DBR_NEXT, &a);
  while ((err == 0) && (arg->size > total_bytes)) {
    if (a.expiration != curr_time) {  //** time change so print the current stats
       if (curr_time != 0) {
          sprintf(text, TT " %d %d " LU " " LU "\n", curr_time, a_count, p_count, bytes_used, bytes);
          log_printf(10, "handle_internal_date_free:  ns=%d sending %s", ns_getid(task->ns), text);
          err = write_netstream_block(task->ns, task->cmd_timeout, text, strlen(text));
          if (err != strlen(text)) {
             log_printf(10, "handle_internal_date_free:  ns=%d erro with write_netstream=%d\n", ns_getid(task->ns), err);
          } else {
             err = 0;
          }
       }

       curr_time = a.expiration;
       a_count = p_count = bytes = bytes_used = 0;
    } 

    if (a.is_alias) { 
       p_count++;
    } else {
       a_count++;
    }

    bytes_used = bytes_used + a.size;
    bytes = bytes + a.max_size;
    total_bytes = total_bytes + a.max_size;

    log_printf(15, "handle_internal_date_free: ns=%d time=" TT " a_count=%d p_count=%d bytes=" LU " total=" LU "\n", ns_getid(task->ns), curr_time, a_count, p_count, bytes, total_bytes);

    if (err == 0) err = get_next_walk_expire_iterator(wei, DBR_NEXT, &a);    
  }

  if (curr_time != 0) {
     sprintf(text, TT " %d %d " LU " " LU "\n", curr_time, a_count, p_count, bytes_used, bytes);
     log_printf(10, "handle_internal_date_free:  ns=%d sending %s", ns_getid(task->ns), text);
     err = write_netstream_block(task->ns, task->cmd_timeout, text, strlen(text));
     if (err != strlen(text)) {
        log_printf(10, "handle_internal_date_free:  ns=%d erro with write_netstream=%d\n", ns_getid(task->ns), err);
     }
  }

  walk_expire_iterator_end(wei);

  //**send the terminator
  err = write_netstream_block(task->ns, task->cmd_timeout, "END\n", 4);
  if (err == 4) err = 0;

  log_printf(5, "handle_internal_date_free: End of routine.  ns=%d error=%d\n",ns_getid(task->ns), err);

  return(0);
}


//*****************************************************************
//  handle_internal_expire_list  - Handles the internal command for generating
//      a list of alloctionsto be expired
//
//*****************************************************************

int handle_internal_expire_list(ibp_task_t *task)
{
  Cmd_state_t *cmd = &(task->cmd); 
  Cmd_internal_expire_log_t *arg = &(cmd->cargs.expire_log);
  int i, err;
  time_t expire_time;
  Allocation_t a;
  walk_expire_iterator_t *wei;
  Resource_t *r;
  char text[512];

  log_printf(5, "handle_internal_expire_list: Start of routine.  ns=%d time= " TT " count= %d\n",ns_getid(task->ns), arg->start_time, arg->max_rec);

  r = resource_lookup(global_config->rl, arg->crid);
  if (r == NULL) {
     log_printf(10, "handle_internal_expire_list:  Invalid RID :%s\n",arg->crid); 
     alog_append_internal_expire_list(task->myid, -1, arg->start_time, arg->max_rec);
     send_cmd_result(task, IBP_E_INVALID_RID);
     return(0);
  }

  alog_append_internal_expire_list(task->myid, r->rl_index, arg->start_time, arg->max_rec);

  //*** Send back the results ***
  send_cmd_result(task, IBP_OK);
  
  wei = walk_expire_iterator_begin(r);

  set_walk_expire_iterator(wei, arg->start_time);
  
  err = get_next_walk_expire_iterator(wei, DBR_NEXT, &a);
  i = 0;
  while ((err == 0) && (i < arg->max_rec)) {
    expire_time = a.expiration;
    sprintf(text, TT " " LU " " LU "\n", expire_time, a.id, a.max_size);
    log_printf(10, "handle_internal_expire_list:  ns=%d sending %s", ns_getid(task->ns), text);
    err = write_netstream_block(task->ns, task->cmd_timeout, text, strlen(text));
    if (err != strlen(text)) {
       log_printf(10, "handle_internal_expire_list:  ns=%d error with write_netstream=%d\n", ns_getid(task->ns), err);
    } else {
       err = 0;
    }

    if (err == 0) err = get_next_walk_expire_iterator(wei, DBR_NEXT, &a);    
    i++;
  }

  walk_expire_iterator_end(wei);

  //**send the terminator
  err = write_netstream_block(task->ns, task->cmd_timeout, "END\n", 4);
  if (err == 4) err = 0;

  log_printf(5, "handle_internal_expire_list: End of routine.  ns=%d error=%d\n",ns_getid(task->ns), err);

  return(0);
}

