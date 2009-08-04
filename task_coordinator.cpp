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

//********************************************************************
//********************************************************************

#include <stdlib.h>
#include <assert.h>
#include "ibp_task.h"
#include "task_coordinator.h"
#include "resource.h"
#include "debug.h"
#include "log.h"
#include "fmttypes.h"

//********************************************************************
// lock_tc - Locks the global task coordinator
//********************************************************************

void lock_tc(ibp_task_t *task)
{
  log_printf(15, "lock_tc: Locking....ns=%d\n", task->ns->id); flush_log();
  pthread_mutex_lock(&(task->tc->lock));
  log_printf(15, "lock_tc: ....got it ns=%d\n", task->ns->id); flush_log();

}

//********************************************************************
// unlock_tc - Unlocks the global task coordinator
//********************************************************************

void unlock_tc(ibp_task_t *task)
{
  log_printf(15, "unlock_tc: Unlocking.... ns=%d\n", task->ns->id); flush_log();
  pthread_mutex_unlock(&(task->tc->lock));
}

//********************************************************************
// create_task_que - Creates a new task queue
//********************************************************************

Task_que_t *create_task_que()
{
  Task_que_t *tq;

//  if ((tq = g_slice_new(Task_que_t)) == NULL) {
  if ((tq = (Task_que_t *)malloc(sizeof(Task_que_t))) == NULL) {
     log_printf(0, "create_task_que: Unable to allocate new struct!\n");
     flush_log();
     return(NULL);
  }

  tq->cid = NULL;
  tq->refcount = 0;
  tq->op = -1;
  tq->owner = -1;
  tq->lock_inuse = 0;
  tq->io_que[TASK_READ_QUE] = new_stack();
  tq->io_que[TASK_WRITE_QUE] = new_stack();

  pthread_mutex_init(&(tq->qlock), NULL);

  return(tq);
}

//********************************************************************
// free_task_que - Frees a task queu data structure
//********************************************************************

void free_task_que(Task_que_t *tq)
{
  char *cid = tq->dummycid;
  log_printf(15, "free_task_que: cid=%s\n",cid);
 
  
  free_stack(tq->io_que[TASK_READ_QUE], 0);
  free_stack(tq->io_que[TASK_WRITE_QUE], 0);

  pthread_mutex_destroy(&(tq->qlock));

//  free(tq->cid);   //** This is done in outside by task_key_destroy

  free(tq);

  log_printf(15, "free_task_que: End cid=%s\n",cid);
  free(cid);
  
//  g_slice_free(Task_que_t, tq);
}

//****************************************************************
// task_hash_destroy - Routines so I can remove key/data pairs
//****************************************************************

void task_hash_destroy(gpointer data)
{
  Task_que_t *tq = (Task_que_t *)data;

  free_task_que(tq);

  return;
}


//****************************************************************
//  task_key_destroy - Removes a HASH key
//****************************************************************

void task_key_destroy(gpointer data)
{
  char *cid = (char *)data;

  log_printf(15, "task_key_destroy: cid=%s\n",cid);

  free(cid);

  return;
}

//********************************************************************
// create_task_coord - Initialize the task coordinator for use
//********************************************************************

Task_coord_t *create_task_coord()
{
  Task_coord_t *tc;

  assert((tc = (Task_coord_t *)malloc(sizeof(Task_coord_t))) != NULL);
  assert((tc->table = g_hash_table_new_full(g_str_hash, g_str_equal, task_key_destroy, task_hash_destroy)) != NULL);
//  assert((tc->table = g_hash_table_new(g_str_hash, g_str_equal) != NULL);

  pthread_mutex_init(&(tc->lock), NULL);

  return(tc);
}


//********************************************************************
// free_task_coord - Closes the task coordinator and frees any memory
//********************************************************************

void free_task_coord(Task_coord_t *tc)
{
  g_hash_table_destroy(tc->table);  

  pthread_mutex_destroy(&(tc->lock));  
  
  free(tc);
}

//********************************************************************
//  task_lock - Tries to acquire a lock on the allocation
//********************************************************************

Task_que_t *task_lock(Task_coord_t *tc, ibp_task_t *task, Resource_t *r, char *cid, int op, 
            int offset, int len, int tryagain, Allocation_t *a, int *lock)
{

  osd_id_t id;
  Task_que_t *tq;
  int err, i;

  *lock = TASK_LOCK_ERROR;
  if (sscanf(cid, LU, &id) == 0) {  
     return(NULL);
  }

  log_printf(10, "task_lock: Lock requested for cid=%s id=" LU " ns=%d ta=%d!\n", cid, id, task->ns->id, tryagain);        

//  lock_tc(tc);

  tq = (Task_que_t *)g_hash_table_lookup(tc->table, cid);
  if (tq == NULL) {  //** Not in the que
     if ((err = get_allocation_resource(r, id, a)) != 0) {   //** Get the allocation
        log_printf(10, "task_lock id =%s not found\n", cid);
//        unlock_tc(tc)
        return(NULL);
     }

  log_printf(10, "task_lock: cid=%s id=" LU " a->id=" LU " ns=%d ta=%d!\n", cid, id, a->id, task->ns->id, tryagain); flush_log();

     if (a->type == IBP_BYTEARRAY) {  //** Don't need to worry about serializing op
        *lock = TASK_LOCK_GRANTED;
//        unlock_tc(tc)
        return(NULL);
     }

     log_printf(10, "task_lock: Creating new task_que for id=%s! ns=%d\n", cid, task->ns->id);   flush_log();
 
     if ((tq = create_task_que()) == NULL) {      //** Get a new slice to add
        log_printf(0, "task_lock:  Error allocating new 'Task_que_t'!\n");        
//        unlock_tc(tc)  
        return(NULL);
     }

     log_printf(10, "task_lock: AFTER creatied new task_que for id=%s! ns=%d\n", cid, task->ns->id);   flush_log();

     //*** Init the new data structure ***
     tq->cid = strdup(cid);  //** Dup the CID
 tq->dummycid = strdup(cid);  //** Debugging only so REMOVE
     tq->a = *a;
     tq->r = r;

     g_hash_table_insert(tc->table, tq->cid, tq);  //**Insert it into the table
     log_printf(10, "task_lock: AFTER g_hash_table_insert  id=%s! ns=%d\n", cid, task->ns->id);   flush_log();

  }

  pthread_mutex_lock(&(tq->qlock));  //** Get the lock

  //** At this point we have a data structure that has to be managed
  if ((op == TASK_MANAGE) || (a->type == IBP_BYTEARRAY)) {  //** Don't need to worry about serializing op
     *lock = TASK_LOCK_GRANTED;
     *a = tq->a;
     pthread_mutex_unlock(&(tq->qlock)); 
//     unlock_tc(tc)
     return(NULL);
  }

  //** Anything else qoes in the queue

  debug_code(
     Stack_t *rque = tq->io_que[TASK_READ_QUE];
     Stack_t *wque = tq->io_que[TASK_WRITE_QUE];
     ibp_task_t *rt;
     ibp_task_t *wt;
     move_to_top(rque); move_to_top(wque);
     rt = (ibp_task_t *)get_ele_data(rque);
     wt = (ibp_task_t *)get_ele_data(wque);
     debug_printf(10, "task_lock:  Start of routine ");
     if (rt == NULL) { debug_printf(10, "rtask=NULL * "); } else { debug_printf(10, "rtask=%d * ", rt->ns->id); }     
     if (wt == NULL) { debug_printf(10, "wtask=NULL\n"); } else { debug_printf(10, "wtask=%d\n", wt->ns->id); }     
  )

  ibp_task_t *t;
  int mode = -1;
  if (op == TASK_READ) {
    mode = TASK_READ_QUE;
  } else if (op == TASK_WRITE) {
    mode = TASK_WRITE_QUE;
  } else {
    log_printf(0, "task_lock: Error invalid op!\n");
    pthread_mutex_unlock(&(tq->qlock)); 
//    unlock_tc(tc)
    return(NULL);
  }  

  *lock = TASK_LOCK_QUEUED;

  tq->refcount++;

  debug_code(
     move_to_top(rque); move_to_top(wque);
     rt = (ibp_task_t *)get_ele_data(rque);
     wt = (ibp_task_t *)get_ele_data(wque);
     debug_printf(10, "task_lock:  Before insert ");
     if (rt == NULL) { debug_printf(10, "rtask=NULL * "); } else { debug_printf(10, "rtask=%d * ", rt->ns->id); }     
     if (wt == NULL) { debug_printf(10, "wtask=NULL\n"); } else { debug_printf(10, "wtask=%d\n", wt->ns->id); }     
  )

  
  if (mode == TASK_READ_QUE) {
    log_printf(15, "task_lock: rque inserting task ns=%d size=%d\n", task->ns->id, stack_size(tq->io_que[mode]));
  } else {
    log_printf(15, "task_lock: wque inserting task ns=%d size=%d\n", task->ns->id, stack_size(tq->io_que[mode]));
  }  

  if (tryagain == 0) {
    move_to_bottom(tq->io_que[mode]);    //** Insert the command on the bottom of the list
    insert_below(tq->io_que[mode], (void *)task);
  } else {
    log_printf(15, "task_lock: skipped insert task ns=%d size=%d\n", task->ns->id, stack_size(tq->io_que[mode]));
  }

  debug_code(
     move_to_top(rque); move_to_top(wque);
     rt = (ibp_task_t *)get_ele_data(rque);
     wt = (ibp_task_t *)get_ele_data(wque);
     debug_printf(10, "task_lock:  After insert ");
     if (rt == NULL) { debug_printf(10, "rtask=NULL * "); } else { debug_printf(10, "rtask=%d * ", rt->ns->id); }     
     if (wt == NULL) { debug_printf(10, "wtask=NULL\n"); } else { debug_printf(10, "wtask=%d\n", wt->ns->id); }     

     
  )

  move_to_top(tq->io_que[mode]);   //** Now check if this command is on the top
  t = (ibp_task_t *)get_ele_data(tq->io_que[mode]);
  if (t == task) *lock = TASK_LOCK_GRANTED;

  log_printf(10, "task_lock: After que top check que=%d state=%d refcnt=%d\n", mode, *lock, tq->refcount);

  debug_code(
    if (t != NULL) {
       log_printf(10, "task_lock: que top is ns=%d and my ns= %d for " LU " \n", t->ns->id, task->ns->id, id);
    } else {
       log_printf(10, "task_lock: Lock que is NULL for op on " LU " ns=%d\n", id, task->ns->id);
    }
  )

  debug_code(
     move_to_top(rque); move_to_top(wque);
     rt = (ibp_task_t *)get_ele_data(rque);
     wt = (ibp_task_t *)get_ele_data(wque);
     debug_printf(10, "task_lock:  End of routine ");
     if (rt == NULL) { debug_printf(10, "rtask=NULL * "); } else { debug_printf(10, "rtask=%d * ", rt->ns->id); }     
     if (wt == NULL) { debug_printf(10, "wtask=NULL\n"); } else { debug_printf(10, "wtask=%d\n", wt->ns->id); }     

      debug_printf(15, "task_lock: Printing rque size=%d\n", stack_size(rque));
      debug_printf(15, "task_lock: ---------------------------\n");
      move_to_top(rque);
      i = 0;
      rt = (ibp_task_t *)get_ele_data(rque);
      while (rt != NULL) {
         debug_printf(15, "task_lock: i=%d r_ns=%d\n", i, rt->ns->id);         
         move_down(rque);
         rt = (ibp_task_t *)get_ele_data(rque);
         i++;
      }

      debug_printf(15, "\ntask_lock: Printing wque size=%d\n", stack_size(wque));
      debug_printf(15, "task_lock: ---------------------------\n");
      move_to_top(wque);
      i = 0;
      wt = (ibp_task_t *)get_ele_data(wque);
      while (wt != NULL) {
         debug_printf(15, "task_lock: i=%d w_ns=%d\n", i, wt->ns->id);         
         move_down(wque);
         wt = (ibp_task_t *)get_ele_data(wque);
         i++;
      }

  )


  if (*lock == TASK_LOCK_GRANTED) {
    if (tq->lock_inuse == 0) {   //** I keep the tq->lock if I get it
       tq->lock_inuse = 1;
       tq->owner = task->ns->id;
       log_printf(15, "task_lock: Got lock!  ns=%d id=" LU "\n", task->ns->id, id);
       pthread_mutex_unlock(&(tq->qlock)); 
    } else {
       log_printf(15, "task_lock: Somebody else(ns=%d) owns it:(  ns=%d id=" LU "\n", tq->owner, task->ns->id, id);
       *lock = TASK_LOCK_QUEUED;
       pthread_mutex_unlock(&(tq->qlock)); 
       tq = NULL;
    }   
  } else {
     log_printf(15, "task_lock: Almost! not on top... Somebody else (ns=%d) owns it:(  ns=%d id=" LU "\n", tq->owner, task->ns->id, id);
     *lock = TASK_LOCK_QUEUED;
     pthread_mutex_unlock(&(tq->qlock)); 
     tq = NULL;
  }   

//  unlock_tc(tc)

  return(tq);
}

//********************************************************************
// task_unlock - Releases an acquired lock
//********************************************************************

void task_unlock(Task_coord_t *tc, Task_que_t *tq, int get_qlock)
{
  int err;

  if (get_qlock == 1) { 
//     lock_tc(tc);
     pthread_mutex_lock(&(tq->qlock)); 
  }

  tq->refcount = stack_size(tq->io_que[TASK_READ_QUE]) + stack_size(tq->io_que[TASK_WRITE_QUE]);

  log_printf(10, "task_unlock:  id=%s * refcount=%d old owner ns=%d\n",tq->cid, tq->refcount, tq->owner);
  tq->owner = -1;

  if (tq->refcount == 0) {
     if ((err = modify_allocation_resource(tq->r, tq->a.id, &(tq->a))) != 0) {
       log_printf(0, "task_unlock:  Error with put_allocation_recource for id %s.  Error=%d\n", tq->cid, err);
     }

     char *cid = strdup(tq->cid);
     g_hash_table_remove(tc->table, cid);
     free(cid);
  } else {
    tq->lock_inuse = 0;
  }

  if (get_qlock == 1) {
//    unlock_tc(tc)
    pthread_mutex_unlock(&(tq->qlock));
  }
}

//********************************************************************
// task_droplockreq - Drops a lock pending request
//********************************************************************

void task_droplockreq(Task_coord_t *tc, ibp_task_t *task, char *cid, int op)
{
  ibp_task_t *t;
  int mode;

  log_printf(15, "task_droplockreq: start!!!!!!!!!!!!!!!!!! ns=%d cid=%s\n",task->ns->id, cid); 
//  lock_tc(task);

  Task_que_t *tq = (Task_que_t *)g_hash_table_lookup(tc->table, cid);
  if (tq == NULL) {  //** Not in the que
     log_printf(0, "task_droplockreq:  Can't find id %s!\n", cid);
  } else {
    pthread_mutex_lock(&(tq->qlock)); 
    
    switch (op) {
      case TASK_READ: mode = TASK_READ_QUE; break;
      case TASK_WRITE: mode = TASK_WRITE_QUE; break;   
      default: mode = -1; break;
    }

    if (mode != -1) {
       Stack_t *s;
       int fin = 0;
       s = tq->io_que[mode];
       move_to_top(s);
       t = (ibp_task_t *)get_ele_data(s);
       while ((t != NULL) && (fin == 0)) {
          if (t == task) {
               log_printf(15, "task_droplockreq: found it! ns=%d cid=%s\n",task->ns->id, cid); 
             fin = 1;
             delete_current(s, 0, 0);
          } else {
            move_down(s);
            t = (ibp_task_t *)get_ele_data(s);
            log_printf(15, "task_droplockreq: Trying again! ns=%d cid=%s\n",task->ns->id, cid); 
          }
       }

      tq->refcount = stack_size(tq->io_que[TASK_READ_QUE]) + stack_size(tq->io_que[TASK_WRITE_QUE]);

      if (tq->refcount == 0) {   //** Delete the key if needed
         log_printf(15, "task_droplockreq: Attempting to remove it. ns=%d cid=%s\n",task->ns->id, cid); 

         pthread_mutex_unlock(&(tq->qlock)); 
         g_hash_table_remove(tc->table, cid);
      } else {
         pthread_mutex_unlock(&(tq->qlock)); 
      }
    }
  } 

//  unlock_tc(task);

  log_printf(15, "task_droplockreq: end!!!!!!!!!!!!!!!!!! ns=%d cid=%s\n",task->ns->id, cid); 

}


