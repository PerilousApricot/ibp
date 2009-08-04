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

//************************************************************************************
//************************************************************************************

#include <string.h>
#include <limits.h>
#include <time.h>
#include <pthread.h>
#include "ibp_server.h"
#include "log.h"
#include "debug.h"
#include "security_log.h"
#include "allocation.h"
#include "resource.h"
#include "network.h"
#include "ibp_task.h"
#include "ibp_protocol.h"
#include "task_coordinator.h"


//************************************************************************************
// wake_up_task - Wakes up a task and adds it back into the list for monitoring
//************************************************************************************

ibp_task_t *get_task_to_wake_up(Stack_t *que)
{
  if (stack_size(que) == 0) {
     log_printf(15, "get_task_to_wake_up:  Nothing to wake up:(\n");
     return(NULL);
  }

  log_printf(15, "get_task_to_wake_up: size(que)=%d(\n", stack_size(que)); flush_log();
  
  move_to_top(que);
  return((ibp_task_t *)get_ele_data(que));
}

//************************************************************************************
// wake_up_task - Wakes up a task and adds it back into the list for monitoring
//************************************************************************************

void wake_up_task(ibp_task_t *task, int ns_mode)
{
  if (task == NULL) return;
  
  lock_task(task);
  if (task->submitted == 1) {
     unlock_task(task);
     return;
  }

//  set_netstream_state(task->net, task->ns, ns_mode);

  if (ns_mode == NS_STATE_ONGOING_READ) {    
     log_printf(15, "wake_up_task: Waking up ns=%d for READ\n", task->ns->id);
//     release_connection(task->net, task->ns);
//     submit_single_command(global_job, task, 0);      
  } else {
     log_printf(15, "wake_up_task: Waking up ns=%d for WRITE\n", task->ns->id);
//     submit_single_command(global_job, task, 0);      
  }

  unlock_task(task);

}

//************************************************************************************
//  read_from_disk - Reads data from the disk buffer and transfers it.  Return values are
//    -1 -- Dead connection
//     0 -- Transfered as much as data as possible
//     1 -- Completed provided task
//************************************************************************************

int read_from_disk(ibp_task_t *task, Allocation_t *a, off_t *left, Resource_t *res)
{
  NetStream_t *ns = task->ns;
  int bufsize = 2*1048576;
  int nbytes, ntotal, nwrite, shortwrite, pos, nleft, err;
  int bpos, btotal, bleft;
  char buffer[bufsize];
  Net_timeout_t dt;
  int task_status;

  nbytes = a->size;
  log_printf(10, "read_from_disk: ns=%d id=" LU " a.size=%d a.r_pos=" LU " len=" OT "\n", task->ns->id, a->id, nbytes, a->r_pos, *left);
flush_log();
  if (*left == 0) return(1);  //** Nothing to do

  task_status = 0;  
  set_net_timeout(&dt, 1, 0);  //** set the max time we'll wait for data  
  
  shortwrite = 0;        
  pos = a->r_pos;  //** Get the disk start pos from the allocation
  nleft = (*left > a->size) ? a->size : *left;
  if (a->size <= 0) {
     return(0);   //** Nothing in the buffer to send
  } 

  ntotal = 0;
  do {
     shortwrite = 0;

     nbytes = (nleft < bufsize) ? nleft : bufsize;
     if (nbytes > a->size) nbytes = a->size;
     if ((pos+nbytes) > a->max_size) nbytes = a->max_size - pos;   //**if FIFO ro CIRQ the start pos may noe be 0
     err = read_allocation_with_id(res, a->id, pos, nbytes, buffer);
     if (err != 0) {
        char tmp[128];
        log_printf(0, "read_disk: Error with read_allocation(%s, " LU ", %d, %d, buffer) = %d\n",
             rid2str(&(res->rid), tmp, sizeof(tmp)), a->id, pos, nbytes, err); 
        shortwrite = 100;
        nwrite = err;
     }

     bpos = 0; btotal = 0; bleft = nbytes;
     do {  //** Loop until data is completely sent or blocked
        nwrite = write_netstream(task->ns, &(buffer[bpos]), bleft, dt);
        if (nwrite > 0) {
           btotal += nwrite;
           bpos += nwrite;
           bleft -= nwrite;
           task->stat.nbytes += nwrite;
        } else if (nwrite == 0) {
           shortwrite++;
        } else {
           shortwrite = 100;  //** closed connection
        }

        log_printf(15, "read_from_disk: id=" LU " nbytes = %d -- bpos=%d, pos=%d nleft=%d, ntotal=%d, nwrite=%d * shortwrite=%d ns=%d\n", 
             a->id, nbytes, bpos, pos, bleft, btotal, nwrite, shortwrite, task->ns->id);
     } while ((btotal < nbytes) && (shortwrite < 3));

     //** Update totals
     pos += btotal;   
     nleft -= btotal;
     *left -= btotal;
     a->r_pos += btotal;

     //**handle buf wrap arounds
     if (a->type != IBP_BYTEARRAY) {
        a->size -= btotal;
        if (a->r_pos > a->max_size) a->r_pos = 0; //** flip to the beginning
        pos = a->r_pos;
     }

     log_printf(15, "read_from_disk: nleft=%d nwrite=%d pos=%d shortwrite=%d\n", nleft, nwrite, pos, shortwrite);
  } while ((nleft > 0) && (shortwrite < 3));


  if ((nwrite < 0) || (shortwrite >= 100)) {        //** Dead connection
     log_printf(10, "read_from_disk: Socket error with ns=%dfrom closing connection\n", ns->id);
     task_status = -1;
  } else {           //** short write
//     r->pos = pos;

     if (*left == 0) {   //** Finished data transfer
        log_printf(10, "read_from_disk: Completed transfer! ns=%d tid=" LU "\n", task->ns->id, task->tid);
        task_status = 1;
     } else {
        log_printf(10, "read_from_disk: returning ns=%d back to caller.  short read.  tid=" LU "\n", task->ns->id, task->tid);
        task_status = 0;
     }
  }


  return(task_status);
}

//************************************************************************************
//  write_to_disk - Writes data to the disk buffer and transfers it.  Return values are
//    -1 -- Dead connection
//     0 -- Transfered as much as data as possible
//     1 -- Completed provided task
//     2 -- Buffer full so block
//************************************************************************************

int write_to_disk(ibp_task_t *task, Allocation_t *a, off_t *left, Resource_t *res)
{
  int bufsize = 2*1048576;
  int nbytes, ntotal, nread, shortread, pos, nleft, err;
  char buffer[bufsize];
  Net_timeout_t dt;
  int task_status;
  int bpos, ncurrread;

  log_printf(10, "write_to_disk: id=" LU " ns=%d\n", a->id, task->ns->id);

  if (*left == 0) return(1);   //** Nothing to do

  task_status = 0;  
  set_net_timeout(&dt, 1, 0);  //** set the max time we'll wait for data  

  shortread = 0;
  if ((a->size == 0) && (a->type != IBP_BYTEARRAY)) { a->r_pos = a->w_pos = 0; }

  pos = a->w_pos;
  nleft = *left;
  if (a->type == IBP_BYTEARRAY) {
     nleft = *left;   //** Already validated range in calling routine
  } else {
     nleft = (*left > (a->max_size - a->size)) ? (a->max_size - a->size) : *left;
  }

  ntotal = 0;
  debug_printf(10, "write_to_disk(BA): start.... id=" LU " * max_size=" LU " * curr_size=" LU " * max_transfer=%d pos=%d left=" OT " ns=%d\n", 
         a->id, a->max_size, a->size, nleft, pos, *left, task->ns->id);
  
  if (nleft == 0) {  //** no space to store anything
     return(0);
  } 

  do {
     bpos = 0;
     nbytes = (nleft < bufsize) ? nleft : bufsize;
     do {
        ncurrread = read_netstream(task->ns, &(buffer[bpos]), nbytes, dt);
        if (ncurrread > 0) {
            nbytes -= ncurrread;
            bpos += ncurrread;
            task->stat.nbytes += ncurrread;
        } else if (ncurrread == 0) {
            shortread++;
        } else {
            shortread = 100;
        }
     } while ((nbytes > 0) && (shortread < 3));        
     nread = bpos;

     if (nread > 0) {
         err = write_allocation_with_id(res, a->id, pos, nread, buffer);
         if (err != 0) {
            char tmp[128];
            log_printf(0, "write_to_disk: Error with write_allocation(%sZ, " LU ", %d, %d, buffer) = %d  tid=" LU "\n",
                    rid2str(&(res->rid), tmp, sizeof(tmp)), a->id, pos, nread, err, task->tid); 
            shortread = 100;
            nread = err;
         }

         ntotal += nread;
         pos += nread;
         nleft -= nread;
      } else {
         shortread++;
      }

     log_printf(15, "write_to_disk: id=" LU " left=" LU " -- pos=%d, nleft=%d, ntotal=%d, nread=%d ns=%d\n", 
              a->id, *left, pos, nleft, ntotal, nread, task->ns->id);
  } while ((ntotal < nleft) && (shortread < 3));

  if (shortread >= 100) {        //** Dead connection
     task_status = -1;
  } else {           //** short write
     if (a->type == IBP_BYTEARRAY) {
        if (pos > a->size) a->size = pos;
     } else {
       a->size += ntotal;
     }
     *left -= ntotal;
//     w->pos = pos;
     a->w_pos = pos;
//     task_status = (a->size == a->max_size) ? 2 : 0;  
     task_status = 0;

     if (*left == 0) {   //** Finished data transfer
        log_printf(10, "write_to_disk: Completed transfer! ns=%d tid=" LU " a.size=" LU " a.w_pos=" LU "\n", task->ns->id, task->tid, a->size, a->w_pos);
        task_status = 1;
     } else {
        log_printf(10, "write_to_disk: task_status=%d returning ns=%d back to caller.  a.size=" LU " short read.  tid=" LU "\n", task_status, task->ns->id, a->size, task->tid);
     }
  }


  return(task_status);
}  


