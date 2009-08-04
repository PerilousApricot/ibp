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

#include "resource.h"
#include "allocation.h"
#include "cap_timestamp.h"
#include "fmttypes.h"
#include "log.h"

//** These are just dummies and are really only needed for a DB implementation
int create_history_table(Resource_t *r) { return(0); }
int mount_history_table(Resource_t *r) { return(0); }
void umount_history_table(Resource_t *r) { return; }


//****************************************************************************
// get_history_table - Retreives the history table fro the provided allocation
//    *** NOTE:  Calling application is responsible for locking!!!! ****
//****************************************************************************

int get_history_table(Resource_t *r, osd_id_t id, Allocation_history_t *h)
{
  int n;

  n = r->fs->read(id, sizeof(Allocation_t), sizeof(Allocation_history_t), h);
  if (n == sizeof(Allocation_history_t)) n = 0;

//log_printf(15, "get_history: r=%s id=" LU " h.id=" LU " write_slot=%d\n", r->name, id, h->id, h->write_slot);

  return(n);
}

//****************************************************************************
// put_history_table - Stores the history table for the given allocation
//    *** NOTE:  Calling application is responsible for locking!!!! ****
//****************************************************************************

int put_history_table(Resource_t *r, osd_id_t id, Allocation_history_t *h)
{
  int n;

  if (id != h->id) {
     if (h->id == 0) { 
       h->id = id;
     } else {
       log_printf(0, " put_history_table: h->id=" LU" differs from given id=" LU "\n", h->id, id);
     }
  }

  n = r->fs->write(id, sizeof(Allocation_t), sizeof(Allocation_history_t), h);
  if (n == sizeof(Allocation_history_t)) n = 0;

//log_printf(15, "put_history: r=%s id=" LU " h.id=" LU " write_slot=%d\n", r->name, id, h->id, h->write_slot);

  return(n);
}

//****************************************************************************
// blank_history - Writes a blank history record
//****************************************************************************

int blank_history(Resource_t *r, osd_id_t id)
{
   Allocation_history_t h;
   int err;

   if ((r->enable_read_history==0) && (r->enable_write_history==0) && (r->enable_manage_history==0)) return(0);

   memset(&h, 0, sizeof(h));
   h.id = id;

   err = put_history_table(r, id, &h);
   if (err != 0) {
      log_printf(0, "blank_history: Error putting history for res=%s id=" LU " err=%d\n", r->name, id, err);
      return(err);
   }

//Allocation_history_t h1;
//get_history_table(r, id, &h1);
//log_printf(0, "blank_history: r=%s id=" LU " h.id=" LU " write_slot=%d\n", r->name, id, h1.id, h1.write_slot);
//flush_log();

   return(0);
}


//****************************************************************************
// update_read_history - Updates the read history table for the allocation
//****************************************************************************

void update_read_history(Resource_t *r, osd_id_t id, Allocation_address_t *add, uint64_t offset, uint64_t size, osd_id_t pid)
{
   Allocation_history_t h;
   int err;

   if (r->enable_read_history == 0) return;

   err = get_history_table(r, id, &h);
   if (err != 0) {
      log_printf(0, "update_read_history: Error getting history for res=%s id=" LU " err=%d\n", r->name, id, err);
      return;
   }

   set_read_timestamp(&h, add, offset, size, pid);

   err = put_history_table(r, id, &h);
   if (err != 0) {
      log_printf(0, "update_read_history: Error putting history for res=%s id=" LU " err=%d\n", r->name, id, err);
      return;
   }

}

//****************************************************************************
// update_write_history - Updates the write history table for the allocation
//****************************************************************************

void update_write_history(Resource_t *r, osd_id_t id, Allocation_address_t *add, uint64_t offset, uint64_t size, osd_id_t pid)
{
   Allocation_history_t h;
   int err;

   if (r->enable_write_history == 0) return;

   err = get_history_table(r, id, &h);
   if (err != 0) {
      log_printf(0, "update_write_history: Error getting history for res=%s id=" LU " err=%d\n", r->name, id, err);
      return;
   }
//log_printf(0, "update_write_history: r=%s id=" LU " h.id=" LU " write_slot=%d\n", r->name, id, h.id, h.write_slot);
//flush_log();
   set_write_timestamp(&h, add, offset, size, pid);

   err = put_history_table(r, id, &h);
   if (err != 0) {
      log_printf(0, "update_write_history: Error putting history for res=%s id=" LU " err=%d\n", r->name, id, err);
      return;
   }

}

//****************************************************************************
// update_manage_history - Updates the manage history table for the allocation
//****************************************************************************

void update_manage_history(Resource_t *r, osd_id_t id, Allocation_address_t *add, int cmd, int subcmd, int reliability, uint32_t expiration, uint64_t size, osd_id_t pid)
{
   Allocation_history_t h;
   int err;

   if (r->enable_manage_history == 0) return;

   err = get_history_table(r, id, &h);
   if (err != 0) {
      log_printf(0, "update_manage_history: Error getting history for res=%s id=" LU " err=%d\n", r->name, id, err);
      return;
   }

   set_manage_timestamp(&h, add, cmd, subcmd, reliability, expiration, size, pid);

   err = put_history_table(r, id, &h);
   if (err != 0) {
      log_printf(0, "update_manage_history: Error putting history for res=%s id=" LU " err=%d\n", r->name, id, err);
      return;
   }

}



