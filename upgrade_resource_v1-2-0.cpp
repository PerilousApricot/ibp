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
#include "allocation_v1-1-6.h"
#include "fmttypes.h"
#include "log.h"

#define _RESOURCE_STATE_BAD  1
#define _RESOURCE_STATE_GOOD 0

int parse_resource(Resource_t *res, GKeyFile *keyfile, char *group);
int write_usage_file(char *fname, Resource_t *r, int state);

//******************************************************************
// migrate_allocation_v120 - Migrates the data to the v120 format
//******************************************************************

void migrate_allocation_v120(Allocation_v116_t *olda, Allocation_t *a)
{
 //** Clear the new record ***
  memset(a, 0, sizeof(Allocation_t));

  //** Migrate existing data ***
  a->expiration = olda->expiration;
  a->id = olda->id;
  a->size = olda->size;
  a->max_size = olda->max_size;
  a->r_pos = olda->r_pos;
  a->w_pos = olda->w_pos;
  a->type = olda->type;
  a->reliability = olda->reliability;
  a->read_refcount = olda->read_refcount;
  a->write_refcount = olda->write_refcount - 1;  //** The old alloc defaulted this to 1
  memcpy(a->caps[0].v, olda->caps[0].v, CAP_SIZE+1);
  memcpy(a->caps[1].v, olda->caps[1].v, CAP_SIZE+1);
  memcpy(a->caps[2].v, olda->caps[2].v, CAP_SIZE+1);

  //** Init the new fields as needed **
  a->is_alias = 0;   //** Technically don't need to to this cause of the memset but it's a reminder
}

//******************************************************************
// upgrade_get_next_v116 - Gets the next allocation from the resource
//******************************************************************

int upgrade_get_next_v116(Resource_t *r, void *iter, Allocation_v116_t *a)
{
  int err;
  osd_id_t id;

  err = 1;
  while (err != sizeof(Allocation_v116_t)) {
     err = r->fs->iterator_next(iter, &id);
     if (err != 0) return(err);
     err = r->fs->read(id, 0, sizeof(Allocation_v116_t), a);
     if (err != sizeof(Allocation_v116_t)) {
         log_printf(0, "upgrade_get_next_v116: rid=%s Can't read id=" LU ".  Skipping...\n", r->name, id);
     }
  }

  return(0);
}


//******************************************************************
// import_resource_v120 - Upgrades the depot from a pre v1.2.0 depot
//     to v1.2.0
//******************************************************************

int import_resource_v120(Resource_t *r, DB_env_t *dbenv, int remove_expired, int truncate_expiration)
{
   char fname[2048];
   int i, nbuff, cnt, ecnt, err;
   void *iter;
   Allocation_t *a;
   Allocation_v116_t olda;
   const int a_size = 1024;
   Allocation_t alist[a_size];
   time_t t, t1, t2, max_expiration;
   osd_id_t id;
   char print_time[128];
 
   t = time(NULL);
   ctime_r(&t, print_time); print_time[strlen(print_time)-1] = '\0';
   log_printf(0, "import_resource(rid=%s):  Rebuilding Resource rid=%s.  Starting at %s  remove_expired=%d truncate_expiration=%d\n", 
        r->name, r->name, print_time, remove_expired, truncate_expiration);

   //** Mount it
   GError *error=NULL;
   GKeyFile *kfd;
   GKeyFileFlags flags;
   
   flags = G_KEY_FILE_NONE;

   //* Load the GKeyFile from keyfile.conf or return. 
   assert((kfd = g_key_file_new()) != NULL);
   snprintf(fname, 2048, "%s/config", r->location);
   if (!g_key_file_load_from_file (kfd, fname, flags, &error)) {
     g_error (error->message);
     abort();
   }
   
   mount_db_generic(kfd, dbenv, &(r->db), 2);   //**Mount the DBes  

   g_key_file_free(kfd);

   //*** Now we have to fill it ***
   r->used_space[0] = 0; r->used_space[1] = 0;
   r->n_allocs = 0; r->n_alias = 0;

   t = time(NULL);
   cnt = 0;
   ecnt = 0;
   nbuff = 0;

   max_expiration = time(0) + r->max_duration;
      
   a = &(alist[nbuff]);

   iter = r->fs->new_iterator();
   err = upgrade_get_next_v116(r, iter, &olda);
   while (err == 0) {
      migrate_allocation_v120(&olda, a);   //** Migrate the data

      id = a->id;
      if (((a->expiration < t) && (remove_expired == 1)) || ((a->read_refcount == 0) && (a->write_refcount == 0))) {
         log_printf(5, "import_resource_v120(rid=%s): Removing expired record with id: " LU "\n", r->name, id);
         if ((err = r->fs->remove(id)) != 0) {
            log_printf(0, "import_resource_v120(rid=%s): Error Removing id " LU "  Error=%d\n", r->name, id, err);
         }
       
         ecnt++;
      } else {  //*** Adding the record
         if ((a->expiration > max_expiration) && (truncate_expiration == 1)) {         
            t1 = a->expiration; t2 = max_expiration;
            log_printf(5, "import_resource_v120(rid=%s): Adding record %d with id: " LU " but truncating expiration curr:" TT " * new:" TT "\n",r->name, cnt, id, t1, t2);
            a->expiration = max_expiration;
         }

         log_printf(5, "import_resource_v120(rid=%s): Adding record %d with id: " LU "\n",r->name, cnt, id);
         r->used_space[a->reliability] += a->max_size;
         
         nbuff++;
         cnt++;
     }

     //**** Buffer is full so update the DB ****
     if (nbuff >= a_size) {
        for (i=0; i<nbuff; i++) {
           r->fs->write(alist[i].id, 0, sizeof(Allocation_t), &(alist[i])); 
           if ((err = _put_alloc_db(&(r->db), &(alist[i]))) != 0) {
              log_printf(0, "import_resource_v120(rid=%s): Error Adding id " LU " to DB Error=%d\n", r->name, alist[i].id, err);
           }
        }
      
        nbuff = 0;
     }

     a = &(alist[nbuff]);
     err = upgrade_get_next_v116(r, iter, &olda);  //** Get the next record
   }

   //**** Push whatever is left into the DB ****
   for (i=0; i<nbuff; i++) {
     r->fs->write(alist[i].id, 0, sizeof(Allocation_t), &(alist[i])); 
     if ((err = _put_alloc_db(&(r->db), &(alist[i]))) != 0) {
           log_printf(0, "import_resource_v120(rid=%s): Error Adding id " LU " to DB Error=%d\n", r->name, alist[i].id, err);
     }
     if ((err = blank_history(r, alist[i].id)) != 0) {
           log_printf(0, "import_resource_v120(rid=%s): Error Adding history for id " LU " err=%d\n", r->name, alist[i].id, err);
     }
   }
   
   r->fs->destroy_iterator(iter);

   r->n_allocs = cnt;

   t = time(NULL);
   log_printf(0, "\nimport_resource_v120(rid=%s): %d allocations added\n", r->name, cnt);
   log_printf(0, "import_resource_v120(rid=%s): %d alloctaions removed\n", r->name, ecnt);
   Rsize_t mb;
   mb = r->used_space[ALLOC_SOFT]/1024/1024; log_printf(0, "\n#(rid=%s) soft_used = " LU "\n", r->name, mb);
   mb = r->used_space[ALLOC_HARD]/1024/1024; log_printf(0, "#(rid=%s) hard_used = " LU "\n", r->name, mb);

   log_printf(0, "\nimport_resource_v120(rid=%s): Finished Rebuilding RID %s at %s\n", r->name, r->name, ctime(&t));
   flush_log();

   return(0);
}

//******************************************************************
//  upgrde_resource_v120 - Performs the rsource upgrade to v120
//******************************************************************

int upgrade_resource_v120(Resource_t *res, GKeyFile *keyfile, char *group, DB_env_t *dbenv)
{
   int err;
   
   //*** Load the resource data ***
   assert(parse_resource(res, keyfile, group) == 0);
   res->pending = 0;

   log_printf(15, "upgrade_resource_v120: rid=%s\n", res->name);

   res->lazy_allocate = 0;

   //*** Now mount the device ***
   if (strcmp("dir", res->device_type)==0) {
      DIR *dir;
      assert((dir = opendir(res->device)) != NULL);
      closedir(dir);

      assert((res->fs = new osd_fs(res->device)) != NULL);
   } else {
      assert((res->fs = new osd_ebofs(res->device, 0)) != NULL);
   }
   
   //*** and also mount the DB ***
   GError *error=NULL;
   GKeyFile *kfd;
   GKeyFileFlags flags;
   char fname[2048];
   
   flags = G_KEY_FILE_NONE;

   /* Load the GKeyFile from keyfile.conf or return. */
   assert((kfd = g_key_file_new()) != NULL);
   snprintf(fname, sizeof(fname), "%s/config", res->location);
   if (!g_key_file_load_from_file (kfd, fname, flags, &error))
   {
     g_error (error->message);
     abort();
   }

   //** Rebuild the DB  **
   snprintf(fname, sizeof(fname), "%s/usage", res->location); 
   err = import_resource_v120(res, dbenv, 1, 1);

   if (err != 0) return(err);

   //** Init the lock **
   pthread_mutex_init(&(res->mutex), NULL);
   pthread_mutex_unlock(&(res->mutex));
  
   //*** clean up ***
   g_key_file_free(kfd);

   //** Update the usage **
   write_usage_file(fname, res, _RESOURCE_STATE_GOOD);    //**Mark it as good

   umount_resource(res);

   return(0);
}

