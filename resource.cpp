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

//***************************************************************************
//***************************************************************************

#include <sys/socket.h>
#include <netinet/in.h>
#include <arpa/inet.h>
#include <sys/types.h>
#include <sys/stat.h>
#include <stdio.h>
#include <string.h>
#include <glib.h>
#include "resource.h"
#include "log.h"
#include "debug.h"
#include "fmttypes.h"

#define _RESOURCE_BUF_SIZE 1048576
char *_blanks[_RESOURCE_BUF_SIZE];

#define _RESOURCE_STATE_GOOD 0
#define _RESOURCE_STATE_BAD  1

typedef struct {    //*** Used to store usage info on a resource to keep from having to rescan the DB
 int version;        
 int state;
 Rsize_t used_space[2];
 Rsize_t n_allocs;
 Rsize_t n_alias;
} Usage_file_t;

typedef struct {  //** Internal resource iterator
  int mode;
  DB_iterator_t *dbi;
  void *fsi;
  Resource_t *r;
  Allocation_t a;
}  res_iterator_t;

void *resource_cleanup_thread(void *data);
int _remove_allocation_for_make_free(Resource_t *r, Allocation_t *alloc, DB_iterator_t *it);

//***************************************************************************
//  rid2str - Converts the RID to a printable format
//***************************************************************************

char *rid2str(RID_t *rid, char *name, int n_size)
{
  name[0] = '\0';
  snprintf(name, n_size, "%d", rid->id);
  return(name);

//  return((char *)inet_ntop(AF_INET6, (void *)&(rid->rid), name, n_size));
}

//***************************************************************************
//  str2rid - Converts the string form of the RID to the network version
//            Could be a single integer.  Is so we add "::" to it 
//***************************************************************************

int str2rid(const char *name, RID_t *rid)
{
  int err;

  err = sscanf(name, "%d", &(rid->id));

//*********************
//  err = inet_pton(AF_INET6, name, (void *)&(rid->rid));
//  if (err <= 0) {  //** Single number so right-shift the numbers by adding ::
//     char token[256];
//     snprintf(token, sizeof(token), "::%s", name);
//     token[sizeof(token)-1] = '\0';
//     err = inet_pton(AF_INET6, token, (void *)&(rid->rid));
//  }
//*********************
  
  if (err == 1) {
     return(0);
  } else {        
    return(1);
  }
}

//***************************************************************************
// empty_rid - Creates a blank RID
//***************************************************************************

void empty_rid(RID_t *rid)
{
  rid->id = 0;

//  memset(&(rid->rid), 0, sizeof(rid->rid));
}

//***************************************************************************
// is_empty_rid - Determines if an RID is blank
//***************************************************************************

int is_empty_rid(RID_t *rid)
{
   return(rid->id == 0);
   
//   RID_t blank;
//   memset(&(blank.rid), 0, sizeof(blank.rid));
//   return(memcmp(&(blank.rid), &(rid->rid), sizeof(blank.rid)) == 0);
}

//***************************************************************************
// compare_rid - Similar to strcmp but for RID's
//***************************************************************************

int compare_rid(RID_t *rid1, RID_t *rid2)
{
  int i;

  if (rid1->id > rid2->id) {
    i = 1;
  } else if (rid1->id == rid2->id) {
    i = 0;
  } else {
    i = -1;
  }

  return(i);     
}

//***************************************************************************
// write_usage_file - Writes the usage file
//***************************************************************************

int write_usage_file(char *fname, Resource_t *r, int state)
{
   FILE *fd;
   Usage_file_t usage;
   log_printf(10, "write_usage_file: fname=%s\n", fname);
   
   assert((fd = fopen(fname, "w")) != NULL);
   usage.version = _RESOURCE_VERSION;
   usage.used_space[0] = r->used_space[0];
   usage.used_space[1] = r->used_space[1];
   usage.n_allocs = r->n_allocs;
   usage.n_alias = r->n_alias;
   usage.state = state;
   fwrite((void *)&usage, sizeof(usage), 1, fd);
   fclose(fd);

    Rsize_t mb;
    log_printf(10, "write_usage_file: rid=%s\n",r->name); 
    mb = r->used_space[ALLOC_SOFT]/1024/1024; log_printf(10, "\n#soft_used = " LU " mb\n", mb);
    mb = r->used_space[ALLOC_HARD]/1024/1024; log_printf(10, "#hard_used = " LU " mb\n", mb);
    mb = r->used_space[ALLOC_SOFT]; log_printf(10, "#soft_used = " LU " b\n", mb);
    mb = r->used_space[ALLOC_HARD]; log_printf(10, "#hard_used = " LU " b\n", mb);
    log_printf(10, "#n_allocations = " LU "\n", r->n_allocs);
    log_printf(10, "#n_alias = " LU "\n", r->n_alias);

   return(0);
}

//***************************************************************************
// read_usage_file - reads the usage file
//***************************************************************************

int read_usage_file(char *fname, Resource_t *r)
{
   FILE *fd;
   Usage_file_t usage;
   int n;
   int docalc = 1;

   log_printf(10, "read_usage_file: fname=%s\n", fname);
   fd = fopen(fname, "r");
   if (fd == NULL) {
      log_printf(10, "read_usage_file: File not round %s\n", fname);
      return(docalc);
   }

   n = fread((void *)&usage, sizeof(usage), 1, fd);
   fclose(fd);
   if (n != 1) {
      log_printf(10, "read_usage_file: Can't read whole record! file: %s\n", fname);
      return(docalc);      
   }
   
   if (usage.version == _RESOURCE_VERSION) {
      if (usage.state == _RESOURCE_STATE_GOOD) {
         docalc = 0;
         r->used_space[0] = usage.used_space[0];
         r->used_space[1] = usage.used_space[1];
         r->n_allocs = usage.n_allocs;
         r->n_alias = usage.n_alias;

         Rsize_t mb;
         log_printf(10, "read_usage_file: rid=%s\n",r->name); 
         mb = r->used_space[ALLOC_SOFT]/1024/1024; log_printf(10, "\n#soft_used = " LU "\n", mb);
         mb = r->used_space[ALLOC_HARD]/1024/1024; log_printf(10, "#hard_used = " LU "\n", mb);
         log_printf(10, "#n_allocations = " LU "\n", r->n_allocs);
         log_printf(10, "#n_alias = " LU "\n", r->n_alias);
      }
   }

//docalc=1;
   return(docalc);
 }

//***************************************************************************
// print_resource_usage - Prints the usage stats to the fd
//***************************************************************************

int print_resource_usage(Resource_t *r, FILE *fd)
{
  fprintf(fd, "n_allocs = " LU "\n", r->n_allocs);
  fprintf(fd, "n_alias = " LU "\n", r->n_alias);
  fprintf(fd, "hard_usage = " LU "\n", r->used_space[ALLOC_HARD]);
  fprintf(fd, "soft_usage = " LU "\n", r->used_space[ALLOC_SOFT]);

  print_db(&(r->db), fd);

  return(0);
}

//***************************************************************************
// mkfs_resource - Creates a new resource
//***************************************************************************

int mkfs_resource(RID_t rid, char *dev_type, char *device_name, char *db_location)
{
   FILE *fd;
   int err;
   char rname[256];
   char dname[2048];
   char fname[2048];
   DIR *dir;
   Resource_t res;
   char kgroup[100], name[100];  
   struct statfs stat;

   if (strlen(db_location) > 1900) {
      printf("mkfs_resource: Can't make fname.  location and device too long\n");
      printf("mkfs_resource: DB location: %s\n", db_location);
      printf("mkfs_resource: device: %s\n", device_name);
      abort();
   }

   //*** Fill in defaults for everything ***
   snprintf(kgroup, sizeof(kgroup), "resource %s", rid2str(&rid, rname, 256)); res.keygroup = kgroup;
   res.name = rid2str(&rid, name, sizeof(name));
   res.rid = rid;
   res.max_duration = 31536000;   //default to 1 year
   res.device_type = dev_type;
   res.device = device_name;
   res.location = db_location;
   res.preallocate = 0;
   res.minfree = 100*1024*1024;  //Default to 100MB free
   res.update_alloc = 1;         
   res.enable_read_history = 1;   
   res.enable_write_history = 1;   
   res.enable_manage_history = 1;   
   res.enable_alias_history = 1;   
  
   //**Make the directory for the DB if needed
   snprintf(dname, sizeof(dname), "%s", db_location); 
   res.location = strdup(dname);
   mkdir(dname, S_IRWXU);
   assert((dir = opendir(dname)) != NULL);  //Make sure I can open it
   closedir(dir);
   
   //**Open the Keyfile for storing resource data into
   snprintf(fname, sizeof(fname), "%s/config", dname); 
   assert((fd = fopen(fname, "w")) != NULL);

   //**Create the DB
   assert(mkfs_db(dname, fd) == 0);

   //**Create the device
   if (strcmp("dir", dev_type)==0) {
      DIR *dir;
      assert((dir = opendir(device_name)) != NULL);
      closedir(dir);
      ::statfs(device_name, &stat);
   } else {
      res.fs = new osd_ebofs(device_name, 1);
      res.fs->statfs(&stat);
   }

   res.max_size[ALLOC_HARD] = stat.f_bavail*stat.f_bsize;
   res.max_size[ALLOC_SOFT] = stat.f_bavail*stat.f_bsize;
   res.max_size[ALLOC_TOTAL] = stat.f_bavail*stat.f_bsize;
   res.used_space[ALLOC_HARD] = 0; res.used_space[ALLOC_SOFT] = 0;
   res.n_allocs = 0; res.n_alias = 0;

   err = create_history_table(&res);
   if (err != 0) {
      printf("mkfs_resource: Can't create the history table.  err=%d\n", err);
      abort();
   }

   //*** Print everything out to the screen for the user to use ***
   print_resource(&res, stdout);   
   
   fclose(fd); //Close the ini file

   return(0);
}

//***************************************************************************
// rebuild_remove_iter - Removes the current record
//***************************************************************************

int rebuild_remove_iter(res_iterator_t *ri)
{
  int err = 0;
  
  if (ri->mode == 1) {
     err = remove_alloc_iter_db(ri->dbi);          
  }

  if (err == 0) {  
     err = ri->r->fs->remove(ri->a.id);
//log_printf(10, "rebuild_put_iter: id=" LU " is_alias=%d\n", ri->a.id, ri->a.is_alias);
//     if (ri->a.is_alias == 0) err = ri->r->fs->remove(ri->a.id);
  }

  return(err);
}

//***************************************************************************
// rebuild_modify_iter - Modifies the current record
//    IF the mode == 2 then the rebuild app buffers the allocations and writes
//    them in bulk using rebuild_put_alloc.  In this case I do nothing
//***************************************************************************

int rebuild_modify_iter(res_iterator_t *ri, Allocation_t *a)
{
  int err = 0;
  
  if (ri->mode == 1) { 
     err = modify_alloc_iter_db(ri->dbi, a);          
  }

  return(err);
}


//***************************************************************************
// rebuild_put_iter - Stores the current record
//    If mode == 1 then nothing needs to be done.  Otherwise if mode == 2/3 then
//    I need to 1st set the a.size to the size of the file minus the header
//    before storing it in the DB
//***************************************************************************

int rebuild_put_iter(res_iterator_t *ri, Allocation_t *a)
{
  int err = 0;

  if (ri->mode != 1) {
     a->size = ri->r->fs->size(a->id) - ALLOC_HEADER;
log_printf(10, "rebuild_put_iter: id=" LU " size=" LU "\n", a->id, a->size);
     err = _put_alloc_db(&(ri->r->db), a);          
  }

  return(err);
}


//***************************************************************************
// rebuild_begin - Creates the rebuild iterator
//***************************************************************************

res_iterator_t *rebuild_begin(Resource_t *r, int wipe_clean)
{
   res_iterator_t *ri = (res_iterator_t *)malloc(sizeof(res_iterator_t));
   assert(ri != NULL);

   dbr_lock(&(r->db));

   ri->mode = wipe_clean;
   ri->fsi = NULL;
   ri->dbi = NULL;
   ri->r = r;

   if (wipe_clean == 1) {
      ri->dbi = id_iterator(&(r->db));
      assert(ri->dbi != NULL);
   } else {
      ri->fsi = r->fs->new_iterator();
      assert(ri->fsi != NULL);
   }

   return(ri);
}

//***************************************************************************
// rebuild_end - Destroys the rebuild iterator
//***************************************************************************

void rebuild_end(res_iterator_t *ri)
{
   if (ri->mode == 1) {
      db_iterator_end(ri->dbi);
   } else {
      ri->r->fs->destroy_iterator(ri->fsi);
   }

   dbr_unlock(&(ri->r->db));

   free(ri);
}

//***************************************************************************
// rebuild_get_next - Retreives the next record for rebuilding
//***************************************************************************

int rebuild_get_next(res_iterator_t *ri, Allocation_t *a)
{
  int err;
  osd_id_t id;


  if (ri->mode != 1) {
      err = ri->r->fs->iterator_next(ri->fsi, &id);
      while (err == 0) {
//         log_printf(15, "rebuild_get_next: r=%s id=" LU " err=%d\n", ri->r->name, id, err);
         err = ri->r->fs->read(id, 0, sizeof(Allocation_t), a);
//         log_printf(15, "rebuild_get_next: r=%s id=" LU " read err=%d sizeof(a)=%d\n", ri->r->name, id, err, sizeof(Allocation_t));
         if (err == 0) { //** Nothing there so delete the filename
            log_printf(0, "rebuild_get_next: rid=%s Empty allocation id=" LU ".  Removing it....\n", ri->r->name, id);
            flush_log();
            err = ri->r->fs->remove(id);
            err = ri->r->fs->iterator_next(ri->fsi, &id);
         } else if (err != sizeof(Allocation_t)) {
            log_printf(0, "rebuild_get_next: rid=%s Can't read id=" LU ".  Skipping...nbytes=%d\n", ri->r->name, id, err);
            flush_log();
            err = ri->r->fs->iterator_next(ri->fsi, &id);
         } else if (id != a->id) {  //** ID mismatch.. throw warning and skip
            log_printf(0, "rebuild_get_next: rid=%s ID mismatch so skipping!!!! fs entry id=" LU ".  a.id=" LU "\n", ri->r->name, id,a->id);
            flush_log();
            err = ri->r->fs->iterator_next(ri->fsi, &id);
        }
      }
  } else {
     err = db_iterator_next(ri->dbi, DB_NEXT, a);
//     log_printf(15, "rebuild_get_next: DB r=%s id=" LU " err=%d\n", ri->r->name, a->id, err);
  }

  ri->a = *a;  //** Keep my copy for mods

  if (err == sizeof(Allocation_t)) err = 0;
  return(err);
}

//***************************************************************************
// rebuild_resource - Rebuilds the resource
//
//  if wipe_clean=1 the ID database is not wiped. Instead it is iterated
//    through to create the secondary indices and also verify the allocation
//    exists on the resource if the size > 0.  This method preserves
//    any blank or unused allocations unlike the next method.
//  If wipe_clean=2 the resource is walked to generate the new DB.  This is
//    significantly slower than wipe_clean=1 for a full depot.
//  if wipe_clean=3 the resource is walked to generate the new DB and all
//    allocations duration are extended to the max.  Even for expired allocations.
//
//    --NOTE:  Any blank allocations will be lost!!! --
//***************************************************************************

int rebuild_resource(Resource_t *r, DB_env_t *env, int remove_expired, int wipe_clean, int truncate_expiration)
{
   char fname[2048];
   int i, nbuff, cnt, ecnt, pcnt, err, estate;
   res_iterator_t *iter;
   Allocation_t *a;
   const int a_size = 1024;
   Allocation_t alist[a_size];
   time_t t, max_expiration, t1, t2;
   osd_id_t id;
   char print_time[128];

   t = time(NULL);
   ctime_r(&t, print_time); print_time[strlen(print_time)-1] = '\0';
   log_printf(0, "rebuild_resource(rid=%s):  Rebuilding Resource rid=%s.  Starting at %s  remove_expired=%d wipe_clean=%d truncate_expiration=%d\n", 
        r->name, r->name, print_time, remove_expired, wipe_clean, truncate_expiration);


   if (wipe_clean == 1) {
      log_printf(0, "rebuild_resource(rid=%s):  wipe_clean == 1 so exiting\n", r->name);
      return(0);
   }

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

   i = wipe_clean;
   if (wipe_clean == 3) i = 2;   
   mount_db_generic(kfd, env, &(r->db), i);   //**Mount the DBes  

   g_key_file_free(kfd);

   //*** Now we have to fill it ***
   r->used_space[0] = 0; r->used_space[1] = 0;
   r->n_allocs = 0;  r->n_alias = 0;

   t = time(NULL);
   if (wipe_clean == 3) t = 0;  //** Nothing gets deleted in this mode

   cnt = 0; pcnt = 0;
   ecnt = 0;
   nbuff = 0;
      
   max_expiration = time(0) + r->max_duration;

   a = &(alist[nbuff]);

   iter = rebuild_begin(r, wipe_clean);
   err = rebuild_get_next(iter, a);
   while (err == 0) {
      id = a->id;
      if (a->expiration < time(NULL)) {
         estate = -1;
      } else {
         estate = (a->expiration > max_expiration) ? 1 : 0;
      }

      if ((a->expiration < t) && (remove_expired == 1)) {
         ecnt++;
         log_printf(5, "rebuild_resource(rid=%s): Removing expired record with id: " LU " * estate: %d (remove count:%d)\n", r->name, id, estate, ecnt);
         if ((err = rebuild_remove_iter(iter)) != 0) {
            log_printf(0, "rebuild_resource(rid=%s): Error Removing id " LU "  from DB Error=%d\n", r->name, id, err);
         }
      } else {         //*** Adding the record
         if (((a->expiration > max_expiration) && (truncate_expiration == 1)) || (wipe_clean == 3)) {
            t1 = a->expiration; t2 = max_expiration;
            log_printf(5, "rebuild_resource(rid=%s, wc=%d): Adding record %d with id: " LU " but truncating expiration curr:" TT " * new:" TT " * estate: %d\n",r->name, wipe_clean, cnt, id, t1, t2, estate);
            a->expiration = max_expiration;
            if ((err = rebuild_modify_iter(iter, a)) != 0) {
                  log_printf(0, "rebuild_resource(rid=%s): Error Adding id " LU " to primary DB Error=%d\n", r->name, a->id, err);
            }
         } else {
           log_printf(5, "rebuild_resource(rid=%s): Adding record %d with id: " LU " * estate: %d\n",r->name, cnt, id, estate);
         }

         r->used_space[a->reliability] += a->max_size;
         
         nbuff++;
         cnt++;
         if (a->is_alias) pcnt++;
     }

     //**** Buffer is full so update the DB ****
     if (nbuff >= a_size) {
        for (i=0; i<nbuff; i++) {
           if ((err = rebuild_put_iter(iter, &(alist[i]))) != 0) {
              log_printf(0, "rebuild_resource(rid=%s): Error Adding id " LU " to DB Error=%d\n", r->name, alist[i].id, err);
           }
        }
      
        nbuff = 0;
     }

     a = &(alist[nbuff]);
     err = rebuild_get_next(iter, a);  //** Get the next record
   }

   //**** Push whatever is left into the DB ****
   for (i=0; i<nbuff; i++) {
     if ((err = rebuild_put_iter(iter, &(alist[i]))) != 0) {
           log_printf(0, "rebuild_resource(rid=%s): Error Adding id " LU " to DB Error=%d\n", r->name, alist[i].id, err);
     }
   }
   
   rebuild_end(iter);

   r->n_allocs = cnt;
   r->n_alias = pcnt;

   t = time(NULL);
   log_printf(0, "\nrebuild_resource(rid=%s): %d allocations added\n", r->name, cnt);
   log_printf(0, "rebuild_resource(rid=%s): %d alias allocations added\n", r->name, pcnt);
   log_printf(0, "rebuild_resource(rid=%s): %d allocations removed\n", r->name, ecnt);
   Rsize_t mb;
   mb = r->used_space[ALLOC_SOFT]/1024/1024; log_printf(0, "#(rid=%s) soft_used = " LU "\n", r->name, mb);
   mb = r->used_space[ALLOC_HARD]/1024/1024; log_printf(0, "#(rid=%s) hard_used = " LU "\n", r->name, mb);

   log_printf(0, "\nrebuild_resource(rid=%s): Finished Rebuilding RID %s at %s\n", r->name, r->name, ctime(&t));
   flush_log();

   return(0);
}

//---------------------------------------------------------------------------

//***************************************************************************
// calc_usage - Cycles through all the records to calcualte the used hard
//    and soft space.  This should only be used if the resource was not 
//    unmounted cleanly.
//***************************************************************************

int calc_usage(Resource_t *r)
{
  DB_iterator_t *dbi;
  Allocation_t a;

  log_printf(15, "calc_usage(rid=%s):  Recalculating usage form scratch\n", r->name);

  r->used_space[0] = 0; r->used_space[1] = 0;
  r->n_allocs = 0;  r->n_alias = 0;

  dbi = id_iterator(&(r->db));
  while (db_iterator_next(dbi, DB_NEXT, &a) == 0) {     
     log_printf(10, "calc_usage(rid=%s): n=" LU " ------------- id=" LU "\n", r->name, r->n_allocs, a.id);
//print_allocation_resource(r, stdout, &a);
     r->used_space[a.reliability] += a.max_size;
     r->n_allocs++;
     if (a.is_alias == 1) r->n_alias++;
  }
  db_iterator_end(dbi);

  log_printf(15, "calc_usage(rid=%s): finished... n_allocs= "LU " n_alias=" LU "\n", r->name, r->n_allocs, r->n_alias);

  return(0);
}

//***************************************************************************
// perform_truncate - Adjusts all allocations to the given max
//    duration.  It will also remove any expired allocations.
//***************************************************************************

int perform_truncate(Resource_t *r)
{
  DB_iterator_t *dbi;
  Allocation_t *a;
  const int a_size = 1024;
  Allocation_t alist[a_size];
  time_t max_expiration, t1, t2;
  int estate, err, cnt, ecnt, nbuff, i;

  log_printf(15, "calc_usage(rid=%s):  Recalculating usage form scratch\n", r->name);

  max_expiration = time(0) + r->max_duration;

  cnt = 0; ecnt = 0; nbuff = 0;
  dbi = id_iterator(&(r->db));
  a = &(alist[nbuff]);
  while (db_iterator_next(dbi, DB_NEXT, a) == 0) {
      if (a->expiration < time(NULL)) {
         estate = -1;
      } else {
         estate = (a->expiration > max_expiration) ? 1 : 0;
      }

      switch (estate) {
        case -1:     //** Expired allocation
           ecnt++;
           log_printf(5, "perform_truncate(rid=%s): Remove cnt=%d  id=" LU " * estate: %d\n",r->name, cnt, a->id,  estate);
           err = _remove_allocation_for_make_free(r, a, dbi);
           break;
        case 0:      //** Ok allocation
           cnt++;
           log_printf(5, "perform_truncate(rid=%s): cnt=%d  id=" LU " * estate: %d\n",r->name, cnt, a->id,  estate);
           break;
        case 1:      //** Truncate expiration
           cnt++;
           t1 = a->expiration;
           a->expiration = time(0) + r->max_duration;
           t2 = a->expiration;
           log_printf(5, "perform_truncate(rid=%s): Truncating duration for record %d with id: " LU " expiration curr:" TT " * new:" TT " * estate: %d\n",r->name, cnt, a->id, t1, t2, estate);
           if ((err = modify_alloc_iter_db(dbi, a)) != 0) {
                  log_printf(0, "perform_truncate(rid=%s): Error modifying id " LU " to primary DB Error=%d\n", r->name, a->id, err);
           } 
           
           if (nbuff >= (a_size-1)) {
              log_printf(5, "perform_truncate(rid=%s): Dumping buffer=%d\n",r->name, nbuff);
              if (r->update_alloc == 1) {
                 for (i=0; i<=nbuff; i++) {
                      a = &(alist[i]);
                    if (a->is_alias == 0) {
                       r->fs->write(a->id, 0, sizeof(Allocation_t), a);
                    } else if (r->enable_alias_history) {
                       r->fs->write(a->id, 0, sizeof(Allocation_t), a);
                    }
                 }
              }
      
              nbuff = 0;
           } else {
              nbuff++;
           }

           a = &(alist[nbuff]);
           break;
      }      
  }
  db_iterator_end(dbi);

  if (nbuff > 0) {
     log_printf(5, "perform_truncate(rid=%s): Dumping buffer=%d\n",r->name, nbuff);
     if (r->update_alloc == 1) {
        for (i=0; i<nbuff; i++) {
           a = &(alist[i]);
           if (a->is_alias == 0) {
              r->fs->write(a->id, 0, sizeof(Allocation_t), a);
           } else if (r->enable_alias_history) {
              r->fs->write(a->id, 0, sizeof(Allocation_t), a);
           }
        }
     }
  }

  log_printf(15, "perform_truncate(rid=%s): finished... n_allocs=%d  n_removed=%d\n", r->name, cnt, ecnt);

  return(0);
}

//***************************************************************************
// parse_resource - Parses the resource Keyfile
//***************************************************************************

int parse_resource(Resource_t *res, GKeyFile *keyfile, char *group)
{
   GKeyFileFlags flags;
   char *str;

   flags = G_KEY_FILE_NONE;

   res->keygroup = group;
   res->name = g_key_file_get_string(keyfile, group, "rid", NULL);
   if (res->name == NULL) {
       printf("parse_resource: (%s) Missing resource ID\n",group);
       abort();  
   }
   str2rid(res->name, &(res->rid));

   res->preallocate = g_key_file_get_integer(keyfile, group, "preallocate", NULL);
   res->update_alloc = g_key_file_get_integer(keyfile, group, "update_alloc", NULL);
   res->enable_write_history = g_key_file_get_integer(keyfile, group, "enable_write_history", NULL);
   res->enable_read_history = g_key_file_get_integer(keyfile, group, "enable_read_history", NULL);
   res->enable_manage_history = g_key_file_get_integer(keyfile, group, "enable_manage_history", NULL);
   res->enable_alias_history = g_key_file_get_integer(keyfile, group, "enable_alias_history", NULL);

   res->max_duration = g_key_file_get_integer(keyfile, group, "max_duration", NULL);
   if (res->max_duration == 0) {
      printf("parse_resource: (%s) Missing max duration: %d\n",group, res->max_duration);
      abort();  
   }

   res->device_type = g_key_file_get_string(keyfile, group, "resource_type", NULL);
   if ((strcmp(res->device_type, DEVICE_DIR) != 0) && 
      (strcmp(res->device_type, DEVICE_EBOFS) != 0)) {
      printf("parse_resource: (%s) Invalid device type: %s\n",group, res->device_type);
      abort();  
   }

   res->device = g_key_file_get_string(keyfile, group, "device", NULL);
   if (res->device == NULL) {
      printf("parse_resource: (%s) Missing resource device\n",group);
      abort();  
   }

   res->location = g_key_file_get_string(keyfile, group, "db_location", NULL);
   if (res->location == NULL) {
      printf("parse_resource: (%s) Missing resource device location for DB and other files\n",group);
      abort();  
   }

   str = g_key_file_get_string(keyfile, group, "max_size", NULL);
   if (str == NULL) {
      printf("parse_resource: (%s) Missing max_size for resource\n",group);
      abort();  
   }
   sscanf(str, "" LU "", &(res->max_size[ALLOC_TOTAL]));
   res->max_size[ALLOC_TOTAL] *= 1024*1024;
   free(str);

   str = g_key_file_get_string(keyfile, group, "soft_size", NULL);
   if (str == NULL) {
      printf("parse_resource: (%s) Missing soft_size for resource\n",group);
      abort();  
   }
   sscanf(str, "" LU "", &(res->max_size[ALLOC_SOFT]));
   res->max_size[ALLOC_SOFT] *= 1024*1024;
   free(str);

   str = g_key_file_get_string(keyfile, group, "hard_size", NULL);
   if (str == NULL) {
      printf("parse_resource: (%s) Missing hard_size for resource\n",group);
      abort();  
   }
   sscanf(str, "" LU "", &(res->max_size[ALLOC_HARD]));
   res->max_size[ALLOC_HARD] *= 1024*1024;
   free(str);

   str = g_key_file_get_string(keyfile, group, "minfree_size", NULL);
   if (str == NULL) {
      printf("parse_resource: (%s) Missing minfreesize for resource\n",group);
      abort();  
   }
   sscanf(str, "" LU "", &(res->minfree));
   res->minfree *= 1024*1024;
   free(str);

   return(0);
}

//***************************************************************************
// mount_resource - Mounts a resource for use
//***************************************************************************

int mount_resource(Resource_t *res, GKeyFile *keyfile, char *group, DB_env_t *dbenv, 
   int force_rebuild, int lazy_allocate, int truncate_expiration)
{
   int err;
   memset(_blanks, 0, _RESOURCE_BUF_SIZE);  //** Thisi s done multiple times and it doesn't have to be but is trivial
   
   //*** Load the resource data ***
   assert(parse_resource(res, keyfile, group) == 0);
   res->pending = 0;

   log_printf(15, "mount_resource: rid=%s force_rebuild=%d\n", res->name, force_rebuild);

   res->lazy_allocate = lazy_allocate;

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

   //** Init the lock **
   pthread_mutex_init(&(res->mutex), NULL);
   pthread_mutex_unlock(&(res->mutex));

   pthread_mutex_init(&(res->cleanup_lock), NULL);
   pthread_mutex_unlock(&(res->cleanup_lock));
   pthread_cond_init(&(res->cleanup_cond), NULL);
   res->cleanup_shutdown = -1;

   //** Rebuild the DB or mount it here **
   snprintf(fname, sizeof(fname), "%s/usage", res->location); 
   if (force_rebuild) {
      switch (force_rebuild) {
         case 1: 
            err = mount_db_generic(kfd, dbenv, &(res->db), 1);
            if (err == 0) {
               calc_usage(res);
               if (truncate_expiration == 1) perform_truncate(res);
            }
            break;
         default:     
            err = rebuild_resource(res, dbenv, 1, force_rebuild, truncate_expiration);
      }
   } else if (read_usage_file(fname, res) == 1) {
      err = mount_db_generic(kfd, dbenv, &(res->db), 1);
      calc_usage(res);
//      err = rebuild_resource(res, dbenv, 1, 1, truncate_expiration);
   } else {   
      err = mount_db_generic(kfd, dbenv, &(res->db), 0);
   }

   if (err != 0) return(err);
  
   err = mount_history_table(res);
   if (err != 0) {
      log_printf(0, "mount_resource:  Error mounting history table! res=%s err=%d\n", res->name, err);
      return(err);
   }

   if (err != 0) return(err);

   //*** clean up ***
   g_key_file_free(kfd);

   //** Update the usage **
   write_usage_file(fname, res, _RESOURCE_STATE_BAD);    //**Mark it as dirty

   //** Launch the cleanup thread
//   pthread_create(&(res->cleanup_thread), NULL, resource_cleanup_thread, (void *)res);

   return(err);
}

//***************************************************************************
// umount_resource - Unmounts the given resource
//***************************************************************************

int umount_resource(Resource_t *res)
{
  char fname[2048];
  void *dummy;
  log_printf(15, "umount_resource:  Unmounting resource %s\n", res->name); flush_log();

  //** Kill the cleanup thread
  if (res->cleanup_shutdown == 0) {
     pthread_mutex_lock(&(res->cleanup_lock));
     res->cleanup_shutdown = 1;
     pthread_cond_signal(&(res->cleanup_cond));
     pthread_mutex_unlock(&(res->cleanup_lock));
     pthread_join(res->cleanup_thread, &dummy);
  }

  umount_db(&(res->db));
  umount_history_table(res);

  res->fs->umount();

  snprintf(fname, sizeof(fname), "%s/usage", res->location);    
  write_usage_file(fname, res, _RESOURCE_STATE_GOOD);

  return(0);
}

//---------------------------------------------------------------------------

//***************************************************************************
// print_resource - Prints the resource information out to fd.
//***************************************************************************

int print_resource(Resource_t *res, FILE *fd)
{
   Rsize_t n;

   fprintf(fd, "[%s]\n", res->keygroup);
   fprintf(fd, "rid = %s\n", res->name);
   fprintf(fd, "max_duration = %d\n", res->max_duration);
   fprintf(fd, "resource_type = %s\n", res->device_type);
   fprintf(fd, "device = %s\n", res->device);
   fprintf(fd, "db_location = %s\n", res->location);
   fprintf(fd, "update_alloc = %d\n", res->update_alloc);
   fprintf(fd, "enable_read_history = %d\n", res->enable_read_history);
   fprintf(fd, "enable_write_history = %d\n", res->enable_write_history);
   fprintf(fd, "enable_manage_history = %d\n", res->enable_manage_history);
   fprintf(fd, "enable_alias_history = %d\n", res->enable_alias_history);

   n = res->max_size[ALLOC_TOTAL]/1024/1024; fprintf(fd, "max_size = " LU "\n", n);
   n = res->max_size[ALLOC_SOFT]/1024/1024; fprintf(fd, "soft_size = " LU "\n", n);
   n = res->max_size[ALLOC_HARD]/1024/1024; fprintf(fd, "hard_size = " LU "\n", n);
   n = res->minfree/1024/1024; fprintf(fd, "minfree_size = " LU "\n", n);
   fprintf(fd, "preallocate = %d\n", res->preallocate);
   fprintf(fd, "\n");

   n = res->used_space[ALLOC_SOFT]/1024/1024; fprintf(fd, "#soft_used = " LU " mb\n", n);
   n = res->used_space[ALLOC_HARD]/1024/1024; fprintf(fd, "#hard_used = " LU " mb\n", n);
   n = res->used_space[ALLOC_SOFT]; fprintf(fd, "#soft_used = " LU " b\n", n);
   n = res->used_space[ALLOC_HARD]; fprintf(fd, "#hard_used = " LU " b\n", n);

   fprintf(fd, "#n_allocations = " LU "\n", res->n_allocs);
   fprintf(fd, "#n_alias = " LU "\n", res->n_alias);
   fprintf(fd, "\n");
   return(0);
}

//---------------------------------------------------------------------------

//***************************************************************************
// _remove_allocation - Removes the given allocation without locking!
//***************************************************************************

int _remove_allocation(Resource_t *r, Allocation_t *alloc, bool dolock)
{
   int err;

   log_printf(10, "_remove_allocation:  Removing " LU "\n", alloc->id);

   //** EVen if this fails we want to try and remove the physical allocation
   if ((err = remove_alloc_db(&(r->db), alloc)) != 0) { 
      debug_printf(1, "_remove_allocation:  Error with remove_alloc_db!  Error=%d\n", err); 
//      return(err); 
   }

   log_printf(10, "_remove_allocation:  Removed db entry\n");

   if (r->enable_alias_history == 1) {
      if ((err = r->fs->remove(alloc->id)) != 0) { 
         debug_printf(1, "_remove_allocation:  Error with fs->remove!  Error=%d\n", err); 
      }
   } else if (alloc->is_alias == 0) {
      if ((err = r->fs->remove(alloc->id)) != 0) { 
         debug_printf(1, "_remove_allocation:  Error with fs->remove!  Error=%d\n", err); 
      }
   } else {
        log_printf(15, "_remove_allocation:  a->is_alias=1.  Skipping fs->remove().\n");
   }
   
   debug_printf(10, "_remove_allocation: After remove\n");

   if (dolock) pthread_mutex_lock(&(r->mutex));
log_printf(15, "_remove_allocation: start rel=%d used=" LU " a.max_size=" LU "\n", alloc->reliability, 
   r->used_space[alloc->reliability], alloc->max_size);

   r->n_allocs--;
   if (alloc->is_alias == 0) {
      r->used_space[alloc->reliability] -= alloc->max_size;   //** Upodate the amount of space used
   } else {
      r->n_alias--;
   }

log_printf(15, "_remove_allocation: end rel=%d used=" LU " a.max_size=" LU "\n", alloc->reliability, 
   r->used_space[alloc->reliability], alloc->max_size);

   if (dolock) pthread_mutex_unlock(&(r->mutex));

   debug_printf(10, "_remove_allocation: end of routine\n");
  
   return(0);  
}

//***************************************************************************
// remove_allocation_resource - Removes the given allocation using locking
//       This should be called by end users.
//***************************************************************************

int remove_allocation_resource(Resource_t *r, Allocation_t *alloc)
{
  return(_remove_allocation(r, alloc, true));
}

//***************************************************************************
// merge_allocation_resource - Merges the space for the child allocation, a,
//    into the master(ma).  THe child allocations data is NOT merged and is lost.
//    The child allocation is also deleted.
//***************************************************************************

int merge_allocation_resource(Resource_t *r, Allocation_t *ma, Allocation_t *a)
{
  int err;
  pthread_mutex_lock(&(r->mutex));
  err = _remove_allocation(r, a, false);
  if (err == 0) {
     r->used_space[ma->reliability] += a->max_size;   //** Update the amount of space used
     ma->max_size += a->max_size;
     if (r->update_alloc == 1) r->fs->write(ma->id, 0, sizeof(Allocation_t), ma);
     err = modify_alloc_db(&(r->db), ma);
  }
  pthread_mutex_unlock(&(r->mutex));
  
  return(err);
}

//***************************************************************************
// _remove_allocation_for_make_free - Removes the given allocation without locking and
//     assumes you are using an iterator;
//***************************************************************************

int _remove_allocation_for_make_free(Resource_t *r, Allocation_t *alloc, DB_iterator_t *it)
{
   int err;

   log_printf(10, "_remove_allocation_for_make_free:  Removing " LU " with space " LU "\n", alloc->id, alloc->max_size);

   //** EVen if this fails we want to try and remove the physical allocation
   if ((err = remove_alloc_iter_db(it)) != 0) { 
      debug_printf(1, "_remove_allocation_for_make_free:  Error with remove_alloc_db!  Error=%d\n", err); 
//      return(err); 
   }

   log_printf(10, "_remove_allocation_for_make_free:  Removed db entry\n");

  if (r->enable_alias_history) {
     if ((err = r->fs->remove(alloc->id)) != 0) { 
        debug_printf(1, "_remove_allocation_for_make_free:  Error with fs->remove!  Error=%d\n", err); 
     }
  } else if (alloc->is_alias == 0) {
     if ((err = r->fs->remove(alloc->id)) != 0) { 
        debug_printf(1, "_remove_allocation_for_make_free:  Error with fs->remove!  Error=%d\n", err); 
     }
   } else {
        log_printf(15, "_remove_allocation_for_make_free:  a->is_alias=1.  Skipping fs->remove().\n");
   }

   debug_printf(10, "_remove_allocation_for_make_free: After remove\n");

   if (alloc->is_alias == 0) {
      r->used_space[alloc->reliability] -= alloc->max_size;   //** Upodate the amount of space used
   }
   r->n_allocs--;
   if (alloc->is_alias == 1) r->n_alias--;

   debug_printf(10, "_remove_allocation_for_make_free: end of routine\n");
  
   return(0);  
}


//***************************************************************************
//  blank_space - Fills an allocation with 0's
//***************************************************************************

int blank_space(Resource_t *r, osd_id_t id, off_t off, size_t size) 
{
  int offset, j, err;
  int bcount = size / _RESOURCE_BUF_SIZE;
  int remainder = size - bcount * _RESOURCE_BUF_SIZE;

  debug_code( int ioff = off;)
  debug_printf(10, "blank_space: id=" LU " off=%d size=" ST " bcount = %d rem = %d\n", id, ioff,size,bcount,remainder);
  offset = off;      // Now store the data in chunks
  for (j=0; j<bcount; j++) {
      err = r->fs->write(id, offset, _RESOURCE_BUF_SIZE, _blanks);
      offset = offset + _RESOURCE_BUF_SIZE;
  }
  if (remainder>0) err = r->fs->write(id, offset, remainder, _blanks);  

//  debug_printf(10, "blank_space: err=%d\n", err);
  return(0);
}

//***************************************************************************
// make_free_space_iterator - Frees space up using the given iterator and 
//    time stamp
//***************************************************************************

int make_free_space_iterator(Resource_t *r, DB_iterator_t *dbi, Rsize_t *nbytesleft, time_t timestamp)
{
  int err;
  Allocation_t a;
  bool finished;
  size_t nleft;

  log_printf(10, "make_free_space_iterator: Attempting to free " LU " bytes\n", *nbytesleft);
  nleft = *nbytesleft;
  finished = 0;
  do {
     if ((err = db_iterator_next(dbi, DB_NEXT, &a)) == 0) {
       if (a.is_alias == 0) err = r->fs->read(a.id, 0, sizeof(Allocation_t), &a);

       if (a.expiration < timestamp) {
          if (nleft < a.max_size) {    //** for alias allocations max_size == 0
             nleft = 0;          //
          } else {
             nleft -= a.max_size;            //** Free to delete it
          }

          err = _remove_allocation_for_make_free(r, &a, dbi);
       } else {
          finished = true;                //** Nothing else has expired:(
       }
     } else {
       finished = true;
     }      

     log_code( if (err == 0) { log_printf(10, "make_free_space_iterator: checked id " LU "\n", a.id); } );
  } while ((nleft > 0) && (err == 0) && (!finished));

  *nbytesleft = nleft;

  log_printf(10, "make_free_space_iterator: Completed with err=%d and " LU " bytes left to free\n", err, *nbytesleft);

  if (nleft <= 0) err = 0;

  return(err);
}

//***************************************************************************
// make_space - Creates enough free space on the device for a subsequent
//      allocation
//***************************************************************************

int make_space(Resource_t *r, size_t size, int atype)
{
  struct statfs stat;
  Rsize_t nbytes, needed;
  DB_iterator_t *dbi;
  int err;

//nbytes = 200000000;
  r->fs->statfs(&stat);
  nbytes = stat.f_bavail*stat.f_bsize;

  needed = r->minfree + size + r->pending;  
  log_printf(10, "make_space: nbytes=" LU " * needed=" LU " size=" ST " used=" LU " max=" LU " pending=" LU " type=%d\n", 
          nbytes, needed, size, r->used_space[atype], r->max_size[atype], r->pending, atype);   
//  if (nbytes >= needed) {       //** Check that there is enough physical space free
//     if (r->used_space[atype] + size < r->max_size[atype]) return(0);   //**Plenty of space so return early
//  }


  //*** Need to free up some space ****
  Rsize_t nleft = 0;
  Rsize_t ntotal;

  //*** Are we over quota for the "type" ***
  if (r->used_space[atype] + size > r->max_size[atype]) {   //**Over quota so trim expired no matter what
     nleft = r->used_space[atype] + size - r->max_size[atype];
     log_printf(10, "make_space: Over quota for type %d needed space " LU "\n", atype, nleft);   
  }

  //*** Are we over the aggregate total ***
  ntotal = r->used_space[ALLOC_HARD] + r->used_space[ALLOC_SOFT];
  if (ntotal > r->max_size[ALLOC_TOTAL]) {
     ntotal = ntotal + size - r->max_size[ALLOC_TOTAL];
     if (nleft < ntotal) nleft = ntotal;
  }

  //*** Check if minfree is Ok ***
  if (nbytes < needed) {
     nleft = nleft + (r->minfree + r->pending - nbytes); 
     log_printf(10, "make_space: Adjusting needed space to satisfy minfree.  neede =" LU "\n", nleft);   
   }

  if (nleft == 0) return(0);  //** Plenty of space so return

  //*** Start by freeing all the expired allocations ***
  time_t now = time(NULL);  //Get the current time so I know when to stop

  dbr_lock(&(r->db));
  dbi = expire_iterator(&(r->db));

  err = make_free_space_iterator(r, dbi, &nleft, now);

  db_iterator_end(dbi);
  dbr_unlock(&(r->db));

  //*** Now free up any soft allocations if needed ***
  if ((nleft > 0) && (err == 0)) {
    now = 0;  //** We can delete everything here if needed
    dbr_lock(&(r->db));
    dbi = soft_iterator(&(r->db));
    err = make_free_space_iterator(r, dbi, &nleft, now);
    db_iterator_end(dbi);
    dbr_unlock(&(r->db));

  }

  if ((nleft > 0) || (err != 0)) {
     return(1);   //*** Didn't have envough space **
  } else {
     return(0);
  }
}

//***************************************************************************
//  free_expired_allocations - Frees all expired allocations on the resource
//***************************************************************************

void free_expired_allocations(Resource_t *r)
{
   size_t size;
   pthread_mutex_lock(&(r->mutex));
   size = r->max_size[ALLOC_HARD];
   make_space(r, size, ALLOC_HARD);
//   size = r->max_size[ALLOC_SOFT];
//   make_space(r, size, ALLOC_SOFT);
   pthread_mutex_unlock(&(r->mutex));
}

//***************************************************************************
// resource_allocable - Returns the max amount of space that can be allocated
//    for the resource.
//***************************************************************************

uint64_t resource_allocable(Resource_t *r, int free_space)
{
  int64_t diff, fsdiff;
  uint64_t allocable;
  struct statfs stat;

  if (free_space == 1) free_expired_allocations(r);

  pthread_mutex_lock(&(r->mutex));  

  diff = r->max_size[ALLOC_TOTAL] - r->used_space[ALLOC_HARD] - r->used_space[ALLOC_SOFT];
  if (diff < 0) diff = 0;

  r->fs->statfs(&stat);
  fsdiff = stat.f_bavail*stat.f_bsize - r->minfree - r->pending;
  if (fsdiff < 0) fsdiff = 0;

  pthread_mutex_unlock(&(r->mutex));  

  allocable = (diff < fsdiff) ? diff : fsdiff;

  return(allocable);
}


//***************************************************************************
// _new_allocation_resource - Creates and returns a uniqe allocation
//        for the resource.
//
//  **NOTE: NO LOCKING IS DONE.  THE ALLOCATION IS NOT BLANKED!  *****
//***************************************************************************

int _new_allocation_resource(Resource_t *r, Allocation_t *a, size_t size, int type, 
    int reliability, time_t length, int is_alias)
{
   int err = 0;

   a->max_size = size;
   a->size = 0;
   a->type = type;
   a->reliability = reliability;
   a->expiration = length;
   a->read_refcount = 1;
   a->write_refcount = 0;
   a->r_pos = 0;
   a->w_pos = 0;
   a->is_alias = is_alias;

   //**Make sure we have enough space if this is a real allocation and record it  
   if (a->is_alias == 0) {
      err = make_space(r, size, reliability);
      if (r->preallocate) r->pending += size;
   }

   if (err != 0) return(err);  //** Exit if not enough space

   a->id = r->fs->create_id();

   create_alloc_db(&(r->db), a);

   //** Always store the initial alloc in the file header
   if (a->is_alias == 0) {
      write_allocation_header(r, a);     //** Store the header
      blank_history(r, a->id);  //** Also store the history
   } else if (r->enable_alias_history) {
      write_allocation_header(r, a);     //** Store the header
      blank_history(r, a->id);  //** Also store the history
   }

   r->n_allocs++;
   if (is_alias == 0) {
      r->used_space[a->reliability] += a->max_size;
   } else {
      r->n_alias++;
   }

debug_printf(5, "_new_allocation_resource: rid=%s rel=%d, used=" LU "\n", r->name, a->reliability, r->used_space[a->reliability]); 
debug_printf(5, "_new_allocation_resource: rcap=%s\n", a->caps[READ_CAP].v); 

   return(err);
}

//***************************************************************************
// create_allocation_resource - Creates and returns a uniqe allocation
//        for the resource
//***************************************************************************

int create_allocation_resource(Resource_t *r, Allocation_t *a, size_t size, int type, 
    int reliability, time_t length, int is_alias, int preallocate_space)
{
   int err;
   size_t total_size;

   memset(a, 0, sizeof(Allocation_t));

   pthread_mutex_lock(&(r->mutex));
   err = _new_allocation_resource(r, a, size, type, reliability, length, is_alias);
   pthread_mutex_unlock(&(r->mutex));

   if (err == 0) {
     if (a->is_alias == 0) {
        total_size = ALLOC_HEADER + size;
        if (preallocate_space == 1) r->fs->reserve(a->id, total_size);       //** Reserve the space
     }

   }

   return(err);
}


//***************************************************************************
// split_allocation_resource - Splits an existing allocation and returns a unique
//      allocation with the correct space and trims the size of the master allocation
//***************************************************************************

int split_allocation_resource(Resource_t *r, Allocation_t *ma, Allocation_t *a, size_t size, int type, 
    int reliability, time_t length, int is_alias, int preallocate_space)
{
   int err;
   size_t total_size;

   if (ma->max_size < size) {
      log_printf(15, "split_allocation_resource: Not enough space left on master id! mid=" LU " msize=" ST " size=" ST "\n", ma->id, ma->size, size);
      return(1);
   }

   memset(a, 0, sizeof(Allocation_t));
   a->split_parent_id = ma->id;

   pthread_mutex_lock(&(r->mutex));
   r->used_space[ma->reliability] = r->used_space[ma->reliability] - size;
   ma->max_size = ma->max_size - size;
   err = _new_allocation_resource(r, a, size, type, reliability, length, is_alias);
   if (err == 0) {
//      r->used_space[ma->reliability] = r->used_space[ma->reliability] + size;    
      if (r->fs->size(ma->id) > ma->max_size)  r->fs->truncate(ma->id, ma->max_size+ALLOC_HEADER);
      if (ma->size > ma->max_size)  ma->size =  ma->max_size;
      if (r->update_alloc == 1) r->fs->write(ma->id, 0, sizeof(Allocation_t), ma);
      err = modify_alloc_db(&(r->db), ma);  //** Store the master back with updated size
   } else {  //** Problem so undo size tweaks
      log_printf(15, "Error with _new_allocation!\n");
      r->used_space[ma->reliability] = r->used_space[ma->reliability] + size;
      ma->max_size = ma->max_size + size;
   }
   pthread_mutex_unlock(&(r->mutex));

   if (err == 0) {
     if (a->is_alias == 0) {
        total_size = ALLOC_HEADER + size;
        if (preallocate_space == 1) r->fs->reserve(a->id, total_size);       //** Reserve the space
     }
   } 

   return(err);
}

//***************************************************************************
// rename_allocation_resource - Renames an allocation.  Actually it just
//    replaces the caps associated with the allocation so the ID
//    stays the same
//***************************************************************************

int rename_allocation_resource(Resource_t *r, Allocation_t *a)
{
   int err;
   pthread_mutex_lock(&(r->mutex));
   err = remove_alloc_db(&(r->db), a);
   if (err == 0)  create_alloc_db(&(r->db), a);
   if (a->is_alias == 0) r->fs->write(a->id, 0, sizeof(Allocation_t), a);
   pthread_mutex_unlock(&(r->mutex));

   return(err);
}

//***************************************************************************
// get_allocation_resource - Returns the allocations data structure
//***************************************************************************

int get_allocation_by_cap_resource(Resource_t *r, int cap_type, Cap_t *cap, Allocation_t *a)
{
  int err;

  err = get_alloc_with_cap_db(&(r->db), cap_type, cap, a);

  return(err);
}

//***************************************************************************
// get_allocation_resource - Returns the allocations data structure
//***************************************************************************

int get_allocation_resource(Resource_t *r, osd_id_t id, Allocation_t *a)
{
  int err;

  err = get_alloc_with_id_db(&(r->db), id, a);

  return(err);
}

//***************************************************************************
// modify_allocation_resource - Stores the allocation data structure
//***************************************************************************

int modify_allocation_resource(Resource_t *r, osd_id_t id, Allocation_t *a)
{
  Allocation_t old_a;
  int err;
  size_t size;

  err = 0;

  if (r->update_alloc == 1) {
     if (a->is_alias == 0) {
        r->fs->write(a->id, 0, sizeof(Allocation_t), a);
     } else if (r->enable_alias_history) {
        r->fs->write(a->id, 0, sizeof(Allocation_t), a);
     }
  }

  if ((err = get_allocation_resource(r, a->id, &old_a)) != 0) {
     log_printf(0, "put_allocation_resource: Can't find id " LU "  db err = %d\n", a->id, err);
     return(err);
  }

  if ((old_a.reliability != a->reliability) || (old_a.max_size != a->max_size)) {
     if (a->is_alias == 0) {
        pthread_mutex_lock(&(r->mutex));
        r->used_space[old_a.reliability] -= old_a.max_size;   //** Update the amount of space used from the old a

        size = 0;
        err = 0;
        if (old_a.max_size < a->max_size) {  //** Growing so need to add space
           size = a->max_size - old_a.max_size;

           if ((err = make_space(r, a->max_size, a->reliability)) == 0) {  //**Make sure we have enough space and record it
              if (r->preallocate) r->pending += size;
           } else {
              log_printf(0, "modify_allocation_resource:  Error with make_space err=%d\n", err);
           }
        }

        if (err == 0) {   
            r->used_space[a->reliability] += a->max_size;  //** Add in the new size if no errors
        } else {
           r->used_space[old_a.reliability] += old_a.max_size;   //** If not enough space revert back
        }

        pthread_mutex_unlock(&(r->mutex));
     
        if (err != 0) {
           return(err);  // ** FAiled on make_space
        } else if ((r->preallocate) && (size > 0)) { //** Actually fill the extra space they requested
           blank_space(r, a->id, old_a.max_size, size);
  
           pthread_mutex_lock(&(r->mutex));
           r->pending -= size;
           pthread_mutex_unlock(&(r->mutex));      
        }  
     }
  }

  
  return(modify_alloc_db(&(r->db), a));
}

//---------------------------------------------------------------------------

//***************************************************************************
// get_manage_allocation_resource - Gets an allocation usign the manage key
//***************************************************************************

int get_manage_allocation_resource(Resource_t *r, Cap_t *mcap, Allocation_t *a)
{
  return(get_allocation_by_cap_resource(r, MANAGE_CAP, mcap, a));
}

//***************************************************************************
// write_allocation_with_id - Writes to a resource using the provided id
//***************************************************************************

int write_allocation_with_id(Resource_t *r, osd_id_t id, off_t offset, size_t len, void *buffer)
{
   int n;

//   pthread_mutex_lock(&(r->mutex));

   n = r->fs->write(id, offset+ALLOC_HEADER, len, buffer);
   if (n == len) n = 0;

//   pthread_mutex_unlock(&(r->mutex));

   return(n);
}

//***************************************************************************
// write_allocation_header - Stores the allocation header
//***************************************************************************

int write_allocation_header(Resource_t *r, Allocation_t *a)
{
   int n;
   char header[ALLOC_HEADER];

   memset(header, 0, ALLOC_HEADER);
   memcpy(header, a, sizeof(Allocation_t));

//   r->fs->truncate(a->id);
   n = r->fs->write(a->id, 0, ALLOC_HEADER, header);   //**Store the header

   if (n == ALLOC_HEADER) n = 0;
   return(n);
}

//***************************************************************************
// write_allocation_with_cap - Writes to a resource using the provided wcap
//***************************************************************************

int write_allocation_with_cap(Resource_t *r, Cap_t *wcap, off_t offset, size_t len, void *buffer)
{
   Allocation_t a;
   int n;

   if ((n = get_allocation_by_cap_resource(r, WRITE_CAP, wcap, &a)) == 0) {
      if (a.max_size < offset+len) return(-1);
      pthread_mutex_lock(&(r->mutex));
      n = r->fs->write(a.id, offset+ALLOC_HEADER, len, buffer);
      pthread_mutex_unlock(&(r->mutex));
//printf("write_allocation_resource: n=%d len=" LU "\n",n,len);
      if (n == len) n = 0;
   }

   return(n);
}

//***************************************************************************
// read_allocation_with_id - Reads to a resource using the provided id
//***************************************************************************

int read_allocation_with_id(Resource_t *r, osd_id_t id, off_t offset, size_t len, void *buffer)
{
   int n;

//   pthread_mutex_lock(&(r->mutex));
   n = r->fs->read(id, offset+ALLOC_HEADER, len, buffer);
   if (n == len) n = 0;
//   pthread_mutex_unlock(&(r->mutex));

   return(n);
}

//***************************************************************************
// read_allcoation_with_cap - Reads a resource using the provided key
//***************************************************************************

int read_allocation_with_cap(Resource_t *r, Cap_t *rcap, off_t offset, size_t len, void *buffer)
{
   Allocation_t a;
   int n;

   if ((n=get_allocation_by_cap_resource(r, READ_CAP, rcap, &a)) == 0) {
      pthread_mutex_lock(&(r->mutex));
      n = r->fs->read(a.id, offset+ALLOC_HEADER, len, buffer);
      pthread_mutex_unlock(&(r->mutex));
      if (n == len) n = 0;
   }

   return(n);
}

//***************************************************************************
// print_allocation_resource - Prints the allocation info to the fd
//***************************************************************************

int print_allocation_resource(Resource_t *r, FILE *fd, Allocation_t *a)
{
  time_t now;
  int64_t diff;

  fprintf(fd, "id = " LU "\n", a->id);
  fprintf(fd, "is_alias = %d\n", a->is_alias);  
  fprintf(fd, "read_cap = %s\n", a->caps[READ_CAP].v);
  fprintf(fd, "write_cap = %s\n", a->caps[WRITE_CAP].v);
  fprintf(fd, "manage_cap = %s\n", a->caps[MANAGE_CAP].v);
  fprintf(fd, "reliability = %d\n", a->reliability);
  fprintf(fd, "type = %d\n", a->type);
  now = time(NULL);  diff = a->expiration - now;
  fprintf(fd, "expiration = %u (expires in " LU " sec) \n", a->expiration, diff);
  fprintf(fd, "read_refcount = %d\n", a->read_refcount);
  fprintf(fd, "write_refcount = %d\n", a->write_refcount);
  fprintf(fd, "max_size = " LU "\n", a->max_size);

  return(0);
}

//*****************************************************************
// walk_expire_iterator_begin - Creates an interator to walk through both
//     the hard and soft expire iterators
//*****************************************************************

walk_expire_iterator_t *walk_expire_iterator_begin(Resource_t *r)
{
  walk_expire_iterator_t *wei;

  assert((wei = (walk_expire_iterator_t *)malloc(sizeof(walk_expire_iterator_t))) != NULL);

  wei->reset = 1;
  wei->r = r;

  dbr_lock(&(r->db));

  wei->hard = expire_iterator(&(r->db));
  if (wei->hard == NULL) {
     log_printf(10, "walk_expire_hard_iterator: wei->hard = NULL! r=%s\n", r->name);
     return(NULL);
  }

  wei->soft = soft_iterator(&(r->db));
  if (wei->hard == NULL) {
     log_printf(10, "walk_expire_hard_iterator: wei->soft = NULL! r=%s\n", r->name);
     return(NULL);
  }

  return(wei);
}


//*****************************************************************
// walk_expire_iterator_end - Destroys the walk through iterator
//*****************************************************************

void walk_expire_iterator_end(walk_expire_iterator_t *wei)
{
  db_iterator_end(wei->hard);
  db_iterator_end(wei->soft);

  dbr_unlock(&(wei->r->db));

  free(wei);
}

//*****************************************************************
// set_walk_expire_iterator - Sets the time for the  walk through iterator
//*****************************************************************

int set_walk_expire_iterator(walk_expire_iterator_t *wei, time_t t)
{
   int i;

   wei->reset = 0;  //** rest the times to trigger a reload on get next

   i = set_expire_iterator(wei->hard, t, &(wei->hard_a));
   if (i!= 0) {
      log_printf(10, "set_walk_expire_iterator: Error with set_soft: %d, time=" TT "\n", i, t);
      wei->hard_a.expiration = 0;
   }

   i = set_expire_iterator(wei->soft, t, &(wei->soft_a));
   if (i!= 0) {
      log_printf(10, "set_walk_expire_iterator: Error with set_hard: %d time=" TT "\n", i, t);
      wei->soft_a.expiration = 0;
   }

   return(0);
}

//*****************************************************************
// get_next_walk_expire_iterator - Gets the next record for the walk through iterator
//*****************************************************************

int get_next_walk_expire_iterator(walk_expire_iterator_t *wei, int direction, Allocation_t *a)
{
  int err, dir;
  int64_t dt;

  if (wei->reset == 1) {  //** Reload starting records
     wei->reset = 0;
     err = db_iterator_next(wei->hard, direction, &(wei->hard_a));
     if (err!= 0) {
        log_printf(10, "get_next_walk_expire_iterator: Error or end with next_hard: %d \n", err);
        wei->hard_a.expiration = 0;
     } else if (wei->hard_a.is_alias == 0) {
       err = wei->r->fs->read(wei->hard_a.id, 0, sizeof(Allocation_t), &(wei->hard_a));
     }
     
     err = db_iterator_next(wei->soft, direction, &(wei->soft_a));
     if (err!= 0) {
        log_printf(10, "get_next_walk_expire_iterator: Error or end with next_soft: %d \n", err);
        wei->soft_a.expiration = 0;
     } else if (wei->soft_a.is_alias == 0) {
       err = wei->r->fs->read(wei->soft_a.id, 0, sizeof(Allocation_t), &(wei->soft_a));
     }
  }

  log_printf(10, "get_next_walk_expire_iterator: hard= %u soft= %u \n", wei->hard_a.expiration, wei->soft_a.expiration);
    
  //** Do a boundary check ***
  if (wei->hard_a.expiration == 0) {
     if (wei->soft_a.expiration == 0) {
        return(1);
     } else {
        *a = wei->soft_a;
        err = db_iterator_next(wei->soft, direction, &(wei->soft_a));
        if (err!= 0) {
           log_printf(10, "get_next_walk_expire_iterator: Error or end with next_soft: %d \n", err);
           wei->soft_a.expiration = 0;
        } else if (wei->soft_a.is_alias == 0) {
           err = wei->r->fs->read(wei->soft_a.id, 0, sizeof(Allocation_t), &(wei->soft_a));
        }

        log_printf(15, "get_next_walk_expire_iterator: 1 expire= %u\n", a->expiration);
        return(0);
     }
  } else if (wei->soft_a.expiration == 0) {
     *a = wei->hard_a;
     err = db_iterator_next(wei->hard, direction, &(wei->hard_a));
     if (err!= 0) {
        log_printf(10, "get_next_walk_expire_iterator: Error or end with next_hard: %d \n", err);
        wei->hard_a.expiration = 0;
     } else if (wei->hard_a.is_alias == 0) {
       err = wei->r->fs->read(wei->hard_a.id, 0, sizeof(Allocation_t), &(wei->hard_a));
     }

     log_printf(15, "get_next_walk_expire_iterator: 2 expire= %u\n", a->expiration);
     return(0);
  }    

  //** If I make it here that means both the hard and soft allocations are valid

  //** Fancy way to unify DBR_PREV/DBR_NEXT into a single set **
  dir = 1;
  if (direction == DBR_PREV) dir = -1;

  dt = dir * (wei->hard_a.expiration - wei->soft_a.expiration);
  if (dt > 0) { //** hard > soft so return the soft one
     *a = wei->soft_a;
     err = db_iterator_next(wei->soft, direction, &(wei->soft_a));
     if (err!= 0) {
        log_printf(10, "get_next_walk_expire_iterator: Error or end with next_soft: %d \n", err);
        wei->soft_a.expiration = 0;
     } else if (wei->soft_a.is_alias == 0) {
        err = wei->r->fs->read(wei->soft_a.id, 0, sizeof(Allocation_t), &(wei->soft_a));
     }
  } else {  //** hard < soft so return the hard a
     *a = wei->hard_a;
     err = db_iterator_next(wei->hard, direction, &(wei->hard_a));
     if (err!= 0) {
        log_printf(10, "get_next_walk_expire_iterator: Error or end with next_hard: %d \n", err);
        wei->hard_a.expiration = 0;
     } else if (wei->hard_a.is_alias == 0) {
       err = wei->r->fs->read(wei->hard_a.id, 0, sizeof(Allocation_t), &(wei->hard_a));
     }
  }    

  log_printf(15, "get_next_walk_expire_iterator: 3 expire= %u\n", a->expiration);
  return(0);  
}
  
//*****************************************************************
// resource_cleanup  - Performs the actual resource cleanup
//*****************************************************************

void resource_cleanup(Resource_t *r)
{
  int max_alloc = 100;
  int i, n, err;
  Allocation_t a[max_alloc], b;
  walk_expire_iterator_t *wei;

  log_printf(5, "resource_background_cleanup: Start of routine.  rid=%s time= " TT "\n",r->name, time(NULL));
  
  n = max_alloc;
  while (n == max_alloc) {
    //** Perform the walk
    wei = walk_expire_iterator_begin(r);
    n = max_alloc;
    for (i=0; i<max_alloc; i++) {
       err = get_next_walk_expire_iterator(wei, DBR_NEXT, &(a[i]));
       if (err != 0) { n = i; break; }
       if (a[i].expiration > time(NULL)) { n = i; break; }
    }
    walk_expire_iterator_end(wei);

    log_printf(5, "resource_background_cleanup: rid=%s n=%d\n", r->name, n);

    //** Do the actual removal
    for (i=0; i<n; i++) {
       log_printf(5, "resource_background_cleanup:i=%d.  rid=%s checking/removing:" LU "\n",i, r->name, a[i].id);
//       pthread_mutex_lock(&(r->mutex));
//       dbr_lock(&(r->db));
       err = get_alloc_with_id_db(&(r->db), a[i].id, &b);
       if (err == 0) {
          if (b.expiration < time(NULL)) _remove_allocation(r, &b, true);
       }
//       dbr_unlock(&(r->db));
//       pthread_mutex_unlock(&(r->mutex));
    }
  }

  log_printf(5, "resource_background_cleanup: End of routine.  rid=%s time= " TT "\n",r->name, time(NULL));

  return;
}

//*****************************************************************
// resource_cleanup_thread - Thread for doing background cleanups
//*****************************************************************

void *resource_cleanup_thread(void *data)
{
  Resource_t *r = (Resource_t *)data;

  struct timespec t;

  log_printf(5, "resource_cleanup_thread: Start.  rid=%s time= " TT "\n",r->name, time(NULL));

  pthread_mutex_lock(&(r->cleanup_lock));
  while (r->cleanup_shutdown == 0) {
     pthread_mutex_unlock(&(r->cleanup_lock));

     resource_cleanup(r);

     t.tv_sec = time(NULL) + 300;    //Cleanup every 5 minutes
     t.tv_nsec = 0;
     pthread_mutex_lock(&(r->cleanup_lock));
     if (r->cleanup_shutdown == 0) {
        log_printf(5, "resource_cleanup_thread: Sleeping rid=%s time= " TT " shutdown=%d\n",r->name, time(NULL), r->cleanup_shutdown);
        pthread_cond_timedwait(&(r->cleanup_cond), &(r->cleanup_lock), &t);
     }
     log_printf(5, "resource_cleanup_thread: waking up rid=%s time= " TT " shutdown=%d\n",r->name, time(NULL), r->cleanup_shutdown);
     flush_log();
  }

  pthread_mutex_unlock(&(r->cleanup_lock));

  log_printf(5, "resource_cleanup_thread: Exit.  rid=%s time= " TT "\n",r->name, time(NULL));
  flush_log();
  pthread_exit(NULL);
}

//*****************************************************************
// launch_resource_cleanup_thread 
//*****************************************************************

void launch_resource_cleanup_thread(Resource_t *r)
{
  r->cleanup_shutdown = 0;
  pthread_create(&(r->cleanup_thread), NULL, resource_cleanup_thread, (void *)r);
}

