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

//*******************************************************************
//*******************************************************************

#ifndef _RESOURCE_H_
#define _RESOURCE_H_

#include "db_resource.h"
#include "osd_abstract.h"
#include "osd_fs.h"
#include "osd_ebofs.h"
#include <pthread.h>
#include <time.h>

#define _RESOURCE_VERSION 100000

#define DEVICE_DIR   "dir"
#define DEVICE_EBOFS "ebofs"

#define ALLOC_TOTAL 2         //Used for determing the total depot size

typedef uint64_t Rsize_t;    //Resource size

typedef struct {
  char  rid[16];
  int id;
} RID_t;      //Resource ID

typedef struct {       //Resource structure
   char *keygroup;         //Keyfile group name
   char *name;             //Descriptive resource name
   RID_t rid;              //Unique resource ID
   int   max_duration;     //MAx duration for an allocation in SECONDS
   int   lazy_allocate;    //If 1 then the actual file is created with the allocation.  Otherwise just the DB entry
   char *device_type;      //Type of resource (dir|ebofs)
   char *device;           //Device name
   char *location;         //Location of DB files, etc. for the device
   int  update_alloc;      //If 1 then the file allocation is also updated in addition to the DB
   int  enable_read_history;  //USed to enable tracking of the various history lists
   int  enable_write_history;
   int  enable_manage_history;
   int  enable_alias_history;
   Rsize_t max_size[3];     //Soft and Hard limits
   Rsize_t minfree;         //Minimum amount of free space in KB
   Rsize_t used_space[2];   //Soft/Hard data used
   Rsize_t pending;         //Pending creates
   Rsize_t n_allocs;        //Total number of allocations (physical and alias)
   Rsize_t n_alias;         //Number of alias allocations
   int    preallocate;     //PReallocate all new allocations
   DB_resource_t db;       //DB for maintaining the resource's caps
//   Ebofs    *edev;         //Raw EBOFS device if used
   osd_abstract *fs;        //Actual filesystem   
   int      rl_index;       //** Index in global resource array
   pthread_mutex_t mutex;  //Lock for creates
   int             cleanup_shutdown; 
   pthread_mutex_t cleanup_lock;  //Used to shutdown cleanup thread
   pthread_cond_t  cleanup_cond;  //Used to shutdown the cleanup thread
   pthread_t       cleanup_thread;
} Resource_t;

typedef struct {
   int reset;
   Allocation_t hard_a;
   Allocation_t soft_a;
   DB_iterator_t *hard;
   DB_iterator_t *soft;
   Resource_t *r;
}  walk_expire_iterator_t;


char *rid2str(RID_t *rid, char *name, int n_size);
int str2rid(const char *name, RID_t *rid);
void empty_rid(RID_t *rid);
int is_empty_rid(RID_t *rid);
int compare_rid(RID_t *rid1, RID_t *rid2);

int mkfs_resource(RID_t rid, char *dev_type, char *device_name, char *db_location);
int mount_resource(Resource_t *res, GKeyFile *keyfile, char *group, DB_env_t *env, int force_rebuild, 
     int lazy_allocate, int truncate_expiration);
int umount_resource(Resource_t *res);
int print_resource(Resource_t *res, FILE *fd);
int print_resource_usage(Resource_t *r, FILE *fd);
int remove_allocation_resource(Resource_t *r, Allocation_t *alloc);
void free_expired_allocations(Resource_t *r);
uint64_t resource_allocable(Resource_t *r, int free_space);
int create_allocation_resource(Resource_t *r, Allocation_t *a, size_t size, int type, 
    int reliability, time_t length, int is_alias, int preallocate_space);
int split_allocation_resource(Resource_t *r, Allocation_t *ma, Allocation_t *a, size_t size, int type,
    int reliability, time_t length, int is_alias, int preallocate_space);
int rename_allocation_resource(Resource_t *r, Allocation_t *a);
int merge_allocation_resource(Resource_t *r, Allocation_t *ma, Allocation_t *a);
int get_allocation_by_cap_resource(Resource_t *r, int cap_type, Cap_t *cap, Allocation_t *a);
int get_allocation_resource(Resource_t *r, osd_id_t id, Allocation_t *a);
int modify_allocation_resource(Resource_t *r, osd_id_t id, Allocation_t *a);
int get_manage_allocation_resource(Resource_t *r, Cap_t *mcap, Allocation_t *a);
int write_allocation_header(Resource_t *r, Allocation_t *a);
int write_allocation_with_id(Resource_t *r, osd_id_t id, off_t offset, size_t len, void *buffer);
int write_allocation_with_cap(Resource_t *r, Cap_t *wcap, off_t offset, size_t len, void *buffer);
int read_allocation_with_id(Resource_t *r, osd_id_t id, off_t offset, size_t len, void *buffer);
int read_allocation_with_cap(Resource_t *r, Cap_t *rcap, off_t offset, size_t len, void *buffer);
int print_allocation_resource(Resource_t *r, FILE *fd, Allocation_t *a);
int calc_expired_space(Resource_t *r, time_t timestamp, Rsize_t *nbytes);
walk_expire_iterator_t *walk_expire_iterator_begin(Resource_t *r);
void walk_expire_iterator_end(walk_expire_iterator_t *wei);
int set_walk_expire_iterator(walk_expire_iterator_t *wei, time_t t);
int get_next_walk_expire_iterator(walk_expire_iterator_t *wei, int direction, Allocation_t *a);
void launch_resource_cleanup_thread(Resource_t *r);

int create_history_table(Resource_t *r);
int mount_history_table(Resource_t *r);
void umount_history_table(Resource_t *r);
int get_history_table(Resource_t *r, osd_id_t id, Allocation_history_t *h);
int put_history_table(Resource_t *r, osd_id_t id, Allocation_history_t *h);
int blank_history(Resource_t *r, osd_id_t id);
void update_read_history(Resource_t *r, osd_id_t id, int is_alias, Allocation_address_t *add, uint64_t offset, uint64_t size, osd_id_t pid);
void update_write_history(Resource_t *r, osd_id_t id, int is_alias, Allocation_address_t *add, uint64_t offset, uint64_t size, osd_id_t pid);
void update_manage_history(Resource_t *r, osd_id_t id, int is_alias, Allocation_address_t *add, int cmd, int subcmd, int reliability, uint32_t expiration, uint64_t size, osd_id_t pid);

#endif

