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

#include "db_resource.h"
#include "allocation.h"
#include "random.h"
#include "fmttypes.h"
#include "log.h"
#include "debug.h"
#include <string.h>
#include <stdlib.h>
#include <assert.h>
#include <stdio.h>
#include <glib.h>
#include <sys/types.h>
#include <sys/stat.h>
#include <dirent.h>
#include <signal.h>

#define DB_INDEX_ID     0
#define DB_INDEX_READ   1
#define DB_INDEX_WRITE  2
#define DB_INDEX_MANAGE 3
#define DB_INDEX_EXPIRE 4
#define DB_INDEX_SOFT   5

pthread_mutex_t dbr_mutex;

#define db_txn(str, err)       \
  DB_TXN  *txn = NULL;                \
  err = dbr->dbenv->txn_begin(dbr->dbenv, NULL, &txn, 0); \
  if (err != 0) {                     \
     log_printf(0, "%s: Transaction begin failed with err %d\n", str, err); \
     return(err);                     \
  }

#define db_commit(str, err)      \
  if (err != 0) {                \
     err = txn->commit(txn, 0);  \
     if (err != 0) {             \
        log_printf(0, "%s: Transaction commit failed with err %d\n", str, err); \
        return(1);               \
     }                           \
  } else {                       \
    txn->abort(txn);             \
  }

//***************************************************************************
// fill_timekey - Fills a DB_timekey_t strcuture
//***************************************************************************

DB_timekey_t *fill_timekey(DB_timekey_t *tk, time_t t, osd_id_t id)
{
  memset(tk, 0, sizeof(DB_timekey_t));

  tk->time = t;
  tk->id = id;

  return(tk);
}

//***************************************************************************
//  dbr_lock - Locks the DB
//***************************************************************************

void dbr_lock(DB_resource_t *dbr)
{
  pthread_mutex_lock(dbr->mutex);
}

//***************************************************************************
//  dbr_unlock - Locks the DB
//***************************************************************************

void dbr_unlock(DB_resource_t *dbr)
{
  pthread_mutex_unlock(dbr->mutex);
}

//***************************************************************************
// get_rcap_key - Returns the read cap key for the 2ndary DB
//***************************************************************************

int get_rcap_key(DB *sdb, const DBT *pkey, const DBT *pdata, DBT *skey) {
   Allocation_t *a = (Allocation_t *)pdata->data;
  
   memset((void *)skey, 0, sizeof(DBT));
   skey->data = a->caps[READ_CAP].v;
   skey->size = CAP_SIZE+1;

//log_printf(10, "get_rcap_key: key=%s\n", (char *)skey->data);
   return(0);
}

//***************************************************************************
// get_wcap_key - Returns the write cap key for the 2ndary DB
//***************************************************************************

int get_wcap_key(DB *sdb, const DBT *pkey, const DBT *pdata, DBT *skey) {
   Allocation_t *a = (Allocation_t *)pdata->data;
  
   memset(skey, 0, sizeof(DBT));
   skey->data = a->caps[WRITE_CAP].v;
   skey->size = CAP_SIZE+1;

//log_printf(10, "get_wcap_key: key=%s\n", (char *)skey->data);

   return(0);
}

//***************************************************************************
// get_mcap_key - Returns the manage cap key for the 2ndary DB
//***************************************************************************

int get_mcap_key(DB *sdb, const DBT *pkey, const DBT *pdata, DBT *skey) {
   Allocation_t *a = (Allocation_t *)pdata->data;
  
   memset(skey, 0, sizeof(DBT));
   skey->data = a->caps[MANAGE_CAP].v;
   skey->size = CAP_SIZE+1;

//log_printf(10, "get_mcap_key: key=%s\n", (char *)skey->data);

   return(0);
}

//***************************************************************************
// get_expire_key - Returns the expire key for the 2ndary DB
//***************************************************************************

int get_expire_key(DB *sdb, const DBT *pkey, const DBT *pdata, DBT *skey) {
   Allocation_t *a = (Allocation_t *)pdata->data;

//   return(DB_DONOTINDEX);
  
   memset(skey, 0, sizeof(DBT));
   skey->data = &(a->expirekey);
   skey->size = sizeof(DB_timekey_t);

//time_t t = a->expirekey.time;
//log_printf(10, "get_expire_key: key=" LU " * " TT "\n", a->expirekey.id, t);

   return(0);
}

//***************************************************************************
// get_soft_key - Returns the expire key for the 2ndary DB
//***************************************************************************

int get_soft_key(DB *sdb, const DBT *pkey, const DBT *pdata, DBT *skey) {
   Allocation_t *a = (Allocation_t *)pdata->data;
  
   memset(skey, 0, sizeof(DBT));

   if (a->reliability == ALLOC_SOFT) {
      skey->data = &(a->softkey);
      skey->size = sizeof(DB_timekey_t);
      return(0);
   } else {
//log_printf(10, "get_soft_key: NOT indexed\n");
      return(DB_DONOTINDEX);
   }
}

//***************************************************************************
// compare_expiration - Compares the expiration of 2 times for BTREE sorting
//***************************************************************************

int compare_expiration(DB *db, const DBT *k1, const DBT *k2)
{
   DB_timekey_t tk1, tk2;

   memcpy(&tk1, k1->data, sizeof(DB_timekey_t));
   memcpy(&tk2, k2->data, sizeof(DB_timekey_t));


//   log_printf(15, "compare_expiration: k1=" LU " * " TT " --- k2= " LU " * " TT "\n",
//        tk1.id, tk1.time, tk2.id, tk2.time);

   if (tk1.time < tk2.time) {
      return(-1);
   } else if (tk1.time == tk2.time) {
      if (tk1.id < tk2.id) {
         return(-1);
      } else if (tk1.id == tk2.id) {
         return(0);
      } else {
         return(1);
      }
   } else {
      return(1);
   }

}

//***************************************************************************
// mkfs_db - Creates a new DB resource
//      loc  - Directory to store the DB files
//      type - Right now this is ignored and dhould be "db"
//      fd   - Key file to store any DB related keys in
//***************************************************************************

int mkfs_db(char *loc, FILE *fd) {
   DB_resource_t dbres;
   u_int32_t flags, bflags, psize;
   int i, err;
   char fname[2048];

   if (strlen(loc) > 2000) {
      printf("mkfs_db:  Need to increase fname size.  strlen(loc)=" ST "\n", strlen(loc));
      abort();
   }

   flags = DB_CREATE | DB_THREAD;
   bflags = flags;

   //*** Create/Open the primary DB containing the ID's ***
   assert(db_create(&(dbres.pdb), NULL, 0) == 0);
   psize = 32 * 1024;
   assert(dbres.pdb->set_pagesize(dbres.pdb, psize) == 0);
   snprintf(fname, sizeof(fname), "%s/id.db", loc);
   remove(fname);
   if ((err=dbres.pdb->open(dbres.pdb, NULL, fname, NULL, DB_HASH, flags, 0)) != 0) {
      printf("mkfs_db: Can't create primary DB: %s\n", fname);
      printf("mkfs_db: %s\n", db_strerror(err));
      abort();
   }
   
   //*** Create/Open DB containing the READ_CAPs ***
   assert(db_create(&(dbres.cap[READ_CAP]), NULL, 0) == 0);
   snprintf(fname, sizeof(fname), "%s/read.db", loc);
   remove(fname);
   if (dbres.cap[READ_CAP]->open(dbres.cap[READ_CAP], NULL, fname, NULL, DB_HASH, flags, 0) != 0) {
      printf("mkfs_db: Can't create read DB: %s\n", fname);
      abort();
   }

   //*** Create/Open DB containing the WRITE_CAPs ***
   assert(db_create(&(dbres.cap[WRITE_CAP]), NULL, 0) == 0);
   snprintf(fname, sizeof(fname), "%s/write.db", loc);
   remove(fname);
   if (dbres.cap[WRITE_CAP]->open(dbres.cap[WRITE_CAP], NULL, fname, NULL, DB_HASH, flags, 0) != 0) {
      printf("mkfs_db: Can't create read DB: %s\n", fname);
      abort();
   }

   //*** Create/Open DB containing the MANAGE_CAPs ***
   assert(db_create(&(dbres.cap[MANAGE_CAP]), NULL, 0) == 0);
   snprintf(fname, sizeof(fname), "%s/manage.db", loc);
   remove(fname);
   if (dbres.cap[MANAGE_CAP]->open(dbres.cap[MANAGE_CAP], NULL, fname, NULL, DB_HASH, flags, 0) != 0) {
      printf("mkfs_db: Can't create manage DB: %s\n", fname);
      abort();
   }

   //*** Create/Open DB containing the expirationss ***
   assert(db_create(&(dbres.expire), NULL, 0) == 0);
   assert(dbres.expire->set_bt_compare(dbres.expire, compare_expiration) == 0);
   snprintf(fname, sizeof(fname), "%s/expire.db", loc);
   remove(fname);
   if ((err = dbres.expire->open(dbres.expire, NULL, fname, NULL, DB_BTREE, bflags, 0)) != 0) {
      printf("mkfs_db: Can't create expire DB: %s\n", fname);
      printf("mkfs_db: %s\n", db_strerror(err));
      abort();
   }

   //*** Create/Open DB containing the soft allocations ***
   assert(db_create(&(dbres.soft), NULL, 0) == 0);
   assert(dbres.soft->set_bt_compare(dbres.soft, compare_expiration) == 0);
   snprintf(fname, sizeof(fname), "%s/soft.db", loc);
   remove(fname);
   if ((err = dbres.soft->open(dbres.soft, NULL, fname, NULL, DB_BTREE, bflags, 0)) != 0) {
      printf("mkfs_db: Can't create soft DB: %s\n", fname);
      printf("mkfs_db: %s\n", db_strerror(err));
      abort();
   }

   //*** Now we can close everything ***
   for (i=0; i<3; i++) dbres.cap[i]->close(dbres.cap[i], 0);
   dbres.expire->close(dbres.expire, 0);
   dbres.soft->close(dbres.soft, 0);
   dbres.pdb->close(dbres.pdb, 0);

   //*** Lastly add the group to the Key file ***
   fprintf(fd, "[db]\n");
   fprintf(fd, "loc = %s\n", loc);

   return(0);
}

//***************************************************************************
// mount_db_generic - Mounts a DB for use using the keyfile for the location
//    and optionally wipes the data files
//***************************************************************************

int mount_db_generic(GKeyFile *kf, DB_env_t *env, DB_resource_t *dbres, int wipe_clean)
{
   char fname[2048];
   u_int32_t flags, bflags, psize;
   int err;
   DB *db;
   DB_ENV *dbenv;
   DB_env_t *lenv;

   //** Get the directory containing everything **
   assert((dbres->loc = g_key_file_get_string(kf, "db", "loc", NULL)) != NULL);

   log_printf(15, "mound_db_generic: wipe_clean=%d\n",wipe_clean);

   dbres->env = env;
   if (env->dbenv == NULL) {  //** Got to make a local environment
      log_printf(10, "moount_db_generic:  Creating local DB environment. loc=%s\n", dbres->loc);
      lenv = create_db_env(dbres->loc, env->max_size);
      lenv->local = 1;
      dbres->env = lenv; 
   }
   
   dbres->dbenv = dbres->env->dbenv;
   dbenv = dbres->dbenv;
   
   //** Now open everything up and associate it **
   if (strlen(dbres->loc) > 2000) {
      printf("mount_db:  Need to increase fname size.  strlen(loc)=" ST "\n", strlen(dbres->loc));
      abort();
   }

   flags = DB_AUTO_COMMIT | DB_THREAD | DB_CREATE;  //** Inheriting most from the environment
//   flags = DB_AUTO_COMMIT | DB_THREAD | DB_CREATE;  //** Inheriting most from the environment
//   if (wipe_clean == 2)  flags = flags | DB_CREATE; //**Only wipe the id DB if wipe_clean=2
   
   //*** Create/Open the primary DB containing the ID's ***
   assert(db_create(&(dbres->pdb), dbenv, 0) == 0);
   psize = 32 * 1024;
   assert(dbres->pdb->set_pagesize(dbres->pdb, psize) == 0);
   snprintf(fname, sizeof(fname), "%s/id.db", dbres->loc);
   if (wipe_clean == 2) remove(fname);
   if (dbres->pdb->open(dbres->pdb, NULL, fname, NULL, DB_HASH, flags, 0) != 0) {
      printf("mount_db: Can't open primary DB: %s\n", fname);
      abort();
   }

   //** Wipe all other DB's **
   if (wipe_clean != 0)  flags = flags | DB_CREATE;
   bflags = flags;
   
   //*** Create/Open DB containing the READ_CAPs ***
   assert(db_create(&db, dbenv, 0) == 0);
   dbres->cap[READ_CAP] = db;
   snprintf(fname, sizeof(fname), "%s/read.db", dbres->loc);
   if (wipe_clean == 2) { err = remove(fname); log_printf(0, "mount_db_generic: fname=%s remove=%d\n", fname, err); }
   if (db->open(db, NULL, fname, NULL, DB_HASH, flags, 0) != 0) {
      printf("mount_db: Can't open read DB: %s\n", fname);
      abort();
   }
   if (dbres->pdb->associate(dbres->pdb, NULL, db, get_rcap_key, 0) != 0) {
      printf("mount_db: Can't associate read DB: %s\n", fname);
      abort();     
   }

   //*** Create/Open DB containing the WRITE_CAPs ***
   
   assert(db_create(&db, dbenv, 0) == 0);
   dbres->cap[WRITE_CAP] = db;
   snprintf(fname, sizeof(fname), "%s/write.db", dbres->loc);
   if (wipe_clean == 2) remove(fname);
   if (db->open(db, NULL, fname, NULL, DB_HASH, flags, 0) != 0) {
      printf("mount_db: Can't open write DB: %s\n", fname);
      abort();
   }
   if (dbres->pdb->associate(dbres->pdb, NULL, db, get_wcap_key, 0) != 0) {
      printf("mount_db: Can't associate write DB: %s\n", fname);
      abort();     
   }

   //*** Create/Open DB containing the MANAGE_CAPs ***
   assert(db_create(&db, dbenv, 0) == 0);
   dbres->cap[MANAGE_CAP] = db;
   snprintf(fname, sizeof(fname), "%s/manage.db", dbres->loc);
   if (wipe_clean == 2) remove(fname);
   if (db->open(db, NULL, fname, NULL, DB_HASH, flags, 0) != 0) {
      printf("mount_db: Can't open manage DB: %s\n", fname);
      abort();
   }
   if (dbres->pdb->associate(dbres->pdb, NULL, db, get_mcap_key, 0) != 0) {
      printf("mount_db: Can't associate manage DB: %s\n", fname);
      abort();     
   }

   //*** Create/Open DB containing the expirationss ***
   assert(db_create(&db, dbenv, 0) == 0);
   dbres->expire = db;
   assert(db->set_bt_compare(db, compare_expiration) == 0);
   snprintf(fname, sizeof(fname), "%s/expire.db", dbres->loc);
   if (wipe_clean == 2) remove(fname);
   if ((err = db->open(db, NULL, fname, NULL, DB_BTREE, bflags, 0)) != 0) {
      printf("mount_db: Can't open expire DB: %s\n", fname);
      printf("mount_db: %s\n", db_strerror(err));
      abort();
   }
   if (dbres->pdb->associate(dbres->pdb, NULL, db, get_expire_key, 0) != 0) {
      printf("mount_db: Can't associate expire DB: %s\n", fname);
      abort();     
   }

   //*** Create/Open DB containing the soft allocations ***
   assert(db_create(&db, dbenv, 0) == 0);
   dbres->soft = db;
   assert(db->set_bt_compare(db, compare_expiration) == 0);
   snprintf(fname, sizeof(fname), "%s/soft.db", dbres->loc);
   if (wipe_clean == 2) remove(fname);
   if ((err = db->open(db, NULL, fname, NULL, DB_BTREE, bflags, 0)) != 0) {
      printf("mount_db: Can't open soft DB: %s\n", fname);
      printf("mount_db: %s\n", db_strerror(err));
      abort();
   }
   if (dbres->pdb->associate(dbres->pdb, NULL, db, get_soft_key, 0) != 0) {
      printf("mount_db: Can't associate soft DB: %s\n", fname);
      abort();     
   }

   //*** init the lock ***
//   dbres->mutex = &dbr_mutex;
   dbres->mutex = (pthread_mutex_t *)malloc(sizeof(pthread_mutex_t));
   pthread_mutex_init(dbres->mutex, NULL);

   return(0);  
}

//***************************************************************************
// mount_db - Mounts a DB for use using the keyfile for the location
//***************************************************************************

int mount_db(GKeyFile *kf, DB_env_t *dbenv, DB_resource_t *dbres)
{
   return(mount_db_generic(kf, dbenv, dbres, 0));
}

//***************************************************************************
// umount_db - Unmounts the given DB
//***************************************************************************

int umount_db(DB_resource_t *dbres) 
{
  int i;

  for (i=0; i<3; i++) dbres->cap[i]->close(dbres->cap[i], 0);
//dbres->cap_read->close(dbres->cap_read, 0);
  dbres->expire->close(dbres->expire, 0);
  dbres->soft->close(dbres->soft, 0);
  dbres->pdb->close(dbres->pdb, 0);

  pthread_mutex_destroy(dbres->mutex);
  free(dbres->mutex);

  return(0);
}

//---------------------------------------------------------------------------

//***************************************************************************
// create_db_env - Creates the DB environment
//***************************************************************************

DB_env_t *create_db_env(const char *loc, int db_mem)
{
   u_int32_t flags;
   DB_ENV *dbenv;
   int err;
   DB_env_t *env;

//return(NULL);  //*******FIXME*********

   assert((env = (DB_env_t *)malloc(sizeof(DB_env_t))) != NULL);

   env->dbenv = NULL;
   env->max_size = db_mem;
   env->local = 0;

   if (strcmp(loc, "local") == 0) return(env);

   flags = DB_CREATE   | DB_INIT_LOCK | DB_INIT_LOG | DB_INIT_MPOOL | 
           DB_INIT_TXN | DB_THREAD    | DB_RECOVER | DB_AUTO_COMMIT;

   assert(db_env_create(&dbenv, 0) == 0);
   env->dbenv = dbenv;

   u_int32_t gbytes = db_mem / 1024;
   u_int32_t bytes = (db_mem % 1024) * 1024*1024;
   log_printf(10, "create_db_env: gbytes=%u bytes=%u\n", gbytes, bytes);
   assert(dbenv->set_cachesize(dbenv, gbytes, bytes, 1) == 0);
//   assert(dbenv->set_flags(dbenv, DB_TXN_NOSYNC | DB_TXN_NOWAIT, 1) == 0);
   assert(dbenv->log_set_config(dbenv, DB_LOG_AUTO_REMOVE, 1) == 0);
   if ((err=dbenv->open(dbenv, loc, flags, 0)) != 0) {
      printf("create_db_env: Warning!  No environment located in %s\n", loc);
      printf("create_db_env: Attempting to create a new environment.\n");
      printf("create_Db_env: DB error: %s\n", db_strerror(err));

      DIR *dir;
      mkdir(loc, S_IRWXU);
      assert((dir = opendir(loc)) != NULL);  //Make sure I can open it
      closedir(dir);

      if ((err=dbenv->open(dbenv, loc, flags, 0)) != 0) {
         printf("create_db_env: Error opening DB environment!  loc=%s\n", loc);
         printf("create_db_env: %s\n", db_strerror(err));
      } 
   }

   return(env);
}

//***************************************************************************
// close_db_env - Closes the DB environment
//***************************************************************************

int close_db_env(DB_env_t *env)
{
  int err = 0;

  if (env->dbenv != NULL) {
    err = env->dbenv->close(env->dbenv, 0);
  }

  free(env);

  return(err);
}

//***************************************************************************
// print_db - Prints the DB information out to fd.
//***************************************************************************

int print_db(DB_resource_t *db, FILE *fd)
{
   fprintf(fd, "DB location: %s\n", db->loc);

   db->pdb->stat_print(db->pdb, 0);
   db->dbenv->stat_print(db->dbenv, DB_STAT_ALL);

   return(0);
}

//***************************************************************************
// get_num_allocations_db - Returns the number of allocations according to
//    primary DB
//***************************************************************************

int get_num_allocations_db(DB_resource_t *db)
{
  int n, err;
  DB_HASH_STAT *dstat;
//  u_int32_t flags = DB_FAST_STAT;
  u_int32_t flags = DB_READ_COMMITTED;

  
  dbr_lock(db);
  err = db->pdb->stat(db->pdb, NULL, (void *)&dstat, flags);   
  if (err != 0) {
     log_printf(0, "get_allocations_db:  error=%d  (%s)\n", err, db_strerror(err));
  }
  dbr_unlock(db);

  n = -1;
  if (err == 0) {
     n = dstat->hash_nkeys;
     free(dstat);
     log_printf(10, "get_allocations_db: nkeys=%d\n", n);     
  }

  return(n);
}


//---------------------------------------------------------------------------

//***************************************************************************
// _get_alloc_with_id_db - Returns the alloc with the given ID from the DB
//      internal version that does no locking
//***************************************************************************

int _get_alloc_with_id_db(DB_resource_t *dbr, osd_id_t id, Allocation_t *alloc)
{
  DBT key, data;
  int err;

  memset(&key, 0, sizeof(DBT));
  memset(&data, 0, sizeof(DBT));

  key.data = &id;
  key.size = sizeof(osd_id_t);

  data.data = alloc;
  data.ulen = sizeof(Allocation_t);
  data.flags = DB_DBT_USERMEM;

  err = dbr->pdb->get(dbr->pdb, NULL, &key, &data, 0);
  if (err != 0) {
     log_printf(10, "_get_alloc_with_id_db:  Unknown ID=" LU " error=%d  (%s)\n", id, err, db_strerror(err));
  }

  return(err);
}

//***************************************************************************
// get_alloc_with_id_db - Returns the alloc with the given ID from the DB
//***************************************************************************

int get_alloc_with_id_db(DB_resource_t *dbr, osd_id_t id, Allocation_t *alloc)
{
  dbr_lock(dbr);
  int err = _get_alloc_with_id_db(dbr, id, alloc);
  dbr_unlock(dbr);
  return(err);
}

//***************************************************************************
// _put_alloc_db - Stores the allocation in the DB
//    Internal routine that performs no locking
//***************************************************************************

int _put_alloc_db(DB_resource_t *dbr, Allocation_t *a)
{
  int err;
  DBT key, data;


  fill_timekey(&(a->expirekey), a->expiration, a->id);
  if (a->reliability == ALLOC_SOFT) fill_timekey(&(a->softkey), a->expiration, a->id);

  memset(&key, 0, sizeof(DBT));
  memset(&data, 0, sizeof(DBT));

  key.data = &(a->id);
  key.size = sizeof(osd_id_t);

  data.data = a;
  data.size = sizeof(Allocation_t);

//db_txn("_put_alloc_db", err);
  if ((err = dbr->pdb->put(dbr->pdb, NULL, &key, &data, 0)) != 0) {
     log_printf(10, "put_alloc_db: Error storing primary key: %d id=" LU "\n", err, a->id);
     return(err);
  }
//db_commit("_put_alloc_db", err)

  time_t t = a->expiration;
  debug_printf(10, "put_alloc_db: err=%d  id=" LU ", r=%s w=%s m=%s a.size=" LU " a.max_size=" LU " expire=" TT "\n", 
      err, a->id, a->caps[READ_CAP].v, a->caps[WRITE_CAP].v, a->caps[MANAGE_CAP].v, a->size, a->max_size, t);

  return(0);
}

//***************************************************************************
// put_alloc_db - Stores the allocation in the DB
//***************************************************************************

int put_alloc_db(DB_resource_t *dbr, Allocation_t *a)
{
  int err;

  dbr_lock(dbr);
  err = _put_alloc_db(dbr, a);
  dbr_unlock(dbr);

//Allocation_t a2;
//err=get_alloc_with_cap_db(dbr, MANAGE_CAP, &(a->caps[MANAGE_CAP]), &a2);
  
//time_t t = a2.expiration;
//debug_printf(10, "put_alloc_db: READ err=%d  id=" LU ", r=%s w=%s m=%s a.size=" LU " a.max_size=" LU " expireation=" TT "\n", 
//      err, a2.id, a2.caps[READ_CAP].v, a2.caps[WRITE_CAP].v, a2.caps[MANAGE_CAP].v, a2.size, a2.max_size, t);


  return(err);
}

//***************************************************************************
// modify_alloc_iter_db - Replaces the current allocation pointed top by the iterator
//***************************************************************************

int modify_alloc_iter_db(DB_iterator_t *it, Allocation_t *a)
{
  int err;
  DBT data;

  debug_printf(10, "modify_alloc_iter_db: Start\n");

  fill_timekey(&(a->expirekey), a->expiration, a->id);
  if (a->reliability == ALLOC_SOFT) fill_timekey(&(a->softkey), a->expiration, a->id);

  memset(&data, 0, sizeof(DBT));
  data.data = a;
  data.size = sizeof(Allocation_t);

  err = it->cursor->put(it->cursor, NULL, &data, DB_CURRENT);
  if (err != 0) {
     log_printf(0, "modify_alloc_iter_db: %s\n", db_strerror(err));
  }

  return(err);
}

//***************************************************************************
// remove_alloc_iter_db - Removes the given key from the DB with an iter
//***************************************************************************

int remove_alloc_iter_db(DB_iterator_t *it)
{
  int err;

  debug_printf(10, "_remove_alloc_iter_db: Start\n");

  err = it->cursor->c_del(it->cursor, 0);
  if (err != 0) {
     log_printf(0, "remove_alloc_iter_db: %s\n", db_strerror(err));
  }

  return(err);
}

//***************************************************************************
// _remove_alloc_db - Removes the given key from the DB
//***************************************************************************

int _remove_alloc_db(DB_resource_t *dbr, Allocation_t *alloc)
{
  DBT key;
  int err;

  memset(&key, 0, sizeof(DBT));
  key.data = &(alloc->id);
  key.size = sizeof(osd_id_t);

  err = dbr->pdb->del(dbr->pdb, NULL, &key, 0);
  if (err != 0) {
     log_printf(0, "remove_alloc_db: %s\n", db_strerror(err));
  }

  return(err);
}

//***************************************************************************
// remove_alloc_db - Removes the given key from the DB
//***************************************************************************

int remove_alloc_db(DB_resource_t *dbr, Allocation_t *a)
{
  int err;

  dbr_lock(dbr);
  err = _remove_alloc_db(dbr, a);
  dbr_unlock(dbr);

  return(err);
}

//***************************************************************************
// modify_alloc_record_db - Stores the modified allocation in the DB
//***************************************************************************

int modify_alloc_db(DB_resource_t *dbr, Allocation_t *a)
{
  return(put_alloc_db(dbr, a));
}

//***************************************************************************
// _lookup_id_with_cap_db - Looks to see if the cap is stored
//***************************************************************************

int _lookup_id_with_cap_db(DB_resource_t *dbr, Cap_t *cap, int cap_type, osd_id_t *id, int *is_proxy)
{
  DBT key, data;
  Allocation_t a;

  memset(&key, 0, sizeof(DBT));
  memset(&data, 0, sizeof(DBT));

  key.data = cap->v;
  key.size = CAP_SIZE+1;

  data.data = &a;
  data.ulen = sizeof(Allocation_t);
  data.flags = DB_DBT_USERMEM;

  int err = dbr->cap[cap_type]->get(dbr->cap[cap_type], NULL, &key, &data, 0);
  if (err != 0) {
     log_printf(10, "lookup_id_with_cap_db: cap=%s err = %s\n", cap->v, db_strerror(err));
     if (err != DB_NOTFOUND) {
        raise(SIGQUIT);
     }
  }

  if (err == 0) {
     *id = a.id;
     *is_proxy = a.is_proxy;
  }

  return(err);      
}

//***************************************************************************
// get_alloc_with_cap_db - Returns the allocation with the given cap
//***************************************************************************

int get_alloc_with_cap_db(DB_resource_t *dbr, int cap_type, Cap_t *cap, Allocation_t *alloc)
{
  DBT key, data;

  memset(&key, 0, sizeof(DBT));
  memset(&data, 0, sizeof(DBT));

  log_printf(10, "get_alloc_with_cap_db: cap_type=%d cap=%s\n", cap_type, cap->v);

  key.data = cap->v;
  key.size = CAP_SIZE+1;

  data.data = alloc;
  data.ulen = sizeof(Allocation_t);
  data.flags = DB_DBT_USERMEM;

  dbr_lock(dbr); 

  log_printf(10, "get_alloc_with_cap_db:  After lock\n"); 
  int err = dbr->cap[cap_type]->get(dbr->cap[cap_type], NULL, &key, &data, 0);
  if (err != 0) {
     log_printf(0, "get_alloc_with_cap_db: cap=%s err = %s\n", cap->v, db_strerror(err));
     dbr_unlock(dbr); 
     return(err);
  }

//time_t t = alloc->expiration;
//debug_printf(10, "get_alloc_db: err=%d  id=" LU ", r=%s w=%s m=%s a.size=" LU " a.max_size=" LU " expireation=" TT "\n", 
//      err, alloc->id, alloc->caps[READ_CAP].v, alloc->caps[WRITE_CAP].v, alloc->caps[MANAGE_CAP].v, alloc->size, alloc->max_size, t);

  dbr_unlock(dbr); 

  return(err);
}

//***************************************************************************
// create_alloc_db - Creates the different unique caps and uses the existing
//      info already stored in the prefilled allocation to add an entry into
//      the DB for the resource
//***************************************************************************

int create_alloc_db(DB_resource_t *dbr, Allocation_t *a)
{
   int i, j, err;
   char key[CAP_SIZE], *b64;
//   osd_id_t id;

   dbr_lock(dbr); 

   for (i=0; i<3; i++) {   //** Get the differnt caps
//==      do {
         get_random((void *)key, CAP_BITS/8);
         b64 = g_base64_encode((const guchar *)key, CAP_BITS/8);
//         debug_printf(10, "create_alloc_db: i=%d b64 cap=%s len=" ST "\n",i, b64, strlen(b64));
         for (j=0; j<CAP_SIZE; j++) {
             if (b64[j] == '/') {
                a->caps[i].v[j] = '-';     //**IBP splits using a "/" so need to change it
             } else {
                a->caps[i].v[j] = b64[j];
             }
         } 
         a->caps[i].v[CAP_SIZE] = '\0';
         free(b64);
//===      } while (_lookup_id_with_cap_db(dbr, &(a->caps[i]), i, &id) != DB_NOTFOUND);
   }


//printf("create_alloc_db: Before put\n");
   if ((err = _put_alloc_db(dbr, a)) != 0) {   //** Now store it in the DB
      log_printf(0, "create_alloc_db:  Error in DB put - %s\n",db_strerror(err));
   }

   dbr_unlock(dbr); 

   return(err);
}

//***************************************************************************
// db_iterator_begin - Returns an iterator to cycle through the DB
//***************************************************************************

DB_iterator_t *db_iterator_begin(DB_resource_t *dbr, DB *db, DB_ENV *dbenv, int index)
{
   DB_iterator_t *it;
   int err;

//   dbr_lock(dbr);

   assert((it = (DB_iterator_t *)malloc(sizeof(DB_iterator_t))) != NULL);
   err = dbenv->txn_begin(dbenv, NULL, &(it->transaction), 0);
   if (err != 0) {                     
      log_printf(0, "db_iterator_begin: index=%d Transaction begin failed with err %s (%d)\n", index, db_strerror(err), err); 
      return(NULL);                     
    }

   err = db->cursor(db, it->transaction, &(it->cursor), 0);
   if (err != 0) {                     
      log_printf(0, "db_iterator_begin: index=%d cursor failed with err %s (%d)\n", index, db_strerror(err), err); 
      return(NULL);                     
    }


   it->id = rand();
   it->db_index = index; 
   it->dbr = dbr;

   debug_printf(10, "db_iterator_begin:  id=%d\n", it->id);

   return(it);
}

//***************************************************************************
// db_iterator_end - Closes an iterator
//***************************************************************************

int db_iterator_end(DB_iterator_t *it)
{
  int err;

  debug_printf(10, "db_iterator_end:  id=%d\n", it->id);

  it->cursor->c_close(it->cursor);

//  dbr_unlock(it->dbr);

  err = it->transaction->commit(it->transaction, DB_TXN_SYNC);
  if (err != 0) {             
     log_printf(0, "db_iterator_end: Transaction commit failed with err %s (%d)\n", db_strerror(err), err); 
  }                 

  free(it);

  
  return(err);
}

//***************************************************************************
// db_iterator_next - Returns the next record from the DB in the given direction
//
//  NOTE: This actually buffers a response to get around a deadlock with the
//        c_* commands and the normal commands which occurs if the cursor
//        is on the same record as another command is.
//
//  **** If the direction changes in mid-stride this command will not work***
// 
//***************************************************************************

int db_iterator_next(DB_iterator_t *it, int direction, Allocation_t *a)
{
  DBT key, data;
  int err;
  osd_id_t id;
  DB_timekey_t tkey;
  Cap_t cap;

  err = -1234;

  memset(&key, 0, sizeof(DBT));  
  memset(&data, 0, sizeof(DBT));

  key.flags = DB_DBT_USERMEM;
  data.flags = DB_DBT_USERMEM;

  data.data = a;
  data.ulen = sizeof(Allocation_t);

  switch (it->db_index) {
    case (DB_INDEX_ID):
        key.data = &id;
        key.ulen = sizeof(id);
 
        err = it->cursor->c_get(it->cursor, &key, &data, direction);  //** Read the 1st
        if (err != 0) {
           log_printf(10, "db_iterator_next: key_index=%d err = %s\n", it->db_index, db_strerror(err));
        }
        return(err);
        break;
    case (DB_INDEX_READ):
    case (DB_INDEX_WRITE):
    case (DB_INDEX_MANAGE):
        key.data = cap.v;
        key.ulen = sizeof(Cap_t);
 
        err = it->cursor->c_get(it->cursor, &key, &data, direction);  //** Read the 1st
        if (err != 0) {
           log_printf(10, "db_iterator_next: key_index=%d err = %s\n", it->db_index, db_strerror(err));
           return(err);
        }
        break;
    case DB_INDEX_EXPIRE:
    case DB_INDEX_SOFT:
        key.data = &tkey;
        key.ulen = sizeof(tkey);

        err = it->cursor->c_get(it->cursor, &key, &data, direction);  //** Read the 1st
        if (err != 0) {
           log_printf(10, "db_iterator_next: key_index=%d err = %s\n", it->db_index, db_strerror(err));
           return(err);
        }
        break;
    default:
        log_printf(0, "db_iterator_next:  Invalid key_index!  key_index=%d\n", it->db_index);
        return(-1);
  }

  return(0);   
}


//***************************************************************************
// expire_iterator - Returns a handle to iterate through the expire DB from
//     oldest to newest times 
//***************************************************************************

DB_iterator_t *expire_iterator(DB_resource_t *dbr)
{
   return(db_iterator_begin(dbr, dbr->expire, dbr->dbenv, DB_INDEX_EXPIRE));
}

//***************************************************************************
// soft_iterator - Returns a handle to iterate through the soft DB from
//     oldest to newest times 
//***************************************************************************

DB_iterator_t *soft_iterator(DB_resource_t *dbr)
{
   return(db_iterator_begin(dbr, dbr->soft, dbr->dbenv, DB_INDEX_SOFT));
}

//***************************************************************************
// id_iterator - Returns a handle to iterate through all the id's 
//***************************************************************************

DB_iterator_t *id_iterator(DB_resource_t *dbr)
{
   return(db_iterator_begin(dbr, dbr->pdb, dbr->dbenv, DB_INDEX_ID));
}

//***************************************************************************
// cap_iterator - Returns a handle to iterate through the given cp index 
//***************************************************************************

DB_iterator_t *cap_iterator(DB_resource_t *dbr, int cap_type)
{
   return(db_iterator_begin(dbr, dbr->cap[cap_type], dbr->dbenv, DB_INDEX_READ+cap_type));
}

//***************************************************************************
// set_expire_iterator - Sets the position for the hard iterator
//***************************************************************************

int set_expire_iterator(DB_iterator_t *dbi, time_t t, Allocation_t *a)
{
  int err;
  DB_timekey_t tk;
  DBT key, data;

  memset(&key, 0, sizeof(DBT)); 
  memset(&data, 0, sizeof(DBT)); 

  key.flags = DB_DBT_USERMEM; 
  data.flags = DB_DBT_USERMEM; 

  key.ulen = sizeof(DB_timekey_t);
  key.data = fill_timekey(&tk, t, 0);
  data.ulen = sizeof(Allocation_t);
  data.data = a;

  if ((err = dbi->cursor->get(dbi->cursor, &key, &data, DB_SET)) != 0) {
     log_printf(5, "set_expire_iterator: Error with get!  time=" TT " * error=%d\n", t, err);
     return(err);
  }

  log_printf(15, "set_expire_iterator: t=" TT " id=" LU "\n", t, a->id);

  return(0);
}


