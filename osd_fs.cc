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

//*******************************************

#define _XOPEN_SOURCE 600
#include <fcntl.h>
#include <stdlib.h>
#include <stdio.h>
#include <unistd.h>
#include <sys/types.h>
#include "osd_fs.h"
#include "fmttypes.h"
#include "log.h"
#ifdef _HAS_XFS
#include <xfs/xfs.h>
#endif

//**************************************************
// _generate_key - Generates a random 64 bit number 
//**************************************************

osd_id_t osd_fs::_generate_key() {     
   osd_id_t r;
  
   get_random(&r, sizeof(osd_id_t));
//cout << "Key: " << r << "\n";

   return(r); 
}

//**************************************************
// _id2fname - Converts an id to a filename
//     The filename is stored in fname. Additionally
//     the return is also fname.
//**************************************************

char *osd_fs::_id2fname(osd_id_t id, char *fname, int len) {
   int dir = (id & DIR_BITMASK);

   snprintf(fname, len, "%s/%d/" LU "", devicename, dir, id);

//   printf("_id2fname: fname=%s\n", fname);
 
   return(fname);
}

//**************************************************
//  reserve - Preallocates space for allocation
//**************************************************

int osd_fs::reserve(osd_id_t id, size_t len) {
//  if (mount_type != XFS_MOUNT) return(0);

  char fname[pathlen];
  int err, fd;

//log_printf(10, "osd_fs: reserve(" LU ", " ST ")\n", id, len);

   FILE *fout = fopen(_id2fname(id, fname, sizeof(fname)), "r+");
   if (fout == NULL) fout = fopen(fname, "w");
   
   if (fout == NULL) { 
      log_printf(0, "osd_fs:reserve: can't open id=" LU "\n", id);
      return -1; 
   }

   fd = fileno(fout);

   if (mount_type == 0) {
log_printf(10, "osd_fs: POSIX reserve(" LU ", " ST ")\n", id, len);
      posix_fallocate(fd, 0, len);
/****************
   int bufsize = 2 * 1024 *1024;
   char buffer[bufsize];
   int nblocks = len / bufsize;
   int rem = len % bufsize;
   int i;
   memset(buffer, 0, bufsize);
   for (i=0; i<nblocks; i++) {
      fwrite(buffer, 1, bufsize, fout);
   }
   fwrite(buffer, 1, rem, fout);
i=nblocks*bufsize + rem;
log_printf(10, "osd_fs: reserve count=%d\n", i);
sleep(1);
******************/
   } else {
log_printf(10, "osd_fs: XFS reserve(" LU ", " ST ")\n", id, len);

#ifdef _HAS_XFS
      xfs_flock64_t arg;

      memset(&arg, 0, sizeof(arg));
      arg.l_whence = 0; arg.l_start = 0; arg.l_len = len;      
      if ((err = xfsctl(fname, fd, XFS_IOC_ALLOCSP64, &arg)) != 0) {
         log_printf(0, "osd_fs:reserve: xfsctl returned an error=%d id=" LU "\n", err, id);
         return(-1);
      }
#endif
   }

  fclose(fout);

  return(0);
}


//**************************************************
// id_exists - Checks to see if the ID exists
//**************************************************

bool osd_fs::id_exists(osd_id_t id) {
  fstream fin;
  char fname[pathlen];

  fin.open(_id2fname(id, fname, sizeof(fname)),ios::in);
  if( fin.is_open() )
  {
    fin.close();
    return true;
  }
  fin.close();
  return false;
}

//**************************************************
// create_id - Creates a new object id for use.
//**************************************************

osd_id_t osd_fs::create_id() {
   lock.Lock();

   osd_id_t id;
//   char  fname[pathlen];

   do {      //Generate a unique key
      id = _generate_key();
//cout << "FS: ID name: " << id << "\n";
   } while (id_exists(id));

//   ofstream ofs(_id2fname(id, fname), ios::out);  
//Q   FILE *fd = fopen(_id2fname(id, fname, sizeof(fname)), "w");   //Make sure and create the file so no one else can use it
//Q   fclose(fd);

   lock.Unlock();
   
   return(id);
}

//**************************************************
//  remove - Removes the id
//**************************************************

int osd_fs::remove(osd_id_t id) {
//cout << "remove(" << id << ")\n";
   char fname[pathlen];

   return(::remove(_id2fname(id, fname, sizeof(fname))));
}

//*************************************************************
// truncate - truncates a file to 0
//*************************************************************

int osd_fs::truncate(osd_id_t id) {
   char fname[pathlen];
   FILE *fout = fopen(_id2fname(id, fname, sizeof(fname)), "w");

   if (fout == NULL) return(-1);

   fclose(fout);

   return(0);
}

//*************************************************************
// size - Returns the file size in bytes
//*************************************************************

size_t osd_fs::size(osd_id_t id) {
   char fname[pathlen];
   FILE *fout = fopen(_id2fname(id, fname, sizeof(fname)), "r");

   if (fout == NULL) return(-1);

   fseeko(fout, 0, SEEK_END);
   size_t n = ftell(fout);

   fclose(fout);

   return(n);
}

//*************************************************************
//  write - Stores data to an id given the offset and length
//*************************************************************

int osd_fs::write(osd_id_t id, off_t offset, size_t len, buffer_t buffer) {
//cout << "write(" << id << ")\n";
   char fname[pathlen];
   int n;
   int err = 0;

   int flags = O_CREAT | O_WRONLY;
   int fd = ::open(_id2fname(id, fname, sizeof(fname)), flags); 
   if (fd == -1) {
      log_printf(0, "osd_fs:write(" LU ", " OT ", " ST ", ...advice..)open error = %d\n", id, offset, len, errno);
      return(-1);
   }

//   err = posix_fadvise(fd, offset, len, POSIX_FADV_DONTNEED);

   lseek(fd, offset, SEEK_SET);
   n = ::write(fd, buffer, len);
   if (n != len) {
      log_printf(0, "osd_fs:write(" LU ", " OT ", " ST ", ...advice..) write error = %d n=%d fd=%d\n", id, offset, len, errno, n, fd);
   }

   err = posix_fadvise(fd, offset, len, POSIX_FADV_DONTNEED);

   close(fd);

   return(n);
}

//*************************************************************
//  read - Reads data from the id given at offset and length
//*************************************************************

int osd_fs::read(osd_id_t id, off_t offset, size_t len, buffer_t buffer) {
   char fname[pathlen];
   int err, n;
   
   int flags = O_CREAT | O_RDONLY;
   int fd = ::open(_id2fname(id, fname, sizeof(fname)), flags); 
   if (fd == -1) {
      log_printf(0, "osd_fs:read(" LU ", " OT ", " ST ", ...advice..) open error = %d\n", id, offset, len, errno);
      return(-1);
   }

//   err = posix_fadvise(fd, offset, len, POSIX_FADV_SEQUENTIAL);

   lseek(fd, offset, SEEK_SET);
   n = ::read(fd, buffer, len);
   if (n != len) {
      log_printf(0, "osd_fs:read(" LU ", " OT ", " ST ", ...advice..) read error = %d n=%d fd=%d\n", id, offset, len, errno, n, fd);
   }

   close(fd);
   
   return(n);
}

//*************************************************************
// statfs - Determine the file system stats 
//*************************************************************

int osd_fs::statfs(struct statfs *buf)
{
  return(::statfs(devicename, buf));
}

//*************************************************************
//  open_fs_dir - Opens a sub dir for the OSD
//*************************************************************

int osd_fs::open_fs_dir(osd_fs_iter_t *iter)
{
   char dname[pathlen];

   snprintf(dname, sizeof(dname), "%s/%d", devicename, iter->n);
   iter->cdir = opendir(dname);

   if (iter->cdir == NULL) return(1);
   return(0);
}
//*************************************************************
//  new_iterator - Creates a new iterator to walk through the files
//*************************************************************

void *osd_fs::new_iterator()
{
   osd_fs_iter_t *iter = (osd_fs_iter_t *)malloc(sizeof(osd_fs_iter_t));

   if (iter != NULL) {
      iter->n = 0;
      if (open_fs_dir(iter) != 0) return(NULL);
   }

   return((void *)iter);
}

//*************************************************************
//  destroy_iterator - Destroys an iterator
//*************************************************************

void osd_fs::destroy_iterator(void *arg)
{
  osd_fs_iter_t *iter = (osd_fs_iter_t *)arg;

  closedir(iter->cdir);

  free(iter);
}

//*************************************************************
//  iterator_next - Returns the next key for the iterator
//*************************************************************

int osd_fs::iterator_next(void *arg, osd_id_t *id)
{
  osd_fs_iter_t *iter = (osd_fs_iter_t *)arg;
  struct dirent *result;
  int finished;

  if (iter->n >= DIR_MAX) return(1);

  finished = 0;
  do {
    readdir_r(iter->cdir, &(iter->entry),  &result);

    if (result == NULL) {   //** Change dir or we're finished
       iter->n++;
       if (iter->n == DIR_MAX) return(1);   //** Finished
       closedir(iter->cdir);
       if (open_fs_dir(iter) != 0) return(1);   //*** Error opening the directory
    } else if ((strcmp(result->d_name, ".") != 0) && (strcmp(result->d_name, "..") != 0))
      finished = 1;               //** Found a valid file
    }
  while (finished == 0);

  sscanf(result->d_name, LU, id);
  return(0);
}

//*************************************************************
// osd_fs contructor - Mounts the device
//*************************************************************
osd_fs::osd_fs(const char *device) {
   devicename = strdup(device);
   pathlen = strlen(devicename) + 50;

   DIR *dir;
   if ((dir = opendir(devicename)) == NULL) {
      cout << "osd_fs:  Directory does not exist!!!!!! checked: " << device << "\n";
   } else {
      closedir(dir);
   }

   //*** Check and make sure all the directories exist ***
   int i;
   char fname[pathlen];
   for (i=0; i<DIR_MAX; i++) {
      snprintf(fname, sizeof(fname), "%s/%d", devicename, i);
      if (mkdir(fname, S_IRUSR | S_IWUSR | S_IXUSR) != 0) {
         if (errno != EEXIST) {
            printf("osd_fs: Error creating/checking sub directories!  errno=%d\n", errno);
            printf("osd_fs: Directory : %s\n", fname);
            abort();
         }
      }
   }

   mount_type = 0;

#ifdef _HAS_XFS
   mount_type = platform_test_xfs_path(device);
#endif

   log_printf(10, "osd_fs_mount: %s mount_type=%d\n", device, mount_type);

   init_random();  // Make sure and initialize the random number generator;
}

//*************************************************************
// umount - Unmounts the dir.  Does nothing.
//*************************************************************

int osd_fs::umount()
{
  return(0);
}

