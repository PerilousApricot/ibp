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

//**************************************************
//
//**************************************************

#ifndef __OSD_FS_H
#define __OSD_FS_H

#include <iostream>
#include <fstream>
#include <cstring>
#include <sys/stat.h>
#include <sys/types.h>
#include <dirent.h>
#include <stdint.h>
#include <time.h>
#include "common/Mutex.h"
#include "osd_abstract.h"
#include <openssl/rand.h>
#include "random.h"
#include <errno.h>
#include "statfs.h"

using namespace std;

//******** These are for the directory splitting ****
#define DIR_BITS    8
#define DIR_BITMASK 0x00FF
#define DIR_MAX     256

#define XFS_MOUNT 1

typedef struct {
   DIR *cdir;            //** DIR handle
   int  n;               //** Directory number
   struct dirent entry;  //** Used by readdir_r
} osd_fs_iter_t;

class osd_fs : public osd_abstract {
private:
    char *devicename;
    int  pathlen;
    int  mount_type;
    Mutex lock;
    uint64_t _generate_key();
    char *_id2fname(osd_id_t id, char *fname, int len);
    int open_fs_dir(osd_fs_iter_t *iter);
    int reserve(osd_id_t id, size_t len);
        
public:
    osd_fs(const char *device);
    ~osd_fs() { free(devicename); };

    osd_id_t create_id();            // Returns an OSD object.  Think of it as a filename
    int remove(osd_id_t id);         // Remove the object
    int truncate(osd_id_t id, size_t size);       // truncate the object
    size_t size(osd_id_t);           // Object size in bytes
    int read(osd_id_t id, off_t offset, size_t len, buffer_t buffer);   //Read data
    int write(osd_id_t id, off_t offset, size_t len, buffer_t buffer);  //Store data to disk
    bool id_exists(osd_id_t id);   //Determine if the id currently exists      
    int statfs(struct statfs *buf);    // Get File system stats
    void *new_iterator();
    void destroy_iterator(void *arg);
    int iterator_next(void *arg, osd_id_t *id);
    int umount();
};

#endif
