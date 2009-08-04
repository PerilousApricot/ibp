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

#ifndef __OSD_EBOFS_H
#define __OSD_EBOFS_H

#include <iostream>
#include <fstream>
#include <cstring>
#include <sys/types.h>
#include <dirent.h>
#include <stdint.h>
#include <time.h>
#include <list>
#include "common/Mutex.h"
#include "osd_abstract.h"
#include <openssl/rand.h>
#include "random.h"
#include "ebofs/Ebofs.h"
#include "statfs.h"

using namespace std;

typedef struct {                   //** Iterator for EBOFS
    int n;                         //** Number of allocations
    list<pobject_t> * ls;             //** List of objects
    list<pobject_t>::iterator it;   //** Object iterator
    bool isnew;
} osd_ebofs_iter_t;

class osd_ebofs : public osd_abstract {
private:
    Ebofs fs;
    Mutex lock;
    int id_len; 
    int read_statfs;
    struct statfs fsbuf;
    uint64_t _generate_key();
    uint64_t _str2uint64(char *sid);
    char  *_uint642str(uint64_t key);
    void elock();
    void eunlock();
    
public:
    osd_ebofs(char *device, int do_mkfs) : fs(device){
       id_len = 21;
       read_statfs = 1;  //** fudge for non-threadsafe EBOFS driver

       if (do_mkfs) fs.mkfs();

       fs.mount();
//****CHECK ME????       assert(fs.mount()>=0);

       init_random();  //Randomize the initial key
    };

    ~osd_ebofs() { };  
    osd_id_t create_id();            // Returns an OSD object.  Think of it as a filename
    int reserve(osd_id_t id, size_t len) { return(0); };
    int remove(osd_id_t id);         // Remove the object
    int truncate(osd_id_t id, size_t size);       // truncate the object
    size_t size(osd_id_t);           // Return the size of the object 
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
