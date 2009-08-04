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

#ifndef __OSD_ABSTRACT_H
#define __OSD_ABSTRACT_H

#include <sys/types.h>
#include "statfs.h"

using namespace std;

typedef u_int64_t osd_id_t;
typedef void * buffer_t;

class osd_abstract {

public:
    virtual ~osd_abstract() { };
    virtual osd_id_t create_id() = 0;    // Returns an OSD object.  Think of it as a filename
    virtual int reserve(osd_id_t id, size_t len) = 0;  // Reserve space for the file
    virtual int remove(osd_id_t id) = 0;  // Remove the object
    virtual int truncate(osd_id_t id, size_t len) = 0;  // Remove the object
    virtual size_t size(osd_id_t) = 0;      // Object size in bytes
    virtual int read(osd_id_t id, off_t offset, size_t len, buffer_t buffer) = 0;   //Read data
    virtual int write(osd_id_t id, off_t offset, size_t len, buffer_t buffer) = 0;  //Store data to disk
    virtual bool id_exists(osd_id_t id) = 0;   //Determine if the id currently exists
    virtual int statfs(struct statfs *buf) = 0;    // Get File system stats
    virtual void *new_iterator() = 0;
    virtual void destroy_iterator(void *arg) = 0;
    virtual int iterator_next(void *arg, osd_id_t *id) = 0;
    virtual int umount() = 0;
};

#endif
