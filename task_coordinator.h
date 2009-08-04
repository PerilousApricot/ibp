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

//*******************************************************************************
//*******************************************************************************


#ifndef __TASK_COORD_H_
#define __TASK_COORD_H_

#include <pthread.h>
#include <glib.h>
#include "allocation.h"
#include "resource.h"
#include "stack.h"

#define TASK_READ   0    //** Read request
#define TASK_WRITE  1    //** Write request
#define TASK_MANAGE 2    //** Manage request

#define TASK_READ_QUE  0
#define TASK_WRITE_QUE 1

#define TASK_LOCK_GRANTED 0
#define TASK_LOCK_QUEUED  1
#define TASK_LOCK_ERROR   2

typedef struct {
   char *cid;         //** Character version ofthe ID
 char *dummycid;      //** Strctly for debugging purposes.. REMOVE
   Resource_t *r;     //** Resource for Allocation
   Allocation_t a;    //** Most recent copy of the allocation structure
   int op;            //** Mode the manager should use for controlling access
   int refcount;      //** Reference count
   int lock_inuse;    //** Signifies the que is already in use
   int owner;         //** Current Q owner
   Stack_t *io_que[2] ;  //** Queue of readers/writers
   pthread_mutex_t qlock;
} Task_que_t;

typedef struct {
   pthread_mutex_t lock;
   GHashTable  *table;
} Task_coord_t;

#endif
