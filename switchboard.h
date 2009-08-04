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


#ifndef __SWITCHBOARD_H_
#define __SWITCHBOARD_H_

#include <pthread.h>
#include <glib.h>
#include "allocation.h"
#include "network.h"
#include "resource.h"

#define SWITCH_READ   0    //** Read request
#define SWITCH_WRITE  1    //** Write request
#define SWITCH_MANAGE 2    //** Manage request

#define LOCK_GRANTED 0    //** Permission to perform the operation
#define LOCK_QUEUED  1    //** Placed in a queued or waiting state
#define LOCK_DENIED  2    //** Request has been unequivocally denied
#define LOCK_ERROR   3    //** Request encountered an error like missing allocation

typedef struct {
   Resource_t *r;          //** Resource managing the allocation
   Allocation_t a;         //** Most recent copy of the allocation structure
   NetStream_t *ns_read;   //** Reading stream
   NetStream_t *ns_write;  //** Writing stream
   NetStream_t *ns_manage; //** Writing stream
   int readcount;          //** Current count of readers to allocation
   int writecount;         //** Current count of writers to allocation
   int readbytes;
   int writebytes;
} Switch_t;

typedef struct {
   pthread_mutex_t lock;
   GHashTable  *table;
} Switchboard_t;

#endif
