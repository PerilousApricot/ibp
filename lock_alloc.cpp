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

//******************************************************************
//******************************************************************

#include <assert.h>
#include <time.h>
#include <stdio.h>
#include <stdint.h>
#include <stdlib.h>
#include <pthread.h>
#include "osd_abstract.h"
#include "fmttypes.h"
#include "log.h"

//******** These are for the directory splitting ****
#define LOCK_BITS    8
#define LOCK_BITMASK 0x00FF
#define LOCK_MAX     256

#define id_slot(id) (id & LOCK_BITMASK)

pthread_mutex_t lock_table[LOCK_MAX];

//******************************************************************
//  lock_osd_id - Locks the ID.  Blocks until a lock can be acquired
//******************************************************************

void lock_osd_id(osd_id_t id)
{
   pthread_mutex_lock(&(lock_table[id_slot(id)]));
}

//******************************************************************
//  unlock_osd_id - Unlocks the ID.
//******************************************************************

void unlock_osd_id(osd_id_t id)
{
   pthread_mutex_unlock(&(lock_table[id_slot(id)]));
}

//******************************************************************
//  lock_alloc_init - Initializes the allocation locking routines
//******************************************************************

void lock_alloc_init()
{
  int i;

  for (i=0; i<LOCK_MAX; i++) {
     pthread_mutex_init(&(lock_table[i]), NULL);
  }
}

//******************************************************************
//  lock_alloc_destroy - Destroys the allocation locking data
//******************************************************************

void lock_alloc_destroy()
{
  int i;

  for (i=0; i<LOCK_MAX; i++) {
     pthread_mutex_destroy(&(lock_table[i]));
  }
}


