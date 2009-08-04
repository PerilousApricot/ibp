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

#include <stdio.h>
#include <stdint.h>
#include <stdlib.h>
#include <pthread.h>
#include <assert.h>
#include "log.h"
#include "pigeon_hole.h"

//***************************************************************************
//  release_pigeon_hole - releases a pigeon hole for use
//***************************************************************************

void release_pigeon_hole(pigeon_hole_t *ph, int slot)
{
  pthread_mutex_lock(&(ph->lock));
  ph->hole[slot] = 0;
  pthread_mutex_unlock(&(ph->lock));
}

//***************************************************************************
//  reserve_pigeon_hole - Allocates a pigeon hole
//***************************************************************************

int reserve_pigeon_hole(pigeon_hole_t *ph)
{
  int i;

  pthread_mutex_lock(&(ph->lock));
  for (i=0; i<ph->nholes; i++) {
     if (ph->hole[i] == 0) {
        ph->hole[i] = 1;
//        log_printf(0, "reserve_pigeon_hole: slot=%d\n", i);
        pthread_mutex_unlock(&(ph->lock));
        return(i);
     }
  }
  pthread_mutex_unlock(&(ph->lock));

  return(-1);
}


//***************************************************************************
// destroy_pigeon_hole - Destroys a pigeon hole structure
//***************************************************************************

void destroy_pigeon_hole(pigeon_hole_t *ph)
{
  pthread_mutex_destroy(&(ph->lock));
  free(ph->hole);
  free(ph);
}

//***************************************************************************
// new_pigeon_hole - Creates a new pigeon hole structure
//***************************************************************************

pigeon_hole_t *new_pigeon_hole(int size)
{
  pigeon_hole_t *ph = (pigeon_hole_t *)malloc(sizeof(pigeon_hole_t));
  assert(ph != NULL);

  ph->hole = (char *)malloc(size);
  assert(ph->hole != NULL);

//log_printf(0, "new_pigeon_hole: size=%d\n", size);
  memset(ph->hole, 0, size);
  pthread_mutex_init(&(ph->lock), NULL);
  ph->nholes = size;

  return(ph);
}


