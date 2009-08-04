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

#include "random.h"
#include <pthread.h>
#include <openssl/rand.h>
#include <assert.h>

pthread_mutex_t lock;

//*******************************************************************
//  init_random - Inititalizes the random number generator for use
//*******************************************************************

int init_random()
{
   long max_bytes = 1024;

   assert (RAND_load_file("/dev/urandom", max_bytes) == max_bytes);

   pthread_mutex_init(&lock, NULL);
   return(0);
}

//*******************************************************************
// get_random - Gets nbytes  of random data and placed it in buf.
//*******************************************************************
int get_random(void *buf, int nbytes)
{
   int err;

   pthread_mutex_lock(&lock);
   err = RAND_bytes((unsigned char *)buf, nbytes);
   pthread_mutex_unlock(&lock);

   return(err);
}

