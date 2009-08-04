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

//**************************************************************************
//
//  Provides a simple DNS cache
//
//**************************************************************************

#include <pthread.h>
#include "dns_cache.h"
#include <assert.h>
#include <limits.h>
#include <stdlib.h>
#include <string.h>
#include <stdio.h>
#include <unistd.h>

#include "log.h"
#include "fmttypes.h"

#define BUF_SIZE 128
#define ADDR_LEN 4
typedef struct {
   char name[BUF_SIZE];
   char addr[ADDR_LEN];
   int  count;
} DNS_entry_t;

typedef struct {
   DNS_entry_t *list;
   int size;
   long int count;
   pthread_mutex_t mutex;
} DNS_cache_t;

DNS_cache_t *_cache;

#ifdef _DARWIN
int gethostbyname2_r (const char *name, int af,
         struct hostent *ret, char *buf, size_t buflen,
         struct hostent **result, int *h_errnop) {


    struct hostent *h;

    h = gethostbyname2(name, af);
    if (h == NULL) {
       return(1);
    } else {
       memcpy(ret, h, sizeof(struct hostent));
       *result = ret;
       return(0);
    }
   
}

#endif

//**************************************************************************
//  lookup_host - Looks up the host.  Make sure that the lock/unlock routines
//      are used to make it threadsafe!
//**************************************************************************
   
int lookup_host(char *name, char *addr) {
  int i, oldest_slot, slot;
  long int oldest_count;
  DNS_entry_t *list;
  int retcode;

log_printf(20, "lookup_host: start time=" TT "\n", time(NULL));

  if (name[0] == '\0') return(1);  //** Return early if name is NULL

  pthread_mutex_lock(&(_cache->mutex));
 
  bzero(addr,ADDR_LEN);

  retcode = 1;

  _cache->count++;

  list = _cache->list;

  i = 0;  oldest_slot = 0; oldest_count = list[i].count;
  while ((i<(_cache->size-1)) && (strcmp(name, list[i].name) != 0)) {
     i++;
     if (oldest_count > list[i].count) {
        oldest_slot = i;
        oldest_count = list[i].count;
     }
  }

  if (strcmp(name, list[i].name) == 0) {  //*** Found it!
     list[i].count = _cache->count;
     memcpy(addr,&(list[i].addr), ADDR_LEN);
     retcode = 0;
     log_printf(20, "lookup_host: Found %s in cache slot %d  (%s) \n", name, i, list[i].name);
     log_printf(20, "lookup_host: Found %s in cache slot %d  (%s,%hhu.%hhu.%hhu.%hhu)\n", name, i, list[i].name, 
             list[i].addr[0],list[i].addr[1],list[i].addr[2],list[i].addr[3]); 
     //flush_log();

  } else {   //*** Got to add it
     char buf[10000];
     struct hostent hostbuf, *host;
     int err;

     memset(&hostbuf, 0, sizeof(struct hostent));

     slot = oldest_slot;

     log_printf(10, "lookup_host:  Before gethostbyname Hostname: %s\n", name); flush_log();
     i = gethostbyname2_r(name, AF_INET, &hostbuf, buf, 10000, &host, &err);
     log_printf(10, "lookup_host:  Hostname: %s  err=%d\n", name, err);
//     n = 0; 
//     while (!host && (n<4)) {
//        log_printf(10, "lookup_host gethostbyname failed.  Hostname: %s  err=%d\n", name, err);
//        sleep(1);
//        i = gethostbyname2_r(name, AF_INET, &hostbuf, buf, 10000, &host, &err);
//        n++;
//     }

     if (host) {  //** Got it
        list[slot].count = _cache->count;
        strncpy(list[slot].name, name, BUF_SIZE);
        list[slot].name[BUF_SIZE-1] = '\0';
        memcpy(addr,host->h_addr, ADDR_LEN);
        memcpy(&(list[slot].addr),host->h_addr, host->h_length);
        log_printf(15, "lookup_host: Added %s to slot %d  (%s,%hhu.%hhu.%hhu.%hhu) h_len=%d\n", name, slot, host->h_name, 
             list[slot].addr[0],list[slot].addr[1],list[slot].addr[2],list[slot].addr[3], host->h_length);
        retcode = 0;
     } else {
      log_printf(10, "lookup_host: %s not found!\n", name);
     }
  }

  if (_cache->count == LONG_MAX) {  //*** Got to renumber the counts
    _cache->count = 0;
    for (i=0; i<_cache->size; i++) {  //This is nonoptimal but shouldn't matter
        if (list[i].count > 0) list[i].count = _cache->count++;
    }  
  }

  pthread_mutex_unlock(&(_cache->mutex));

log_printf(20, "lookup_host: end time=" TT "\n", time(NULL));

  return(retcode);
}

//**************************************************************************

void dns_cache_init(int size) {
   int i;

   log_printf(20, "dns_cache_init: Start!!!!!!!!!!!!\n");

   _cache = (DNS_cache_t *)malloc(sizeof(DNS_cache_t));
   assert(_cache != NULL);

   _cache->list = (DNS_entry_t *)malloc(sizeof(DNS_entry_t)*size);
   assert(_cache->list != NULL);

   _cache->count = 0;
   _cache->size = size;

   for (i=0; i<size; i++) {
       _cache->list[i].count = -1;
       _cache->list[i].name[0] = '\0';
   }

  pthread_mutex_init(&(_cache->mutex), NULL);
  pthread_mutex_unlock(&(_cache->mutex));

   log_printf(20, "dns_cache_init: End!!!!!!!!!!!!\n");

  return;
}

//**************************************************************************

void finalize_dns_cache() {
   free(_cache->list);
   free(_cache);
}


