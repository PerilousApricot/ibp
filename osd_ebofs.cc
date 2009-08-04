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


#include "osd_abstract.h"
#include "osd_ebofs.h"
#include <cstdlib>
#include <stdio.h>
#include <unistd.h>
#include <sys/types.h>
#include <list>
#ifndef _OSD_EBOFS

//******** Dummy headers if no EBOFS support is needed ********
bool osd_ebofs::id_exists(osd_id_t cid) { return(true); }
osd_id_t osd_ebofs::create_id() { return(0); }
int osd_ebofs::remove(osd_id_t cid) { return(-1); }
int osd_ebofs::truncate(osd_id_t cid, size_t size) { return(-1); }
int osd_ebofs::write(osd_id_t cid, off_t offset, size_t len, buffer_t buffer) { return(-1); }
int osd_ebofs::read(osd_id_t cid, off_t offset, size_t len, buffer_t buffer) { return(-1); }
size_t osd_ebofs::size(osd_id_t cid) { return(0); };
int osd_ebofs::statfs(struct statfs *buf) { return(-1); }
void *osd_ebofs::new_iterator() { return(NULL); }
void osd_ebofs::destroy_iterator(void *arg) {  }
int osd_ebofs::iterator_next(void *arg, osd_id_t *id) { return(-1); }
int osd_ebofs::umount() { return(-1); }
#else

//**************************************************
//  elock - Lock fs
//**************************************************

void osd_ebofs::elock()
{
//  lock.Lock();
}

//**************************************************
//  eunlock - Unlock fs
//**************************************************

void osd_ebofs::eunlock()
{
//  lock.Unlock();
}

//**************************************************
// _generate_key - Generates a random 64 bit number 
//**************************************************

osd_id_t osd_ebofs::_generate_key() {     
  osd_id_t r;

  get_random(&r, sizeof(osd_id_t));

//cout << "Key: " << r << "\n";

   return(r); 
}

//**************************************************
// _uint642str - Converts a 64-bit number to a string
//    NOTE: the string needs to be freed by the calling program
//**************************************************

char *osd_ebofs::_uint642str(uint64_t key) {     
   char *id = new char[id_len];

   snprintf(id, id_len, "%lu", key);

   return(id); 
}

//**************************************************
// _str2uint64 - Converts a string to a 64-bit number 
//**************************************************

uint64_t osd_ebofs::_str2uint64(char *cid) {     
   uint64_t id;
   sscanf(cid, "%lu", &id);
//cout << "_str2uint64 cid:" << cid << " id: " << id << "\n";

   return(id); 
}

//**************************************************
// id_exists - Checks to see if the ID exists
//**************************************************

bool osd_ebofs::id_exists(osd_id_t cid) {
  object_t oid(cid, 0);
  pobject_t id(0,0,oid);

  elock();
  int err = fs.exists(0, id);
  eunlock();

  return(err);
}

//**************************************************
// create_id - Creates a new object id for use.
//**************************************************

osd_id_t osd_ebofs::create_id() {
   elock();

   pobject_t id;

   do {      //Generate a unique key
      id.oid.ino = _generate_key();
//cout << "ID name: " << id.oid.ino << "\n";
   } while (fs.exists(0, id));

   fs.truncate(0, id, 0);  //Make sure and create the file so no one else can use it
      
   eunlock();
   
   return(id.oid.ino);
}

//**************************************************
//  remove - Removes the id
//**************************************************

int osd_ebofs::remove(osd_id_t cid) {
//cout << "remove(" << id << ")\n";
   object_t oid(cid, 0);
   pobject_t id(0,0,oid);

   elock();
   int err = fs.remove(0, id);
   eunlock();
   
   return(err);
}

//**************************************************
//  truncate - truncate the id to 0 bytes
//**************************************************

int osd_ebofs::truncate(osd_id_t cid, size_t size) {
   object_t oid(cid, 0);
   pobject_t id(0,0,oid);

   elock();
   int err = fs.truncate(0, id, size, 0);
   eunlock();
   
   return(err);
}

//**************************************************
//  size - Returns the size of the object in bytes
//$$$$$$$$$$$$$$$$$$$$$$$$ FIX ME $$$$$$$$$$$$$$$$$$$$$$$$$$$
//**************************************************

size_t osd_ebofs::size(osd_id_t cid) {
   object_t oid(cid, 0);
   pobject_t id(0,0,oid);

   fprintf(stderr, "osd_ebofs: size not currently supported!\n");
   printf("osd_ebofs: size not currently supported!\n");

   return(0);
}


//*************************************************************
//  write - Stores data to an id given the offset and length
//*************************************************************

int osd_ebofs::write(osd_id_t cid, off_t offset, size_t len, buffer_t buffer) {
//cout << "write(" << cid << ")\n";
   object_t oid(cid, 0); 
   pobject_t id(0,0,oid);
   elock();

   bufferptr wp((char *)buffer, len);
   bufferlist w;
   w.append(wp);

   int err = fs.write(0, id, offset, len, w, 0);
   eunlock();

   return(err);  
}

//*************************************************************
//  read - Reads data from the id given at offset and length
//*************************************************************

int osd_ebofs::read(osd_id_t cid, off_t offset, size_t len, buffer_t buffer) {
   object_t oid(cid, 0); 
   pobject_t id(0,0,oid);

   bufferlist bl;

   elock();
   int err = fs.read(0, id, offset, len, bl);
   int l = MIN(len,bl.length());
   if (l) bl.copy(0, l, (char *)buffer);
   eunlock();
   
   return(err);
}

//*************************************************************
// statfs - Determine the file system stats 
//*************************************************************

int osd_ebofs::statfs(struct statfs *buf)
{

   elock();
   int err = fs.statfs(buf);  //** This is what the routine should be if not for the threading bug in EBOFS
   eunlock();

   return(err);

//  if (read_statfs == 1) {
//      read_statfs = 0;
//      fs.statfs(&fsbuf);
//  }

//  *buf = fsbuf;
//  return(0);
}

//*************************************************************
//  new_iterator - Creates a new iterator to walk through the files
//*************************************************************

void *osd_ebofs::new_iterator()
{
   osd_ebofs_iter_t *iter = (osd_ebofs_iter_t *)malloc(sizeof(osd_ebofs_iter_t));

  if (iter != NULL) {
     iter->ls = new list<pobject_t>;
//     iter->it = new list<pobject_t>::iterator;
     elock();
     iter->n = fs.list_objects(*(iter->ls));
     eunlock();
     iter->isnew = true;
   }

   return((void *)iter);
}

//*************************************************************
//  destroy_iterator - Destroys an iterator
//*************************************************************

void osd_ebofs::destroy_iterator(void *arg)
{
   osd_ebofs_iter_t *iter = (osd_ebofs_iter_t *)arg;

   iter->ls->clear();
   delete iter->ls;
//   delete iter->it;
   free(iter);
}

//*************************************************************
//  iterator_next - Returns the next key for the iterator
//*************************************************************

int osd_ebofs::iterator_next(void *arg, osd_id_t *id)
{
  osd_ebofs_iter_t *iter = (osd_ebofs_iter_t *)arg;

  if (iter->isnew) {
     iter->isnew = false;
     iter->it = iter->ls->begin();
  } else {
     iter->it++;
  }

  if (iter->it == iter->ls->end()) {
     return(1);
  } else { 
    pobject_t pob = *(iter->it);
    *id = pob.oid.ino;
    return(0);
  }

  return(1);  //** should never get here
}

//*************************************************************
//  umount -Unmounts the ebofs device
//*************************************************************

int osd_ebofs::umount()
{
  int err;

  err = fs.umount();

  return(err);
}
   
#endif

