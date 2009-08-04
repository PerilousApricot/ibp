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

//***************************************************************
//  fs_tester - check the performance of a filesystem
//***************************************************************

#include "allocation.h"
#include "resource.h"
#include <stdio.h>
#include "ceph/common/Thread.h"
#define CREATE_TEST  0
#define REMOVE_TEST  1

#define WRITE_TEST  0
#define READ_TEST  1
#define RANDOM_TEST  2

class Createremove_test : public Thread {
private:
   Resource_t *r;
   int ncount, which_test, my_id;
   double results[2];
   osd_id_t *id_list;

//*****************************************************************************

   void _create_test() {
      double stime = time(0);
      size_t size = 1;
      int type = 0;
      int reliability = ALLOC_HARD;
      time_t length = 1000;
      Allocation_t a;

      for (int i=0; i<ncount; i++) {
        assert(create_allocation_resource(r, &a, size, type, reliability, length) == 0); 
        id_list[i] = a.id;
      }

      results[CREATE_TEST] = (1.0*ncount) / (time(0) - stime);
   };

//*****************************************************************************

   void _remove_test() {
      double stime = time(0);
      Allocation_t a;

      for (int i=0; i<ncount; i++) {
          assert(get_allocation_resource(r, id_list[i], &a) == 0);
          assert(remove_allocation_resource(r, &a) == 0);
      } 

      results[REMOVE_TEST] = (1.0*ncount) / (time(0) - stime);

   };

public:
   Createremove_test(int uid, Resource_t *res, int n) { 
       my_id = uid;
       ncount = n;
       which_test = 0;
       r = res;

       id_list = new osd_id_t [ncount];
   };

   ~Createremove_test() { delete id_list; };

   void *entry() {
      cout << "createremove::entry my_id=" << my_id << " test=" << which_test << endl;

      switch (which_test) {
      case CREATE_TEST:
          _create_test();
          break;
      case REMOVE_TEST:
          _remove_test();
          break;
      }

      return(NULL);
   };

   void set_test(int i) { which_test = i; };
   double get_result(int i) { return(results[i]); };
};

//*****************************************************************************
//*****************************************************************************

class Readwrite_test : public Thread {
private:
   Resource_t *r;
   int ncount, which_test, my_id;
   int readwrite_size;
   double read_frac, results[3];
   osd_id_t *id_list;

//*****************************************************************************

   void _write_test() {
      const int BUFSIZE = 1024*1024;
      char buffer[BUFSIZE];
      int offset;
      size_t size = readwrite_size;
      int type = 0;
      int reliability = ALLOC_HARD;
      time_t length = 1000;
      Allocation_t a;

      int bcount = readwrite_size / BUFSIZE;
      int remainder = readwrite_size - bcount * BUFSIZE;

cout << "_write_test:  bcount = " << bcount << " * remainder = " << remainder << endl;

      //** Fill the buffer
      for (int i=0; i < BUFSIZE; i++) buffer[i] = '0';

      double stime = time(0);

      for (int i=0; i<ncount; i++) { 
           assert(create_allocation_resource(r, &a, size, type, reliability, length) == 0); 
           id_list[i] = a.id;
             
           offset = 0;      // Now store the data in chunks
           for (int j=0; j<bcount; j++) {
               assert(write_allocation_with_cap(r, &(a.caps[WRITE_CAP]), offset, BUFSIZE, buffer) == 0);
               offset = offset + BUFSIZE;
           }
           if (remainder>0) assert(write_allocation_with_cap(r, &(a.caps[WRITE_CAP]), offset, remainder, buffer) == 0);
      }

      results[WRITE_TEST] = (1.0*readwrite_size*ncount) / (time(0) - stime);
   };

//*****************************************************************************

   void remove_data() {
      Allocation_t a;

      for (int i=0; i<ncount; i++) {
          assert(get_allocation_resource(r, id_list[i], &a) == 0);
          assert(remove_allocation_resource(r, &a) == 0);
      } 
   };

//*****************************************************************************

   void _read_test() {
      double stime = time(0);
      int offset;
      const int BUFSIZE = 1024*1024;
      char buffer[BUFSIZE];
      Allocation_t a;

      int bcount = readwrite_size / BUFSIZE;
      int remainder = readwrite_size - bcount * BUFSIZE;

cout << "read_test: ncount=" << ncount << " * rw_size=" << readwrite_size << endl;
cout << "read_test:  bcount = " << bcount << " * remainder = " << remainder << endl;

cout << "read_test start " << time(0) << endl;

      for (int i=0; i<ncount; i++) { 
           assert(get_allocation_resource(r, id_list[i], &a) == 0);

           offset = 0;      // Now read the data in chunks
           for (int j=0; j<bcount; j++) {
               assert(read_allocation_with_cap(r, &(a.caps[READ_CAP]), offset, BUFSIZE, buffer) == 0);
               offset = offset + BUFSIZE;
           }
           if (remainder>0) assert(read_allocation_with_cap(r, &(a.caps[READ_CAP]), offset, remainder, buffer) == 0);

      }

      results[READ_TEST] = (1.0*readwrite_size*ncount) / (time(0) - stime);

cout << "read_test end   " << time(0) << endl;
cout << "read_test bytes=" << readwrite_size*ncount << endl;
cout << "read_test results=" << results[READ_TEST] << endl;

   };


//********************************************************************

   void _random_test() {
      double stime = time(0);
      int offset;
      const int BUFSIZE = 1024*1024;
      char buffer[BUFSIZE];
      Allocation_t a;

      int bcount = readwrite_size / BUFSIZE;
      int remainder = readwrite_size - bcount * BUFSIZE;

cout << "random_test: ncount=" << ncount << " * rw_size=" << readwrite_size << endl;
cout << "random_test:  bcount = " << bcount << " * remainder = " << remainder << endl;

cout << "random_test start " << time(0) << endl;
      double rnd;
      int slot;
      for (int i=0; i<ncount; i++) { 
           rnd = rand()/(RAND_MAX+1.0);
           slot = ncount * rnd; 

           assert(get_allocation_resource(r, id_list[slot], &a) == 0);

           rnd = rand()/(RAND_MAX + 1.0);
           if (rnd < read_frac) {
//cout << "random_read slot=" << slot <<endl;
               offset = 0;      // Now read the data in chunks
               for (int j=0; j<bcount; j++) {
                  assert(read_allocation_with_cap(r, &(a.caps[READ_CAP]), offset, BUFSIZE, buffer) == 0);
                  offset = offset + BUFSIZE;
               }
              if (remainder>0) assert(read_allocation_with_cap(r, &(a.caps[READ_CAP]), offset, remainder, buffer) == 0);

           } else {
//cout << "random_write slot=" << slot <<endl;
              offset = 0;      // Now store the data in chunks
              for (int j=0; j<bcount; j++) {
                  assert(write_allocation_with_cap(r, &(a.caps[WRITE_CAP]), offset, BUFSIZE, buffer) == 0);
                  offset = offset + BUFSIZE;
              }
              if (remainder>0) assert(write_allocation_with_cap(r, &(a.caps[WRITE_CAP]), offset, remainder, buffer) == 0);

           }
      }

      results[RANDOM_TEST] = (1.0*readwrite_size*ncount) / (time(0) - stime);

      remove_data();   // ** Clean up
};

//********************************************************************

public:
   Readwrite_test(int uid, Resource_t *res, int n, int rw_size, double frac) { 
cout << "readwrite_test: rw_size=" << rw_size<<endl;
       my_id = uid;
       ncount = n;
       readwrite_size = rw_size;
       read_frac = frac;
       which_test = 0;
       r = res;

       id_list = new osd_id_t [ncount];
   };

   ~Readwrite_test() { delete id_list; };

   void *entry() {
      cout << "readwrite::entry my_id=" << my_id << " test=" << which_test << endl;

      switch (which_test) {
      case WRITE_TEST:
          _write_test();
          break;
      case READ_TEST:
          _read_test();
          break;
      case RANDOM_TEST:
          _random_test();
          break;
      }

      return(NULL);
   };

   void set_test(int i) { which_test = i; };
   double get_result(int i) { return(results[i]); };
};


//*****************************************************************************
//*****************************************************************************
//*****************************************************************************


int main(int argc, char **argv)
{
  vector<char*> args;
  argv_to_vec(argc, argv, args);
  parse_config_options(args);

  cout << "args.size() = " << args.size() << endl;

  if (args.size() < 7) {
     cout << endl;
     cout << "rperf inifile resource nthreads  createremove_count" << endl;
     cout << "          readwrite_count readwrite_size random_count read_mix_fraction" << endl;
     cout << endl;
     cout << "inifile             - INI file defining the resources" << endl; 
     cout << "resource            - Name(INI group) of reource to use in INI file" << endl;
     cout << "nthreads            - Number of simultaneous threads to use" << endl;
     cout << "createremove_count* - Number of 0 byte files to create and remove to test metadata performance" << endl;
     cout << "readwrite_count*    - Number of files to write sequentially then read sequentially" << endl;
     cout << "readwrite_size      - Size of each file in KB for sequential and random tests" << endl;
     cout << "read_mix_fraction   - Fraction of Random I/O operations that are READS" << endl;
     cout << endl;
     cout << "*If the variable is set to 0 then the test is skipped" << endl;
     cout << endl;

     return(-1);
  }

  int i = 0;
  Resource_t r;
  DB_ENV *dbenv;

  //*** Get the fs type ***
  GKeyFile *kfd;
  GKeyFileFlags flags;

  printf("Opening key file\n");
  kfd = g_key_file_new();
  flags = G_KEY_FILE_NONE;
  g_key_file_load_from_file(kfd, args[0], flags, NULL); 

  printf("Mounting resource\n");
  dbenv = create_db_env("db/dbenv");
  mount_resource(&r, kfd, args[1], dbenv, 1);

  g_key_file_free(kfd);

  printf("Device : %s\n", args[i+1]); 
  i = i + 2;

  //*** Get thread count ***
  int nthreads = atoi(args[i]);
  if (nthreads < 0) { cout << "nthreads invalid: " << nthreads << endl;  return(i); }
  i++;

   //****** Get the different counts *****
  int createremove_count = atoi(args[i]); i++;
  int readwrite_count = atoi(args[i]); i++;
  int readwrite_size = atoi(args[i])*1024; i++;
  double read_mix_fraction = atof(args[i]); i++;

  //*** Print summary of options ***
  cout << "createremove_count = " << createremove_count << endl;
  cout << "readwrite_count = " << readwrite_count << endl;
  cout << "readwrite_size = " << readwrite_size/1024 << "kb" << endl;
  cout << "read_mix_fraction = " << read_mix_fraction << endl;
  cout << endl;
  cout << "Approximate I/O for sequential tests: " << readwrite_count*readwrite_size/1024/1024 << "MB" << endl;
  cout << endl;


  if (createremove_count > 0) {
     int perthread = createremove_count/nthreads;
     int total = perthread*nthreads;
     cout << "Starting Create test (total=" << total << ", per thread=" << perthread << ")" << endl;

     //**** Spawn the threads and run the initial test*****
     list<Createremove_test *> ls;
     for (int i=0; i<nthreads; i++) {
        Createremove_test *t = new Createremove_test(i, &r, perthread);
        t->set_test(CREATE_TEST);
        t->create();
        ls.push_back(t);
     }

     //**** Wait for all threads to stop and ****     
     //**** Collect the results *****
     double create_result = 0;
     list<Createremove_test *>::iterator p = ls.begin();
     while (p != ls.end()) {
        (*p)->join();
        create_result += (*p)->get_result(CREATE_TEST);
        p++;
     }

     cout << "Create : " << create_result << " creates/sec" << endl;

     //*** Now delete everything ***
     p = ls.begin();
     while (p != ls.end()) {
        (*p)->set_test(REMOVE_TEST);
        (*p)->create();
        p++;
     }
     //*** and collect the results ***
     double remove_result = 0;
     p = ls.begin();
     Createremove_test *thread;
     while (p != ls.end()) {
        thread = *p;
        thread->join();
        remove_result += thread->get_result(REMOVE_TEST);
        delete thread;
        p++;
     }

     cout << "Remove : " << remove_result << " removes/sec" << endl;

  }

  //**************** Read/Write tests ***************************
  if (readwrite_count > 0) {
     int perthread = readwrite_count/nthreads;
     int total = perthread*nthreads;
     cout << "Starting Write test (total files =" << total << ", per thread=" << perthread << ")" << endl;

     //**** Spawn the threads and run the initial test*****
     list<Readwrite_test *> ls;
     for (int i=0; i<nthreads; i++) {
        Readwrite_test *t = new Readwrite_test(i, &r, perthread, readwrite_size, read_mix_fraction);
        t->set_test(WRITE_TEST);
        t->create();
        ls.push_back(t);
     }

     //**** Wait for all threads to stop and ****     
     //**** Collect the results *****
     double write_result = 0;
     list<Readwrite_test *>::iterator p = ls.begin();
     while (p != ls.end()) {
        (*p)->join();
        write_result += (*p)->get_result(WRITE_TEST);
        p++;
     }

     cout << "Write : " << write_result/1024/1024 << " MB/sec" << endl;

     //*** Now read everything ***
     p = ls.begin();
     while (p != ls.end()) {
        (*p)->set_test(READ_TEST);
        (*p)->create();
        p++;
     }
     //*** and collect the results ***
     double read_result = 0;
     p = ls.begin();
     Readwrite_test *thread;
     while (p != ls.end()) {
        thread = *p;
        thread->join();
        read_result += thread->get_result(READ_TEST);
cout << "read total=" <<read_result << endl;
        p++;
     }

     cout << "Read : " << read_result/1024/1024 << " MB/sec" << endl;

     //*** Perform random I/O ***
     p = ls.begin();
     while (p != ls.end()) {
        (*p)->set_test(RANDOM_TEST);
        (*p)->create();
        p++;
     }
     //*** and collect the results ***
     double random_result = 0;
     p = ls.begin();
     while (p != ls.end()) {
        thread = *p;
        thread->join();
        random_result += thread->get_result(RANDOM_TEST);
cout << "random total=" <<random_result << endl;
        delete thread;
        p++;
     }

     cout << "Random : " << random_result/1024/1024 << " MB/sec" << endl;

  }

  print_resource_usage(&r, stdout);
  printf("Unmounting resource\n");
  umount_resource(&r);

  close_db_env(dbenv);
  
  return(0);
}
