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

#include "allocation.h"
#include "resource.h"
#include <stdio.h>

//***********************************************************************************
//***********************************************************************************
//***********************************************************************************

int main(int argc, char **argv)
{
  vector<char*> args;
  Resource_t r;
  GKeyFile *kfd;
  GKeyFileFlags flags;
  DB_ENV *dbenv;
  Allocation_t a, b;
  char buf1[2048], buf2[2048];
  char *cptr;
  int err;

  argv_to_vec(argc, argv, args);
  parse_config_options(args);
   
   if (args.size() != 2) {
      printf("test.resource resource_ini_file rid_group\n");
      printf("resource_ini_file - Key file with resource definition\n");
      printf("rid_group         - Group in key file defining the resource\n");
      printf("\n");      
      return(1);
   }

   printf("Opening key file\n");
   kfd = g_key_file_new();
   flags = G_KEY_FILE_NONE;
   g_key_file_load_from_file(kfd, args[0], flags, NULL); 

   printf("Mounting resource\n");
   dbenv = create_db_env("db/dbenv");
   mount_resource(&r, kfd, args[1], dbenv, 0);

   printf("\nPrinting resource information\n");
   print_resource(&r, stdout);
   print_resource_usage(&r, stdout);

   printf("\nTesting allocation creation\n");
   err = create_allocation_resource(&r, &a, 1024, 1, ALLOC_HARD, 1000);   
   if (err != 0) {
      printf("Error creating allocation!  err=%d\n", err);
      abort();
   }
   err = create_allocation_resource(&r, &b, 1024, 1, ALLOC_SOFT, 1000);   
   if (err != 0) {
      printf("Error creating allocation!  err=%d\n", err);
      abort();
   }

   printf("\nLooking up allocation based on id\n");
   err = get_allocation_resource(&r, a.id, &b);   
   if (err != 0) {
      printf("Error searching allocation by id!  err=%d\n", err);
      abort();
   }
   print_allocation_resource(&r, stdout, &a);

   printf("\nLooking up allocation based on MANAGE cap\n");
   err = get_manage_allocation_resource(&r, &(a.caps[MANAGE_CAP]), &b);   
   if (err != 0) {
      printf("Error searching allocation by manage cap!  err=%d\n", err);
      abort();
   }
   print_allocation_resource(&r, stdout, &a);

   printf("\nWriting to the allocation\n");
   cptr = "This is a test write";
   err = write_allocation_with_cap(&r, &(a.caps[WRITE_CAP]), 0, strlen(cptr)+1, cptr);
   if (err != 0) {
      printf("Error writing allocation!  err=%d\n", err);
      abort();
   }

   printf("\nReading allocation\n");
   err = read_allocation_with_cap(&r, &(a.caps[READ_CAP]), 0, strlen(cptr)+1, buf1);
   if (err != 0) {
      printf("Error reading allocation!  err=%d\n", err);
      abort();
   }
   printf("\nRead from allocation: %s\n", buf1);

   printf("\nPrinting Usage information\n");
   print_resource_usage(&r, stdout);

//return(0);

   printf("\nDeleting allocation\n");
   err = remove_allocation_resource(&r, &a);
   if (err != 0) {
      printf("Error removing allocation!  err=%d\n", err);
      abort();
   }

   printf("\nPrinting Usage information\n");
   print_resource_usage(&r, stdout);

   printf("\nUnmounting resource\n");
   umount_resource(&r);
  
   close_db_env(dbenv);
   
   g_key_file_free(kfd);
   return(0);
}


