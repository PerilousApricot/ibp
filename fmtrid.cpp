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
#include "ceph/config.h"

//***********************************************************************************
//***********************************************************************************
//***********************************************************************************

int main(int argc, const char **argv)
{
  vector<const char*> args;
  argv_to_vec(argc, argv, args);
  parse_config_options(args);
   
   if (args.size() != 1) {
      printf("fmtrid RID -- formats the RID for the local naming convention\n");
      printf("\n");
      printf("RID    - Resource ID (integer)\n");
      return(1);
   }

   RID_t rid;
   char name[256];
   int i = str2rid(args[0], &rid);
   rid2str(&rid, name, sizeof(name));
   printf("%s\n", name);
printf("read=!%s! err = %d\n", args[0], i);

   int j, k;
   unsigned int n[4];
   printf("full=");
   k = 0;
   for(i=0; i<4; i++) {
     if (i!=0) printf(":");
     for (j=0; j<4; j++) {
         n[j] = (unsigned char)rid.rid[k];
         k++;
     }
     printf("%u.%u.%u.%u", n[0], n[1], n[2], n[3]);
   }
   printf("\n");
}

