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
//  Header file to simplify using "?printf" routines and different 
//  data types, like size_t. 
//******************************************************************

#ifndef __FMTTYPES_
#define __FMTTYPES_

#ifdef _LINUX64BIT
#  define LU  "%lu"      // uint64_t 
#  define OT  "%lu"      // offset_t
#  define ST  "%lu"      // size_t
#  define TT  "%lu"      // time_t
#  define SST "%ld"      // suseconds_T
#  define I64T "%ld"     // uint64_t
#elif _LINUX32BIT
#  define LU  "%llu"
#  define OT  "%llu"     // offset_t
#  define ST  "%d"       // size_t
#  define TT  "%lu"
#  define SST "%lu"      // suseconds_T
#  define I64T "%lld"     // uint64_t
#else
#  define LU  "%llu"
#  define OT  "%llu"     // offset_t
#  define ST  "%lu"      // size_t
#  define TT  "%lu"
#  define SST "%d"      // suseconds_T
#  define I64T "%ld"     // uint64_t
#endif

#endif

