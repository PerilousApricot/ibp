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

//**********************************************************
//**********************************************************

#ifndef __SECURITYLOG_H_
#define __SECURITYLOG_H_

#ifndef _DISABLE_SECURITYLOG

#include <stdio.h>

extern FILE *_security_log_fd;
extern int _security_log_level;

#define security_log_printf(...) if (_security_log_level > 0) { fprintf(_security_log_fd, __VA_ARGS__); }
#define set_security_log_level(n) _security_log_level = n
#define open_security_log(fname) \
  if (strcmp(fname, "stdout") == 0) { \
     _security_log_fd = stdout; \
  } else if (strcmp(fname, "stderr") == 0) { \
     _security_log_fd = stderr; \
  } else if ((_security_log_fd = fopen(fname, "a")) == NULL) { \
     fprintf(stderr, "OPEN_security_log failed! Attempted to us security_log file %s\n", fname); \
     perror("OPEN_security_log: "); \
     return(1); \
  }

#define close_security_log()   fclose(_security_log_fd)
#define truncate_security_log() ftruncate(fileno(_security_log_fd), 0)
#define security_log_fd()      _security_log_fd
#define assign_security_log_fd(fd) _security_log_fd = fd
#define flush_security_log() fflush(_security_log_fd)
#else
#define security_log_level(...)
#define set_security_log_level(n)
#define open_security_log(fname)
#define close_security_log()
#define security_log_fd()     stdout
#define truncate_security_log()
#define assign_security_log_fd(fd)
#define flush_security_log()
#endif

#endif

