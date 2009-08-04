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

#ifndef __LOG_H_
#define __LOG_H_

#ifndef _DISABLE_LOG

#include <unistd.h>
#include <sys/types.h>
#include <stdio.h>
#include <pthread.h>
#include <string.h>

extern FILE *_log_fd;
extern int _log_level;
extern int _log_maxsize;
extern int _log_currsize;
extern pthread_mutex_t _log_lock;
extern char _log_fname[1024];

void _open_log(const char *fname, int dolock);
void _close_log();

#define _lock_log() pthread_mutex_lock(&_log_lock)
#define _unlock_log() pthread_mutex_unlock(&_log_lock)
//#define truncate_log() ftruncate(fileno(_log_fd), 0); _log_currsize = 0
#define log_code(a) a
#define set_log_level(n) _log_level = n
#define set_log_maxsize(n) _log_maxsize = n
#define close_log()  _close_log()
#define log_fd()     _log_fd

#define open_log(fname) _open_log(fname, 1)
#define log_printf(n, ...) \
   if ((n) <= _log_level) { \
      _lock_log(); \
      _log_currsize += fprintf(_log_fd, __VA_ARGS__); \
      if (_log_currsize > _log_maxsize) { close_log(); _open_log(NULL, 0); } \
      _unlock_log(); \
   }


#define assign_log_fd(fd) _log_fd = fd
#define flush_log() \
   _lock_log();     \
   fflush(_log_fd); \
   _unlock_log();
#else
#define log_code(a)
#define log_level(n, ...)
#define set_log_level(n)
#define open_log(fname)
#define close_log()
#define log_fd()     stdout
#define truncate_log()
#define assign_log_fd(fd)
#define flush_log()
#endif

#endif

