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
//***************************************************************

#include "log.h"

#ifndef _DISABLE_LOG
FILE *_log_fd = stdout;
int _log_level = 0;
int _log_currsize = 0;
int _log_maxsize = 100*1024*1024;
pthread_mutex_t _log_lock = PTHREAD_MUTEX_INITIALIZER;
char _log_fname[1024];


void _open_log(const char *fname, int dolock) {
  if (dolock == 1) _lock_log(); 
  _log_currsize = 0; 

  if (fname != NULL) {    //** If NULL we'll just use the old name
     strncpy(_log_fname, fname, sizeof(_log_fname)-1); 
     _log_fname[sizeof(_log_fname)-1]= '\0';
  }

  if (strcmp(_log_fname, "stdout") == 0) { 
     _log_fd = stdout; 
  } else if (strcmp(_log_fname, "stderr") == 0) { 
     _log_fd = stderr; 
  } else if ((_log_fd = fopen(_log_fname, "w")) == NULL) { 
     fprintf(stderr, "OPEN_LOG failed! Attempted to us log file %s\n", _log_fname); 
     perror("OPEN_LOG: "); 
     _unlock_log(); 
  } 
              
  if (dolock == 1) _unlock_log();
}

void _close_log() {
  if ((strcmp(_log_fname, "stdout") != 0) && (strcmp(_log_fname, "stderr") != 0)) {
     fclose(_log_fd);
  } 
}

#endif


