// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*- 
// vim: ts=8 sw=2 smarttab
/*
 * Ceph - scalable distributed file system
 *
 * Copyright (C) 2004-2006 Sage Weil <sage@newdream.net>
 *
 * This is free software; you can redistribute it and/or
 * modify it under the terms of the GNU Lesser General Public
 * License version 2.1, as published by the Free Software 
 * Foundation.  See file COPYING.
 * 
 */


#ifndef __THREAD_H
#define __THREAD_H

#include <pthread.h>
#include <signal.h>
#include <errno.h>
#include "config.h"

void *_entry_func(void *arg);

class Thread {
 private:
  pthread_t thread_id;

 public:
  Thread() : thread_id(0) {}
  virtual ~Thread() {}

// protected:
  virtual void *entry() = 0;

// private:
//  static void *_entry_func(void *arg) {
//    return ((Thread*)arg)->entry();
//  }

 public:
  pthread_t &get_thread_id() { return thread_id; }
  bool is_started() { return thread_id != 0; }
  bool am_self() { return (pthread_self() == thread_id); }

  int kill(int signal) {
    return pthread_kill(thread_id, signal);
  }
  int create() {
    return pthread_create( &thread_id, NULL, _entry_func, (void*)this );
//    return pthread_create( &thread_id, NULL, (void*)this, NULL );
  }
  int join(void **prval = 0) {
    if (thread_id == 0) {
      generic_derr(0) << "WARNING: join on thread that was never started" << dendl;
      //assert(0);
      return -EINVAL;   // never started.
    }

    int status = pthread_join(thread_id, prval);
    if (status != 0) {
      switch (status) {
      case -EINVAL:
	generic_derr(0) << "thread " << thread_id << " join status = EINVAL" << dendl;
	break;
      case -ESRCH:
	generic_derr(0) << "thread " << thread_id << " join status = ESRCH" << dendl;
	assert(0);
	break;
      case -EDEADLK:
	generic_derr(0) << "thread " << thread_id << " join status = EDEADLK" << dendl;
	break;
      default:
	generic_derr(0) << "thread " << thread_id << " join status = " << status << dendl;
      }
      assert(0); // none of these should happen.
    }
    thread_id = 0;
    return status;
  }

};

void *_entry_func(void *arg) {
    cout << "Starting thread" << endl;
    return ((Thread*)arg)->entry();
}

#endif
