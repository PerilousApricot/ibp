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

#ifndef __OBJECT_H
#define __OBJECT_H

#include <stdint.h>

#include <iostream>
#include <iomanip>
using namespace std;

#include <ext/hash_map>
using namespace __gnu_cxx;

//include "common/hash.h"

typedef uint32_t objectrev_t;

struct object_t {  
  uint64_t ino;

  object_t() {}
  object_t(uint64_t i, uint32_t b) {}
  object_t(uint64_t i, uint32_t b, uint32_t r) {}

} __attribute__ ((packed));

#endif
