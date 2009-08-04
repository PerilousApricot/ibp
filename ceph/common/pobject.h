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

#ifndef __CEPH_POBJECT_H
#define __CEPH_POBJECT_H

#include "object.h"

/*
 * "physical" object stored in an individual OSD's object store.
 * includes fields to describe which volume the logical object_t
 * belongs to, and/or a specific part of the object (if striped
 * or encoded for redundancy, etc.).
 */
struct pobject_t {
  uint32_t volume;     // "volume"
  uint32_t rank;       // rank/stripe id (e.g. for parity encoding)
  object_t oid;        // logical object
  pobject_t() : volume(0), rank(0) {}
  //pobject_t(object_t o) : volume(0), rank(0), oid(o) {}  // this should go away eventually
  pobject_t(uint16_t v, uint16_t r, object_t o) : volume(v), rank(r), oid(o) {}
} __attribute__ ((packed));


#endif
