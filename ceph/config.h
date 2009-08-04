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

#ifndef __CONFIG_H
#define __CONFIG_H

#include <vector>
#include <map>

#include "common/Mutex.h"

/**
 * command line / environment argument parsing
 */
void env_to_vec(std::vector<const char*>& args);
void argv_to_vec(int argc, const char **argv,
                 std::vector<const char*>& args);
void vec_to_argv(std::vector<const char*>& args,
                 int& argc, const char **&argv);

void parse_config_options(std::vector<const char*>& args);

#define generic_dout(x) cout 
#define generic_derr(x) cerr
#define dendl endl

#endif
