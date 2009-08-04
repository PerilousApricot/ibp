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


#include "config.h"
#include <fstream>
#include <stdlib.h>
#include <string.h>


void env_to_vec(std::vector<const char*>& args) 
{
  const char *p = getenv("CEPH_ARGS");
  if (!p) return;
  
  static char buf[1000];  
  int len = strlen(p);
  memcpy(buf, p, len);
  buf[len] = 0;
  //cout << "CEPH_ARGS " << buf << endl;

  int l = 0;
  for (int i=0; i<len; i++) {
    if (buf[i] == ' ') {
      buf[i] = 0;
      args.push_back(buf+l);
      //cout << "arg " << (buf+l) << endl;
      l = i+1;
    }
  }
  args.push_back(buf+l);
  //cout << "arg " << (buf+l) << endl;
}


void argv_to_vec(int argc, const char **argv,
                 std::vector<const char*>& args)
{
  for (int i=1; i<argc; i++)
    args.push_back(argv[i]);
}

void vec_to_argv(std::vector<const char*>& args,
                 int& argc, const char **&argv)
{
  argv = (const char**)malloc(sizeof(char*) * argc);
  argc = 1;
  argv[0] = "asdf";

  for (unsigned i=0; i<args.size(); i++) 
    argv[argc++] = args[i];
}


void parse_config_options(std::vector<const char*>& args)
{
  std::vector<const char*> nargs;

  for (unsigned i=0; i<args.size(); i++) {
      nargs.push_back(args[i]);
  }

  args = nargs;
}

