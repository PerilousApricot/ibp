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

#include "ibp_server.h"
#include "commands.h"

//*************************************************************************
//  install_commands - Install all the depot commans in the global function table
//*************************************************************************

void install_commands(GKeyFile *kf) {
  gsize n;

  //** Load the default ACLs
  global_config->server.default_acl = g_key_file_get_string_list(kf, "access_control", "default", &n, NULL);

  //** Default commands **
  add_command(IBP_ALLOCATE, "ibp_allocate", kf, NULL, NULL, NULL, NULL, read_allocate, handle_allocate);
  add_command(IBP_SPLIT_ALLOCATE, "ibp_split_allocate", kf, NULL, NULL, NULL, NULL, read_allocate, handle_allocate);
  add_command(IBP_MERGE_ALLOCATE, "ibp_merge_allocate", kf, NULL, NULL, NULL, NULL, read_merge_allocate, handle_merge);
  add_command(IBP_STATUS, "ibp_status", kf, NULL, NULL, NULL, NULL, read_status, handle_status);
  add_command(IBP_MANAGE, "ibp_manage", kf, NULL, NULL, NULL, NULL, read_manage, handle_manage);
  add_command(IBP_WRITE, "ibp_write", kf, NULL, NULL, NULL, NULL, read_write, handle_write);
  add_command(IBP_STORE, "ibp_store", kf, NULL, NULL, NULL, NULL, read_write, handle_write);
  add_command(IBP_LOAD, "ibp_load", kf, NULL, NULL, NULL, NULL, read_read, handle_read);
  add_command(IBP_SEND, "ibp_send", kf, NULL, NULL, NULL, NULL, read_read, handle_copy);
  add_command(IBP_PHOEBUS_SEND, "ibp_phoebus_send", kf, phoebus_load_config, phoebus_init, phoebus_destroy, phoebus_print, read_read, handle_copy);
  add_command(IBP_RENAME, "ibp_rename", kf, NULL, NULL, NULL, NULL, read_rename, handle_rename);
  add_command(IBP_ALIAS_ALLOCATE, "ibp_alias_allocate", kf, NULL, NULL, NULL, NULL, read_alias_allocate, handle_alias_allocate);
  add_command(IBP_ALIAS_MANAGE, "ibp_alias_manage", kf, NULL, NULL, NULL, NULL, read_manage, handle_manage);
  add_command(IBP_PUSH, "ibp_push", kf, NULL, NULL, NULL, NULL, read_read, handle_copy);
  add_command(IBP_PULL, "ibp_pull", kf, NULL, NULL, NULL, NULL, read_read, handle_copy);

  //*** Extra commands go below ****
  add_command(INTERNAL_GET_ALLOC, "internal_get_alloc", kf, NULL, NULL, NULL, NULL, read_internal_get_alloc, handle_internal_get_alloc);
  add_command(INTERNAL_DATE_FREE, "internal_date_free", kf, NULL, NULL, NULL, NULL, read_internal_date_free, handle_internal_date_free);
  add_command(INTERNAL_EXPIRE_LIST, "internal_expire_list", kf, NULL, NULL, NULL, NULL, read_internal_expire_list, handle_internal_expire_list);

}

