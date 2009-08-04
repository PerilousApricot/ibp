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

//****************************************************************
//****************************************************************

#include <assert.h>
#include <string.h>
#include "resource_list.h"
#include "resource.h"

//****************************************************************
// ghash_destroy - NULL routines so I can remove key/data pairs
//****************************************************************

void ghash_destroy(gpointer data)
{
  return;
}

//****************************************************************
//  create_resource_list - Takes a list of resources and
//     creates a hash map from RID -> resource
//****************************************************************

Resource_list_t *create_resource_list(Resource_t *r, int n)
{
  Resource_list_t *rl;
  char *crid;

  assert((rl = (Resource_list_t *)malloc(sizeof(Resource_list_t))) != NULL);
  assert((rl->name = (rl_name_t *)malloc(sizeof(rl_name_t)*n)) != NULL);
  assert((rl->hash_table = g_hash_table_new_full(g_str_hash, g_str_equal, ghash_destroy, ghash_destroy)) != NULL);
  rl->n = n;

  int i;
  char str[100];
  for (i=0; i<n; i++) {
    rid2str(&(r[i].rid), str, 100);     
    assert((crid = strdup(str)) != NULL);
    rl->name[i].crid = crid;
    g_hash_table_insert(rl->hash_table, crid, &(r[i]));
  }

  return(rl);
}

//****************************************************************
// free_resource_list - Frees the resource list
//****************************************************************

void free_resource_list(Resource_list_t *rl)
{
  int i;

  g_hash_table_destroy(rl->hash_table);

  for (i=0; i<rl->n; i++) {
     free(rl->name[i].crid);
  }

  free(rl->name);
  free(rl);  
}

//****************************************************************
//  resource_lookup - Returns the resource associated with the RID
//****************************************************************

Resource_t *resource_lookup(Resource_list_t *rl, char *rid)
{
//  char name[256];
//  RID_t r;

  //** Have to put it in the OS's common format
//  str2rid(rid, &r);   
//  rid2str(&r, name, sizeof(name));
 
  return((Resource_t *)(g_hash_table_lookup(rl->hash_table, rid)));
}
