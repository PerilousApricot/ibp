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

//*****************************************************************
//*****************************************************************

#include <string.h>
#include <limits.h>
#include "ibp_ClientLib.h"
#include "ibp_server.h"
#include "log.h"
#include "debug.h"
#include "security_log.h"
#include "allocation.h"
#include "resource.h"
#include "network.h"
#include "ibp_task.h"
#include "server_version.h"

//*****************************************************************
// get_command_timeout - Gets the command timeout from the NS
//      and stores it i nt he cmd data structure
//*****************************************************************

int get_command_timeout(ibp_task_t *task, char **bstate)
{
   unsigned long lu; 
   int fin;

   lu = 0; sscanf(string_token(NULL, " ", bstate, &fin), "%lu", &lu);
   task->cmd_timeout = time(NULL) + lu;
   if (lu == 0) {        
      log_printf(1, "get_command_timeout: Bad timeout value %lu changing to 2 for LoRS compatibility\n", lu);
      task->cmd_timeout = time(NULL) + 2;
//      log_printf(1, "get_command_timeout: Bad timeout value %lu\n", lu);
//      task->cmd.state = CMD_STATE_FINISHED;
//      return(send_cmd_result(task, IBP_E_INVALID_PARAMETER));
   }

   return(1);
}

//*****************************************************************
// parse_key - Splits the key into RID and cap. Format is RID#key
//*****************************************************************

int parse_key(char **bstate, Cap_t *cap, RID_t *rid, char *crid, int ncrid)
{
  int finished;
  char *tmp = string_token(NULL, "#", bstate, &finished);

  if (crid != NULL) {  //** Store the character version if requested
     crid[ncrid-1] = '\0';
     strncpy(crid, tmp, ncrid-1);
  }
  
  //** Check the validity of the RID
  if (str2rid(tmp, rid) != 0) {
    log_printf(5, "parse_key: Bad RID: %s\n", tmp);
    return(-1);
  }

  //** Lastly Get the key
  cap->v[sizeof(Cap_t)-1] = '\0';
  strncpy(cap->v, string_token(NULL, " ", bstate, &finished), sizeof(Cap_t)-1);
  log_printf(10, "parse_key: cap=%s\n", cap->v);
  log_printf(10, "parse_key: RID=%s\n", tmp);

  return(0);
}


//*****************************************************************
// read_allocate - Reads an allocate command
//
// 1.3:
//    version IBP_ALLOCATE IBP_SOFT|IBP_HARD TYPE DURATION SIZE TIMEOUT \n
//      %d         %d              %d         %d     %ll   %llu   %d
// 1.4:
//    version IBP_ALLOCATE RID IBP_SOFT|IBP_HARD TYPE DURATION SIZE TIMEOUT \n
//      %d         %d      %llu         %d         %d     %ll   %llu  %d
//
//    version IBP_SPLIT_ALLOCATE mkey mtypekey IBP_SOFT|IBP_HARD TYPE DURATION SIZE TIMEOUT\n
//      %d           %d           %s     %s            %d         %d     %ll   %llu   %d
//
//  TYPE: IBP_BYTEARRAY | IBP_BUFFER | IBP_FIFO | IBP_CIRQ
//*****************************************************************

int read_allocate(ibp_task_t *task, char **bstate)
{
   int d, fin;
   unsigned long long int llu;
   Cmd_state_t *cmd = &(task->cmd);

   fin = 0;

   debug_printf(1, "read_allocate:  Starting to process buffer\n");
   debug_printf(10, "read_allocate:   tm.sec=" TT " tm.usec = " SST "\n", global_config->server.timeout.tv_sec, global_config->server.timeout.tv_usec);

   Allocation_t *a = &(cmd->cargs.allocate.a);
   RID_t *rid = &(cmd->cargs.allocate.rid);

   //** Parse the RID ***
   if (cmd->version > IBPv031) {
      if (cmd->command != IBP_SPLIT_ALLOCATE) {
         char *tmp = string_token(NULL, " ", bstate, &fin);
         log_printf(15, "read_allocate:  cmd(rid)=%s\n", tmp);
         if (str2rid(tmp, rid) != 0) {
            log_printf(10, "read_allocate: Bad RID: %s\n", tmp);
            send_cmd_result(task, IBP_E_INVALID_RID);
            return(-1);
         }
      } else {  //** IBP_SPLIT_ALLOCATE
        if (parse_key(bstate, &(cmd->cargs.allocate.master_cap), rid, NULL, 0) != 0) {
            log_printf(10, "read_allocate: Bad RID/mcap!\n");
            send_cmd_result(task, IBP_E_INVALID_RID);
            return(-1);
        }
        string_token(NULL, " ", bstate, &fin);  //** Drop the WRMkey
      }
   } else {
     empty_rid(rid);   //** Don't care which resource we use
   }

   char dummy[128];
   log_printf(15, "read_allocate:  rid=%s\n", rid2str(rid, dummy, sizeof(dummy)));

   //***Reliability: IBP_HARD | IBP_SOFT ***
   d = -1; sscanf(string_token(NULL, " ", bstate, &fin), "%d", &d);
   a->reliability = d;
   if (d == IBP_HARD) {
      a->reliability = ALLOC_HARD;
   } else if (d == IBP_SOFT) {
      a->reliability = ALLOC_SOFT;
   } else {
      log_printf(1, "read_allocate: Bad reliability: %d\n", d);
      send_cmd_result(task, IBP_E_BAD_FORMAT);
      return(-1);
   } 

   //***Type: IBP_BYTEARRAY | IBP_BUFFER | IBP_FIFO | IBP_CIRQ ***
   d = -1; sscanf(string_token(NULL, " ", bstate, &fin), "%d", &d);
   a->type = d;
   if ((d != IBP_BYTEARRAY) && (d != IBP_BUFFER) && (d != IBP_FIFO) && (d != IBP_CIRQ)) {
       log_printf(1, "read_allocate: Bad type: %d\n", d);
       send_cmd_result(task, IBP_E_INVALID_PARAMETER);
       return(-1);
   } 

   //================ Disabling all allocation types except BYTEARRAYS ================
   if (a->type != IBP_BYTEARRAY) {
       log_printf(1, "read_allocate: Only support IBP_BYTEARRAY!  type: %d\n", d);
       send_cmd_result(task, IBP_E_TYPE_NOT_SUPPORTED);
       return(-1);

   }
   //================ Disabling all allocation types except BYTEARRAYS ================

   //*** Duration ***
   d = 0; sscanf(string_token(NULL, " ", bstate, &fin), "%d", &d);   
   a->expiration = time(NULL) + d;
   if (d == 0) {
      log_printf(1, "read_allocate: Bad duration: %d\n", d);
      send_cmd_result(task, IBP_E_INVALID_PARAMETER);
      return(-1);
   } else if (d == -1) {  //** Infinite duration is requested
      a->expiration = INT_MAX;
   }

   //** Allocation size ** 
   llu = 0; sscanf(string_token(NULL, " ", bstate, &fin), "%llu", &llu);   
   a->max_size = llu; 
   if (llu == 0) {
      log_printf(1, "read_allocate: Bad size : %llu\n", llu);
      send_cmd_result(task, IBP_E_INVALID_PARAMETER);
      return(-1);
   }            

   get_command_timeout(task, bstate);

   debug_printf(1, "read_allocate: Successfully parsed allocate command\n");
   return(0);
}

//*****************************************************************
// read_merge_allocate - Merges 2 allocations if possible. The child
//    allocation is removed if successful.
//
// v1.4
//    version IBP_MERGE_ALLOCATE mkey mtypekey ckey ctypekey TIMEOUT\n
//      %d           %d           %s     %s     %s     %s       %d
//
//*****************************************************************

int read_merge_allocate(ibp_task_t *task, char **bstate)
{
   int fin;
   Cmd_state_t *cmd = &(task->cmd);
   Cmd_merge_t *op = &(cmd->cargs.merge);
   RID_t child_rid;

   fin = 0;

   debug_printf(1, "read_merge_allocate:  Starting to process buffer\n");

   //** Parse the master key  
   if (parse_key(bstate, &(op->mkey), &(op->rid), op->crid, sizeof(op->crid)) != 0) {
       log_printf(10, "read_merge_allocate: Bad RID/mcap!\n");
       send_cmd_result(task, IBP_E_INVALID_RID);
       return(-1);
   }
   string_token(NULL, " ", bstate, &fin);  //** Drop the WRMkey

   //** Parse the child key  
   if (parse_key(bstate, &(op->ckey), &child_rid, NULL, 0) != 0) {
       log_printf(10, "read_merge_allocate: Child RID/mcap!\n");
       send_cmd_result(task, IBP_E_INVALID_RID);
       return(-1);
   }
   string_token(NULL, " ", bstate, &fin);  //** Drop the WRMkey

   //** Now compare the rid's to make sure they are the same
   if (compare_rid(&(op->rid), &child_rid) != 0) {
       log_printf(10, "read_merge_allocate: Child/Master RID!\n");
       send_cmd_result(task, IBP_E_INVALID_RID);
       return(-1);
   }

   get_command_timeout(task, bstate);

   debug_printf(1, "read_merge_allocate: Successfully parsed allocate command\n");
   return(0);   
}


//*****************************************************************
//  read_status - Parses the IBP_STATUS command
//
// 1.4
//   version IBP_STATUS RID   IBP_ST_INQ password  TIMEOUT \n
//      %d       %d     %llu      %d         %31s       %d 
//   version IBP_STATUS RID   IBP_ST_CHANGE password  TIMEOUT \n max_hard max_soft max_duration \n
//      %d       %d     %llu      %d            %31s       %d      %llu     %llu      %d
//   version IBP_STATUS  IBP_ST_RES  TIMEOUT \n
//      %d       %d         %d         %d 
//   version IBP_STATUS  IBP_ST_STATS  start_time   TIMEOUT \n
//      %d       %d         %d            %d           %d
//   version IBP_STATUS  IBP_ST_VERSION TIMEOUT \n
//      %d       %d           %d           %d
//   
//*****************************************************************

int read_status(ibp_task_t *task, char **bstate)
{
   int d, finished;
   Cmd_state_t *cmd = &(task->cmd);

   finished = 0;

   debug_printf(1, "read_status:  Starting to process buffer\n");
   debug_printf(10, "read_status:   tm.sec=%ld tm.usec = " SST "\n", global_config->server.timeout.tv_sec, global_config->server.timeout.tv_usec);
  
   Cmd_status_t *status = &(cmd->cargs.status);
   status->subcmd = 0;

   //*** Only way to distinguish between commands is based on the number of args.
   int nargs;
   char *dupstr = strdup(*bstate);
   char *dstate;
   string_token(dupstr, " ", &dstate, &finished);
   nargs = 2;    
   while (finished == 0) {
      nargs++;
      string_token(NULL, " ", &dstate, &finished);
   }
   finished = 0;
   free(dupstr);

//   log_printf(10, "read_status: ns=%d nargs = %d\n", task->ns->id, nargs);

   //** Parse the RID (or the sub command) ***
   if (cmd->version > IBPv031) {   
      char *tmp;
      tmp = string_token(NULL, " ", bstate, &finished);
      status->crid[sizeof(status->crid)-1] = '\0';
      if (nargs < 5) {  //** IBP_ST_RES/IBP_ST_VERSION command
         sscanf(tmp, "%d", &(status->subcmd));
//         log_printf(10, "read_status: ns=%d subcmd = %d\n", task->ns->id, status->subcmd);
      } else {
         if (str2rid(tmp, &(status->rid)) != 0)   {
            log_printf(1, "read_status: Bad RID: %s\n", tmp);
            send_cmd_result(task, IBP_E_INVALID_RID);                
            return(-1);
         }

         if (is_empty_rid(&(status->rid)) == 1) { //** Pick one at random
            unsigned int ui;
            get_random(&ui, sizeof(ui));
            double r = (1.0 * ui) / (UINT_MAX + 1.0);
            d = global_config->n_resources * r;
            status->rid = global_config->res[d].rid;
            log_printf(6, "read_status: Read rid=0 so picking one at random rid=%s\n", status->crid);
         }

         rid2str(&(status->rid), status->crid, sizeof(status->crid)); 

         //*** Get the sub command ***
         sscanf(string_token(NULL, " ", bstate, &finished), "%d", &(status->subcmd));
      }
   } else {
      //** Pick a random resource to use
      unsigned int ui;
      get_random(&ui, sizeof(ui));
      double r = (1.0 * ui) / (UINT_MAX + 1.0);
      d = global_config->n_resources * r;
      status->rid = global_config->res[d].rid;
      rid2str(&(status->rid), status->crid, sizeof(status->crid)); 
      log_printf(6, "read_status: IBP_v031 doesn't support RID.  Picked one at random rid=%s\n", status->crid);

      //*** Get the sub command ***
      char *tmp=string_token(NULL, " ", bstate, &finished);
      debug_printf(15, "read_status: should be getting the subcmd =!%s!\n", tmp);
      sscanf(tmp, "%d", &(status->subcmd));
   }

   //*** Process the subcommand ***    
   switch (status->subcmd) {
      case IBP_ST_RES :
         empty_rid(&(status->rid));
         status->password[PASSLEN-1] = '\0';
         strncpy(status->password, global_config->server.password, sizeof(status->password)-1);
         get_command_timeout(task, bstate);  //** Get the timeout
         break;
      case IBP_ST_VERSION: //** Return the version
         break;
      case IBP_ST_STATS :    //** Stats command not standard IBP
         empty_rid(&(status->rid));
         status->password[PASSLEN-1] = '\0';
         strncpy(status->password, global_config->server.password, sizeof(status->password)-1);
         d = -1; sscanf(string_token(NULL, " ", bstate, &finished), "%d", &d);
         status->start_time = d;
         if (d == 0) {
            log_printf(1, "read_status: Invalid start time %d\n", d);
            send_cmd_result(task, IBP_E_BAD_FORMAT);
            return(-1);
         }
         get_command_timeout(task, bstate);  //** Get the timeout
         break;
      case IBP_ST_INQ :
         status->password[0] = '\0'; sscanf(string_token(NULL, " ", bstate, &finished), "%31s", status->password);
         debug_printf(15, "read_status: IBP_ST_INQ password = %s\n", status->password);
         get_command_timeout(task, bstate);  //** Get the timeout
         break;
      case IBP_ST_CHANGE :
         status->password[0] = '\0'; sscanf(string_token(NULL, " ", bstate, &finished), "%31s", status->password);
         get_command_timeout(task, bstate);  //** Get the timeout

//================= IBP_ST_CHANGED NOT ALLOWED===============================
log_printf(1, "read_status: IBP_ST_CHANGE request ignored! ns=%d\n", task->ns->id);
send_cmd_result(task, IBP_E_INVALID_CMD);
close_netstream(task->ns);
return(-1);
         
         int nbytes;
         char buffer[100];
         nbytes = readline_netstream(task->ns, buffer, sizeof(buffer), global_config->server.timeout);
         if (nbytes < 0) {
            return(nbytes);  //** Return the error back up the food chain
         }
         //*** Grab the new hard size ***
         status->new_size[ALLOC_HARD] = 0; sscanf(string_token(buffer, " ", bstate, &finished), LU, &(status->new_size[ALLOC_HARD]));

         //*** Grab the new soft size ***
         status->new_size[ALLOC_SOFT] = 0; sscanf(string_token(NULL, " ", bstate, &finished), LU, &(status->new_size[ALLOC_SOFT]));

         //*** Grab the new hard size ***
         status->new_duration = 0; sscanf(string_token(NULL, " ", bstate, &finished), "%ld", &(status->new_duration));

         debug_printf(1, "read_status: change request of h:" LU " s:" LU " d:%ld  ns=%d\n", 
             status->new_size[ALLOC_HARD], status->new_size[ALLOC_SOFT], status->new_duration, task->ns->id);

         break;
      default :
         log_printf(1, "read_status: Unknown sub-command %d\n", status->subcmd);
         send_cmd_result(task, IBP_E_BAD_FORMAT);
         return(-1);
   }


   debug_printf(1, "read_status: Successfully parsed status command\n");
   return(0);   
}

//*****************************************************************
//  read_manage - Reads an ibp_manage command
//
// 1.4
//    version IBP_MANAGE key typekey IBP_INCR|IBP_DECR captype timeout \n
// 
//    version IBP_MANAGE key typekey IBP_CHNG|IBP_PROBE captype maxSize life reliability timeout \n
//
// 1.5
//    version IBP_ALIAS_MANAGE key typekey IBP_INCR|IBP_DECR captype mkey mtypekey timeout \n
// 
//    version IBP_ALIAS_MANAGE key typekey IBP_CHNG offset len duration mkey mtypekey timeout \n
//
//    version IBP_ALIAS_MANAGE key typekey IBP_PROBE timeout \n
//
//*****************************************************************

int read_manage(ibp_task_t *task, char **bstate)
{
   int d, finished;
   unsigned long long int llu;
   Cmd_state_t *cmd = &(task->cmd);
   Cmd_manage_t *manage = &(cmd->cargs.manage);

   finished = 0;

   debug_printf(1, "read_manage:  Starting to process buffer\n");

   //** Get the RID for the key
   manage->crid[sizeof(manage->crid)-1] = '\0';
   strncpy(manage->crid, string_token(NULL, "#", bstate, &finished), sizeof(manage->crid)-1);
   if (str2rid(manage->crid, &(manage->rid)) != 0) {
      log_printf(1, "read_manage: Bad RID: %s\n", manage->crid);
      send_cmd_result(task, IBP_E_INVALID_RID);                
      return(-1);
   }

   //** Get the key
   manage->cap.v[sizeof(manage->cap.v)-1] = '\0';
   strncpy(manage->cap.v, string_token(NULL, " ", bstate, &finished), sizeof(manage->cap.v)-1);
   debug_printf(10, "read_manage: cap=%s\n", manage->cap.v);

   debug_printf(10, "read_mange: RID=%s\n", manage->crid);
   string_token(NULL, " ", bstate, &finished);  //** Drop the WRMkey

    
   //*** Get the subcommand ***    
   d = -1; sscanf(string_token(NULL, " ", bstate, &finished), "%d", &d);
   if ((d != IBP_INCR) && (d != IBP_DECR) && (d != IBP_CHNG) && (d != IBP_PROBE)) {
      log_printf(1, "read_manage: Unknown sub-command %d\n", d);
      send_cmd_result(task, IBP_E_BAD_FORMAT);
      return(-1);
   }
   manage->subcmd = d;

   //** Get the cap type
   d = -1; sscanf(string_token(NULL, " ", bstate, &finished), "%d", &d);
   switch (d) {
     case IBP_READCAP:    manage->captype = READ_CAP;  break;
     case IBP_WRITECAP:   manage->captype = WRITE_CAP;  break;
     case IBP_MANAGECAP:  manage->captype = MANAGE_CAP;  break;
     default:
        if ((manage->subcmd == IBP_INCR) || (manage->subcmd == IBP_DECR)) {  //** Ignored for other commands
           log_printf(10, "read_manage:  Invalid cap type (%d)!\n", d);
           send_cmd_result(task, IBP_E_BAD_FORMAT);
           return(-1);
        }
   }

   switch(manage->subcmd) {
      case IBP_INCR:
      case IBP_DECR:
         if (cmd->command == IBP_ALIAS_MANAGE) {  //** Get the "master" key if this is a alias command
            string_token(NULL, "#", bstate, &finished);   //** Strip the RID.  We only keep it for the alias

            //** Get the master key
            manage->master_cap.v[sizeof(manage->master_cap.v)-1] = '\0';
            strncpy(manage->master_cap.v, string_token(NULL, " ", bstate, &finished), sizeof(manage->master_cap.v)-1);
            log_printf(10, "read_manage: master cap=%s\n", manage->cap.v);

            string_token(NULL, " ", bstate, &finished);  //** Drop the WRMkey
         }

         get_command_timeout(task, bstate);
         return(0);

      case IBP_PROBE:   //**Skip the unused fields - This needs to be cleaned up in the protocol!
           if (cmd->command == IBP_MANAGE) {
              string_token(NULL, " ", bstate, &finished); 
              string_token(NULL, " ", bstate, &finished); 
              string_token(NULL, " ", bstate, &finished); 
           }
           get_command_timeout(task, bstate);
           return(0);
      case IBP_CHNG:
         if (cmd->command == IBP_ALIAS_MANAGE) { //** Get the new offset
            llu = 0; sscanf(string_token(NULL, " ", bstate, &finished), "%llu", &llu);
            manage->offset = llu;
         }

         //** Get the new size
         llu = 0; sscanf(string_token(NULL, " ", bstate, &finished), "%llu", &llu);
         manage->new_size = llu;
         if ((llu == 0) && (manage->subcmd == IBP_CHNG)) {
            log_printf(10, "read_manage:  Invalid new size (" LU ")!\n", manage->new_size);
            send_cmd_result(task, IBP_E_WOULD_DAMAGE_DATA);
            return(-1);
         }

         //**Read the new duration
         d = 0; sscanf(string_token(NULL, " ", bstate, &finished), "%d", &d);
         if (cmd->command == IBP_MANAGE) {
            manage->new_duration = time(NULL) + d;
            if (d == 0) {
               log_printf(1, "read_manage: Bad duration: %d\n", d);
               send_cmd_result(task, IBP_E_INVALID_PARAMETER);
               return(-1);
            } else if (d == -1) {  //** Infinite duration is requested
               manage->new_duration = INT_MAX; //** This is fixed in handle_manage
            }
         } else {
            manage->new_duration = time(NULL) + d;
            if (d == 0) manage->new_duration = 0;
         }

         if (cmd->command == IBP_MANAGE) {
            //**Read the new reliability
            d = -1; sscanf(string_token(NULL, " ", bstate, &finished), "%d", &d);
            if (d == IBP_HARD) {
               manage->new_reliability = ALLOC_HARD;
            } else if (d == IBP_SOFT) {
               manage->new_reliability = ALLOC_SOFT;
            } else if (manage->subcmd == IBP_CHNG) {
               log_printf(1, "read_manage: Bad reliability: %d\n", d);
               send_cmd_result(task, IBP_E_BAD_FORMAT);
               return(-1);
            }
         }

         if (cmd->command == IBP_ALIAS_MANAGE) {  //** Get the "master" key if this is a alias command
            string_token(NULL, "#", bstate, &finished);   //** Strip the RID.  We only keep it for the alias

            //** Get the master key
            manage->master_cap.v[sizeof(manage->master_cap.v)-1] = '\0';
            strncpy(manage->master_cap.v, string_token(NULL, " ", bstate, &finished), sizeof(manage->master_cap.v)-1);
            log_printf(10, "read_manage: master cap=%s\n", manage->cap.v);

            string_token(NULL, " ", bstate, &finished);  //** Drop the WRMkey
         }

         get_command_timeout(task, bstate);
         return(0);
      default:
        log_printf(10, "read_manage:  Invalid subcmd (%d)!\n", manage->subcmd);
        send_cmd_result(task, IBP_E_BAD_FORMAT);
        return(-1);
   }

   return(-100);  //** NEver get here
}


//*****************************************************************
//  read_rename - Reads an ibp_rename command
//
// 1.5
//    version IBP_RENAME key typekey timeout\n
//
//*****************************************************************

int read_rename(ibp_task_t *task, char **bstate)
{
   int finished;
   Cmd_state_t *cmd = &(task->cmd);
   Cmd_manage_t *manage = &(cmd->cargs.manage);

   finished = 0;

   debug_printf(1, "read_rename:  Starting to process buffer\n");

   //** Get the RID, uh I mean the key...... the format is RID#key
   manage->crid[sizeof(manage->crid)-1] = '\0';
   strncpy(manage->crid, string_token(NULL, "#", bstate, &finished), sizeof(manage->crid)-1);
   if (str2rid(manage->crid, &(manage->rid)) != 0) {
      log_printf(1, "read_manage: Bad RID: %s\n", manage->crid);
      send_cmd_result(task, IBP_E_INVALID_RID);                
      return(-1);
   }

   //** Get the key
   manage->cap.v[sizeof(manage->cap.v)-1] = '\0';
   strncpy(manage->cap.v, string_token(NULL, " ", bstate, &finished), sizeof(manage->cap.v)-1);
   debug_printf(10, "read_manage: cap=%s\n", manage->cap.v);

   debug_printf(10, "read_mange: RID=%s\n", manage->crid);
   string_token(NULL, " ", bstate, &finished);  //** Drop the WRMkey


   get_command_timeout(task, bstate);
   return(0);
}

//*****************************************************************
//  read_alias_allocate - Reads an ibp_alias_allocate command
//
// 1.5
//    version IBP_ALIAS_ALLOCATE key typekey offset len duration timeout\n
//
//   if offset=len=0 then can use entire alocation
//   if duration=0 then use master allocations expiration
//*****************************************************************

int read_alias_allocate(ibp_task_t *task, char **bstate)
{
   int finished;
   uint64_t lu;

   Cmd_alias_alloc_t *cmd = &(task->cmd.cargs.alias_alloc);

   finished = 0;

   debug_printf(1, "read_alias_allocate:  Starting to process buffer\n");

   //** Get the RID and key the format is RID#key
   cmd->crid[sizeof(cmd->crid)-1] = '\0';
   strncpy(cmd->crid, string_token(NULL, "#", bstate, &finished), sizeof(cmd->crid)-1);
   if (str2rid(cmd->crid, &(cmd->rid)) != 0) {
      log_printf(5, "read_alias_allocate: Bad RID: %s\n", cmd->crid);
      send_cmd_result(task, IBP_E_INVALID_RID);                
      return(-1);
   }

   //** Get the key
   cmd->cap.v[sizeof(cmd->cap.v)-1] = '\0';
   strncpy(cmd->cap.v, string_token(NULL, " ", bstate, &finished), sizeof(cmd->cap.v)-1);
   log_printf(10, "read_alias_allocate: cap=%s\n", cmd->cap.v);

   log_printf(10, "read_alias_allocate: RID=%s\n", cmd->crid);
   string_token(NULL, " ", bstate, &finished);  //** Drop the WRMkey

   cmd->offset = cmd->len = cmd->expiration == 0;
   
   //** Get the offset **
   if (sscanf(string_token(NULL, " ", bstate, &finished), LU, &(cmd->offset)) != 1) { 
      log_printf(5, "read_alias_allocate: Bad offset cap= %s\n", cmd->crid);
      send_cmd_result(task, IBP_E_INVALID_PARAMETER);                
      return(-1);
   }

   //** Get the len **
   if (sscanf(string_token(NULL, " ", bstate, &finished), LU, &(cmd->len)) != 1) { 
      log_printf(5, "read_alias_allocate: Bad length cap= %s\n", cmd->crid);
      send_cmd_result(task, IBP_E_INVALID_PARAMETER);                
      return(-1);
   }

   //** Get the duration **
   if (sscanf(string_token(NULL, " ", bstate, &finished), LU, &lu) != 1) { 
      log_printf(5, "read_alias_allocate: Bad duration cap= %s\n", cmd->crid);
      send_cmd_result(task, IBP_E_INVALID_PARAMETER);                
      return(-1);
   }
   cmd->expiration = 0;
//   log_printf(15, "read_alias_allocate: cmp->expire=%lu\n", lu); 
   if (lu != 0) cmd->expiration = lu + time(NULL);
  
   get_command_timeout(task, bstate);
   return(0);
}

//*****************************************************************
//  read_write - Reads an IBP_write command
//
// 1.4
//    version IBP_WRITE key typekey offset length timeout \n
//    version IBP_STORE key typekey length timeout \n
//*****************************************************************

int read_write(ibp_task_t *task, char **bstate)
{
   int finished;
   unsigned long long int llu;
   Cmd_state_t *cmd = &(task->cmd);
   Cmd_write_t *w = &(cmd->cargs.write);

   finished = 0;

   debug_printf(1, "read_write:  Starting to process buffer\n");

   w->tq = NULL;
   w->sending = 0;  //** Flag the command as nopt sending data so the handle will load the res, etc.

   //** Get the RID, uh I mean the key...... the format is RID#key
   char *tmp;
   w->crid[sizeof(w->crid)-1] = '\0'; 
   tmp = string_token(NULL, "#", bstate, &finished);
   if (str2rid(tmp, &(w->rid)) != 0) {
      log_printf(1, "read_write: Bad RID: %s\n", tmp);
      send_cmd_result(task, IBP_E_INVALID_RID);                
      return(-1);
   }
   rid2str(&(w->rid), w->crid, sizeof(w->crid));

   //** Get the write key
   w->cap.v[sizeof(w->cap.v)-1] = '\0';
   strncpy(w->cap.v, string_token(NULL, " ", bstate, &finished), sizeof(w->cap.v)-1);
   debug_printf(10, "read_write: cap=%s\n", w->cap.v);

   debug_printf(10, "read_write: RID=%s\n", w->crid);
   string_token(NULL, " ", bstate, &finished);  //** Drop the WRMkey

   //** Get the offset
   if (cmd->command == IBP_WRITE) {
      llu = 0; sscanf(string_token(NULL, " ", bstate, &finished), "%llu", &llu);
      w->offset = llu;
      if (llu < 0) {
         log_printf(10, "read_write:  Invalid offset (%llu)!\n", llu);
         send_cmd_result(task, IBP_E_FILE_SEEK_ERROR);
         return(-1);
      }
   } else {  //** For IBP_STORE the offset is APPEND
     w->offset = -1;   
   }

   //** Get the length
   llu = 0; sscanf(string_token(NULL, " ", bstate, &finished), "%llu", &llu);
   w->len = llu;
   if (llu < 0) {
      log_printf(10, "read_write:  Invalid length (%llu)!\n", llu);
      send_cmd_result(task, IBP_E_INV_PAR_SIZE);
      return(-1);
   }

   //** and finally the timeout
   get_command_timeout(task, bstate);

   debug_printf(1, "read_write: Successfully parsed off=" OT " len=" OT "\n", w->offset, w->len);
   return(0);   
}

//*****************************************************************
//  read_read - Reads an IBP_load command
//
// v1.4
//    version IBP_LOAD key typekey offset length timeout \n

// v1.4 - IBP_send command
//    version IBP_SEND key typekey remote_wcap offset length src_timeout dest_timeout dest_ClientTimeout\n//

// IBP_PHOEBUS_SEND
//    version IBP_PHOEBUS_SEND phoebus_path|auto key typekey remote_wcap offset length src_timeout dest_timeout dest_ClientTimeout\n
//
// IBP_PUSH|IBP_PULL
//    version IBP_PUSH|IBP_PULL ctype key typekey remote_wcap local_offset remote_offset length src_timeout dest_timeout\n
//
//    
//    ctype - Connection type:
//         IBP_TCP - Normal transfer mode
//         IBP_PHOEBUS phoebus_path|auto - USe phoebus transfer
//    phoebus_path - Comma separated List of phoebus hosts/ports, eg gateway1/1234,gateway2/4321
//        Use 'auto' to use the default phoebus path
//    *_offset - If -1 this is an append operation.  This is only valid for the following combinations:
//        IBP_PUSH - remote_offset and IBP_PULL - local_offset
//*****************************************************************

int read_read(ibp_task_t *task, char **bstate)
{
   int finished, ctype, get_remote_cap;
   char *path;
   unsigned long int lu;
   long long int ll;
   long long unsigned int llu;
   Cmd_state_t *cmd = &(task->cmd);
   Cmd_read_t *r = &(cmd->cargs.read);

   memset(r, 0, sizeof(Cmd_read_t));
   finished = 0;

   debug_printf(1, "read_read:  Starting to process buffer\n");

   r->tq = NULL;
   r->recving = 0;  //** Flag the command as not recving data so the handle will load the res, etc.
  
   r->write_mode = 1;  //** Default is to append
   r->transfer_dir = IBP_PUSH;
   
   ctype = IBP_TCP; r->path[0] = '\0';
   get_remote_cap = 0;
   switch (cmd->command) {
       case IBP_PULL:
          r->transfer_dir = IBP_PULL;
       case IBP_PUSH:
          get_remote_cap = 1;
          ctype = IBP_TCP; sscanf(string_token(NULL, " ", bstate, &finished), "%d", &ctype);
          if ((ctype != IBP_TCP) && (ctype != IBP_PHOEBUS)) {
             log_printf(10, "read_read:  Invalid ctype(%d)!\n", ctype);
             send_cmd_result(task, IBP_E_TYPE_NOT_SUPPORTED);
             return(-1);
          } 
          break;
       case IBP_PHOEBUS_SEND: 
          r->transfer_dir = IBP_PUSH;
          get_remote_cap = 1;
          ctype = IBP_PHOEBUS; 
          break;
       case IBP_SEND: 
          r->transfer_dir = IBP_PUSH;
          get_remote_cap = 1;
          break;
   }

   r->ctype = ctype;
  
   if (ctype == IBP_PHOEBUS) { //** Get the phoebus path
      r->path[sizeof(r->path)-1] = '\0';
      path = string_token(NULL, " ", bstate, &finished);
      if (strcmp(path, "auto") == 0) path="\0";
      strncpy(r->path, path, sizeof(r->path)-1);
      debug_printf(10, "read_read: phoebus_path=%s\n", r->path);      
   }

   //** Get the RID and key...... the format is RID#key
   char *tmp;
   tmp = string_token(NULL, "#", bstate, &finished);
   if (str2rid(tmp, &(r->rid)) != 0) {
      log_printf(1, "read_read: Bad RID: %s\n", tmp);
      send_cmd_result(task, IBP_E_INVALID_RID);                
      return(-1);
   }
   rid2str(&(r->rid), r->crid, sizeof(r->crid));

   //** Get the read key
   r->cap.v[sizeof(r->cap.v)-1] = '\0';
   strncpy(r->cap.v, string_token(NULL, " ", bstate, &finished), sizeof(r->cap.v)-1);
   debug_printf(10, "read_read: cap=%s\n", r->cap.v);

   if (get_remote_cap == 1) {  //** For send/push/pull commands get the remote cap
      task->child = NULL;
      r->remote_cap[sizeof(r->remote_cap)-1] = '\0';
      strncpy(r->remote_cap, string_token(NULL, " ", bstate, &finished), sizeof(r->remote_cap)-1);
      debug_printf(10, "read_read: remote_cap=%s\n", r->remote_cap);
   }

   debug_printf(10, "read_read: RID=%s\n", r->crid);
   string_token(NULL, " ", bstate, &finished);  //** Drop the WRMkey

   //** Get the offset
   ll = 0; sscanf(string_token(NULL, " ", bstate, &finished), "%lld", &ll);
   r->offset = ll;
   if (cmd->command == IBP_PULL) {            
      if (ll > -1) {     //** PULL allows the 1st cap to be append
        r->write_mode = 0;
      } else {
         r->offset = 0;
      }
   }

   //** Get the remote offset if needed
   if ((cmd->command == IBP_PUSH) || (cmd->command == IBP_PULL)) {
      ll = 0; sscanf(string_token(NULL, " ", bstate, &finished), "%lld", &ll);
      r->remote_offset = 0;
      if (cmd->command == IBP_PUSH) {
         if (ll > -1) {
            r->write_mode = 0;
            r->remote_offset = ll;
         }
      } else if (ll > -1){
         r->remote_offset = ll;
      }
   }

   //** Get the length
   llu = 0; sscanf(string_token(NULL, " ", bstate, &finished), "%llu", &llu);
   r->len = llu;
   if (llu < 0) {
      log_printf(10, "read_read:  Invalid length (%llu)!\n", llu);
      send_cmd_result(task, IBP_E_INV_PAR_SIZE);
      return(-1);
   }

   //** Get the connections timeout
   get_command_timeout(task, bstate);

   if ((cmd->command == IBP_SEND) || (cmd->command == IBP_PHOEBUS_SEND)) {
      //** Now get the remote timeouts
      lu = 0; sscanf(string_token(NULL, " ", bstate, &finished), "%lu", &lu);
      r->remote_sto = lu;
      log_printf(1, "read_read(SEND): remote_timeout value=%lu\n", lu);
      if (lu == 0) {        
         log_printf(1, "read_read(SEND): Bad Remote server timeout value %lu\n", lu);
         task->cmd.state = CMD_STATE_FINISHED;
         return(send_cmd_result(task, IBP_E_INVALID_PARAMETER));
      }

      lu = 0; sscanf(string_token(NULL, " ", bstate, &finished), "%lu", &lu);
      r->remote_cto = lu;
      if (lu == 0) {        
         log_printf(1, "read_read(SEND): Bad Remote client timeout value %lu\n", lu);
         task->cmd.state = CMD_STATE_FINISHED;
         return(send_cmd_result(task, IBP_E_INVALID_PARAMETER));
      }
      log_printf(20, "read_read: remote_sto=%lu  remote_cto=%lu\n", r->remote_sto, r->remote_cto);
   }

   debug_printf(1, "read_read: Successfully parsed off=" OT " len=" OT "\n", r->offset, r->len);
   return(0);   
}

//*****************************************************************
//  read_internal_get_alloc - Private command for getting the raw 
//    allocation.
//
// PRIVATE command
//    version INTERNAL_GET_ALLOC RID key_type key offset len timeout\n
//
// key_type = IBP_READCAP|IBP_WRITECAP|IBP_MANAGECAP|INTERNAL_ID
// offset   = -1 No allocation data is transferred. Otherwsie alloc offset
// len      = 0  All available data is returned otherwise len bytes are returned if available
//*****************************************************************

int read_internal_get_alloc(ibp_task_t *task, char **bstate)
{
   int finished, i;
   char *str;
   Cmd_state_t *cmd = &(task->cmd);
   Cmd_internal_get_alloc_t *arg = &(cmd->cargs.get_alloc);

   finished = 0;

   debug_printf(1, "read_internal_get_alloc:  Starting to process buffer\n");

   //** Get the RID
   arg->crid[sizeof(arg->crid)-1] = '\0';
   strncpy(arg->crid, string_token(NULL, " ", bstate, &finished), sizeof(arg->crid)-1);
   if (str2rid(arg->crid, &(arg->rid)) != 0) {
      log_printf(1, "read_internal_get_alloc: Bad RID: %s\n", arg->crid);
      send_cmd_result(task, IBP_E_INVALID_RID);                
      return(-1);
   }

   //*** Get the key type ***
   arg->key_type = atoi(string_token(NULL, " ", bstate, &finished));
      
   //** Get the key ***
   switch(arg->key_type) {
     case IBP_READCAP:
     case IBP_WRITECAP:
     case IBP_MANAGECAP:
         arg->cap.v[sizeof(arg->cap.v)-1] = '\0';
         strncpy(arg->cap.v, string_token(NULL, " ", bstate, &finished), sizeof(arg->cap.v)-1);
         debug_printf(10, "read_internal_get_alloc: cap=%s\n", arg->cap.v);
         break;
     case INTERNAL_ID:
         sscanf(string_token(NULL, " ", bstate, &finished), LU, &(arg->id));
         debug_printf(10, "read_internal_get_alloc: id=" LU "\n", arg->id);
         break;
     default:
         log_printf(10, "read_internal_get_alloc: Invalid key type!  type=%d\n", arg->key_type);
         task->cmd.state = CMD_STATE_FINISHED;
         return(send_cmd_result(task, IBP_E_INVALID_PARAMETER));
   }

   //** Get the offset **
   str = string_token(NULL, " ", bstate, &finished);
   sscanf(str, "%d", &i);
   arg->offset = i;
   if (i != -1) sscanf(str, LU, &(arg->offset));

   //** and the length
   sscanf(string_token(NULL, " ", bstate, &finished), LU, &(arg->len));
   

   debug_printf(10, "read_internal_get_alloc: RID=%s\n", arg->crid);

   get_command_timeout(task, bstate);
   return(0);
}

//*****************************************************************
// read_internal_date_free - Reads the internal date_free command
//
//    RID size timeout timeout\n
//
//*****************************************************************

int read_internal_date_free(ibp_task_t *task, char **bstate)
{
   int d, fin;
   Cmd_state_t *cmd = &(task->cmd);
   Cmd_internal_date_free_t *arg = &(cmd->cargs.date_free);

   fin = 0;

   debug_printf(1, "read_internal_date_free:  Starting to process buffer\n");
   debug_printf(10, "read_internal_date_free:   tm.sec=" TT " tm.usec = " SST "\n", global_config->server.timeout.tv_sec, global_config->server.timeout.tv_usec);

   //** Get the RID
   arg->crid[sizeof(arg->crid)-1] = '\0';
   strncpy(arg->crid, string_token(NULL, " ", bstate, &fin), sizeof(arg->crid)-1);
   if (str2rid(arg->crid, &(arg->rid)) != 0) {
      log_printf(1, "read_internal_date_free: Bad RID: %s\n", arg->crid);
      send_cmd_result(task, IBP_E_INVALID_RID);                
      return(-1);
   }

   //*** Get the size ***
   arg->size = 0;
   d = sscanf(string_token(NULL, " ", bstate, &fin), LU, &(arg->size));
   if (d == 0) {
      log_printf(1, "read_internal_date_free: Bad size: " LU "\n", arg->size);
      send_cmd_result(task, IBP_E_BAD_FORMAT);
      return(-1);
   } 

   get_command_timeout(task, bstate);

   debug_printf(1, "read_internal_date_free: Successfully parsed allocate command\n");
   return(0);
}

//*****************************************************************
// read_internal_expire_list - Reads the internal expire_list command
//
//    RID mode time(sec) count timeout\n
//
//   mode - 0=relative time, 1=absolute time
//
//*****************************************************************

int read_internal_expire_list(ibp_task_t *task, char **bstate)
{
   int d, fin;
   Cmd_state_t *cmd = &(task->cmd);
   Cmd_internal_expire_log_t *arg = &(cmd->cargs.expire_log);

   fin = 0;

   debug_printf(1, "read_internal_expire_list:  Starting to process buffer\n");
   debug_printf(10, "read_internal_expire_list:   tm.sec=" TT " tm.usec = " SST "\n", global_config->server.timeout.tv_sec, global_config->server.timeout.tv_usec);

   //** Get the RID
   arg->crid[sizeof(arg->crid)-1] = '\0';
   strncpy(arg->crid, string_token(NULL, " ", bstate, &fin), sizeof(arg->crid)-1);
   if (str2rid(arg->crid, &(arg->rid)) != 0) {
      log_printf(1, "read_internal_expire_list: Bad RID: %s\n", arg->crid);
      send_cmd_result(task, IBP_E_INVALID_RID);                
      return(-1);
   }

   //*** Get the mode ***
   arg->mode = 0;
   d = sscanf(string_token(NULL, " ", bstate, &fin), "%d", &(arg->mode));
   if (d == 0) {
      log_printf(1, "read_internal_expire_list: Bad mode: %d\n", arg->mode);
      send_cmd_result(task, IBP_E_BAD_FORMAT);
      return(-1);
   } 

   //*** Get the time ***
   arg->start_time = 0;
   d = sscanf(string_token(NULL, " ", bstate, &fin), TT, &(arg->start_time));
   if (d == 0) {
      log_printf(1, "read_internal_expire_list: Bad time: " TT "\n", arg->start_time);
      send_cmd_result(task, IBP_E_BAD_FORMAT);
      return(-1);
   } 
   
   if ((arg->mode == 0) && (arg->start_time != 0)) arg->start_time = arg->start_time + time(NULL);
   
   //*** Get the record count ***
   arg->max_rec = 0;
   d = sscanf(string_token(NULL, " ", bstate, &fin), "%d", &(arg->max_rec));
   if (d == 0) {
      log_printf(1, "read_internal_expire_list: Bad recrd count: %d\n", arg->max_rec);
      send_cmd_result(task, IBP_E_BAD_FORMAT);
      return(-1);
   } 

   arg->direction = DBR_NEXT;

   get_command_timeout(task, bstate);

   debug_printf(1, "read_internal_expire_list: Successfully parsed allocate command\n");
   return(0);
}


