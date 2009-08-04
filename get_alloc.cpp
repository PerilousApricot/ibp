int main(int argc, char ** argv) { }

#if 0
#include <string.h>
#include <stdio.h>
#include <stdlib.h>
#include <time.h>
#include <sys/types.h>
#include <sys/socket.h>
#include <netinet/in.h>
#include <arpa/inet.h>
#include <sys/uio.h>
#include <fcntl.h>
#include <errno.h>
#include <time.h>
#include "allocation.h"
#include "ibp_ClientLib.h"
#include "ibp_server.h"
#include "network.h"
#include "net_sock.h"
#include "log.h"
#include "dns_cache.h"
#include "fmttypes.h"
#include "subnet.h"

void print_manage_history(Allocation_manage_ts_t *ts_list, int start)
{
  char print_time[128];
  char print_time2[128];
  char hostip[256];
  const char *cmd, *subcmd, *rel;
  time_t t, t2;
  int i, slot;
  Allocation_manage_ts_t *ts;

  for (i=0; i<ALLOC_HISTORY; i++) {
     slot = (i + start) % ALLOC_HISTORY;
     ts = &(ts_list[slot]);

     t = ts->ts.time;
     if (t != 0) {
        switch (ts->subcmd) {
          case IBP_PROBE: subcmd = "IBP_PROBE"; break;        
          case IBP_INCR : subcmd = "IBP_INCR "; break;        
          case IBP_DECR : subcmd = "IBP_DECR "; break;        
          case IBP_CHNG : subcmd = "IBP_CHNG "; break;        
          default : subcmd = "UNKNOWN";
        }
    
        switch (ts->cmd) {
          case IBP_MANAGE:         cmd = "IBP_MANAGE       "; break;        
          case IBP_PROXY_MANAGE:   cmd = "IBP_PROXY_MANAGE "; break;        
          case IBP_RENAME:         cmd = "IBP_RENAME        "; subcmd = "        "; break;
          case IBP_PROXY_ALLOCATE: cmd = "IBP_PROXY_ALLOCATE"; subcmd = "        "; break;
          default : cmd = "UNKNOWN";
        }

        switch (ts->reliability) {
          case ALLOC_HARD: rel = "IBP_HARD"; break;        
          case ALLOC_SOFT: rel = "IBP_SOFT"; break;
          default : rel = "UNKNOWN";
        }
       
        ctime_r(&t, print_time); print_time[strlen(print_time)-1] = '\0';
        t2 = ts->expiration;
        ctime_r(&t2, print_time2); print_time2[strlen(print_time2)-1] = '\0';
        address2ipdecstr(hostip, ts->ts.host.ip, ts->ts.host.atype);
        if ((ts->cmd == IBP_PROXY_ALLOCATE) || (ts->cmd == IBP_PROXY_MANAGE)) {
          printf("   " TT " * %s  * %s * " LU " * %s * %s * " LU " * " LU " * " TT " * %s\n", t, print_time, hostip, ts->id, cmd, subcmd, ts->reliability, ts->size, t2, print_time2);
        } else {
          printf("   " TT " * %s  * %s * " LU " * %s * %s * %s * " LU " * " TT " * %s\n", t, print_time, hostip, ts->id, cmd, subcmd, rel, ts->size, t2, print_time2);
        }
     }
  }
}

void print_rw_history(Allocation_rw_ts_t *ts_list, int start)
{
  char print_time[128];
  char hostip[256];
  time_t t;
  int i, slot;
  Allocation_rw_ts_t *ts;

  for (i=0; i<ALLOC_HISTORY; i++) {
     slot = (i + start) % ALLOC_HISTORY;
     ts = &(ts_list[slot]);

     t = ts->ts.time;
     if (t != 0) {
       
        ctime_r(&t, print_time); print_time[strlen(print_time)-1] = '\0';
        address2ipdecstr(hostip, ts->ts.host.ip, ts->ts.host.atype);
        printf("   " TT " * %s * %s * " LU " * " LU " * " LU "\n", t, print_time, hostip, ts->id, ts->offset, ts->size);
     }
  }
}

//*************************************************************************
//*************************************************************************

int main(int argc, const char **argv)
{
  int bufsize = 1024*1024;
  char buffer[bufsize], *bstate;
  Allocation_t a;
  Allocation_history_t h;
  int err, type_key;
  int n, npos, offset, len, ndata, i;
  int  max_wait = 10;
  Net_timeout_t dt;
  char print_time[128];
  char hostip[256];
  char *fname = NULL;
  FILE *fd;
  time_t t;

//printf("sizeof(Allocation_t)=" LU " ALLOC_HEADER=%d\n", sizeof(Allocation_t), ALLOC_HEADER);

  if (argc < 5) {
     printf("get_alloc host port RID key_type key [--file fname offset len]\n");
     printf("where key_type is read|write|manage|id\n");
     printf("   --file stores a portion of the allocation to fname based on the given offset and length\n");
     printf("   fname of stdout or stderr redirects to that device\n");
     printf("   len of 0 means return all data available starting from offset\n");
     printf("\n");
     return(0);
  }

  char *host = argv[1];
  int port = atoi(argv[2]);
  char *rid = argv[3];  
  char *key = argv[5];

  if (strcmp("read", argv[4])==0) {
     type_key = IBP_READCAP;
  } else if (strcmp("write", argv[4])==0) {
     type_key = IBP_WRITECAP;
  } else if (strcmp("manage", argv[4])==0) {
     type_key = IBP_MANAGECAP;
  } else if (strcmp("id", argv[4])==0) {
     type_key = INTERNAL_ID;
  } else {
     printf("invalid type_key = %s\n", argv[3]);
    return(1);
  }

  offset = -1;
  len = 0;
  ndata = 0;
  if (argc > 5) {
     i = 5;
     if (strcmp("--file", argv[i]) == 0) {
        i++;
        fname = argv[i];
        offset = atoi(argv[i]); i++;
        len = atoi(argv[i]);
     }
  }

  dns_cache_init(10);

  NetStream_t *ns = new_netstream();
  ns_config_sock(ns, -1, 0);
  set_net_timeout(&dt, 5, 0);

  err = net_connect(ns, host, port, dt);
  if (err != 0) {
     printf("get_alloc:  Can't connect to host!  host=%s port=%d  err=%d\n", host, port, err);
     return(err);
  }

  sprintf(buffer, "1 %d %s %d %s %d %d 10\n", INTERNAL_GET_ALLOC, rid, type_key, key, offset, len);
  write_netstream_block(ns, time(NULL) + max_wait, buffer, strlen(buffer));
  n = readline_netstream(ns, buffer, bufsize, dt);
  if (n != NS_OK) {  
     n = atoi(string_token(buffer, " ", &bstate, &err));
     if (n != IBP_OK) {
        printf("Error %d returned!\n", n);
        close_netstream(ns);
        return(n);
     }

     ndata = atoi(string_token(buffer, " ", &bstate, &err));
  }

  //** Read the Allocation ***
  err = read_netstream_block(ns, time(NULL) + max_wait, (char *)&a, sizeof(a));
  if (err != NS_OK) {
     printf("get_alloc:  Error reading allocation!  err=%d\n", err);
     abort();
  }

  //** and now the history 
  n = read_netstream_block(ns, time(NULL) + max_wait, (char *)&h, sizeof(h));
  if (err != NS_OK) {
     printf("get_alloc:  Error reading the history!  err=%d\n", err);
     abort();
  }

  close_netstream(ns);

  //** Print the allocation information
  printf("Allocation summary\n");
  printf("-----------------------------------------\n");
  printf("ID: " LU "\n", a.id); 
  t = a.creation_ts.time;
  ctime_r(&t, print_time); print_time[strlen(print_time)-1] = '\0';
  address2ipdecstr(hostip, a.creation_ts.host.ip, a.creation_ts.host.atype);
  printf("Created on: " TT " -- %s by host %s\n", t, print_time, hostip);
  address2ipdecstr(hostip, a.creation_cert.ca_host.ip, a.creation_cert.ca_host.atype);
  printf("Certifed by %s with CA id: " LU "\n", hostip, a.creation_cert.id);
  t = a.expiration;
  ctime_r(&t, print_time); print_time[strlen(print_time)-1] = '\0';
  if (time(NULL) > t) {
     printf("Expiration: " TT " -- %s (EXPIRED)\n", t, print_time);
  } else {
     printf("Expiration: " TT " -- %s\n", t, print_time);
  }

  printf("is_proxy: %d\n", a.is_proxy);
  printf("Read cap: %s\n", a.caps[READ_CAP].v);
  printf("Write cap: %s\n", a.caps[WRITE_CAP].v);
  printf("Manage cap: %s\n", a.caps[MANAGE_CAP].v);

  switch (a.type) {
    case IBP_BYTEARRAY: printf("Type: IBP_BYTEARRAY\n"); break;
    case IBP_BUFFER: printf("Type: IBP_BUFFER\n"); break;
    case IBP_FIFO: printf("Type: IBP_FIFO\n"); break;
    case IBP_CIRQ: printf("Type: IBP_CIRQ\n"); break;
    default: printf("Type: (%d) UNKNOWN TYPE!!!! \n", a.type); break;
  }

  switch (a.reliability) {
    case ALLOC_HARD: printf("Reliability: IBP_HARD\n"); break;
    case ALLOC_SOFT: printf("Reliability: IBP_SOFT\n"); break;
    default: printf("Reliability: (%d) UNKNOWN RELIABILITY!!!\n", a.reliability);
  }

  printf("Current size: " LU "\n", a.size);
  printf("Max size: " LU "\n", a.max_size);
  printf("Read pos: " LU "\n", a.r_pos);
  printf("Write pos: " LU "\n", a.w_pos);
  printf("Read ref count: %u\n", a.read_refcount);
  printf("Write ref count: %u\n", a.write_refcount);

  if (a.is_proxy) {
     printf("Proxy offset: " LU "\n", a.proxy_offset);
     printf("Proxy size: " LU "\n", a.proxy_size);
     printf("Proxy ID: " LU "\n", a.proxy_id);

  }

  printf("\n");
  printf("Read history (slot=%d) (epoch, time, host, id, offset, size\n", h.read_slot);
  printf("---------------------------------------------\n");
  print_rw_history(h.read_ts, h.read_slot);

  printf("\n");
  printf("Write history (slot=%d) (epoch, time, host, id, offset, size)\n", h.write_slot);
  printf("---------------------------------------------\n");
  print_rw_history(h.write_ts, h.write_slot);

  printf("\n");
  printf("Manage history (slot=%d) (epoch, time, host, id, cmd, subcmd, reliability, size, expiration_epoch, expiration)\n", h.manage_slot);
  printf("---------------------------------------------\n");
  print_manage_history(h.manage_ts, h.manage_slot);

  //** Lastly print any data that is requested.
  if (offset > -1) {
     i = 1;
     if (strcmp(fname, "stdout") == 0) {
        fd = stdout;
        i = 0;
     } else if (strcmp(fname, "stderr") == 0) {
        fd = stderr;
        i = 0;
     } else if ((fd = fopen(fname, "w")) != NULL) {
        printf("get_alloc: Can't open file %s!\n", argv[i]);
     }

     printf("\n");
     printf("Storing %d bytes in %s\n", ndata, fname);

     while (ndata > 0) {
        npos = (bufsize > ndata) ? bufsize : ndata;        
        n = read_netstream(ns, buffer, npos, dt);
        if (n > 0) {     
           ndata = ndata - n;
           fwrite(buffer, n, 1, fd);
        }
     }

     fclose(fd);
  }

  printf("\n");

  return(0);
}
#endif
