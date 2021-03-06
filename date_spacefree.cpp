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

void parse_line(char *buffer, time_t *t, uint64_t *a_count, uint64_t *p_count, uint64_t *bytes_used, uint64_t *bytes)
{
  char *bstate;
  int fin;

  *t = 0;
  *a_count = *p_count = *bytes_used = *bytes = 0;

  sscanf(string_token(buffer, " ", &bstate, &fin), TT, t);
  sscanf(string_token(NULL, " ", &bstate, &fin), LU, a_count);
  sscanf(string_token(NULL, " ", &bstate, &fin), LU, p_count);
  sscanf(string_token(NULL, " ", &bstate, &fin), LU, bytes_used);
  sscanf(string_token(NULL, " ", &bstate, &fin), LU, bytes);

//  printf("t=" TT " * ac=" LU " * pc=" LU " * bu=" LU " * b=" LU "\n", *t, *a_count, *p_count, *bytes_used, *bytes);
}


//*************************************************************************
//*************************************************************************

int main(int argc, char **argv)
{
  int bufsize = 1024*1024;
  char buffer[bufsize], *bstate;
  int err, print_full, i;
  Net_timeout_t dt;
  time_t t;
  uint64_t size;
  char print_time[128];
  uint64_t a_count, p_count, count, a_total, p_total, total;
  uint64_t bytes_used, bytes, total_bytes_used, total_bytes;
  double fb1, fb2, fb3, fb4;
  uint64_t base_unit;
  double units;

  if (argc < 4) {
     printf("date_spacefree [-full] host port RID size(mb)\n");
     return(0);
  }

  base_unit = 1024 * 1024;
  units = base_unit;
  units = 1.0 / units;

  print_full = 0;
  i = 1;
  if (strcmp(argv[i], "-full") == 0) { print_full = 1; i++; }

  char *host = argv[i]; i++;
  int port = atoi(argv[i]); i++;
  char *rid = argv[i]; i++;
  sscanf(argv[i], "%lf", &fb1);
  size = fb1 * base_unit;

  dns_cache_init(10);

  NetStream_t *ns = new_netstream();
  ns_config_sock(ns, -1, 0);

  set_net_timeout(&dt, 5, 0);

  err = net_connect(ns, host, port, dt);
  if (err != 0) {
     printf("get_alloc:  Can't connect to host!  host=%s port=%d  err=%d\n", host, port, err);
     return(err);
  }

  sprintf(buffer, "1 %d %s " LU " 10\n", INTERNAL_DATE_FREE, rid, size);
  err = write_netstream(ns, buffer, strlen(buffer), dt);
  err = readline_netstream(ns, buffer, bufsize, dt);
  if (err > 0) {  
     err = atoi(string_token(buffer, " ", &bstate, &err));
     if (err != IBP_OK) {
        printf("Error %d returned!\n", err);
        close_netstream(ns);
        return(err);
     }
  }

  //** Cycle through the data **
  a_total = p_total = 0;
  total_bytes_used = total_bytes = 0;

  if (print_full) {
     printf("Time date Tallocs Talias Ttotal mb_used mb_max Nallocs Nalias Ntotal total_used_mb  total_max_mb\n");
  } else {
     printf("Time date Ttotal mb_max Ntotal total_max_mb\n");
  }
  printf("------------------------------------------------------------------------------------------------------\n");
  err = 0;  
  while (err != -1) {
     buffer[0] = '\0';
     err = readline_netstream(ns, buffer, bufsize, dt);
//printf("err=%d buf=%s\n", err, buffer);
     if (err > 0) {     
        if (strcmp("END", buffer) == 0) { //** Finished
           err = -1;
        } else {
          parse_line(buffer, &t, &a_count, &p_count, &bytes_used, &bytes);
          ctime_r(&t, print_time); print_time[strlen(print_time)-1] = '\0';
          count = a_count + p_count;
          a_total = a_total + a_count; p_total = p_total + p_count;
          total_bytes_used = total_bytes_used + bytes_used;
          total_bytes = total_bytes + bytes;
          total = a_total + p_total;

          fb1 = bytes_used * units;
          fb2 = bytes * units;
          fb3 = total_bytes_used * units;
          fb4 = total_bytes  * units;

          if (print_full) {
              printf(TT " * %s " LU " * " LU " * " LU " * %lf * %lf * " LU " * " LU " * " LU " * %lf * %lf\n", 
                      t, print_time, a_count, p_count, count, fb1, fb2, a_total, p_total, total, fb3, fb4);
          } else {
             printf(TT " * %s * " LU " * %lf * " LU " * %lf\n", 
                     t, print_time, count, fb2, total,fb4);
          }
        }
     }
  }

  close_netstream(ns);

  printf("\n");

  return(0);
}
