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

time_t parse_time(char *buffer)
{
  int time_unit[4] = { 1, 60, 3600, 86400 };
  int err, count, i, j;
  int num[4];
  time_t t;
  char *tmp, *bstate;

//printf("parse_time: buffer = %s\n", buffer);

  count = 0;
  tmp = string_token(buffer, ":", &bstate, &err);
  while (( err == 0) && (count < 4)) {
     num[count] = atoi(tmp);
//printf("parse_time: num[%d] = %d\n", count, num[count]);
     count++;
     tmp = string_token(NULL, ":", &bstate, &err);
  }

  count--;
  t = 0; j = 0;
  for (i=count; i >= 0; i--) {
     t = t + time_unit[j] * num[i];
     j++;
  }

//printf("parse_time: time = " TT "\n", t);
  return(t);
}

//*************************************************************************

void parse_line(char *buffer, time_t *t, osd_id_t *id, uint64_t *bytes)
{
  char *bstate;
  int fin;

  *t = 0;
  *id = 0;
  *bytes = 0;

  sscanf(string_token(buffer, " ", &bstate, &fin), TT, t);
  sscanf(string_token(NULL, " ", &bstate, &fin), LU, id);
  sscanf(string_token(NULL, " ", &bstate, &fin), LU, bytes);

//  printf("t=" TT " * id=" LU " * b=" LU "\n", *t, *id, *bytes);
}


//*************************************************************************
//*************************************************************************

int main(int argc, char **argv)
{
  int bufsize = 1024*1024;
  char buffer[bufsize], *bstate;
  int err, i, mode;
  Net_timeout_t dt;
  time_t t;
  osd_id_t id;
  char print_time[128];
  uint64_t bytes, total_bytes;
  double fb1, fb2;
  uint64_t base_unit;
  double units;

  if (argc < 7) {
     printf("expire_list host port RID mode time count\n");
     printf("\n");
     printf("  mode  - abs or rel\n");
     printf("  time  - Future time with format of days:hours:min:sec\n");
     printf("  count - Number of allocations to retreive\n");
     printf("\n");
     return(0);
  }

  base_unit = 1024 * 1024;
  units = base_unit;
  units = 1.0 / units;

  i = 1;
  char *host = argv[i]; i++;
  int port = atoi(argv[i]); i++;
  char *rid = argv[i]; i++;
  mode = 0;
  if (strcmp(argv[i], "abs") == 0) mode = 1; 
  i++;
  t = parse_time(argv[i]); i++;
  int count = atoi(argv[i]);

  dns_cache_init(10);

  NetStream_t *ns = new_netstream();
  ns_config_sock(ns, -1, 0);

  set_net_timeout(&dt, 5, 0);

  err = net_connect(ns, host, port, dt);
  if (err != 0) {
     printf("get_alloc:  Can't connect to host!  host=%s port=%d  err=%d\n", host, port, err);
     return(err);
  }

  sprintf(buffer, "1 %d %s %d " TT " %d 10\n", INTERNAL_EXPIRE_LIST, rid, mode, t, count);
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
  total_bytes = 0;

  printf("n Time date ID  mb_max total_max_mb\n");
  printf("------------------------------------------------------------------------------------------------------\n");
  err = 0;  
  i = 0;
  while (err != -1) {
     buffer[0] = '\0';
     err = readline_netstream(ns, buffer, bufsize, dt);
//printf("err=%d buf=%s\n", err, buffer);
     if (err > 0) {     
        if (strcmp("END", buffer) == 0) { //** Finished
           err = -1;
        } else {
          parse_line(buffer, &t, &id, &bytes);
          ctime_r(&t, print_time); print_time[strlen(print_time)-1] = '\0';
          total_bytes = total_bytes + bytes;
          fb1 = bytes * units;
          fb2 = total_bytes  * units;

          printf("%4d " TT " * %s * " LU " * %lf * %lf\n", i, t, print_time, id, fb1, fb2);
        }
     }

     i++;
  }

  close_netstream(ns);

  printf("\n");

  return(0);
}
