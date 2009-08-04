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
#include "network.h"
#include "net_sock.h"
#include "log.h"
#include "dns_cache.h"
#include "string_token.h"

//** This is a hack to not have to have the ibp source
#define IBP_OK 1

//*************************************************************************
//*************************************************************************

int main(int argc, char **argv)
{
  int bufsize = 1024*1024;
  char buffer[bufsize], *bstate;
  int err;
  int n;
  Net_timeout_t dt;


  if (argc < 3) {
     printf("get_version host port\n");
     return(0);
  }

  char *host = argv[1];
  int port = atoi(argv[2]);
  char *cmd = argv[3];

  cmd = "1 4 5 10\n";  // IBP_ST_VERSION command

  dns_cache_init(10);

  NetStream_t *ns = new_netstream();
  ns_config_sock(ns, -1, 0);
  set_net_timeout(&dt, 5, 0);

  //** Make the host connection
  err = net_connect(ns, host, port, dt);
  if (err != 0) {
     printf("get_version:  Can't connect to host!  host=%s port=%d  err=%d\n", host, port, err);
     return(err);
  }


  //** Send the command
  n = write_netstream(ns, cmd, strlen(cmd), dt);
  n = readline_netstream(ns, buffer, bufsize, dt);
  if (n == IBP_OK) {  
     n = atoi(string_token(buffer, " ", &bstate, &err));
     if (n != IBP_OK) {
        printf("Error %d returned!\n", n);
        close_netstream(ns);
        return(n);
     }
  }

  //** Read the result.  Termination occurs when the line "END" is read.
  //** Note that readline_netstream strips the "\n" from the end of the line
  n = 1;
  while (n > 0) {
     n = readline_netstream(ns, buffer, bufsize, dt);
     if (n > 0) {     
        if (strcmp(buffer, "END") == 0) {
           n = 0;
        } else {  
           printf("%s\n", buffer);
        }
     }
  }

  //** Close the connection
  close_netstream(ns);

  return(0);
}
