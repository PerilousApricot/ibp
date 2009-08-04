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

//*********************************************************************
//*********************************************************************

#include <sys/types.h>
#include <sys/socket.h>
#include <netinet/in.h>
#include <netinet/tcp.h>
#include <arpa/inet.h>
#include <sys/uio.h>
#include <netdb.h>
#include <unistd.h>
#include <string.h>
#include <stdlib.h>
#include <time.h>
#include <assert.h>
#include <signal.h>
#include <errno.h>
#include <fcntl.h>
#include "network.h"
#include "debug.h"
#include "log.h"
#include "dns_cache.h"
#include "fmttypes.h"
#include "net_sock.h"
#include "net_fd.h"

//*********************************************************************
// sock_set_peer - Gets the remote sockets hostname 
//*********************************************************************

void sock_set_peer(net_sock_t *nsock, char *address, int add_size)
{
   network_sock_t *sock = (network_sock_t *)nsock;   

   fd_set_peer(sock->fd, address, add_size);

   return;
}

//*********************************************************************
//  sock_status - Returns 1 if the socket is connected and 0 otherwise
//*********************************************************************

int sock_status(net_sock_t *nsock)
{
  network_sock_t *sock = (network_sock_t *)nsock;   
  if (sock == NULL) return(0);

  return(fd_status(sock->fd));
}

//*********************************************************************
//  sock_close - Base socket close call
//*********************************************************************

int sock_close(net_sock_t *nsock)
{
  network_sock_t *sock = (network_sock_t *)nsock;   

  if (sock == NULL) return(0);
  if (sock->fd == -1) return(0);

log_printf(15, "sock_close: closing fd=%d\n", sock->fd); 

  int err = close(sock->fd);

  free(sock);

  return(err);
}

//*********************************************************************
//  sock_write
//*********************************************************************

long int sock_write(net_sock_t *nsock, const void *buf, size_t count, Net_timeout_t tm)
{
  network_sock_t *sock = (network_sock_t *)nsock;   

  if (sock == NULL) return(-1);   //** If closed return

  return(fd_write(sock->fd, buf, count, tm));
}

//*********************************************************************
//  sock_read
//*********************************************************************

long int sock_read(net_sock_t *nsock, void *buf, size_t count, Net_timeout_t tm)
{
  network_sock_t *sock = (network_sock_t *)nsock;   

  if (sock == NULL) return(-1);   //** If closed return

  return(fd_read(sock->fd, buf, count, tm));
}

//*********************************************************************
// sock_connect - Creates a connection to a remote host
//*********************************************************************

int sock_connect(net_sock_t *nsock, const char *hostname, int port, Net_timeout_t timeout)
{
   network_sock_t *sock = (network_sock_t *)nsock;   

   if (sock == NULL) return(-1);   //** If NULL exit

   return(fd_connect(&(sock->fd), (char *) hostname, port, sock->tcpsize, timeout));
}

//*********************************************************************
// sock_connection_request - Waits for a connection request or times out 
//     If a request is made then 1 is returned otherwise 0 for timeout.
//     -1 signifies an error.
//*********************************************************************

int sock_connection_request(net_sock_t *nsock, int timeout)
{
  network_sock_t *sock = (network_sock_t *)nsock;   

  if (sock == NULL) return(-1);

  return(fd_connection_request(sock->fd, timeout));
}

//*********************************************************************
//  sock_accept - Accepts a socket request
//*********************************************************************

net_sock_t *sock_accept(net_sock_t *nsock)
{
  network_sock_t *psock = (network_sock_t *)nsock;   

  network_sock_t *sock = (network_sock_t *)malloc(sizeof(network_sock_t));
  assert(sock != NULL);

  sock->fd = fd_accept(psock->fd);
  if (sock->fd == -1) {
     free(sock);
     sock = NULL;
  }

  return(sock);    
}

//*********************************************************************
//  sock_bind - Binds a socket to the requested port
//*********************************************************************

int sock_bind(net_sock_t *nsock, char *address, int port)
{
  network_sock_t *sock = (network_sock_t *)nsock;   

  if (sock == NULL) return(1);

  sock->fd = fd_bind(address, port);
  if (sock->fd < 0) return(1);

  return(0);
}

//*********************************************************************
//  sock_listen
//*********************************************************************

int sock_listen(net_sock_t *nsock, int max_pending)
{
  network_sock_t *sock = (network_sock_t *)nsock;   

  if (sock == NULL) return(1);

  return(fd_listen(sock->fd, max_pending));
}


//*********************************************************************
// ns_config_sock - Configure the connection to use standard sockets 
//*********************************************************************

void ns_config_sock(NetStream_t *ns, int fd, int tcpsize)
{
  log_printf(10, "ns_config_sock: ns=%d, fd=%d\n", ns->id, fd);

  _ns_init(ns, 0);

  ns->sock_type = NS_TYPE_SOCK;
  network_sock_t *sock = (network_sock_t *)malloc(sizeof(network_sock_t));
  assert(sock != NULL);
  ns->sock = (net_sock_t *)sock;
  sock->fd = fd;
  sock->tcpsize = tcpsize;
  ns->connect = sock_connect;
  ns->sock_status = sock_status;
  ns->set_peer = sock_set_peer;
  ns->close = sock_close;
  ns->read = sock_read;
  ns->write = sock_write;
  ns->accept = sock_accept;
  ns->bind = sock_bind;
  ns->listen = sock_listen;
  ns->connection_request = sock_connection_request;
}

