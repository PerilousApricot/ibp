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

#include "network.h"
#include "net_sock.h"
#include "net_2_ssl.h"

//*********************************************************************
//  d_ssl_close - dual SSL close
//*********************************************************************

int d_ssl_close(net_sock_t *nsock)
{
  int err;
  network_2ssl_t *sock = (network_2ssl_t *)nsock;

  if (sock == NULL) return(0);
  
  err = close(sock->rfd);
  err = close(sock->wfd);

  free(sock);

  return(err);
}

//*********************************************************************
//  d_ssl_write
//*********************************************************************

long int d_ssl_write(net_sock_t *nsock, const void *buf, size_t count, Net_timeout_t tm)
{
  long int n;
  int err;
  fd_set wfd;
  network_2ssl_t *sock = (network_2ssl_t *)nsock;

  if (sock == NULL) return(-1);

  n = write(sock->wfd, buf, count);
//log_printf(15, "d_ssl_write: n=%ld errno=%d\n", n, errno);

  if ((n==-1) && ((errno == EAGAIN) || (errno == EINTR))) n = 0;

  if (n == 0) {  //** Nothing written to let's try and wait
     FD_ZERO(&wfd);
     FD_SET(sock->wfd, &wfd);

     err = select(sock->wfd+1, NULL, &wfd, NULL, &tm);
     if (err > 0) {
        n = write(sock->wfd, buf, count);
	if (n == 0) n = -1;  //** Dead connection
     } 
  }

  return(n);
}

//*********************************************************************
//  d_ssl_read
//*********************************************************************

long int d_ssl_read(net_sock_t *nsock, void *buf, size_t count, Net_timeout_t tm)
{
  long int n;
  int err;
  fd_set rfd;
  network_2ssl_t *sock = (network_2ssl_t *)nsock;

  if (sock == NULL) return(-1);

  n = read(sock->rfd, buf, count);
//log_printf(15, "d_ssl_read: n=%ld errno=%d\n", n, errno);
  if ((n==-1) && ((errno == EAGAIN) || (errno == EINTR))) n = 0;

  if (n == 0) {  //** Nothing written to let's try and wait
     FD_ZERO(&rfd);
     FD_SET(sock->rfd, &rfd);

     err = select(sock->rfd+1, &rfd, NULL, NULL, &tm);
     if (err > 0) {
        n = read(sock->rfd, buf, count);
//log_printf(15, "d_ssl_read2: n=%ld select=%d\n", n, err);
	if (n == 0) n = -1;  //** Dead connection
     } 
  }

  return(n);
}

//*********************************************************************
// ns_merge_ssl - Converts an existing socket ns to a *dual* SSL
//   After the call ns2 can be safely closed/deleted.
//   The resulting *merged* conection is held in ns1.
//   ns1=READ   ns2=WRITE
//*********************************************************************

int ns_merge_ssl(NetStream_t *ns1, NetStream_t *ns2)
{
   if (ns1->sock_type != NS_TYPE_1_SSL) {
      log_printf(0, "ns_merge_ssl:  ns1=%d has wrong type (%d)!\n", ns_getid(ns1), ns1->sock_type);
      return(1);
   }

   if (ns2->sock_type != NS_TYPE_1_SSL) {
      log_printf(0, "ns_merge_ssl:  ns2=%d has wrong type (%d)!\n", ns_getid(ns2), ns2->sock_type);
      return(1);
   }

   network_2ssl_t *sock = (network_2ssl_t *)malloc(sizeof(network_2ssl_t));
   network_sock_t *sock1 = (network_sock_t *)ns1->sock; 
   network_sock_t *sock2 = (network_sock_t *)ns2->sock; 
   
   ns1->sock_type = NS_TYPE_2_SSL;
   ns1->sock = (net_sock_t *)sock;

   sock->rfd = sock1->fd;
   sock->wfd = sock2->fd;

   ns1->close = d_ssl_close;
   ns1->read = d_ssl_read;
   ns1->write = d_ssl_write;

   //****FIXME******
   ns1->set_peer = sock_set_peer;
   ns1->sock_status = sock_status;
   ns1->connect = NULL;
   //****************

   //** Clean up ns2 **
   ns2->sock_type = NS_TYPE_SOCK;
   sock2->fd = -1;

   //** Clean up ns1
   free(sock1);

   return(0);
}

