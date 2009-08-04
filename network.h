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

//*************************************************************************
//*************************************************************************

#ifndef __NETWORK_H_
#define __NETWORK_H_

#define N_BUFSIZE  1024

#include <sys/select.h>
#include <sys/time.h>
#include <pthread.h>
#include "phoebus.h"
#ifdef _ENABLE_PHOEBUS
#include "liblsl_client.h"
#else 
  typedef void liblslSess;
#endif

#ifdef __cplusplus
extern "C" {
#endif

#define NETWORK_MON_MAX 10   //** Max number of ports allowed to monitor

   //** Return values for write_netstream_block **
#define NS_OK       0   //** Command completed without errors
#define NS_TIMEOUT -1   //** Didn't complete in given time
#define NS_SOCKET  -2   //** Socket error

#define NS_STATE_DISCONNECTED  0   //NetStream is disconnected
#define NS_STATE_CONNECTED     1   //NS is connected with no ongoing transaction
#define NS_STATE_ONGOING_READ  2   //NS is connected and has partially processed a command (in read state)
#define NS_STATE_ONGOING_WRITE 3   //NS is connected and has partially processed a command (in write state)
#define NS_STATE_READ_WRITE    4   //NS is connected and doing both Rread and write operations
#define NS_STATE_IGNORE        5   //NS is connected but is in a holding pattern so don't monitor it for traffic

//******** Type of network connections ***
#define NS_TYPE_UNKNOWN -1      //** Unspecified type
#define NS_TYPE_SOCK     0      //** Base socket implementation
#define NS_TYPE_PHOEBUS  1      //** Phoebus socket implementation
#define NS_TYPE_1_SSL    2      //** Single SSL connection -- openssl/gnutls/NSS are not thread safe so this is **slow**
#define NS_TYPE_2_SSL    3      //** Dual SSL connection -- Allows use of separate R/W locks over SSL much faster than prev

typedef struct timeval Net_timeout_t;

typedef void net_sock_t;

typedef struct {
   int id;                  //ID for tracking purposes
   int cuid;                //Unique ID for the connection.  Changes each time the connection is open/closed
   int start;               //Starting position of buffer data
   int end;                 //End position of buffer data
   int sock_type;           //Socket type
   net_sock_t *sock;        //Private socket data.  Depends on socket type
   time_t last_read;        //Last time this connection was used
   time_t last_write;        //Last time this connection was used
   char buffer[N_BUFSIZE];  //intermediate buffer for the conection
   pthread_mutex_t read_lock;    //Read lock
   pthread_mutex_t write_lock;   //Write lock
   char peer_address[128];
   int (*close)(net_sock_t *sock);  //** Close socket
   long int(*write)(net_sock_t *sock, const void *buf, size_t count, Net_timeout_t tm);
   long int (*read)(net_sock_t *sock, void *buf, size_t count, Net_timeout_t tm);
   void (*set_peer)(net_sock_t *sock, char *address, int add_size);
   int (*sock_status)(net_sock_t *sock);
   int (*connect)(net_sock_t *sock, const char *hostname, int port, Net_timeout_t timeout);
   net_sock_t *(*accept)(net_sock_t *sock);
   int (*bind)(net_sock_t *sock, char *address, int port);
   int (*listen)(net_sock_t *sock, int max_pending);
   int (*connection_request)(net_sock_t *sock, int timeout);
} NetStream_t;

typedef struct {   //** Struct used to handle ports being monitored
   NetStream_t *ns;       //** Connection actually being monitored
   char *address;         //** Interface to bind to
   int port;              //** Port to use
   int is_pending;        //** Flags the connections as ready for an accept call
   int shutdown_request;  //** Flags the connection to shutdown
   pthread_t thread;      //** Execution thread handle
   pthread_mutex_t lock;  //** Lock used for blocking pending accept
   pthread_cond_t cond;   //** cond used for blocking pending accept
   pthread_mutex_t *trigger_lock;  //** Lock used for sending globabl pending trigger 
   pthread_cond_t *trigger_cond;   //** cond used for sending globabl pending accept
   int *trigger_count;             //** Gloabl count of pending requests
} ns_monitor_t;

typedef struct {
   int trigger;             //Trigger used to wake up the network
   int accept_pending;      //New connection is pending
   int used_ports;          //Number of monitor ports used
   ns_monitor_t nm[NETWORK_MON_MAX];  //List of ports being monitored
   pthread_mutex_t ns_lock; //Lock for serializing ns modifications
   pthread_cond_t cond;   //** cond used for blocking pending accept
} Network_t;

#define ns_getid(ns) ns->id
void set_network_tcpsize(int tcpsize);
int get_network_tcpsize(int tcpsize);
int ns_merge_ssl(NetStream_t *ns1, NetStream_t *ns2);
int ns_socket2ssl(NetStream_t *ns);
void set_ns_slave(NetStream_t *ns, int slave);
int connection_is_pending(Network_t *net);
int wait_for_connection(Network_t *net, int max_wait);
void lock_ns(NetStream_t *ns);
void unlock_ns(NetStream_t *ns);
int network_counter(Network_t *net);
int net_connect(NetStream_t *ns, const char *host, int port, Net_timeout_t timeout);
int bind_server_port(Network_t *net, NetStream_t *ns, char *address, int port, int max_pending);
Network_t *network_init();
void close_netstream(NetStream_t *ns);
void destroy_netstream(NetStream_t *ns);
NetStream_t *new_netstream();
void network_close(Network_t *net);
void network_destroy(Network_t *net);
int sniff_connection(NetStream_t *ns);
int write_netstream(NetStream_t *ns, const char *buffer, int bsize, Net_timeout_t timeout);
int write_netstream_block(NetStream_t *ns, time_t end_time, char *buffer, int size);
int read_netstream_block(NetStream_t *ns, time_t end_time, char *buffer, int size);
int read_netstream(NetStream_t *ns, char *buffer, int size, Net_timeout_t timeout);
int readline_netstream_raw(NetStream_t *ns, char *buffer, int size, Net_timeout_t timeout, int *status);
int readline_netstream(NetStream_t *ns, char *buffer, int size, Net_timeout_t timeout);
int accept_pending_connection(Network_t *net, NetStream_t *ns);
Net_timeout_t *set_net_timeout(Net_timeout_t *tm, int sec, int us);
void ns_init(NetStream_t *ns);
void _ns_init(NetStream_t *ns, int incid);
void wakeup_network(Network_t *net);

#ifdef __cplusplus
}
#endif

#endif

