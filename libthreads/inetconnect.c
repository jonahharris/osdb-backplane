/*
 * INETCONNECT.C - Support internet domain clients (threaded)
 *
 * (c)Copyright 2000-2002 Backplane, Inc.  Please refer to the COPYRIGHT 
 * file at the base of the distribution tree.
 *
 * $Backplane: rdbms/libthreads/inetconnect.c,v 1.3 2002/08/20 22:05:57 dillon Exp $
 */

#include "defs.h"

#include <sys/socket.h>         /* internet sockets     */
#include <netinet/in.h>
#include <arpa/inet.h>
#include <netdb.h>

Export iofd_t t_ConnectINetSocket(const char *host, int port, const char **perr, int to);

/*
 * t_ConnectINetSocket() - Connect to the specified host/port on the internet
 */
iofd_t
t_ConnectINetSocket(const char *host, int port, const char **perr, int to)
{
    struct sockaddr_in sock;
    struct hostent *he;
    int ufd;
    iofd_t io;

    /*
     * Construct internet domain socket
     */
    bzero(&sock, sizeof(sock));
    if ((ufd = socket(AF_INET, SOCK_STREAM, 0)) < 0) {
	*perr= "socket(AF_INET)";
	return(NULL);
    }
    sock.sin_family = AF_INET;
    sock.sin_port = htons(port);

    if (host == NULL) {
	sock.sin_addr.s_addr = 0;
    } else if (inet_aton(host, &sock.sin_addr) == 0) {
	if ((he = gethostbyname2(host, AF_INET)) == NULL) {
	    *perr = "host lookup failed";
	    return(NULL);
	}
	bcopy(he->h_addr, &sock.sin_addr, he->h_length);
    }

    io = allocIo(ufd);
    errno = 0;

    /*
     * Alloc an iofd_t (which also sets the descriptor non-blocking) and
     * wait for the connect to succeed.
     */
    while (connect(ufd, (void *)&sock, sizeof(sock)) < 0) {
	int r;

	if (errno != EINPROGRESS && errno != EALREADY && errno != EISCONN) {
	    *perr = strerror(errno);
	    closeIo(io);
	    io = NULL;
	    break;
	}
	if (errno == EISCONN)
	    break;
	_ioStart(io, NULL, SD_WRITE, to);
	if ((r = waitIo(io)) < 0) {
	    *perr = "Connection timed out";
	    closeIo(io);
	    io = NULL;
	    break;
	}
	errno = 0;
    }
    return(io);
}

