/*
 * INETCONNECT.C - Support internet domain clients
 *
 * (c)Copyright 1999-2002 Backplane, Inc.  Please refer to the COPYRIGHT
 * file at the base of the distribution tree.
 *
 * $Backplane: rdbms/libsupport/inetconnect.c,v 1.3 2002/08/20 22:05:55 dillon Exp $
 */

#include "defs.h"

#include <sys/socket.h>         /* internet sockets     */
#include <netinet/in.h>
#include <arpa/inet.h>
#include <netdb.h>

Export int ConnectINetSocket(const char *host, int port, const char **perr);

/*
 * ConnectINetSocket() - Connect to the specified host/port on the internet
 *			 WARNING: not threaded!
 */
int
ConnectINetSocket(const char *host, int port, const char **perr)
{
    struct sockaddr_in sock;
    struct hostent *he;
    int ufd;

    /*
     * Construct internet domain socket
     */
    bzero(&sock, sizeof(sock));
    if ((ufd = socket(AF_INET, SOCK_STREAM, 0)) < 0) {
	*perr= "socket(AF_INET)";
	return(-1);
    }
    sock.sin_family = AF_INET;
    sock.sin_port = htons(port);

    if (host == NULL) {
	sock.sin_addr.s_addr = 0;
    } else if (inet_aton(host, &sock.sin_addr) == 0) {
	if ((he = gethostbyname2(host, AF_INET)) == NULL) {
	    *perr = "host lookup failed";
	    return(-1);
	}
	bcopy(he->h_addr, &sock.sin_addr, he->h_length);
    }

    /*
     * Bind to the interface/port
     */
    if (connect(ufd, (void *)&sock, sizeof(sock)) < 0) {
	*perr = strerror(errno);
	close(ufd);
	ufd = -1;
    }
    return(ufd);
}

