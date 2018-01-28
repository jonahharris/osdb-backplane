/*
 * INETLISTEN.C - Setup an internet domain socket to listen on
 *
 * (c)Copyright 1999-2002 Backplane, Inc.  Please refer to the COPYRIGHT
 * file at the base of the distribution tree.
 *
 * $Backplane: rdbms/libsupport/inetlisten.c,v 1.3 2002/08/20 22:05:55 dillon Exp $
 */

#include "defs.h"
#include <sys/socket.h>         /* internet sockets     */
#include <netinet/in.h>
#include <arpa/inet.h>
#include <netdb.h>


Export int BuildINetSocket(const char *host, int port, const char **perr);

/*
 * BuildINetSocket() - create internet domain socket on port, bound to
 *		       an optional host, and listen on it.
 */
int
BuildINetSocket(const char *host, int port, const char **perr)
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
    {
	int on = 1;
	setsockopt(ufd, SOL_SOCKET, SO_REUSEADDR, (void *)&on, sizeof(on));
    }

    sock.sin_family = AF_INET;
    sock.sin_port = htons(port);

    if (host == NULL) {
	sock.sin_addr.s_addr = INADDR_ANY;
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
    if (bind(ufd, (void *)&sock, sizeof(sock)) < 0) {
	*perr = strerror(errno);
	close(ufd);
	return(-1);
    }

    /*
     * Listen on it (non-blocking)
     */
    if (listen(ufd, 32) < 0) {
	*perr = strerror(errno);
	close(ufd);
	ufd = -1;
    }
    return(ufd);
}

