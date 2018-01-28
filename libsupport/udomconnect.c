/*
 * UDOMCONNECT.C - Support unix domain clients
 *
 * (c)Copyright 1999-2002 Backplane, Inc.  Please refer to the COPYRIGHT
 * file at the base of the distribution tree.
 *
 * $Backplane: rdbms/libsupport/udomconnect.c,v 1.9 2002/08/20 22:05:55 dillon Exp $
 */

#include "defs.h"

#include <sys/socket.h>         /* internet sockets     */
#include <sys/un.h>             /* unix domain sockets  */

Export int ConnectUDomSocket(const char *path, const char **perr);

/*
 * ConnectUDomSocket() - connect to the specified unix domain socket,
 *			 specified by path, and return a descriptor or -1
 *
 */
int
ConnectUDomSocket(const char *path, const char **perr)
{
    struct sockaddr_un sou;
    int ufd;
    int i;

    /*
     * Construct unix domain socket
     */
    bzero(&sou, sizeof(sou));
    if ((ufd = socket(AF_UNIX, SOCK_STREAM, 0)) < 0) {
	*perr = "socket(AF_UNIX) failed";
	return(-1);
    }
    sou.sun_family = AF_UNIX;

    snprintf(sou.sun_path, sizeof(sou.sun_path), "%s", path);
    i = strlen(sou.sun_path);
    i = offsetof(struct sockaddr_un, sun_path[i+1]);

    /*
     * Determine if a replication server is already running by
     * attempting to connect to the rendezvous, and fail if
     * it is.
     */
    if (connect(ufd, (void *)&sou, i) < 0) {
	*perr = "Connect(unix-domain) failed";
	close(ufd);
	ufd = -1;
    }
    return(ufd);
}

