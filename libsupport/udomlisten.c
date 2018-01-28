/*
 * UDOMLISTEN.C - Setup a unix domain socket to listen on
 *
 * (c)Copyright 1999-2002 Backplane, Inc.  Please refer to the COPYRIGHT
 * file at the base of the distribution tree.
 *
 * $Backplane: rdbms/libsupport/udomlisten.c,v 1.12 2002/08/20 22:05:55 dillon Exp $
 */

#include "defs.h"

#include <sys/socket.h>         /* internet sockets     */
#include <sys/un.h>             /* unix domain sockets  */

Export int BuildUDomSocket(const char *path, const char **perr);

/*
 * BuildUDomSocket() -	setup a unix domain socket at the specified path
 *			and listen on it, returning the descriptor or -1
 *
 */
int
BuildUDomSocket(const char *path, const char **perr)
{
    struct sockaddr_un sou;
    int ufd;
    int i;
    /* int um; */

    /*
     * Construct unix domain socket
     */
    bzero(&sou, sizeof(sou));
    if ((ufd = socket(AF_UNIX, SOCK_STREAM, 0)) < 0) {
	*perr= "socket(AF_UNIX)";
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
    if (connect(ufd, (void *)&sou, i) == 0) {
	*perr= "Someone is already listening on the socket";
	close(ufd);
	return(-1);
    }

    /*
     * Bind to it.  Remove any prior rendezvous.
     */
    remove(sou.sun_path);
    /* um = umask(0077);  SECURITY HOLE */
    if (bind(ufd, (void *)&sou, i) < 0) {
	*perr= "bind(unix-domain) failed";
	close(ufd);
	return(-1);
    }
    /* umask(um); */

    /*
     * Listen on it (non-blocking)
     */
    if (listen(ufd, 32) < 0) {
	*perr= "listen(unix-domain) failed";
	close(ufd);
	ufd = -1;
	remove(sou.sun_path);
    }

    return(ufd);
}

