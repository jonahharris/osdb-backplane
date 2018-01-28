/*
 * UDOMCONNECT.C - Support unix domain clients (threaded)
 *
 * (c)Copyright 2000-2002 Backplane, Inc.  Please refer to the COPYRIGHT 
 * file at the base of the distribution tree.
 *
 * $Backplane: rdbms/libthreads/udomconnect.c,v 1.2 2002/08/20 22:05:57 dillon Exp $
 */

#include "defs.h"

#include <sys/socket.h>         /* internet sockets     */
#include <sys/un.h>             /* unix domain sockets  */

Export iofd_t t_ConnectUDomSocket(const char *path, const char **perr, int to);

/*
 * t_ConnectUDomSocket() - connect to the specified unix domain socket,
 *			 specified by path, and return a descriptor or NULL
 *
 */
iofd_t
t_ConnectUDomSocket(const char *path, const char **perr, int to)
{
    struct sockaddr_un sou;
    int ufd;
    int i;
    iofd_t io;

    /*
     * Construct unix domain socket
     */
    bzero(&sou, sizeof(sou));
    if ((ufd = socket(AF_UNIX, SOCK_STREAM, 0)) < 0) {
	*perr = "socket(AF_UNIX) failed";
	return(NULL);
    }
    sou.sun_family = AF_UNIX;

    snprintf(sou.sun_path, sizeof(sou.sun_path), "%s", path);
    i = strlen(sou.sun_path);
    i = offsetof(struct sockaddr_un, sun_path[i+1]);

    io = allocIo(ufd);
    errno = 0;

    /*
     * Alloc an iofd_t (which also sets the descriptor non-blocking) and 
     * wait for the connect to succeed.
     */
    while (connect(ufd, (void *)&sou, i) < 0) {
	int r;

	if (errno != EINPROGRESS && errno != EALREADY && errno != EISCONN) {
	    *perr = "Connect(unix-domain) failed";
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

