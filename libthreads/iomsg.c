/*
 * IO.C		- IO operations and event waiting
 *
 * (c)Copyright 2000-2002 Backplane, Inc.  Please refer to the COPYRIGHT 
 * file at the base of the distribution tree.
 *
 * $Backplane: rdbms/libthreads/iomsg.c,v 1.7 2002/08/20 22:05:57 dillon Exp $
 */

#include "defs.h"
#include <sys/socket.h>
#include <sys/uio.h>

Export int t_recvmsg_fd(iofd_t io, void *buf, int bytes, iofd_t *pio, int to);
Export int t_sendmsg_fd(iofd_t io, void *buf, int bytes, iofd_t xfd, int to);

#ifdef sun
/*
 * XXX SENDMSG/RECVMSG() FOR SUN
 */

#else

int 
t_recvmsg_fd(IOFd *io, void *buf, int bytes, IOFd **pio, int to)
{
    struct msghdr msg;
    struct {
	struct cmsghdr cmsg;
	int fd;
    } cmsg;
    struct iovec iov;
    int r;

    bzero(&msg, sizeof(msg));
    iov.iov_base = buf;
    iov.iov_len = bytes;

    if (pio) {
	msg.msg_control = (caddr_t)&cmsg;
	msg.msg_controllen = sizeof(cmsg);
	bzero(&cmsg, sizeof(cmsg));
	cmsg.cmsg.cmsg_len = sizeof(cmsg);
	cmsg.fd = -1;
    }
    msg.msg_iov = &iov;
    msg.msg_iovlen = 1;

    while ((r = recvmsg(io->io_Fd, &msg, MSG_EOR)) < 0) {
	if (errno == EINTR)
	    continue;
	if (errno == EAGAIN) {
	    _ioStart(io, NULL, SD_READ, to);
	    if ((r = waitIo(io)) < 0)
		break;
	    continue;
	}
	break;
    }
    if (pio) {
	if (r >= 0 && cmsg.fd >= 0)
	    *pio = allocIo(cmsg.fd);
	else
	    *pio = NULL;
    }
    return(r);
}

int 
t_sendmsg_fd(IOFd *io, void *buf, int bytes, iofd_t xfd, int to)
{
    struct msghdr msg;
    struct {
	struct cmsghdr cmsg;
	int fd;
    } cmsg;
    struct iovec iov;
    int r;

    bzero(&msg, sizeof(msg));
    iov.iov_base = buf;
    iov.iov_len = bytes;

    if (xfd) {
	msg.msg_control = (caddr_t)&cmsg;
	msg.msg_controllen = sizeof(cmsg);
	bzero(&cmsg, sizeof(cmsg));
	cmsg.cmsg.cmsg_level = SOL_SOCKET;
	cmsg.cmsg.cmsg_type = SCM_RIGHTS;
	cmsg.cmsg.cmsg_len = sizeof(cmsg);
	cmsg.fd = xfd->io_Fd;
    }
    msg.msg_iov = &iov;
    msg.msg_iovlen = 1;

    while ((r = sendmsg(io->io_Fd, &msg, 0)) < 0) {
	if (errno == EINTR)
	    continue;
	if (errno == EAGAIN) {
	    _ioStart(io, NULL, SD_WRITE, to);
	    if ((r = waitIo(io)) < 0) 
		break;
	    continue;
	}
	break;
    }
    return(r);
}

#endif
