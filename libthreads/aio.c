/*
 * AIO.C	- Asynchronous IO operations
 *
 * (c)Copyright 2000-2002 Backplane, Inc.  Please refer to the COPYRIGHT 
 * file at the base of the distribution tree.
 *
 * $Backplane: rdbms/libthreads/aio.c,v 1.4 2002/08/20 22:05:57 dillon Exp $
 */

#include "defs.h"
#include <sys/socket.h>

Export void t_accepta(iofd_t io, void *sa, int *saLen, int to);
Export void t_reada(iofd_t io, void *buf, int bytes, int to);
Export void t_read1a(iofd_t io, void *buf, int bytes, int to);
Export void t_writea(iofd_t io, void *buf, int bytes, int to);

Prototype void _ioQueue(IOFd *io);

int
_ioAcceptAInt(IOFd *io)
{
    int rfd;

    if ((rfd = accept(io->io_Fd, io->io_Buf, io->io_Alt)) < 0) {
	if (errno == EINTR || errno == EWOULDBLOCK)
	    return(-1);
	io->io_Error = -errno;
    } else {
	io->io_Error = rfd;
    }
    return(0);
}

void 
t_accepta(IOFd *io, void *sa, int *saLen, int to)
{
    io->io_Buf = sa;
    io->io_Alt = saLen;
    _ioStart(io, _ioAcceptAInt, SD_READ, to);
}

int
_ioReadAInt(IOFd *io)
{
    int r;

    r = read(io->io_Fd, (char *)io->io_Buf + io->io_Index, io->io_Len - io->io_Index);
    if (r < 0) {
	if (errno == EINTR || errno == EAGAIN)
	    return(-1);
	if (io->io_Error == 0)
	    io->io_Error = -errno;
    } else {
	io->io_Index += r;
	io->io_Error = io->io_Index;
	if (io->io_Index != io->io_Len)
	    return(-1);
    }
    return(0);
}

void 
t_reada(IOFd *io, void *buf, int bytes, int to)
{
    io->io_Buf = buf;
    io->io_Index = 0;
    io->io_Len = bytes;
    _ioStart(io, _ioReadAInt, SD_READ, to);
}

int
_ioRead1AInt(IOFd *io)
{
    int r;

    r = read(io->io_Fd, (char *)io->io_Buf + io->io_Index, io->io_Len - io->io_Index);
    if (r < 0) {
	if (errno == EINTR || errno == EAGAIN)
	    return(-1);
	if (io->io_Error == 0)
	    io->io_Error = -errno;
    } else {
	io->io_Index += r;
	io->io_Error = io->io_Index;
    }
    return(0);
}

void 
t_read1a(IOFd *io, void *buf, int bytes, int to)
{
    io->io_Buf = buf;
    io->io_Index = 0;
    io->io_Len = bytes;
    _ioStart(io, _ioRead1AInt, SD_READ, to);
}

int
_ioWriteAInt(IOFd *io)
{
    int r;

    r = write(io->io_Fd, (char *)io->io_Buf + io->io_Index, io->io_Len - io->io_Index);
    if (r < 0) {
	if (errno == EINTR || errno == EAGAIN)
	    return(-1);
	if (io->io_Error == 0)
	    io->io_Error = -errno;
    } else {
	io->io_Index += r;
	io->io_Error = io->io_Index;
	if (io->io_Index != io->io_Len)
	    return(-1);
    }
    return(0);
}

void 
t_writea(IOFd *io, void *buf, int bytes, int to)
{
    io->io_Buf = buf;
    io->io_Index = 0;
    io->io_Len = bytes;
    _ioStart(io, _ioWriteAInt, SD_READ, to);
}

