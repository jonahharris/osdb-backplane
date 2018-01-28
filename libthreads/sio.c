/*
 * IO.C		- IO operations and event waiting
 *
 * (c)Copyright 2000-2002 Backplane, Inc.  Please refer to the COPYRIGHT 
 * file at the base of the distribution tree.
 *
 * $Backplane: rdbms/libthreads/sio.c,v 1.17 2003/05/09 05:57:36 dillon Exp $
 */

#include "defs.h"
#if 0
#include <poll.h>
#endif
#include <sys/socket.h>

Export iofd_t t_accept(iofd_t io, void *sa, int *salen, int to);
Export char *t_gets(iofd_t io, int to);
Export int t_getc(iofd_t io, int to);
Export int t_printf(iofd_t io, int to, char *fmt, ...);
Export int t_read(iofd_t io, void *buf, int bytes, int to);
Export int t_read1(iofd_t io, void *buf, int bytes, int to);
Export int t_write(iofd_t io, const void *buf, int bytes, int to);
Export int t_mread(iofd_t io, void *buf, int bytes, int to);
Export int t_mread1(iofd_t io, void *buf, int bytes, int to);
Export int t_mwrite(iofd_t io, const void *buf, int bytes, int to);
Export int t_mflush(iofd_t io, int to);
Export int t_mprintf(iofd_t io, int to, char *fmt, ...);
Export int t_shutdown(iofd_t io, int how);
Export int t_poll_read(iofd_t io);
Export int t_poll(iofd_t io, int how);

IOFd *
t_accept(IOFd *io, void *sa, int *salen, int to)
{
    int rfd;

    while ((rfd = accept(io->io_Fd, sa, salen)) < 0) {
	if (errno == EINTR)
	    continue;
	if (errno == EWOULDBLOCK) {
	    int r;

	    _ioStart(io, NULL, SD_READ, to);
	    if ((r = waitIo(io)) < 0)
		return(NULL);
	    continue;
	}
	break;
    }
    return(allocIo(rfd));
}

char *
t_gets(iofd_t io, int to)
{
    for (;;) {
	int i;

	/*
	 * If the buffer is empty use this opportunity to reset the
	 * indexes.
	 */
	if (io->io_MStart == io->io_MEnd) {
	    io->io_MStart = 0;
	    io->io_MEnd = 0;
	    io->io_MNewLine = 0;
	}

	/*
	 * Look for the line terminator
	 */
	for (i = io->io_MNewLine; i < io->io_MEnd; ++i) {
	    if (io->io_MBuf[i] == '\n') {
		char *buf;

		io->io_MBuf[i] = 0;
		io->io_MNewLine = i + 1;
		buf = io->io_MBuf + io->io_MStart;
		io->io_MStart = io->io_MNewLine;
		return(buf);
	    }
	}
	io->io_MNewLine = i;

	/*
	 * Not found, realign the buffer
	 */
	if (io->io_MStart != 0) {
	    bcopy(
		io->io_MBuf + io->io_MStart,
		io->io_MBuf,
		io->io_MEnd - io->io_MStart
	    );
	    io->io_MNewLine -= io->io_MStart;
	    io->io_MEnd -= io->io_MStart;
	    io->io_MStart = 0;
	}

	/*
	 * Extend the buffer if necessary
	 */
	if (io->io_MEnd == io->io_MSize) {
	    int newSize = io->io_MSize ? io->io_MSize * 2 : IOMBUF_SIZE;
	    char *buf = zalloc(newSize);

	    if (io->io_MBuf) {
		bcopy(io->io_MBuf, buf, io->io_MSize);
		zfree(io->io_MBuf, io->io_MSize);
	    }
	    io->io_MBuf = buf;
	    io->io_MSize = newSize;
	}

	/*
	 * read more
	 */
	i = t_read1(
	    io, 
	    io->io_MBuf + io->io_MEnd,
	    io->io_MSize - io->io_MEnd, 
	    to
	);
	if (i <= 0)
	    return(NULL);
	io->io_MEnd += i;
    }
    return(NULL);
}

int
t_getc(iofd_t io, int to)
{
    int c;

    while (io->io_MStart == io->io_MEnd) {
	io->io_MStart = 0;
	io->io_MEnd = 0;
	io->io_MNewLine = 0;

	/*
	 * Extend the buffer if necessary
	 */
	if (io->io_MSize == 0) {
	    io->io_MSize = IOMBUF_SIZE;
	    io->io_MBuf = zalloc(io->io_MSize);
	}

	/*
	 * read more
	 */
	c = t_read1(io, io->io_MBuf, io->io_MSize, to);
	if (c <= 0)
	    return(-1);
	io->io_MStart = 0;
	io->io_MNewLine = 0;
	io->io_MEnd = c;
    }
    c = (unsigned char)io->io_MBuf[io->io_MStart];
    ++io->io_MStart;
    io->io_MNewLine = io->io_MStart;

    return(c);
}

int
t_printf(iofd_t io, int to, char *fmt, ...)
{
    va_list va;
    char *buf;
    int len;

    va_start(va, fmt);
    len = vasprintf(&buf, fmt, va);
    va_end(va);

    len = t_write(io, buf, len, to);
    free(buf);
    return(len);
}


/*
 * t_read() -	 Read data from descriptor until EOF or bytes
 */

int
t_read(IOFd *io, void *buf, int bytes, int to)
{
    int r = 0;

    while (bytes > 0) {
	int n = read(io->io_Fd, buf, bytes);
	if (n < 0) {
	    if (errno == EINTR)
		continue;
	    if (errno == EAGAIN) {
		int r2;

		_ioStart(io, NULL, SD_READ, to);
		if ((r2 = waitIo(io)) < 0) {
		    if (r == 0)
			r = r2;
		    break;
		}
		continue;
	    }
	    if (r == 0)
		r = -1;
	    break;
	}
	if (n == 0)
	    break;
	bytes -= n;
	r += n;
	buf = (char *)buf + n;
    }
    return(r);
}

/*
 * t_read1() -	 Read data from descriptor until EOF or at least one byte
 */

int
t_read1(IOFd *io, void *buf, int bytes, int to)
{
    int r = 0;

    while (bytes > 0) {
	int n = read(io->io_Fd, buf, bytes);
	if (n < 0) {
	    if (errno == EINTR)
		continue;
	    if (errno == EAGAIN) {
		int r2;

		_ioStart(io, NULL, SD_READ, to);
		if ((r2 = waitIo(io)) < 0) {
		    if (r == 0)
			r = r2;
		    break;
		}
		continue;
	    }
	    if (r == 0)
		r = -1;
	    break;
	}
	bytes -= n;
	r += n;
	buf = (char *)buf + n;
	break;
    }
    return(r);
}

/*
 * t_write() -	 Write data from buffer to descriptor
 */

int
t_write(IOFd *io, const void *buf, int bytes, int to)
{
    int r = 0;

    while (bytes > 0) {
	int n = write(io->io_Fd, buf, bytes);
	if (n < 0) {
	    if (errno == EINTR)
		continue;
	    if (errno == EAGAIN) {
		int r2;

		_ioStart(io, NULL, SD_WRITE, to);
		if ((r2 = waitIo(io)) < 0) {
		    if (r == 0)
			r = r2;
		    break;
		}
		continue;
	    }
	    if (r == 0)
		r = -1;
	    break;
	}
	bytes -= n;
	r += n;
	buf = (const char *)buf + n;
    }
    return(r);
}

/*
 * t_mread() - buffered full read.
 *
 *	WARNING!  You can call t_mread*() after calling t_read*(), but once
 *	you've called t_mread*() data may be buffered and you should not
 *	make any further direct calls to t_read*().
 */

int
t_mread(iofd_t io, void *buf, int bytes, int to)
{
    int r = 0;
    int total = 0;

    /*
     * Allocate the buffer if necessary
     */
    if (io->io_MSize == 0) {
	io->io_MSize = IOMBUF_SIZE;
	io->io_MBuf = zalloc(io->io_MSize);
    }

    /*
     * Loop until we get everything or EOF, buffer the read
     */
    while (bytes) {
	if (io->io_MStart != io->io_MEnd) {
	    /*
	     * Copy from the buffer, if data is available
	     */
	    if ((r = io->io_MEnd - io->io_MStart) > bytes)
		r = bytes;
	    bcopy(io->io_MBuf + io->io_MStart, buf, r);
	    io->io_MStart += r;
	} else if (bytes > io->io_MSize / 2) {
	    /*
	     * Direct read
	     */
	    io->io_MStart = io->io_MEnd = 0;
	    r = t_read(io, buf, bytes, to);
	} else {
	    /*
	     * Buffer fill
	     */
	    io->io_MStart = io->io_MEnd = 0;
	    r = t_read1(io, io->io_MBuf, io->io_MSize, to);
	    if (r > 0) {
		io->io_MEnd += r;
		continue;
	    }
	}

	/*
	 * Process the amount of data read, terminate loop on EOF or ERROR,
	 * or if we read the requested number of bytes.
	 */
	if (r <= 0) {
	    if (total == 0)
		total = r;
	    break;
	}
	bytes -= r;
	total += r;
	buf = (char *)buf + r;
    }
    return(total);
}

/*
 * t_mread1() - buffered partial read.
 *
 *	WARNING!  You can call t_mread*() after calling t_read*(), but once
 *	you've called t_mread*() data may be buffered and you should not
 *	make any further direct calls to t_read*().
 */

int
t_mread1(iofd_t io, void *buf, int bytes, int to)
{
    int r = 0;
    int total = 0;

    /*
     * Allocate the buffer if necessary
     */
    if (io->io_MSize == 0) {
	io->io_MSize = IOMBUF_SIZE;
	io->io_MBuf = zalloc(io->io_MSize);
    }

    /*
     * Loop until we get at least one byte
     */
    while (bytes) {
	if (io->io_MStart != io->io_MEnd) {
	    /*
	     * Copy from the buffer, if data is available
	     */
	    if ((r = io->io_MEnd - io->io_MStart) > bytes)
		r = bytes;
	    bcopy(io->io_MBuf + io->io_MStart, buf, r);
	    io->io_MStart += r;
	} else {
	    /*
	     * Buffer fill
	     */
	    io->io_MStart = io->io_MEnd = 0;
	    r = t_read1(io, io->io_MBuf, io->io_MSize, to);
	    if (r > 0) {
		io->io_MEnd += r;
		continue;
	    }
	}

	/*
	 * Process the amount of data read.  For t_mread1() we always
	 * terminate the loop.
	 */
	if (r <= 0) {
	    if (total == 0)
		total = r;
	} else {
	    bytes -= r;
	    total += r;
	    buf = (char *)buf + r;
	}
	break;
    }
    return(total);
}

/*
 * t_mwrite() - buffered write
 *
 *	This implements a buffered I/O write.  The data is not actually
 *	flushed to the descriptor until the internal I/O buffer fills up.
 *	See also t_mflush().
 *
 *	The number of bytes written is returned or -1 if an error occured.
 */
int
t_mwrite(iofd_t io, const void *buf, int bytes, int to)
{
    /*
     * Allocate the buffer if necessary
     */
    if (io->io_MSize == 0) {
	io->io_MSize = IOMBUF_SIZE;
	io->io_MBuf = zalloc(io->io_MSize);
    }

    /*
     * Flush the buffer if the data does not fit
     */
    if (io->io_MEnd + bytes > io->io_MSize) {
	if (t_mflush(io, to) < 0)
	    return(-1);
	/*
	 * Write the data directly if it still does not fit,
	 * otherwise buffer it.
	 */
	if (io->io_MEnd + bytes > io->io_MSize)
	    return(t_write(io, buf, bytes, to));
    }
    bcopy(buf, io->io_MBuf + io->io_MEnd, bytes);
    io->io_MEnd += bytes;
    return(bytes);
}

/*
 * t_mflush() -	flush pending buffered write data
 *
 *	Flush any remaining buffered data to the descriptor.  0 is returned
 *	on success, -1 on failure.
 */
int
t_mflush(iofd_t io, int to)
{
    int len = io->io_MEnd - io->io_MStart;
    int r = 0;

    if (len) {
	if (t_write(io, io->io_MBuf + io->io_MStart, len, to) != len)
	    r = -1;
	io->io_MStart = io->io_MEnd = 0;
    }
    return(r);
}

/*
 * t_mprintf() - buffered formatted printf
 */
int
t_mprintf(iofd_t io, int to, char *fmt, ...)
{
    va_list va;
    char *buf;
    int len;

    va_start(va, fmt);
    len = vasprintf(&buf, fmt, va);
    va_end(va);

    len = t_mwrite(io, buf, len, to);
    free(buf);
    return(len);
}

int 
t_shutdown(IOFd *io, int how)
{
    return(shutdown(io->io_Fd, how));
}

#if 0

int
t_poll_read(iofd_t io)
{
    struct pollfd pfd = { 0 };
    pfd.fd = io->io_Fd;
    pfd.events = POLLRDNORM|POLLHUP;
    if (poll(&pfd, 1, 0) >= 0) {
	if (pfd.revents & (POLLRDNORM | POLLHUP))
	    return(0);
    }
    return(-1);
}

int
t_poll(iofd_t io, int how)
{
    if (how == 0)
	return(t_poll_read(io));
    DBASSERT(0);
    return(-1);
}

#endif

