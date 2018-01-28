/*
 * IO.C		- IO operations and event waiting
 *
 * (c)Copyright 2000-2002 Backplane, Inc.  Please refer to the COPYRIGHT 
 * file at the base of the distribution tree.
 *
 * $Backplane: rdbms/libthreads/io.c,v 1.13 2003/04/02 05:27:15 dillon Exp $
 */

#include "defs.h"
#include <sys/socket.h>

Export iofd_t allocIo(int fd);
Export iofd_t dupIo(iofd_t io);
Export void freeIo(iofd_t io);
Export iofd_t openIo(const char *path, int flags, int perm);
Export void closeIo(iofd_t io);
Export void abortIo(iofd_t io);
Export int waitIo(iofd_t io);
Export void setIoDispatch(iofd_t io, io_func_t func, void *data, int pri);
Export pid_t t_fork(void);
Export void t_didFork(void);

Prototype void	_ioStart(IOFd *io, int (*func)(IOFd *io), int how, int to);
Prototype void	_ioSelect(struct timeval *tv);
Prototype void	_ioInit(iofd_t io, int fd);
Prototype void  _ioTimerInt(IOFd *io);

void _ioSoftInt(IOFd *io, int force);

#if USE_KQUEUE
static int	KQueue = -1;
#else
static int     TaskMaxFds;
static fd_set  TaskRd;
static fd_set  TaskWr;
#endif
static Node    TaskDesc[FD_SETSIZE];

/*
 * allocIo() - 	Allocate and initialize an I/O descriptor for an existing 
 *		OS descriptor
 */
IOFd * 
allocIo(int fd)
{
    IOFd *io = zalloc(sizeof(IOFd));

    fcntl(fd, F_SETFD, 1);		/* set close-on-exec */
    fcntl(fd, F_SETFL, O_NONBLOCK);	/* set non-blocking */
    _ioInit(io, fd);
    return(io);
}

/*
 * dupIo() -	Create a duplicate reference to the same descriptor
 *
 *	This allows you to queue several asynchronous I/O ops on
 *	the same descriptor simultaniously.  The physical descriptor
 *	will not be closed until both IOFd's are closed.
 */

IOFd *
dupIo(iofd_t fromIo)
{
    IOFd *io = zalloc(sizeof(IOFd));

    _ioInit(io, fromIo->io_Fd);
    if (fromIo->io_FdRefs == &fromIo->io_SimpleFdRefs) {
	fromIo->io_FdRefs = zalloc(sizeof(int));
	*fromIo->io_FdRefs = 1;
    }
    io->io_FdRefs = fromIo->io_FdRefs;
    ++*io->io_FdRefs;
    return(io);
}

/*
 * freeIo() -	Free an I/O descriptor (does not close OS descriptor)
 */
void 
freeIo(IOFd *io)
{
    abortIo(io);
    waitIo(io);
    if (io->io_MBuf) {
	zfree(io->io_MBuf, io->io_MSize);
	io->io_MStart = 0;
	io->io_MEnd = 0;
	io->io_MSize = 0;
	io->io_MBuf = NULL;
    }
    zfree(io, sizeof(IOFd));
}

/*
 * openIo() - 	Allocate an I/O descriptor and open the underlying OS desc.
 */
iofd_t 
openIo(const char *path, int flags, int perm)
{
    int fd;

    if ((fd = open(path, flags, perm)) >= 0)
	return(allocIo(fd));
    return(NULL);
}

/*
 * ioClose() -	Close the underlying OS descriptor then free the I/O desc.
 */
void 
closeIo(IOFd *io)
{
    abortIo(io);
    waitIo(io);
    if (io->io_Fd >= 0) {
	if (--*io->io_FdRefs == 0) {
	    if (io->io_FdRefs != &io->io_SimpleFdRefs)
		zfree(io->io_FdRefs, sizeof(int));
	    close(io->io_Fd);
	}
	io->io_FdRefs = &io->io_SimpleFdRefs;
	io->io_Fd = -1;
    }
    if (io->io_MBuf)
	zfree(io->io_MBuf, io->io_MSize);
    zfree(io, sizeof(IOFd));
}

/*
 * setIoDispatch() - set the user I/O software interrupt dispatch function
 */
void
setIoDispatch(IOFd *io, io_func_t func, void *data, int pri)
{
    io->io_UserFunc = func;
    io->io_UserData = data;
    setSoftIntPri(&io->io_SoftInt, pri);
}

/*
 * abortIo() -	Abort in-progress I/O.
 *
 *	This function has no effect if the I/O has already completed.
 */
void
abortIo(IOFd *io)
{
    /*
     * If the I/O is queued to the selector
     */
    abortSoftInt(&io->io_SoftInt);
}

int
waitIo(IOFd *io)
{
    /*
     * Wait for the I/O to complete or the timeout to occur.  The I/O may
     * also be manually aborted.
     */
    return(waitSoftInt(&io->io_SoftInt));
}

/*
 * _ioInit() -	(INTERNAL) Initialize a new I/O descriptor
 */
void
_ioInit(IOFd *io, int fd)
{
    DBASSERT(fd >= 0 && fd < FD_SETSIZE);

    initSoftInt(&io->io_SoftInt, _ioSoftInt);
    setSoftIntPri(&io->io_SoftInt, 1);
    initTimer(&io->io_Timer, 0, 0);
    setTimerDispatch(&io->io_Timer, _ioTimerInt, io, SIPL_TIMER);
    io->io_Fd = fd;
    io->io_FdRefs = &io->io_SimpleFdRefs;
    io->io_SimpleFdRefs = 1;
}

/*
 * _ioStart() -	(INTERNAL) Start an I/O operation on a descriptor
 */
void
_ioStart(IOFd *io, int (*func)(IOFd *io), int how, int to)
{
    Node *node;

    DBASSERT((io->io_Flags & (SIF_RUNNING | SIF_COMPLETE)) == 0);

    /*
     * Setup the softint
     */
    io->io_Flags |= SIF_QUEUED | SIF_RUNNING;
    io->io_Flags &= ~SIF_ABORTED;
    io->io_Error = 0;
    io->io_CtlFunc = func;
    io->io_SoftInt.si_Task = CURTASK;

    /*
     * Set the I/O type and enqueue it to the select()or
     */
    io->io_How = how;
    node = &TaskDesc[io->io_Fd];
    if (node->no_Next == NULL)
	initCNode(node);
    insertNodeBefore(node, &io->io_Node);

    /*
     * Start the timer
     */
    if (to) {
	setTimerTimeout(&io->io_Timer, to, 0);
	startTimer(&io->io_Timer);
    }

#if USE_KQUEUE
    if (KQueue < 0) {
	KQueue = kqueue();
	DBASSERT(KQueue >= 0);
    }
    {
	struct kevent kev;
	struct timespec ts = { 0, 0 };
	int n;

	if (io->io_How == SD_READ)
	    EV_SET(&kev, io->io_Fd, EVFILT_READ, EV_ADD|EV_ENABLE|EV_ONESHOT, 0, 0, NULL);
	else
	    EV_SET(&kev, io->io_Fd, EVFILT_WRITE, EV_ADD|EV_ENABLE|EV_ONESHOT, 0, 0, NULL);
	n = kevent(KQueue, &kev, 1, NULL, 0, &ts);
	DBASSERT(n == 0);
    }
#else
    /*
     * Fixup the select() bitmap and MaxFds.
     */
    if (io->io_How == SD_READ)
	FD_SET(io->io_Fd, &TaskRd);
    else
	FD_SET(io->io_Fd, &TaskWr);

    if (io->io_Fd >= TaskMaxFds)
	TaskMaxFds = io->io_Fd + 1;
#endif
}

#if USE_KQUEUE

void
_ioSelect(struct timeval *tv)
{
    struct kevent kevAry[16];
    struct timespec ts;

    ts.tv_sec = tv->tv_sec;
    ts.tv_nsec = tv->tv_usec * 1000;

    if (KQueue < 0) {
	KQueue = kqueue();
	DBASSERT(KQueue >= 0);
    }

    /*
     * Loop until we are out of events
     */
    for (;;) {
	int i;
	int n;

	n = kevent(KQueue, NULL, 0, kevAry, arysize(kevAry), &ts);
	for (i = 0; i < n; ++i) {
	    struct kevent *kev = &kevAry[i];

	    if (kev->filter == EVFILT_READ) {
		IOFd *io;
		Node *iop = &TaskDesc[kev->ident];

		while (&(io = getSucc(iop))->io_Node != &TaskDesc[kev->ident]) {
		    if (io->io_How == SD_READ) {
			issueSoftInt(&io->io_SoftInt);
		    } else {
			iop = &io->io_Node;
		    }
		}
	    } 
	    if (kev->filter == EVFILT_WRITE) {
		IOFd *io;
		Node *iop = &TaskDesc[kev->ident];

		while (&(io = getSucc(iop))->io_Node != &TaskDesc[kev->ident]) {
		    if (io->io_How == SD_WRITE) {
			issueSoftInt(&io->io_SoftInt);
		    } else {
			iop = &io->io_Node;
		    }
		}
	    }
	}
	if (n < 0 || n != arysize(kevAry))
	    break;
	/*
	 * Subsequent loops poll
	 */
	ts.tv_sec = 0;
	ts.tv_nsec = 0;
    }
}

/*
 * _ioDequeue() -	Dequeue an I/O bypassing the select code
 *
 *	When we do this we have to determine if the descriptor bit in
 *	the select bitmaps representing this I/O should be cleared or not.
 */

void
_ioDequeue(IOFd *io)
{
    IOFd *scan;
    int fdClear = 1;

    removeNode(&io->io_Node);
    io->io_Flags &= ~SIF_QUEUED;
    scan = (IOFd *)&TaskDesc[io->io_Fd];
    while (&(scan = getSucc(&scan->io_Node))->io_Node != &TaskDesc[io->io_Fd]) {
	if (scan->io_How == io->io_How) {
	    fdClear = 0;
	    break;
	}
    }
    if (fdClear) {
	struct kevent kev;
	struct timespec ts = { 0, 0 };

	switch(io->io_How) {
	case SD_READ:
	    EV_SET(&kev, io->io_Fd, EVFILT_READ, EV_DELETE, 0, 0, NULL);
	    kevent(KQueue, &kev, 1, NULL, 0, &ts);
	    break;
	case SD_WRITE:
	    EV_SET(&kev, io->io_Fd, EVFILT_WRITE, EV_DELETE, 0, 0, NULL);
	    kevent(KQueue, &kev, 1, NULL, 0, &ts);
	    break;
	}
    }
}

#else
/*
 * _ioSelect() - (INTERNAL) Wait for events
 */
void
_ioSelect(struct timeval *tv)
{
    int i;
    fd_set rfds = TaskRd;
    fd_set wfds = TaskWr;

    if (select(TaskMaxFds, &rfds, &wfds, NULL, tv) <= 0)
	return;
    for (i = 0; i < TaskMaxFds; ++i) {
	if (FD_ISSET(i, &rfds)) {
	    IOFd *io;
	    Node *iop = &TaskDesc[i];

	    FD_CLR(i, &TaskRd);
	    while (&(io = getSucc(iop))->io_Node != &TaskDesc[i]) {
		if (io->io_How == SD_READ) {
		    issueSoftInt(&io->io_SoftInt);
		} else {
		    iop = &io->io_Node;
		}
	    }
	}
	if (FD_ISSET(i, &wfds)) {
	    IOFd *io;
	    Node *iop = &TaskDesc[i];

	    FD_CLR(i, &TaskWr);
	    while (&(io = getSucc(iop))->io_Node != &TaskDesc[i]) {
		if (io->io_How == SD_WRITE) {
		    issueSoftInt(&io->io_SoftInt);
		} else {
		    iop = &io->io_Node;
		}
	    }
	}
    }

    /*
     * Trim the number of descriptors needed for select()
     */
    i = TaskMaxFds;
    while (i > 0 && TaskDesc[i-1].no_Next == &TaskDesc[i-1])
	--i;
    TaskMaxFds = i;
}

/*
 * _ioDequeue() -	Dequeue an I/O bypassing the select code
 *
 *	When we do this we have to determine if the descriptor bit in
 *	the select bitmaps representing this I/O should be cleared or not.
 */

void
_ioDequeue(IOFd *io)
{
    IOFd *scan;
    int fdClear = 1;

    removeNode(&io->io_Node);
    io->io_Flags &= ~SIF_QUEUED;
    scan = (IOFd *)&TaskDesc[io->io_Fd];
    while (&(scan = getSucc(&scan->io_Node))->io_Node != &TaskDesc[io->io_Fd]) {
	if (scan->io_How == io->io_How) {
	    fdClear = 0;
	    break;
	}
    }
    if (fdClear) {
	switch(io->io_How) {
	case SD_READ:
	    FD_CLR(io->io_Fd, &TaskRd);
	    break;
	case SD_WRITE:
	    FD_CLR(io->io_Fd, &TaskWr);
	    break;
	}
    }
}

#endif

/*
 * _ioTimerInt() -	I/O TIMED OUT
 *
 *	The I/O is aborted.  This is not a real abort in that we do not
 *	set the SIF_ABORTED.  We still want the dispatch to occur as
 *	appropriate so we issue the software itnerrupt for the I/O after
 *	dequeueing it.
 */
void
_ioTimerInt(IOFd *io)
{
    DBASSERT(io->io_Flags & SIF_RUNNING);
    _ioDequeue(io);
    if (io->io_Error == 0)
	io->io_Error = -2;
    issueSoftInt(&io->io_SoftInt);
}

void
_ioSoftInt(IOFd *io, int force)
{
    if (io->io_Flags & SIF_COMPLETE) {
	/*
	 * Completion processing
	 */
	int dispatch = 0;

	if (io->io_UserFunc &&
	    (io->io_Flags & (SIF_ABORTED | SIF_SYNCWAIT)) == 0
	) {
	    dispatch = 1;
	}

	/*
	 * If ready to roll and therer is a control function, call it
	 * to determine if the request is really complete.  If the
	 * control function doesn't think it is, we restart the I/O.
	 */
	if ((force || dispatch) && (io->io_Flags & SIF_ABORTED) == 0) {
	    if (io->io_Error > -2 && io->io_CtlFunc) {
		if (io->io_CtlFunc(io) < 0) {
		    io->io_Flags &= ~SIF_COMPLETE;
		    _ioStart(io, io->io_CtlFunc, io->io_How, 0);
		    return;
		}
	    }
	}

	/*
	 * If the timer is running, abort & wait for it
	 */
	abortTimer(&io->io_Timer);
	waitTimer(&io->io_Timer);

	if (force || dispatch) {
	    io->io_Flags &= ~SIF_COMPLETE;
	    io->io_SoftInt.si_Task = NULL;
	}
	if (dispatch)
	    io->io_UserFunc(io, io->io_Buf, io->io_Error);
    } else {
	/*
	 * Abort Processing
	 */
	DBASSERT(io->io_Flags & SIF_ABORTED);
	DBASSERT(io->io_Flags & SIF_RUNNING);
	abortTimer(&io->io_Timer);
	waitTimer(&io->io_Timer);
	_ioDequeue(io);
	issueSoftInt(&io->io_SoftInt);
	io->io_Error = -1;	/* indicate aborted I/O */
    }
}

pid_t
t_fork(void)
{
    pid_t pid;

    if ((pid = fork()) == 0)
	t_didFork();
    return(pid);
}

void
t_didFork(void)
{
#if USE_KQUEUE
    KQueue = -1;
#endif
}

