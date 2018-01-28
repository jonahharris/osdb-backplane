/*
 * IO.H	- Threaded I/O
 *
 * (c)Copyright 2000-2002 Backplane, Inc.  Please refer to the COPYRIGHT 
 * file at the base of the distribution tree.
 *
 * $Backplane: rdbms/libthreads/io.h,v 1.8 2002/08/20 22:05:57 dillon Exp $
 */

typedef struct IOFd {
    SoftInt	io_SoftInt;
    int		io_Fd;			/* descriptor to select on */
    int		*io_FdRefs;
    int		io_How;			/* what kind of select */
    int		(*io_CtlFunc)(struct IOFd *io);
    void	(*io_UserFunc)(struct IOFd *io, void *buf, int r);
    void	*io_UserData;
    void	*io_Buf;
    void	*io_Alt;

    char	*io_MBuf;		/* for t_gets() / future buffered */
    int		io_MStart;
    int		io_MNewLine;
    int		io_MEnd;
    int		io_MSize;

    int		io_Index;
    int		io_Len;
    SoftTimer	io_Timer;
    int		io_SimpleFdRefs;
    pid_t	io_Pid;			/* pid for t_popen() */
} IOFd;

#define io_Node		io_SoftInt.si_Node
#define io_Flags	io_SoftInt.si_Flags
#define io_Error	io_SoftInt.si_Error

#define SD_READ		1
#define SD_WRITE	2

#define IOMBUF_SIZE	8192

