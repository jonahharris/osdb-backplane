/*
 * PTHREAD.C
 */

#include "defs.h"
#include <sys/uio.h>

Prototype tlock_t allocTLock(void);
Prototype void freeTLock(tlock_t *plock);
Prototype void getTLock(tlock_t lock);
Prototype void relTLock(tlock_t lock);
Prototype iofd_t allocIo(int fd);
Prototype void abortIo(iofd_t fd);
Prototype void closeIo(iofd_t fd);
Prototype iofd_t dupIo(iofd_t fd);
Prototype void taskSleep(int ms);

Prototype int t_read(iofd_t fd, void *buf, int bytes, int to);
Prototype int t_mread(iofd_t fd, void *buf, int bytes, int to);
Prototype int t_write(iofd_t fd, void *buf, int bytes, int to);
Prototype int t_mwrite(iofd_t fd, void *buf, int bytes, int to);
Prototype int t_mflush(iofd_t fd, int to);
Prototype int t_recvmsg_fd(iofd_t fd, void *buf, int bytes, iofd_t *rfd, int flags);
Prototype int t_sendmsg_fd(iofd_t fd, void *buf, int bytes, iofd_t xfd, int flags);

tlock_t 
allocTLock(void)
{
    tlock_t lock = zalloc(sizeof(struct TLock));

    pthread_mutex_init(&lock->mutex, NULL);
    return(lock);
}

void
freeTLock(tlock_t *plock)
{
    tlock_t lock = *plock;

    pthread_mutex_destroy(&lock->mutex);
    *plock = NULL;
    zfree(lock, sizeof(struct TLock));
}

void
getTLock(tlock_t lock)
{
    pthread_mutex_lock(&lock->mutex);
}

void
relTLock(tlock_t lock)
{
    pthread_mutex_unlock(&lock->mutex);
}

iofd_t
allocIo(int fd)
{
    return(fd);
}

void
abortIo(int fd)
{
    /* nothing to do */
}

void
closeIo(iofd_t fd)
{
    close(fd);
}

iofd_t
dupIo(iofd_t fd)
{
    return(dup(fd));
}

void
taskSleep(int ms)
{
    usleep(ms * 1000);
}

int
t_read(iofd_t fd, void *buf, int bytes, int to)
{
    int n;
    int total = 0;

    while (bytes > 0) {
	n = read(fd, buf, bytes);
	if (n < 0) {
	    if (errno == EINTR)
		continue;
	    if (total == 0)
		total = n;
	    break;
	}
	bytes -= n;
	total += n;
	buf = (char *)buf + n;
    }
    return(total);
}

int
t_mread(iofd_t fd, void *buf, int bytes, int to)
{
    return(t_read(fd, buf, bytes, to));
}

int
t_write(iofd_t fd, void *buf, int bytes, int to)
{
    int n;
    int total = 0;

    while (bytes > 0) {
	n = write(fd, buf, bytes);
	if (n < 0) {
	    if (errno == EINTR)
		continue;
	    if (total == 0)
		total = n;
	    break;
	}
	bytes -= n;
	total += n;
	buf = (char *)buf + n;
    }
    return(total);
}

int
t_mwrite(iofd_t fd, void *buf, int bytes, int to)
{
    return(t_write(fd, buf, bytes, to));
}

int
t_mflush(iofd_t fd, int to)
{
    return(0);
}

int
t_recvmsg_fd(iofd_t fd, void *buf, int bytes, iofd_t *rfd, int to)
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

    if (rfd) {
	msg.msg_control = (caddr_t)&cmsg;
	msg.msg_controllen = sizeof(cmsg);
	bzero(&cmsg, sizeof(cmsg));
	cmsg.cmsg.cmsg_len = sizeof(cmsg);
	cmsg.fd = -1;
    }
    msg.msg_iov = &iov;
    msg.msg_iovlen = 1;

    while ((r = recvmsg(fd, &msg, MSG_EOR)) < 0) {
	if (errno == EINTR)
	    continue;
	if (errno == EAGAIN)	/* XXX should sleep */
	    continue;
	break;
    }
    if (rfd) {
	if (r >= 0 && cmsg.fd >= 0)
	    *rfd = cmsg.fd;
	else
	    *rfd = -1;
	if (*rfd > 0)
	    fcntl(*rfd, F_SETFL, 0);    /* ensure a blocking descriptor */ 
    }
    return(r);
}

int
t_sendmsg_fd(iofd_t fd, void *buf, int bytes, iofd_t xfd, int to)
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

    if (xfd >= 0) {
	msg.msg_control = (caddr_t)&cmsg;
	msg.msg_controllen = sizeof(cmsg);
	bzero(&cmsg, sizeof(cmsg));
	cmsg.cmsg.cmsg_level = SOL_SOCKET;
	cmsg.cmsg.cmsg_type = SCM_RIGHTS;
	cmsg.cmsg.cmsg_len = sizeof(cmsg);
	cmsg.fd = xfd;
    }
    msg.msg_iov = &iov;
    msg.msg_iovlen = 1;

    while ((r = sendmsg(fd, &msg, 0)) < 0) {
	if (errno == EINTR)
	    continue;
	if (errno == EAGAIN)	/* XXX should sleep */
	    continue;
	break;
    }
    return(r);
}

