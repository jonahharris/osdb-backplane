/*
 * UTILS/DRD_LINK.C	- Provides link to replication service on local host
 *
 * (c)Copyright 2000-2002 Backplane, Inc.  Please refer to the COPYRIGHT
 * file at the base of the distribution tree.
 *
 * $Backplane: rdbms/utils/drd_link.c,v 1.8 2002/08/20 22:06:06 dillon Exp $
 *
 *	The replication manager usually runs this link program on each
 *	remote host it ties into via ssh.  This program ties its standard
 *	input and output into the local replication server over a unix
 *	domain socket.
 */

#include "defs.h"

void UDReader(void *dummy);
void UDWriter(void *dummy);

iofd_t ReplFdR;
iofd_t ReplFdW;
iofd_t StdinFd;
iofd_t StdoutFd;
int QuietOpt = 0;

void
task_main(int ac, char **av)
{
    int i;
    int fd;
    const char *emsg;
    const char *dbDir = DefaultDBDir();
    char *udomPath;

    for (i = 1; i < ac; ++i) {
	char *ptr = av[i];
	if (*ptr != '-')
	    fatal("Unexpected argument: %s\n", ptr);
	ptr += 2;
	switch(ptr[-1]) {
	case 'D':
	    dbDir = (*ptr) ? ptr : av[++i];
	    break;
	case 'q':
	    QuietOpt = 1;
	    break;
	default:
	    fatal("Unexpected option: %s\n", ptr - 2);
	}
    }
    if (i > ac)
	fatal("Missing argument to last option");

    if (asprintf(&udomPath, "%s/.drd_socket", dbDir) < 0)
	fatalmem();

    if ((fd = ConnectUDomSocket(udomPath, &emsg)) < 0) {
	fatalsys("can't connect to %s (%s)", udomPath, emsg);
    }
    ReplFdR = allocIo(fd);
    ReplFdW = dupIo(ReplFdR);
    StdinFd = allocIo(0);
    StdoutFd = allocIo(1);

    taskCreate(UDReader, NULL);
    taskCreate(UDWriter, NULL);
}

void 
UDReader(void *dummy)
{
    char buf[256];
    int n;

    while ((n = t_read1(ReplFdR, buf, sizeof(buf), 0)) > 0) {
	if (t_write(StdoutFd, buf, n, 0) != n)
	    break;
    }
    shutdown(1, SHUT_RDWR);
}

void 
UDWriter(void *dummy)
{
    char buf[256];
    int n;

    while ((n = t_read1(StdinFd, buf, sizeof(buf), 0)) > 0) {
	if (t_write(ReplFdW, buf, n, 0) != n)
	    break;
    }
    t_shutdown(ReplFdW, SHUT_WR);
}

