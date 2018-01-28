/*
 * PIO.C	- Process I/O operations
 *
 * (c)Copyright 2000-2002 Backplane, Inc.  Please refer to the COPYRIGHT 
 * file at the base of the distribution tree.
 *
 * $Backplane: rdbms/libthreads/pio.c,v 1.10 2002/08/20 22:05:57 dillon Exp $
 */

#include "defs.h"
#include <sys/socket.h>
#include <sys/wait.h>

Export iofd_t t_popen(const char *command, ...);
Export int t_pclose(iofd_t fd);

/*
 * Returns 0 if execution fails.
 */
iofd_t
t_popen(const char *command, ...)
{
    iofd_t fd;
    int sv[2];
    int pid;

    DBASSERT(command != NULL);

    socketpair(AF_UNIX, SOCK_STREAM, PF_UNSPEC, sv);
    switch (pid = t_fork()) {
    case -1:			/* Error. */
	close(sv[0]);
	close(sv[1]);
	return(NULL);
	/* NOTREACHED */

    case 0:			/* Child. */
    {
	va_list va;
	char **argv;
	char *pos;
	int n = 2;		/* 2 arguments: filename and terminating NULL */

	va_start(va, command);
	while (va_arg(va, char *))
	    n++;
	va_end(va);

	argv = safe_malloc(n * sizeof(char *));
	bzero(argv, n * sizeof(char *));
	va_start(va, command);
	pos = strrchr(command, '/');
	if (pos == NULL)
	    argv[0] = (char *)command;
	else
	    argv[0] = pos + 1;
	for (n = 1; (pos = va_arg(va, char *)) != NULL; n++)
	    argv[n] = pos;	
	va_end(va);

	if (sv[0] != 0)
	    dup2(sv[0], 0);		/* Child stdin. */
	if (sv[0] != 1)
	    dup2(sv[0], 1);		/* Child stdout. */
	if (sv[0] > 1)
	    close(sv[0]);
	close(sv[1]);
	execv(command, argv);
	exit(127);
	/* NOTREACHED */
    }
    default:			/* Parent. */
	break;
    }
    close(sv[0]);
    fd = allocIo(sv[1]);
    fd->io_Pid = pid;
    return(fd);
}

int
t_pclose(iofd_t fd)
{
    int i = 0;
    pid_t pid;

    t_shutdown(fd, SHUT_RDWR);
    if (fd->io_Pid != 0) {
	while ((pid = waitpid(fd->io_Pid, &i, WNOHANG)) == 0)
	    taskSleep(20);
    }
    closeIo(fd);
    return(i);
}

