/*
 * FATAL.C
 *
 * (c)Copyright 1999-2002 Backplane, Inc.  Please refer to the COPYRIGHT
 * file at the base of the distribution tree.
 *
 * $Backplane: rdbms/libsupport/fatal.c,v 1.6 2002/08/20 22:05:55 dillon Exp $
 */

#include "defs.h"

Export void fatal(const char *ctl, ...);
Export void fatalmem(void);
Export void fatalsys(const char *ctl, ...);
Export void vfatal(const char *ctl, va_list va, int error);

/*
 * fatal() - Generate fatal error message and exit
 *
 *	The caller should NOT supply a line terminator
 */
void
fatal(const char *ctl, ...)
{
    va_list va;

    va_start(va, ctl);
    vfatal(ctl, va, 0);
    va_end(va);
}

/*
 * fatal() - Generate a fatal error message in regards to running out of
 *	     memmory and exit.
 */
void
fatalmem(void)
{
    fatal("Ran out of memory");
}

/*
 * fatalsys() - Generate a fatal error message along with the system errno
 *
 *	The caller should NOT supply a line terminator
 */
void
fatalsys(const char *ctl, ...)
{
    va_list va;

    va_start(va, ctl);
    vfatal(ctl, va, errno);
    va_end(va);
}

/*
 * vfatal() - Generate a fatal error messages given a va-args list and an
 *	      optional system error number (0 if none).
 *
 *	The caller should NOT supply a line terminator
 */
void
vfatal(const char *ctl, va_list va, int error)
{
    vfprintf(stderr, ctl, va);
    if (error)
	fprintf(stderr, "(%s)\n", strerror(error));
    else
	fprintf(stderr, "\n");
    exit(1);
}

