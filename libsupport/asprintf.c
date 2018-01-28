/*
 * ASPRINTF.C
 *
 * (c)Copyright 1999-2002 Backplane, Inc.  Please refer to the COPYRIGHT
 * file at the base of the distribution tree.
 *
 * $Backplane: rdbms/libsupport/asprintf.c,v 1.6 2002/08/20 22:05:54 dillon Exp $
 */

#include "defs.h"

Export void safe_asprintf(char **pptr, const char *ctl, ...);
Export void safe_vasprintf(char **pptr, const char *ctl, va_list va);

/*
 * safe_asprintf() -	safe version of asprintf()
 *
 *	This routine implements a safe version of asprintf(), which allocates
 *	the result string and stored it in *ptr.  The program will dump
 *	core if the allocation fails.
 *
 *	*ptr need not be initialized to anything in particular prior to
 *	calling this routine.
 *
 *	Since the return value from the stdc asprintf() is not portable, we
 *	simply return void in our safe version.
 */
void 
safe_asprintf(char **pptr, const char *ctl, ...)
{
    va_list va;
    int r;

    va_start(va, ctl);
    r = vasprintf(pptr, ctl, va);
    va_end(va);
    if (r < 0)
	fatalmem();
}

/*
 * safe_vasprintf() -	safe version of var-args asprintf.
 *
 *	This routine implements a safe version of vasprintf(), which allocates
 *	the result string and stored it in *ptr.  The program will dump
 *	core if the allocation fails.
 *
 *	*ptr need not be initialized to anything in particular prior to
 *	calling this routine.
 *
 *	Since the return value from the stdc vasprintf() is not portable, we
 *	simply return void in our safe version.
 */
void
safe_vasprintf(char **pptr, const char *ctl, va_list va)
{
    int r;

    r = vasprintf(pptr, ctl, va);
    if (r < 0)
	fatalmem();
}

