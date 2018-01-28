/*
 * COMPAT.C
 *
 * (c)Copyright 1999-2002 Backplane, Inc.  Please refer to the COPYRIGHT
 * file at the base of the distribution tree.
 *
 *	Implements compatibility functions for other operating systems.
 *
 * $Backplane: rdbms/libsupport/compat.c,v 1.4 2002/08/20 22:05:54 dillon Exp $
 */

#include "defs.h"

/*
 * Missing functions for SUN - asprintf (malloced printf)
 */
#ifdef sun

int
asprintf(char **ret, const char *format, ...)
{
    int r;

    va_list va;
    va_start(va, format);
    r = vasprintf(ret, format, va);
    va_end(va);
    return(r);
}

int
vasprintf(char **ret, const char *format, va_list ap)
{
    char c;
    int n;

    n = vsnprintf(&c, 1, format, ap);
    if ((*ret = malloc(n + 1)) != NULL) {
        vsnprintf(*ret, n + 1, format, ap);
    } else {
	n = -1;
    }
    return(n);
}

#endif

