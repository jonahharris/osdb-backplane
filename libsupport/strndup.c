/*
 * STRNDUP.C
 *
 * (c)Copyright 1999-2002 Backplane, Inc.  Please refer to the COPYRIGHT
 * file at the base of the distribution tree.
 *
 * $Backplane: rdbms/libsupport/strndup.c,v 1.6 2002/08/20 22:05:55 dillon Exp $
 */

#include "defs.h"

Export char *safe_strndup(const char *s, int l);

/*
 * safe_strndup() - duplicate l bytes of string s.  Return allocated space.
 *
 *	The returned string will be null terminated and should be freed
 *	by the caller when it is finished with it.
 */
char *
safe_strndup(const char *s, int l)
{
    char *r;

    if ((r = malloc(l + 1)) == NULL)
	fatalmem();
    bcopy(s, r, l);
    r[l] = 0;
    return(r);
}

