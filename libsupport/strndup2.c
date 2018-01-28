/*
 * STRNDUP2.C
 *
 * (c)Copyright 2000-2002 Backplane, Inc.  Please refer to the COPYRIGHT
 * file at the base of the distribution tree.
 *
 * $Backplane: rdbms/libsupport/strndup2.c,v 1.2 2002/08/20 22:05:55 dillon Exp $
 */

#include "defs.h"

Export char *safe_strndup_tolower(const char *s, int l);

/*
 * safe_strndup() - duplicate l bytes of string s.  Return allocated space.
 *
 *	The returned string will be null terminated and should be freed
 *	by the caller when it is finished with it.
 */
char *
safe_strndup_tolower(const char *s, int l)
{
    char *r;
    int i;

    if ((r = malloc(l + 1)) == NULL)
	fatalmem();
    for (i = 0; i < l; ++i)
	r[i] = tolower(s[i]);
    r[l] = 0;
    return(r);
}

