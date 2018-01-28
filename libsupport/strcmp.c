/*
 * STRCMP.C
 *
 * (c)Copyright 1999-2002 Backplane, Inc.  Please refer to the COPYRIGHT
 * file at the base of the distribution tree.
 *
 * $Backplane: rdbms/libsupport/strcmp.c,v 1.5 2002/08/20 22:05:55 dillon Exp $
 */

#include "defs.h"

Export int safe_strcmp(const char *s1, const char *s2);

/*
 * safe_strcmp() - safe version of strcmp, either or both args may be NULL
 *
 *	NULL always compares less then non-NULL.  NULL vs NULL compares
 *	equal (0)
 */
int
safe_strcmp(const char *s1, const char *s2)
{
    if (s1 == NULL) {
	if (s2 == NULL)
	    return(0);
	return(-1);
    } else if (s2 == NULL) {
	return(1);
    } else {
	return(strcmp(s1, s2));
    }
}

