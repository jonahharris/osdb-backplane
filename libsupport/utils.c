/*
 * UTILS.C
 *
 * (c)Copyright 1999-2002 Backplane, Inc.  Please refer to the COPYRIGHT
 * file at the base of the distribution tree.
 *
 * $Backplane: rdbms/libsupport/utils.c,v 1.5 2002/08/20 22:05:55 dillon Exp $
 */

#include "defs.h"

Export int ofType(const char *str, int (*func)(int));
Export int oneof(const char *val, const char *ary[], int count);

/*
 * ofType() - determine of string is of the specified type
 *
 *	The type is determined by making a callback for each character
 *	until one returns success (0).  On success, the index into str
 *	is returned (0...N-1).  On failure, -1 is returned.
 */
int     
ofType(const char *str, int (*func)(int))
{
    int i, len = strlen(str);

    for (i = 0; i < len; i++) {
	if (!func((int)(unsigned char)str[i]))
	    return(i);
    }       
    return(-1);
}

/*
 * oneof() - case insensitive check of string against strings in array
 *
 *	Returns the index into the array of the element matching the
 *	passed string, or -1 if no element matches.  Individual array
 *	elements may be NULL.  The size of the array (in elements) must
 *	be passed to this function.
 */
int
oneof(const char *val, const char *ary[], int count)
{ 
    int i;

    for (i = 0; i < count; i++) {
	if (ary[i] && strcasecmp(val, ary[i]) == 0)
	    return(i);
    }
    return(-1);
}

