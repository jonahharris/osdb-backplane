/*
 * STRCHR.C
 *
 * (c)Copyright 1999-2002 Backplane, Inc.  Please refer to the COPYRIGHT
 * file at the base of the distribution tree.
 *
 * $Backplane: rdbms/libsupport/strchr.c,v 1.5 2002/08/20 22:05:55 dillon Exp $
 */

#include "defs.h"

Export const char *safe_strchr(const char *str, int c);
Export const char *safe_strchr_escape(const char *str, int c, int escape);
Export const char *strchr_escape(const char *str, int c, int escape);

/*
 * safe_strchr() - safe version of strchr().
 *
 *	Like strchr(), but str may be NULL (causing NULL to be returned).
 */
const char *
safe_strchr(const char *str, int c)
{
    if (str == NULL)
	return(NULL);
    return(strchr(str, c));
}

/*
 * safe_strchr() - safe version of strchr().
 *
 *	Like strchr_escape(), but str may be NULL (causing NULL to be
 *	returned).
 */
const char *
safe_strchr_escape(const char *str, int c, int escape)
{
    if (str == NULL)
	return(NULL);
    return(strchr_escape(str, c, escape));
}

/*
 * strchr_escape() - strchr() that ignores characters preceeded by the given
 *		     escape character.
 */
const char *
strchr_escape(const char *str, int c, int escape)
{
    const char *scan;

    scan = str;
    do {
	/* Get the next location of the given character in the string. */
	if ((scan = strchr(scan, c)) == NULL)
	    return(NULL);

	/* If the character is preceeded by the escape character, ignore it. */
	if (scan > str && *(scan - 1) == escape) {
	    scan++;
	    continue;
	}

	/* If we make it this far, then we have a match we can return. */
    } while(0);

    return(scan);
}

