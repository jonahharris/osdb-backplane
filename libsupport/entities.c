/*
 * ENTITIES.C
 *
 * (c)Copyright 1999-2002 Backplane, Inc.  Please refer to the COPYRIGHT
 * file at the base of the distribution tree.
 *
 *	Routines for manipulating text with embedded HTML character entities.
 *
 * $Backplane: rdbms/libsupport/entities.c,v 1.11 2002/08/20 22:05:54 dillon Exp $
 */

#include "defs.h"

Export char *HTMLConvertSpaces(const char *text);
Export char *HTMLConvertSpecial(const char *text);
Export char *HTMLConvertEntities(const char *text);
Export char *HTMLEntities(const char *text);

/*
 * HTMLConvertSpaces() - Replaces spaces with &nbsp; entities.
 *
 *	Returns a newly-allocated string with all spaces replaced with
 *	&nbsp; entities.  The caller should free the returned string.
 */
char *
HTMLConvertSpaces(const char *text)
{
    const char *src;
    char *new, *dest;
    int newlen;

    if (EMPTY(text))
	return(safe_strdup(PROT(text)));

    /* Compute the number of characters needed to hold the new string. */
    for (newlen = 0, src = text; *src != '\0'; src++) {
	if (isspace(*src))
	    newlen += 6;		/* Length of "&nbsp;". */
	else
	    newlen++;
    }

    /* Allocate enough memory to hold the new string plus terminating null. */
    new = safe_malloc(newlen + 1);

    /* Build the new string. */
    for (src = text, dest = new; *src != '\0'; src++) {
	if (isspace(*src)) {
	    memcpy(dest, "&nbsp;", 6);
	    dest += 6;
	} else {
	    *dest = *src;
	    dest++;
	}
    }
    /* Append terminating null to new string. */
    *dest = '\0';

    return(new);
}

/*
 * HTMLConvertSpecial() - Replace HTML special characters with entities.
 *
 *	Returns a newly-allocated string with all HTML special characters
 *	replaced with the coresponding entities.
 *
 *	WARNING: Any entity references in the original string will be mangled
 *		 as the leading ampersand is escaped. If the source string
 *		 contains entity references and you do not wish them to be
 *		 escaped, use HTMLEntities() instead.
 */
char *
HTMLConvertSpecial(const char *text)
{
    const char *src;
    char *new, *dest;
    int newlen;

    if (EMPTY(text))
	return(safe_strdup(PROT(text)));

    /* Compute the number of characters needed to hold the new string. */
    for (newlen = 0, src = text; *src != '\0'; src++) {
	if (*src == '"')
	    newlen += 6;	/* Length of "&quot;". */
	else if (*src == '&')
	    newlen += 5;	/* Length of "&amp;". */
	else if (*src == '<')
	    newlen += 4;	/* Length of "&lt;". */
	else if (*src == '>')
	    newlen += 4;	/* Length of "&gt;". */
	else
	    newlen++;
    }

    /* Allocate enough memory to hold the new string plus terminating null. */
    new = safe_malloc(newlen + 1);

    /* Build the new string. */
    for (src = text, dest = new; *src != '\0'; src++) {
	if (*src == '"') {
	    memcpy(dest, "&quot;", 6);
	    dest += 6;
	} else if (*src == '&') {
	    memcpy(dest, "&amp;", 5);
	    dest += 5;
	} else if (*src == '<') {
	    memcpy(dest, "&lt;", 4);
	    dest += 4;
	} else if (*src == '>') {
	    memcpy(dest, "&gt;", 4);
	    dest += 4;
	} else {
	    *dest = *src;
	    dest++;
	}
    }
    /* Append terminating null to new string. */
    *dest = '\0';

    DBASSERT(strlen(new) == newlen);

    return(new);
}

/*
 * HTMLConvertEntities() - Replace HTML special and non-ascii characters with
 *			   entities.
 *
 *	Returns a newly-allocated string with all HTML special and non-ascii
 *	characters converted to entity references.
 *
 *	WARNING: Any entity references in the original string will be mangled
 *		 as the leading ampersand is escaped. If the source string
 *		 may contain entity references, use HTMLEntities() instead.
 */
char *
HTMLConvertEntities(const char *text)
{
    const char *src;
    char *new, *temp, *dest;
    int newlen;

    if (EMPTY(text))
	return(safe_strdup(PROT(text)));

    /* Convert HTML special characters first since most often that is all we
     * need to do.
     */
    temp = HTMLConvertSpecial(text);

    /* Compute the number of characters needed to hold the new string. */
    for (newlen = 0, src = temp; *src != '\0'; src++) {
	if (!isascii(*src))
	    newlen += 6;	/* Length of "&#XXX;". */
	else
	    newlen++;
    }

    /* Handle the common case where we don't have any non-ascii characters. */
    if (strlen(temp) == newlen)
	return(temp);

    /* Allocate enough memory to hold the new string plus terminating null. */
    new = safe_malloc(newlen + 1);

    /* Build the new string. */
    for (src = temp, dest = new; *src != '\0'; src++) {
	if (isascii(*src)) {
	    *dest = *src;
	    dest++;
	} else {
	    snprintf(dest, 7, "&#%03u;", (unsigned char)*src);
	    dest += 6;
	}
    }
    /* Append terminating null to new string. */
    *dest = '\0';

    DBASSERT(strlen(new) == newlen);

    safe_free(&temp);
    return(new);
}

/*
 * HTMLEntities() - Replace HTML special and non-ascii characters with
 *		    entities, leaving any existing entity references intact.
 *
 *	This is basically the same function as HTMLConvertEntities() except
 *	that entities already in the text are left untouched.
 */
char *
HTMLEntities(const char *text)
{
    const char *src, *scan;
    char *new, *dest;
    int newlen;

    if (EMPTY(text))
	return(safe_strdup(PROT(text)));

    /* Compute the maximum number of characters needed to hold the new string.*/
    for (newlen = 0, src = text; *src != '\0'; src++) {
	if (!isascii(*src))
	    newlen += 6;	/* Length of "&#XXX;". */
	else if (*src == '"')
	    newlen += 6;	/* Length of "&quot;". */
	else if (*src == '&')
	    newlen += 5;	/* Length of "&amp;". */
	else if (*src == '<')
	    newlen += 4;	/* Length of "&lt;". */
	else if (*src == '>')
	    newlen += 4;	/* Length of "&gt;". */
	else
	    newlen++;
    }

    /* Allocate enough memory to hold the new string plus terminating null. */
    new = safe_malloc(newlen + 1);

    /* Build the new string. */
    for (src = text, dest = new; *src != '\0'; src++) {
	if (! isascii(*src)) {
	    snprintf(dest, 7, "&#%03u;", (unsigned char)*src);
	    dest += 6;
	} else if (*src == '"') {
	    memcpy(dest, "&quot;", 6);
	    dest += 6;
	} else if (*src == '&') {
	    /* 
	     * Special case; make sure this isn't an entity reference.
	     */
	    for (scan = src + 1; *scan != '\0'; scan++) {
		if (!isalnum(*(unsigned char *)scan) && *scan != '#')
		    break;
	    }

	    /*
	     * Entities always end with a semicolon...
	     */
	    if (*scan != ';') {
		memcpy(dest, "&amp;", 5);
		dest += 5;
	    } else {
		*dest = *src;
		dest++;
		newlen -= 4;	/* Adjust newlen after skipping an esc seq. */
	    }
	} else if (*src == '<') {
	    memcpy(dest, "&lt;", 4);
	    dest += 4;
	} else if (*src == '>') {
	    memcpy(dest, "&gt;", 4);
	    dest += 4;
	} else {
	    *dest = *src;
	    dest++;
	}
    }
    /* Append terminating null to new string. */
    *dest = '\0';

    DBASSERT(strlen(new) == newlen);

    return(new);
}

