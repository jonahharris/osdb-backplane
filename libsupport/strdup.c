/*
 * STRDUP.C
 *
 * (c)Copyright 1999-2002 Backplane, Inc.  Please refer to the COPYRIGHT
 * file at the base of the distribution tree.
 *
 * $Backplane: rdbms/libsupport/strdup.c,v 1.17 2002/08/20 22:05:55 dillon Exp $
 */

#include "defs.h"

Export char *safe_strdup(const char *s);
Export char *safe_strdup_segment(const char *start, const char *end);
Export char *safe_replace(char **pptr, const char *s);
Export char *safe_replacef(char **pptr, const char *ctl, ...);
Export char *safe_append(char **pptr, const char *s);
Export char *safe_appendf(char **pptr, const char *ctl, ...);
Export int safe_strlen(char *s);
Export char *safe_insert(char **s, int offset, char *newstr);
Export char *safe_insertf(char **s, int offset, char *ctl, ...);

/*
 * safe_strdup() -	Safe version of strdup(), core dump if alloc fails
 */
char *
safe_strdup(const char *s)
{
    char *r;

    if (s == NULL)
	return(NULL);
    if ((r = strdup(s)) == NULL)
	fatalmem();
    return(r);
}

/*
 * safe_replace() -	Free existing string, allocate copy
 *
 *	Allocates a copy of 's' and stores the copy in *pptr.  The
 *	previous contents of *ptr is freed.
 *
 *	Typical useage is to initialize *pptr to NULL, then use
 *	safe_replace() as many times as necessary (or within a loop),
 *	then safe_free(pptr) after all operations are complete.
 *
 *	This code optimizes the case where 's' is the same as *pptr.
 */
char *
safe_replace(char **pptr, const char *s)
{
    /*
     * Same data (also occurs if s == *ptr), nothing to do
     */
    if (*pptr) {
	if (s && strcmp(s, *pptr) == 0)
	    return(*pptr);
	free(*pptr);
    }

    /*
     * free old, dup new.
     */
    *pptr = (s) ? safe_strdup(s) : NULL;
    return(*pptr);
}

/*
 * safe_replacef() -	Free existing string, allocate copy, with formatting
 *
 *	This operates the same as safe_replace(), except a printf-style
 *	format string and arguments is passed rather then a simple string.
 */
char *
safe_replacef(char **pptr, const char *ctl, ...)
{
    va_list va;
    char *optr = *pptr;

    if (ctl) {
	va_start(va, ctl);
	if (vasprintf(pptr, ctl, va) < 0)
	    fatalmem();
	va_end(va);
    }
    safe_free(&optr);
    return(*pptr);
}

/*
 * safe_append() -	Append to an existing string, reallocating as required
 *
 *	*pptr represents allocated storage or NULL.  *pptr is replaced 
 *	with a new string which is the original string with the 's' argument
 *	appended.  The original string is deallocated.
 *
 *	*pptr is usually initialized to NULL, causing this routine to do
 *	the initial allocation as well as the reallocation in successive
 *	calls.  safe_free(pptr) is typically called after all operations
 *	are complete and the result string is no longer needed.
 */
char *
safe_append(char **pptr, const char *s)
{
    char *old;
    char *new;

    if ((old = *pptr) != NULL) {
	int newLen = strlen(old) + strlen(s) + 1;
	new = malloc(newLen);
	snprintf(new, newLen, "%s%s", old, s);
	free(old);
    } else {
	new = safe_strdup(s);
    }
    *pptr = new;
    return(new);
}

/*
 * safe_appendf() -	Var-args version of safe_append()
 *
 *	Operates like safe_append(), but using a printf-like format string
 *	and additional arguments to generate the string to append.
 */
char *
safe_appendf(char **pptr, const char *ctl, ...)
{
    char *old;
    char *new;
    va_list va;

    va_start(va, ctl);
    if ((old = *pptr) != NULL) {
	if (vasprintf(&new, ctl, va) < 0)
	    fatalmem();
	*pptr = new;
	asprintf(&new, "%s%s", old, new);
	free(*pptr);
	free(old);
    } else {
	if (vasprintf(&new, ctl, va) < 0)
	    fatalmem();
    }
    va_end(va);
    *pptr = new;
    return(new);
}

/*
 * safe_strdup_segment() - duplicate a portion of a string
 *
 *	Returns an allocated string representing the specified segment
 *	between start and end (end non-inclusive).  Dumps core if the
 *	allocation fails.  The returned string will be null terminated.
 */
char *
safe_strdup_segment(const char *start, const char *end)
{
    char *new;
    int len;

    if (start == NULL || end == NULL)
	return(NULL);

    if (start > end) {
	const char *temp = end;
	end = start;
	start = temp;
    }

    len = end - start;
    new = safe_malloc(len + 1);
    memcpy(new, start, len);
    new[len] = '\0';
    return(new);
}


int
safe_strlen(char *s)
{
    if (s == NULL)
	return(0);
    return(strlen(s));
}


char *
safe_insert(char **s, int offset, char *newstr)
{
    char *result;

    if (*s == NULL || newstr == NULL)
	return(*s);

    result = safe_strdup_segment(*s, *s + offset);
    safe_append(&result, newstr);
    safe_append(&result, *s + offset);
    safe_free(s);
    *s = result;
    return(result);
}


char *
safe_insertf(char **s, int offset, char *ctl, ...)
{
    va_list va;
    char *new;

    va_start(va, ctl);
    if (vasprintf(&new, ctl, va) < 0)
	fatalmem();
    va_end(va);

    safe_insert(s, offset, new);
    safe_free(&new);
    return(*s);
}

