/*
 * MALLOC.C
 *
 * (c)Copyright 1999-2002 Backplane, Inc.  Please refer to the COPYRIGHT
 * file at the base of the distribution tree.
 *
 * $Backplane: rdbms/libsupport/malloc.c,v 1.13 2002/08/20 22:05:55 dillon Exp $
 */

#include "defs.h"

Export void *safe_malloc(size_t bytes);
Export void *safe_realloc(void *ptr, size_t size);
Export void safe_free(char **ptr);

/*
 * safe_malloc() - safe version of malloc.  Dumps core if the allocation fails
 *
 *	Do not allow 0-byte allocations.
 */
void *
safe_malloc(size_t bytes)
{
    char *ptr;

    DBASSERT(bytes != 0);

    ptr = malloc(bytes);
    if (ptr == NULL)
	fatalmem();
    return(ptr);
}

/*
 * safe_realloc() - safe version of realloc.  Dumps core if the
 *		    allocation fails
 *
 *	Do not allow 0-byte reallocations.
 */
void *
safe_realloc(void *ptr, size_t size)
{
    DBASSERT(size != 0);

    if (ptr == NULL)
	ptr = malloc(size);
    else
	ptr = realloc(ptr, size);
    if (ptr == NULL)
	fatalmem();
    return(ptr);
}

/*
 * safe_free() - free a string pointer safely.
 *
 *	Given the address of the pointer (rather then the pointer itself),
 *	this routine is a NOP if the pointer is NULL, and will free() and
 *	NULL-out the pointer if it is non-NULL.
 *
 *	This function is typically only used on string pointers.  Structural
 *	allocations should use zalloc() and zfree().
 */

void
safe_free(char **ptr)
{
    if (*ptr) {
	free(*ptr);
	*ptr = NULL;
    }
}

