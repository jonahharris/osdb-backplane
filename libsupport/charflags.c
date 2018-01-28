/*
 * CHARFLAGS.C
 *
 * (c)Copyright 1999-2002 Backplane, Inc.  Please refer to the COPYRIGHT
 * file at the base of the distribution tree.
 *
 * Routines for working with strings of flag characters.
 *
 * $Backplane: rdbms/libsupport/charflags.c,v 1.7 2002/08/20 22:05:54 dillon Exp $
 *
 * These routines keep the flag character strings in sorted order with a
 * given flag never appearing more than once in the string.
 */

#include "defs.h"

Export void flagSet(char **base, const char *flags);
Export int flagClear(char **base, const char *flags);
Export int flagTest(const char *base, const char *flags);

static inline int flagFind(const char *base, char flagchar, char **location);
static int flagOrder(const void *flag1, const void *flag2);


/*
 * flagSet() - Set one or more flags in the given flag character string.
 *
 *	Takes the passed set of flags, represented by *base (which may be 
 *	NULL), and adds the specified flags to *base.  If *base is not
 *	passed as NULL it must be malloc'd and may be reallocated.
 *
 *	Any given flag will appear in the flag set only once.  The flag
 *	set may be resorted.
 */
void
flagSet(char **base, const char *flags)
{
    const char *scan;
    char *newbase;
    int len = 0;

    DBASSERT(base != NULL && flags != NULL);

    /*
     * Make sure the base string is sorted
     */
    if (!EMPTY(*base)) {
	len = strlen(*base);
	qsort(*base, sizeof(char), len, flagOrder);
    }	

    /*
     * Allocate worst-case space and copy the string
     */
    newbase = safe_malloc(len + strlen(flags) + 1);
    if (*base != NULL)
	memcpy(newbase, *base, len + 1);
    else
	*newbase = '\0';

    /*
     * Add each flag to the new string.  Flags may appear at most once
     * in the string.  Sorted order is maintained.
     */
    for (scan = flags; *scan != '\0'; scan++) {
	char *insert;
	if (!flagFind(newbase, *scan, &insert)) {
	    memmove(insert + 1, insert, strlen(insert) + 1);
	    *insert = *scan;	    
	}
    }
    safe_free(base);
    *base = newbase;    
}

/*
 * flagClear() - Clear one or more flags in the given flag character string.
 *
 *	Takes the passed set of flags, represented by *base (which may be 
 *	NULL), and removes the specified flags from *base.  If *base is not
 *	passed as NULL it must be malloc'd and may be reallocated.
 *
 *	The number of flags removed is returned.  Flags passed in flags that
 *	do not exist in *base are ignored.
 */
int
flagClear(char **base, const char *flags)
{
    const char *scan;
    int numcleared = 0;

    DBASSERT(base != NULL && flags != NULL);
    if (*base == NULL)
	return(0);

    /* The current scan position in the flags string is only updated after
     * it has been established that all instances of a flag have been removed
     * from the base string.
     */
    for (scan = flags; *scan != '\0'; ) {
	char *find;

	/*
	 * Remove all instances of the flag from the base string.  When no
	 * instances remain, advance to the next flag.
	 */
	if (!flagFind(*base, *scan, &find)) {
	    scan++;
	} else {
	    memmove(find, find + 1, strlen(find + 1) + 1);
	    numcleared++;
	}
    }
    return(numcleared);
}


/*
 * flagTest() - Determine whether the flag string has all given flags set.
 *
 *	Returns boolean indicating whether the given base string contains
 *	all flags specified by the flags string.  If any flag is missing,
 *	return 0.  If all flags are present, return 1.
 */
int
flagTest(const char *base, const char *flags)
{
    const char *scan;

    DBASSERT(flags != NULL);
    if (base == NULL)
	return(0);
    if (flags == NULL)
	return(1);

    for (scan = flags; *scan != '\0'; scan++) {
	if (strchr(base, *scan) == NULL)
	    return(0);
    }
    return(1);
}


/*
 * flagFind() - Determine if a character exists in a flag string.
 *
 *	If the character is present in the string, returns true and sets
 *	*location to the address of the character.  If the character is not
 *	present in the string, returns false and sets *location to where
 *	the character should have been (assuming sorted order).
 *
 *	The location field may be passed as NULL if the character position
 *	is not needed.
 */
static inline
int
flagFind(const char *base, char flagchar, char **location) 
{
    const char *scan;
    int r = 0;

    for (scan = base; *scan != '\0'; scan++) {
	if (*scan == flagchar) {
	    r = 1;
	    break;
	} 
	if (*(const unsigned char *)scan > (unsigned char)flagchar)
	    break;
    }
    if (location != NULL)
	*location = (char *)scan;
    return(r);
}


/*
 * flagOrder() - Callback function for qsort() to sort flags.
 */
static
int
flagOrder(const void *flag1, const void *flag2) 
{
    return((int)*(unsigned char *)flag1 - (int)*(unsigned char *)flag2);
}

