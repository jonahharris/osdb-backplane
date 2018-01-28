/*
 * ARGS.C 
 *
 * (c)Copyright 1999-2002 Backplane, Inc.  Please refer to the COPYRIGHT
 * file at the base of the distribution tree.
 *
 * Routines for splitting up command strings into argv lists
 *
 * $Backplane: rdbms/libsupport/args.c,v 1.4 2002/08/20 22:05:54 dillon Exp $
 */

#include "defs.h"

Export char **splitArgs(const char *str, int *pcount);
Export void freeArgs(char **av);

/*
 * splitArgs() - simple stupid argument splitter
 *
 *	Takes the supplied input string and returns a char **argv array.
 *	The original input string may be discarded after the call.  The
 *	allocated result will be returned and freeArgs() should be used
 *	to free it up after you are done with it.
 */
char **
splitArgs(const char *str, int *pcount)
{
    const char *base;
    int count = 0;
    int mcount = 8;
    char **av = safe_malloc(mcount * sizeof(char *));

    for (base = str; *str; ++str) {
	if (*str == ' ' || *str == '\t') {
	    if (count == mcount) {
		mcount *= 2;
		av = safe_realloc(av, mcount * sizeof(char *));
	    }
	    if (base != str)
		av[count++] = safe_strndup(base, str - base);
	    while (*str == ' ' || *str == '\t')
		++str;
	    base = str;
	}
    }
    if (count >= mcount - 2) {
	mcount *= 2;
	av = safe_realloc(av, mcount * sizeof(char *));
    }
    if (base != str)
	av[count++] = safe_strndup(base, str - base);
    av[count] = NULL;
    if (pcount)
	*pcount = count;
    return(av);
}

/*
 * freeArgs() -	free arguments split with splitArgs()
 */
void
freeArgs(char **av)
{
    if (av) {
	int i;

	for (i = 0; av[i]; ++i)
	    safe_free(&av[i]);
	safe_free((char **)&av);
    }
}

