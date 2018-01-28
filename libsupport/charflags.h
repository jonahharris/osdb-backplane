/*
 * CHARFLAGS.H
 *
 * (c)Copyright 1999-2002 Backplane, Inc.  Please refer to the COPYRIGHT
 * file at the base of the distribution tree.
 *
 * Routines for working with strings of flag characters.
 *
 * $Backplane: rdbms/libsupport/charflags.h,v 1.3 2002/08/20 22:05:54 dillon Exp $
 */


/*
 * flagTestAny() - Determine whether the flag string has any given flags set.
 *
 *	Returns boolean indicating whether any flag specified by the flags
 *	string is set in the base string.
 */
static inline
int
flagTestAny(const char *base, const char *flags)
{
    DBASSERT(flags != NULL);
    if (base == NULL)
	return(0);

    return(strpbrk(base, flags) != NULL);
}


/*
 * flagEqual() - Determine whether two flag strings are equal.
 *
 *	Returns boolean indicating whether the two flag strings contain all
 *	of the same flags and none others.
 */
static inline
int
flagEqual(const char *flag1, const char *flag2)
{
    return(flagTest(PROT(flag1), PROT(flag2)) &&
	   flagTest(PROT(flag2), PROT(flag1)));
}
