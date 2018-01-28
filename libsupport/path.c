/*
 * PATH.C - Path processing routines
 *
 * (c)Copyright 1999-2002 Backplane, Inc.  Please refer to the COPYRIGHT
 * file at the base of the distribution tree.
 *
 * $Backplane: rdbms/libsupport/path.c,v 1.3 2002/08/20 22:05:55 dillon Exp $
 */

#include "defs.h"

Export char *baseName(const char *fileSpec);
Export char *rootName(const char *fileName);
Export char *suffixName(const char *fileName);


/*
 * baseName - Return the last element in a directory path
 *
 * Arguments:	str		String to convert
 * Globals:	None
 *
 * Returns:	A dynamically allocated string containing the last element
 *			in a directory path if successful, NULL if not.
 *
 * Description:	baseName
 */
char *
baseName(const char *fileSpec)
{
    const char	*f;
    const char	*slash;

    if (fileSpec == NULL)
	return(NULL);

    for (f=fileSpec, slash=fileSpec; *f != '\0'; f++) {
	if (*f == '/')
	    slash = f;
    }

    /* 
     * If any slashed have been found in the file spec, 
     * point to the character *after* the slash
     */
    if (slash != fileSpec)
	++slash;

    if (f-slash > 0)
	return(safe_strdup(slash));

    return(NULL);
}

/*
 * rootName - Return the root of a filename
 *
 * Arguments:	fileName	String containing filename
 * Globals:	None
 *
 * Returns:	A dynamically allocated string containing the root filename
 *			if successful
 *		NULL if not
 *
 * Description:	rootName returns the part of a filename that preceeds any
 *		'.' suffixes.
 */
char *
rootName(const char *fileName)
{
    const char	*dot;

    if (fileName == NULL)
	return(NULL);

    /* If the first character is '.', then there is no root name */
    if (*fileName == '.')
	return(NULL);

    for (dot=fileName; *dot != '\0'; dot++)
	if (*dot == '.')
	    break;

    /* No '.' was found, so fileName is all root name (no suffix) */
    if (*dot == '\0')
	return(safe_strdup(fileName));

    if (dot-fileName > 0)
	return(safe_strndup(fileName, dot-fileName));

    return(NULL);
}

/*
 * suffixName - Return the suffix (extension) of a filename
 *
 * Arguments:	fileName	String containing filename
 * Globals:	None
 *
 * Returns:	A dynamically allocated string containing the suffix
 *			if successful
 *		NULL if not
 *
 * Description:	rootName returns the part of a filename that follows the '.',
 *		or NULL.
 */
char *
suffixName(const char *fileName)
{
    const char	*dot;

    if (fileName == NULL)
	return(NULL);

    for (dot=fileName; *dot != '\0'; dot++)
	if (*dot == '.')
	    break;

    /* Dot '.' was found; return anything after it */
    if (*dot != '\0')
	if (strlen(dot) > 1)
	    return(safe_strdup(++dot));

    return(NULL);
}

