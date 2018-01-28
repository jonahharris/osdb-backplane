/*
 * INDEX.C
 *
 * (c)Copyright 1999-2002 Backplane, Inc.  Please refer to the COPYRIGHT
 * file at the base of the distribution tree.
 *
 * $Backplane: rdbms/libsupport/index.c,v 1.7 2002/08/20 22:05:55 dillon Exp $
 *
 *	Generate indexed string names.  Indexed name strings are in the
 *	format BASENAME:index[:subindex[:...]], where the index does not 
 *	have to be numeric.  The index can be (and often is) any character
 *	string.
 */

#include "defs.h"

Export const char *indexNameInt(char **save, const char *str, int idx);
Export const char *indexNameInt2(char **save, const char *str, int idx, int idx2);
Export const char *indexNameInt3(char **save, const char *str, int idx, int idx2, int idx3);
Export const char *indexNameStr(char **save, const char *str, const char *idx);
Export const char *indexNameStr2(char **save, const char *str, const char *idx, const char *extra);

/*
 * indexNameInt() -	Generate "name:value"
 *
 *	This routine stores the allocated copy of the index in *save,
 *	freeing any previous contents of *save.  *save must be set to NULL
 *	prior to first use, and is typically safe_free()'d after last use.
 */
const char *
indexNameInt(char **save, const char *str, int idx)
{
    char *n;

    safe_asprintf(&n, "%s:%d", str, idx);
    if (*save)
	free(*save);
    *save = n;
    return(n);
}

const char *
indexNameInt2(char **save, const char *str, int idx, int idx2)
{
    char *n;

    safe_asprintf(&n, "%s:%d:%d", str, idx, idx2);
    if (*save)
	free(*save);
    *save = n;
    return(n);
}

const char *
indexNameInt3(char **save, const char *str, int idx, int idx2, int idx3)
{
    char *n;

    safe_asprintf(&n, "%s:%d:%d:%d", str, idx, idx2, idx3);
    if (*save)
	free(*save);
    *save = n;
    return(n);
}

/*
 * indexNameStr() -	Generate "name:string"
 *
 *	This routine stores the allocated copy of the index in *save,
 *	freeing any previous contents of *save.  *save must be set to NULL
 *	prior to first use, and is typically safe_free()'d after last use.
 */
const char *
indexNameStr(char **save, const char *str, const char *idx)
{
    char *n;

    safe_asprintf(&n, "%s:%s", str, idx);
    if (*save)
	free(*save);
    *save = n;
    return(n);
}

/*
 * indexNameStr2() -	Generate "name:string.string"
 *
 *	Note: this does not create an name:index:subindex, it instead
 *	creates a single level of indexing by combining the two strings.
 *	The extra string may be NULL or empty, which degenerates into
 *	indexNameStr().
 *
 *	This routine stores the allocated copy of the index in *save,
 *	freeing any previous contents of *save.  *save must be set to NULL
 *	prior to first use, and is typically safe_free()'d after last use.
 */
const char *
indexNameStr2(char **save, const char *str, const char *idx, const char *extra)
{
    char *n;

    if (extra && extra[0])
	safe_asprintf(&n, "%s:%s.%s", str, idx, extra);
    else
	safe_asprintf(&n, "%s:%s", str, idx);
    if (*save)
	free(*save);
    *save = n;
    return(n);
}

