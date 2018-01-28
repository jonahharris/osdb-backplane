/*
 * STRSUBST.C
 *
 * (c)Copyright 1999-2002 Backplane, Inc.  Please refer to the COPYRIGHT
 * file at the base of the distribution tree.
 *
 * $Backplane: rdbms/libsupport/strsubst.c,v 1.3 2002/08/20 22:05:55 dillon Exp $
 */

#include "defs.h"

Export const char *strsubst(const char *str, const List *varlist, char **alloc);

#define STRSUBST_VARFLAG	'$'
#define STRSUBST_BRACKET_OPEN	'{'
#define STRSUBST_BRACKET_CLOSE	'}'

/*
 * strsubst() - Perform variable substitution of a string.
 *
 *	Returns string with all occurrences of $VARIABLE or ${VARIABLE}
 *	replaced with the value associated with that variable name. Name/value
 *	pairs are defined in the varlist argument.
 */
const char *
strsubst(const char *str, const List *varlist, char **alloc)
{
    char *result = NULL;
    const char *pos, *nextpos;
    const char *varstart, *varend;
    int bracketed = 0;

    DBASSERT(varlist != NULL);

    if (str == NULL)
	return(NULL);

    if ((pos = strchr(str, STRSUBST_VARFLAG)) == NULL)
	return(str);

    result = safe_strdup_segment(str, pos);

    varstart = pos + 1;
    if (*varstart == STRSUBST_BRACKET_OPEN) {
	bracketed = 1;
	varstart++;
    }

    for (varend = varstart, nextpos = varstart;
	 isalpha(*varend) || (bracketed && *varend == STRSUBST_BRACKET_CLOSE);
	 varend++, nextpos++) {

	if (*varend == STRSUBST_BRACKET_CLOSE) {
	    nextpos++;
	    bracketed = 0;
	    break;
	}
    }

    if (varend > varstart) {
	char *substr = safe_strdup_segment(varstart, varend);
	const char *replace = varlistGet(varlist, substr, NULL);

	if (!EMPTY(replace))
	    safe_append(&result, replace);
	safe_free(&substr);
    }

    if (!EMPTY(nextpos)) {
	char *temp = NULL;
	safe_append(&result, strsubst(nextpos, varlist, &temp));
	safe_free(&temp);
    }

    safe_free(alloc);
    *alloc = result;
    return(result);
}
