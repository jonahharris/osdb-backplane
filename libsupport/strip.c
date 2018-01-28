/*
 * STRIP.C
 *
 * (c)Copyright 1999-2002 Backplane, Inc.  Please refer to the COPYRIGHT
 * file at the base of the distribution tree.
 *	
 * $Backplane: rdbms/libsupport/strip.c,v 1.8 2002/08/20 22:05:55 dillon Exp $
 */

#include "defs.h"

Export char *StripString(const char *str);
Export char *StripDuplicateWhitespace(const char *str);
Export char *stripWhite(const char *input);

/*
 * StripString() - Strip all characters not in the range of [0-9a-zA-Z] from a
 *		   string.
 *
 *	Returns a newly-allocated string.
 */
char *
StripString(const char *str)
{
    int i = 0;
    char *ret;

    if (str == NULL) {
	ret = NULL;
    } else {
	const char *ptr;

	ret = safe_malloc(strlen(str) + 1);
	ptr = str;
	while (*ptr) {
	    if (isalnum(*ptr)) {
		ret[i] = *ptr;
		i++;
	    }
	    ptr++;
	}
	ret[i] = 0;
    }
    return (ret);
}


/*
 * StripDuplicateWhitespace() - Remove duplicate whitespace from a string.
 *
 *	Returns a newly-allocated string. The stripping yields output similar
 *	to browsers render HTML text save for the fact the multiple newlines
 *	are preserved.
 */
char *
StripDuplicateWhitespace(const char *str)
{
    char *result;
    char *src, *dest;

    if (str == NULL)
	return(NULL);

    result = LTrim(safe_strdup(str));
    if (EMPTY(result))
	return(result);

    for (src = dest = result + 1;
	 *src != '\0';
	 src++) {

	/* Don't ever copy carriage returns. */
	if (*src == '\r')
	    continue;

	if (!isspace(*src) || *src == '\n') {
	    *dest = *src;
	    dest++;
	    continue;
	}

	/* Remove duplicate whitespace. */
	if (*src == *(dest - 1))
	    continue;

	/* Always strip whitespace after a newline. */
	if (*(dest - 1) == '\n')
	    continue;

	/* Keep the space. */
	*dest = *src;
	dest++;
    }

    *dest = '\0';

    return(result);
}


/* stripWhite - Strip leading and trailing white space from a string
 *
 * Arguments:	input	String to be stripped
 * Globals:	None
 *   
 * Returns:	Dynamically allocated buffer containing stripped string
 *
 * Description:	stripWhite strips leading and trailing white space from
 *		the string argument provided.
 */
char *
stripWhite(const char *input)
{
    const char	*begin;
    const char	*end;
    int		i;
    int		inputLength;

    if (input == NULL)
	return(safe_strdup(""));

    inputLength = strlen(input);

    /*
     * Scan leading white space
     */
    for (i=0, begin=NULL; i<inputLength; i++) {
	if (!isspace((unsigned char)input[i])) {
	    begin = &input[i];
	    break;
	}
    }

    /*
     * Input string contains nothing but white space
     */
    if (begin == NULL)
	return(safe_strdup(""));
 
    /*
     * Scan trailing white space
     */
    for (i=inputLength-1, end=NULL; i > -1; i--) {
	if (!isspace((unsigned char)input[i])) {
	    end = &input[i];
	    break;
	}
    }

    return(safe_strndup(begin, end - begin + 1));
}

