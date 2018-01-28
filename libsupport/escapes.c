/*
 * ESCAPES.C
 *
 * (c)Copyright 1999-2002 Backplane, Inc.  Please refer to the COPYRIGHT
 * file at the base of the distribution tree.
 *
 * $Backplane: rdbms/libsupport/escapes.c,v 1.25 2002/08/20 22:05:55 dillon Exp $
 *
 * Escape character detection, processing and storage for embedded SQL
 */

#include "defs.h"

typedef struct {
    Node	escapeNode;
    char	*escapeString;
} EscapeEntry;

Export const char *EscapeStr(List *escapeList, const char *input);
Export void FreeEscapes(List *escapeListconst );
Export const char *BinaryEscape(const void *data, int size, char **alloc);
Export void *BinaryUnEscape(const char *data, int *size);

Export const char *DBMSEscape(const char *data, char **alloc, int len);
Export const char *CSVEscape(const char *data, char **alloc);

Export const char *URLEscape(const char *data, char **alloc);
Export const char *URLUnescape(const char *data, char **alloc);

Export int X2C(const char *hex);

/*
 * EscapeStr() - convert the input into a single-quoted 
 *		 escaped string
 *
 *	A tracking structure is added to List.  Use FreeEscapes() to
 *	free all tracking data (including the strings returned by this
 *	function).
 */

const char *
EscapeStr(List *escapeList, const char *input)
{
    const char *inStr;
    char *outStr;
    int escapeCount;
    EscapeEntry	*newEscape;

    /*
     * deal with NULL strings
     */
    if (input == NULL)
	return("NULL");

    /*
     * Calculate number of extra characters necessary
     */
    inStr = input;
    escapeCount = 0;
    while (*inStr != '\0') {
	switch(*inStr) {
	case '\'':
	case '\\':
	    ++escapeCount;
	    break;
	case '\n':
	case '\r':
	    escapeCount += 3;
	    break;
	default:
	    break;
	}
	++inStr;
    }
    escapeCount += 2;	/* include room for surrounding single quotes */

    newEscape = zalloc(sizeof(EscapeEntry));

    newEscape->escapeString = malloc(strlen(input) + escapeCount + 1);

    /*
     * Copy the original string to the new string, escaping those characters
     * that need it
     */ 
    inStr = input;
    outStr = newEscape->escapeString;
    *outStr++ = '\'';
    --escapeCount;
    while (*inStr != '\0') {
	switch(*inStr) {
	case '\'':
	case '\\':
	    *outStr++ = '\\';
	    *outStr++ = *inStr;
	    --escapeCount;
	    break;
	case '\n':
	case '\r':
	    snprintf(outStr, 5, "\\x%02x", (int)(unsigned char)*inStr);
	    outStr += 4;
	    escapeCount -= 3;
	    break;
	default:
	    *outStr++ = *inStr;
	    break;
	}
	++inStr;
    }
    *outStr++ = '\'';
    --escapeCount;
    *outStr = 0;
    DBASSERT(escapeCount == 0);

    addTail(escapeList, &newEscape->escapeNode);

    return(newEscape->escapeString);
}

/*
 * FreeEscapes() -	Free tracking structures and their strings
 */
void
FreeEscapes(List *escapeList)
{
    EscapeEntry *deleteNode;

    if (escapeList == NULL)
	return;

    while ((deleteNode = (EscapeEntry *)remTail(escapeList)) != NULL) {
	safe_free(&deleteNode->escapeString);
	zfree(deleteNode, sizeof(EscapeEntry));
    }
}

/*
 * DBMSEscape() -	Convert data to database-escaped form
 *
 *	Converts data to a form suitable for constructing string constants
 *	in database queries.  The caller must supply the surrounding single
 *	quots (as necessary).
 *
 *	*alloc is used to hold any allocation that must be done to support
 *	this function, or the argument 'data' may be returned directory.  If
 *	*alloc is reused, any prior contents of *alloc is freed. 
 *
 *	The caller typically initializes *alloc to NULL, calls this
 *	function repeatedly in a loop, then does a safe_free() of alloc
 *	when finished.  Unlike *asprintf(), *alloc MUST be initialized
 *	to NULL prior to first use.
 */
const char *
DBMSEscape(const char *data, char **alloc, int len)
{
    int i;
    int escCount = 0;
    char *hexTable = "0123456789ABCDEF";

    if (len < 0)
	len = strlen(data);

    for (i = 0; i < len; ++i) {
	if (data[i] == '\'' || data[i] == '\\')
	    ++escCount;
	if (data[i] == '\n' || data[i] == '\r' || data[i] == '\0')
	    escCount+=3;
    }
    if (escCount) {
	char *ptr;
	char *base;

	ptr = base = malloc(i + escCount + 1);
	for (i = 0; data[i]; ++i) {
	    if (data[i] == '\'' || data[i] == '\\') {
		*ptr++ = '\\';
		*ptr++ = data[i];
	    } else if (data[i] == '\n' || data[i] == '\r' || data[i] == '\0') {
		*ptr++ = '\\';
		*ptr++ = 'x';
		*ptr++ = hexTable[((data[i] >> 4) & 0x0F)];
		*ptr++ = hexTable[(data[i] & 0x0F)];
	    } else {
		*ptr++ = data[i];
	    }
	}
	*ptr = 0;
	if (*alloc)
	    free(*alloc);
	*alloc = base;
	data = base;
    }
    return(data);
}

/*
 * CSVEscape() - Convert data to quoted/comma-delimited-escaped form suitable
 *		 for CSV quoted/comma-delimited output.  The caller must
 *		 supply the surrounding quotes (if any).
 *
 *	*alloc is used to hold any allocation that must be done to support
 *	this function, or the argument 'data' may be returned directory.  If
 *	*alloc is reused, any prior contents of *alloc is freed. 
 *
 *	The caller typically initializes *alloc to NULL, calls this
 *	function repeatedly in a loop, then does a safe_free() of alloc
 *	when finished.  Unlike *asprintf(), *alloc MUST be initialized
 *	to NULL prior to first use.
 */
const char *
CSVEscape(const char *data, char **alloc)
{
    char *str;
    int i;
    int len;
    int count = 0;
    int qcount = 0;

    for (i = 0; data[i]; ++i) {
	switch(data[i]) {
	case '\"':
	    ++qcount;
	    /* fall through */
	case ',':
	case '#':
	case ' ':
	case '\t':
	    ++count;
	    break;
	default:
	    break;
	}
    }
    if (count == 0)
	return(data);
    len = i + qcount + 2;
    str = safe_malloc(len + 1);

    i = 0;
    str[i++] = '\"';
    while (*data) {
	switch(*data) {
	case '\"':
	    str[i++] = '\"';
	    str[i++] = '\"';
	    break;
	default:
	    str[i++] = *data;
	    break;
	}
	++data;
    }
    str[i++] = '\"';
    str[i] = 0;
    DBASSERT(i == len);
    safe_free(alloc);
    *alloc = str;
    return(str);
}


/*
 * URLEscape() - Escape string so it is suitable for inclusion in a URL.
 *
 *	Escapes "special" characters in data per RFC 1738 and returns
 *	the result.
 *
 *	*alloc is used to hold any allocation that must be done to support
 *	this function, or the argument 'data' may be returned directory.  If
 *	*alloc is reused, any prior contents of *alloc is freed. 
 *
 *	The caller typically initializes *alloc to NULL, calls this
 *	function repeatedly in a loop, then does a safe_free() of alloc
 *	when finished.  Unlike *asprintf(), *alloc MUST be initialized
 *	to NULL prior to first use.
 */
const char *
URLEscape(const char *data, char **alloc)
{
    int count = 0;
    int src, dest;
    char *result;
    unsigned char c;

    DBASSERT(alloc != NULL);

    if (data == NULL)
	return("");

    /* Determine the number of additional characters needed to encode string. */
    for (src = 0; (c = data[src]) != '\0'; src++) {
	if (!IsValidURLChar(c))
	    count += 2;
    }

    /* If there is nothing to escape, just return the original string. */
    if (count == 0)
	return(data);

    /* Allocate room for the encoded string. */
    *alloc = safe_realloc(*alloc, count + src + 1);
    result = *alloc;

    for (src = 0, dest = 0; (c = data[src]) != '\0'; src++) {
	if (IsValidURLChar(c)) {
	    result[dest] = c;
	    dest++;
	    continue;
	}
	snprintf(&result[dest], 4, "%%%02x", c);
	dest += 3;
    }
    result[dest] = '\0';
    return(result);
}


/*
 * URLUnescape() - Unescape URL-encoded string.
 *
 *	*alloc is used to hold any allocation that must be done to support
 *	this function, or the argument 'data' may be returned directory.  If
 *	*alloc is reused, any prior contents of *alloc is freed. 
 *
 *	The caller typically initializes *alloc to NULL, calls this
 *	function repeatedly in a loop, then does a safe_free() of alloc
 *	when finished.  Unlike *asprintf(), *alloc MUST be initialized
 *	to NULL prior to first use.
 */
const char *
URLUnescape(const char *data, char **alloc)
{
    int src, dest;
    unsigned char c;
    char *result;

    DBASSERT(alloc != NULL);

    if (data == NULL)
	return("");

    *alloc = safe_realloc(*alloc, strlen(data) + 1);
    result = *alloc;

    for (src = 0, dest = 0; (c = data[src]) != '\0'; src++, dest++) {
	if (c == '&')
	    break;
	if (c == '%') {
	    int xc = X2C(&data[src + 1]);
	    if (xc == -1) {
		/* X2C() conversion failed, truncate string. */
		break;
	    }
	    result[dest] = xc;
	    src += 2;
	    continue;
	}
	if (c == '+')
	    c = ' ';
	result[dest] = c;
    }
    result[dest] = '\0';
    return(result);
}


/*
 * BinaryEscape() - Escape a string into a really safe form
 *
 *	Escapes anything that is not simple alpha numerics for use in any
 *	situation where whitespace, punctuation, control, or other types
 *	characters might interfere with operations.
 *
 *	*alloc is used to hold any allocation that must be done to support
 *	this function, or the argument 'data' may be returned directory.  If
 *	*alloc is reused, any prior contents of *alloc is freed. 
 *
 *	The caller typically initializes *alloc to NULL, calls this
 *	function repeatedly in a loop, then does a safe_free() of alloc
 *	when finished.  Unlike *asprintf(), *alloc MUST be initialized
 *	to NULL prior to first use.
 *
 *	*Note that this encoding is similar, but not identical to URL encoding.
 */
const char *
BinaryEscape(const void *data, int size, char **alloc)
{
    int i;
    int bytes;
    int nbytes;
    const char *base = data;
    char *res;

    if (data == NULL)
	return("");

    for (i = bytes = 0; i < size; ++i) {
	if ((base[i] >= 'a' && base[i] <= 'z') ||
	    (base[i] >= 'A' && base[i] <= 'Z') ||
	    (base[i] >= '0' && base[i] <= '9') ||
	    base[i] == '_'
	) {
	    bytes += 1;
	} else {
	    bytes += 3;
	}
    }
    res = malloc(bytes + 1);
    for (i = nbytes = 0; i < size; ++i) {
	if ((base[i] >= 'a' && base[i] <= 'z') ||
	    (base[i] >= 'A' && base[i] <= 'Z') ||
	    (base[i] >= '0' && base[i] <= '9') ||
	    base[i] == '_'
	) {
	    res[nbytes] = base[i];
	    nbytes += 1;
	} else {
	    snprintf(res + nbytes, 4, "%%%02x", (unsigned char)base[i]);
	    nbytes += 3;
	}
    }
    DBASSERT(nbytes == bytes);
    res[bytes] = 0;
    if (*alloc)
	safe_free(alloc);
    *alloc = res;
    return(res);
}

/*
 * BinaryUnEscape() - unescape the binary data and return an allocated string.
 *
 *	Unescapes a previously binary-escaped string, returning the original
 *	data.
 *
 *	Returns an allocated copy of the result as well as the size of the
 *	unescaped result in *size.  The result string is zero-terminated
 *	(the termination is not included in the returned size), but
 *	the caller should be aware that the original data might have contained
 *	NILs as well.  *size is the actual number of bytes of the returned
 *	binary data.
 *
 *	*Note that this encoding is similar, but not identical to URL encoding.
 */
void *
BinaryUnEscape(const char *data, int *size)
{
    int i;
    int bytes;
    int nbytes;
    char *res;

    if (data == NULL) {
	if (size)
	    *size = 0;
	return(safe_strdup(""));
    }

    for (i = bytes = 0; data[i]; ++i) {
	if (data[i] == '%' && data[i+1] && data[i+2])
	    i += 2;
	++bytes;
    }
    res = malloc(bytes + 1);
    if (size)
	*size = bytes;

    for (i = nbytes = 0; data[i]; ++i) {
	if (data[i] == '%' && data[i+1] && data[i+2]) {
	    res[nbytes] = X2C(data + i + 1);
	    i += 2;
	} else {
	    res[nbytes] = data[i];
	}
	++nbytes;
    }
    res[nbytes] = 0;
    DBASSERT(bytes == nbytes);
    return(res);
}

/*
 * X2C() - convert two hex-ascii digits into hex 0-255.
 *
 *	This function returns -1 on failure.
 */
int
X2C(const char *hex)
{
    int r = -1;
    char c;

    c = hex[0];
    if (c >= 'a' && c <= 'f')
	r = c - 'a' + 10;
    else if (c >= 'A' && c <= 'F')
	r = c - 'A' + 10;
    else if (c >= '0' && c <= '9')
	r = c - '0';
    if (r >= 0) {
	c = hex[1];
	r <<= 4;
	if (c >= 'a' && c <= 'f')
	    r |= c - 'a' + 10;
	else if (c >= 'A' && c <= 'F')
	    r |= c - 'A' + 10;
	else if (c >= '0' && c <= '9')
	    r |= c - '0';
	else
	    r = -1;
    }
    return(r);
}

