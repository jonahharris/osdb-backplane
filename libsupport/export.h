/*
 * EXPORT.H
 *
 * (c)Copyright 1999-2002 Backplane, Inc.  Please refer to the COPYRIGHT
 * file at the base of the distribution tree.
 *
 * $Backplane: rdbms/libsupport/export.h,v 1.52 2003/05/09 05:57:34 dillon Exp $
 */

extern void _DBAssert(const char *file, int line, const char *func);
extern void _DBAssertF(const char *ctl, ...);
extern int DebugOpt;

#define DBASSERT(exp)		\
	if (!(exp)) _DBAssert(__FILE__, __LINE__, __FUNCTION__)

#define DBASSERTF(exp, fmt)	\
	if (!(exp)) (_DBAssertF fmt, _DBAssert(__FILE__,__LINE__,__FUNCTION__))

#define arysize(ary)	(sizeof(ary)/sizeof((ary)[0]))

#define DEFSTRING(def)  #def
#define EXPSTRING(def)  DEFSTRING(def)
#ifndef _BACKPLANE_BASE
#error _BACKPLANE_BASE not defined
#endif
#define BACKPLANE_BASE  EXPSTRING(_BACKPLANE_BASE)
#define DEFAULT_DBDIR   BACKPLANE_BASE "drdbms"

/*
 * General machine/OS feature overrides
 */
#ifdef linux
#define HAS_SRANDOMDEV	0
#define HAS_MD5DATA	0
#endif

#ifdef __APPLE__
#define HAS_SRANDOMDEV		0
#define HAS_MD5DATA		0
#define USE_OPENSSL_CRYPTO	1
#endif

/*
 * Defaults
 */
#ifndef HAS_SRANDOMDEV
#define HAS_SRANDOMDEV	1
#endif
#ifndef HAS_MD5DATA
#define HAS_MD5DATA	1
#endif
#ifndef USE_OPENSSL_CRYPTO
#define USE_OPENSSL_CRYPTO	0
#endif

#include "log.h"
#include "align.h"
#include "lists.h"
#include "lock.h"
#include "simplehash.h"
#include "varlist.h"
#include "debug.h"
#include "version.h"
#include "cache.h"
#include "stamp.h"
#include "libsupport/support-exports.h"

#ifdef MEMDEBUG
#define zalloc(bytes)		_zalloc_debug(bytes, __FILE__, __LINE__)
#define zfree(ptr, bytes)	_zfree_debug(ptr, bytes, __FILE__, __LINE__)
#else
#define zalloc			_zalloc
#define zfree			_zfree
#endif

/*
 * Sun compatibility
 */
#ifdef sun
#define strtoq strtoll
int asprintf(char **ret, const char *format, ...);
int vasprintf(char **ret, const char *format, va_list ap);
#endif

/* Common macros to determine the maximum or minimum of a pair of values.
 * FreeBSD is nice enough to include these macros in <sys/param.h>, but
 * for source that doesn't include that header, we define them again here.
 */
#ifndef MAX
#define MAX(a, b)	(((a) > (b)) ? (a) : (b))
#endif
#ifndef MIN
#define MIN(a, b)	(((a) < (b)) ? (a) : (b))
#endif


/* Define TRUE and FALSE. */
#ifndef TRUE
#define TRUE		1
#endif

#ifndef FALSE
#define FALSE		0
#endif


/* More useful macros:
 *	PROT is a wrapper for strings that replaces NULL strings with empty
 *	strings useful for passing to strdup(); think "PROTect".
 *
 *	EMPTY returns boolean true when a string is NULL or zero-length.
 *
 *	TEMPSTR sets tmp to s and returns s, first freeing tmp if it is
 *	not NULL. This is useful when dealing with many routines that
 *	return newly-allocated strings. See backend/invoice/format.e for a
 *	good example.
 *
 *	PRE is useful for building strings from a prefix and a suffix. It
 *	is used in libcgi routines which operate on similar variables
 *	differing only the first part of their names.
 */
#define PROT(s)		((s)? (s): "")
#define EMPTY(s)	((s)? *(s) == '\0': 1)
#define TEMPSTR(tmp,s)	(safe_free(&(tmp)), (tmp) = (s))
#define PRE(tmp, p, s)  (safe_free(&(tmp)), safe_appendf(&tmp, "%s%s", p, s))

#include <ctype.h>

/* AAN returns "a" or "an" depending on whether s starts with a vowel. */
static inline const
char *
AAN(const char *s) {
    return(memchr("AEIOUaeiou", *(s), 10) == NULL? "a": "an");
}

/* RTrim removes trailing whitespace from the given string. */
static inline
char *
RTrim(char *s) {
    int end;
    for (end = strlen(s) - 1; end >= 0 && isspace(*(unsigned char *)(s + end)); end--)
	*(s + end) = '\0';
    return(s);
}

/* LTrim removes leading whitespace from the given string. */
static inline
char *
LTrim(char *s) {
    char *pos = s;
    while (isspace(*(unsigned char *)pos))
	pos++;
    memmove(s, pos, strlen(pos) + 1);
    return(s);
}

/* Trim removes leading and trailing whitespace from the given string. */
static inline
char *
Trim(char *s) {
    return(LTrim(RTrim(s)));
}

/* LTrimZero removes leading zeros from the given string. */
static inline
char *
LTrimZero(char *s) {
    char *pos = s;
    while (*pos == '0')
	pos++;
    memmove(s, pos, strlen(pos) + 1);
    return(s);
}

static inline
char *
StripChar(char *s, char c) {
    char *pos;
    if (s == NULL)
	return(NULL);
    while ((pos = strchr(s, c)) != NULL)
	memmove(pos, pos + 1, strlen(pos));
    return(s);
}

static inline
char *
ReplaceChar(char *s, char c, char r) {
    char *pos;
    DBASSERT(c != r);
    if (s == NULL)
	return(NULL);
    while ((pos = strchr(s, c)) != NULL)
	*pos = r;
    return(s);
}

/* Returns newly-allocated string with all characters in upper-case. */
static inline
char *
UpperCaseStr(const char *s)
{
    char *upper, *scan;
    upper = safe_strdup(s);
    for (scan = upper; *scan != '\0'; scan++)
        *scan = toupper(*scan);
    return(upper);
}

/* Returns newly-allocated string with all characters in lower-case. */
static inline
char *
LowerCaseStr(const char *s)
{
    char *lower, *scan;
    lower = safe_strdup(s);
    for (scan = lower; *scan != '\0'; scan++)
        *scan = tolower(*scan);
    return(lower);
}

/* Returns boolean indicating whether RFC 1738 allows the given character to
 * appear in URLs unescaped.
 */
static inline
int
IsValidURLChar(const unsigned char c)
{
    return(isalnum(c) || strchr("$-_.!*'()", c) != NULL);
}

/* Wrapper for HTMLEntities to support existing code. */
static inline
const char *
HTMLEscape(const char *data, char **alloc)
{
    char *old = *alloc;
    *alloc = HTMLEntities(data);
    safe_free(&old);
    return(*alloc);
}   

#include "charflags.h"
