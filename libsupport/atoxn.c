/*
 * ATOXN.C
 *
 * (c)Copyright 1999-2002 Backplane, Inc.  Please refer to the COPYRIGHT
 * file at the base of the distribution tree.
 *
 * $Backplane: rdbms/libsupport/atoxn.c,v 1.6 2002/08/20 22:05:54 dillon Exp $
 *
 *	note: for portability use long long instead of quad_t
 */

#include "defs.h"

Export long long atoqn(const char *nptr, size_t len);
Export long atoln(const char *nptr, size_t len);
Export int atoin(const char *nptr, size_t len);

/*
 * atoqn() -	convert substring to quad int (base 10)
 */
long long
atoqn(const char *nptr, size_t len)
{
    char *s;
    long long result;

    s = safe_strndup(nptr, len);
    result = strtoq(s, NULL, 10);
    safe_free(&s);

    return(result);
}

/*
 * atoln() -	convert substring to long int (base 10)
 */
long
atoln(const char *nptr, size_t len)
{
    char *s;
    long result;

    s = safe_strndup(nptr, len);
    result = strtol(s, NULL, 10);
    safe_free(&s);

    return(result);
}

/*
 * atoln() -	convert substring to int (base 10)
 */
int
atoin(const char *nptr, size_t len)
{
    char *s;
    int result;

    s = safe_strndup(nptr, len);
    result = (int)strtol(s, NULL, 10);
    safe_free(&s);

    return(result);
}

