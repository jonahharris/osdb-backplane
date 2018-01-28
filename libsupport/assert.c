/*
 * MISC.C
 *
 * (c)Copyright 1999-2002 Backplane, Inc.  Please refer to the COPYRIGHT
 * file at the base of the distribution tree.
 *
 * $Backplane: rdbms/libsupport/assert.c,v 1.6 2002/08/20 22:05:54 dillon Exp $
 */

#include "defs.h"

/*
 * _DBAssertF() - see assert.h DBASSERTF() macro
 */
void 
_DBAssertF(const char *ctl, ...)
{
    va_list va;

    va_start(va, ctl);
    vfprintf(stderr, ctl, va);
    fprintf(stderr, "\n");
    va_end(va);
}

/*
 * _DBAssert() - see assert.h DBASSERT() macro
 *
 *	This provides an assertion failure message and segfault's the
 *	program.
 */
void 
_DBAssert(const char *file, int line, const char *func)
{
    fprintf(stderr, "Assertion failed %s line %d in %s\n", file, line, func);
    fflush(stderr);
    LogWrite(HIPRI, "Assertion failed %s line %d in %s", file, line, func);
#ifndef NO_ASSERT_CORE
    *(long *)0 = 1;
#endif
    exit(1);
}

