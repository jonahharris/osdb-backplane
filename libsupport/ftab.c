/*
 * FTAB.C
 *
 * (c)Copyright 1999-2002 Backplane, Inc.  Please refer to the COPYRIGHT
 * file at the base of the distribution tree.
 *
 * $Backplane: rdbms/libsupport/ftab.c,v 1.4 2002/08/20 22:05:55 dillon Exp $
 */

#include "defs.h"

Export void ftab(FILE *fo, int level);

/*
 * Generates an indent to level N (0, 1, 2, ...) to the specified
 * file descriptor.
 */
void 
ftab(FILE *fo, int level)
{
    fprintf(fo, "%*.*s", level * 4, level * 4, "");
}

