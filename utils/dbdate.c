/*
 * UTILS/DBDATE.C
 *
 * (c)Copyright 2000-2002 Backplane, Inc.  Please refer to the COPYRIGHT
 * file at the base of the distribution tree.
 *
 *	DBDATE 64-bit-timestamp
 *
 *	Convert timestamp to real date.
 */

#include "defs.h"
#include <ctype.h>

int
main(int ac, char **av)
{
    int i;

    for (i = 1; i < ac; ++i) {
	dbstamp_t dbts = strtouq(av[i], NULL, 16);
        time_t t = dbts / 1000000;
        struct tm *tp;
        char buf[128];

	tp = localtime(&t);
        strftime(buf, sizeof(buf), "%a, %d %b %Y %H:%M:%S", tp);
	puts(buf);
    }
    return(0);
}
