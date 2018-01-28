/*
 * STRHASH.C
 *
 * (c)Copyright 1999-2002 Backplane, Inc.  Please refer to the COPYRIGHT
 * file at the base of the distribution tree.
 *
 * $Backplane: rdbms/libsupport/strhash.c,v 1.5 2002/08/20 22:05:55 dillon Exp $
 */

#include "defs.h"

Export int strhash(const char *s, int len);

/*
 * strhash() -	generate a simple 32 bit hash value given a string and its
 *		length.
 */
int
strhash(const char *s, int len)
{
    int hv = 0xA34FCD10;

    while (len) {
	hv = (hv << 5) ^ *(unsigned char *)s ^ (hv >> 23);
	--len;
	++s;
    }
    return(hv ^ (hv >> 16));
}

