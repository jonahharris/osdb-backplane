/*
 * RANDOM.C
 *
 * (c)Copyright 1999-2002 Backplane, Inc.  Please refer to the COPYRIGHT
 * file at the base of the distribution tree.
 *
 * $Backplane: rdbms/libsupport/random.c,v 1.5 2003/04/02 05:27:13 dillon Exp $
 */

#include "defs.h"

Export void randominit(void);
Export char *getRandom(void);

static int RandomInitDone;

/*
 * randominit() - Initialize the random number generator if not already
 *		  initialized.
 *
 *	This routine must generate a cryptographically strong random seed.
 *	We only initialize the seed once, however.
 *
 *	XXX I have no idea what the support procedures are to initialize
 *	a truely random sequence for linux.  In FreeBSD you just call
 *	srandomdev() which is equivalent to initializing the random 
 *	number generator with a huge seed range (the state array itself)
 *	from /dev/random.  Linux seems to have no equivalent.
 */
void
randominit(void)
{
    if (RandomInitDone == 0) {
#if HAS_SRANDOMDEV
        srandomdev();
#else
	int fd = open("/dev/random", O_RDONLY);
	unsigned int n;

	if (fd < 0)
	    fatal("No srandomdev(), no /dev/random, cannot init random seed!");
	if (read(fd, &n, sizeof(n)) != sizeof(n))
	    fatal("/dev/random: short read!");
	close(fd);
	srandom(n);
	(void)random();
#endif
        RandomInitDone = 1;
    }
}


/* getRandom - Return a string that contains a 128-bit random number
 *
 * Arguments:	None
 * Globals:	None
 *
 * Returns:	Dynamically allocated string containing random number.
 *
 * Description: getRandom() returns a dynamically allocated string
 *		that contains a 128-bit random number represented 
 *		in ASCII hexadecimal.
 */
char *
getRandom(void)
{
    char	*r;
    char	*randomStr;
    int		i;
    long	randomBuf[4];
    char	*hexTable = "0123456789ABCDEF";
    unsigned char	*rand;

    LogWrite(DEBUGPRI, "getRandom starting");

    randominit();

    randomStr = safe_malloc(sizeof(randomBuf) * 2 + 1);

    randomBuf[0] = random();
    randomBuf[1] = random();
    randomBuf[2] = random();
    randomBuf[3] = random();

    /* Convert the random number into a hex string */
    rand = (unsigned char *)randomBuf;
    for (r=randomStr, i=0; i < sizeof(randomBuf); i++) {
	*r++ = hexTable[((rand[i] >> 4) & 0x0F)];
	*r++ = hexTable[(rand[i] & 0x0F)];
    }
    *r = '\0';

    LogWrite(DEBUGPRI, "getRandom done");

    return(randomStr);
}

