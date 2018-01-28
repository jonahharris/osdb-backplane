/*
 * LIBDBCORE/DBTIME.C
 *
 * (c)Copyright 1999-2002 Backplane, Inc.  Please refer to the COPYRIGHT
 * file at the base of the distribution tree.
 *
 * $Backplane: rdbms/libdbcore/dbtime.c,v 1.12 2002/08/20 22:05:51 dillon Exp $
 */

#include "defs.h"

Export dbstamp_t dbstamp(dbstamp_t minTime, dbstamp_t id);

dbstamp_t 
dbstamp(dbstamp_t minTime, dbstamp_t id)
{
    struct timeval tv;
    dbstamp_t dbTime;

    gettimeofday(&tv, NULL);
    dbTime = (dbstamp_t)tv.tv_sec * 1000000 + tv.tv_usec;
    dbTime = (dbTime & ~DBSTAMP_ID_MASK) | id;
    if (dbTime < minTime) {
	dbTime = (minTime & ~DBSTAMP_ID_MASK) | id;
	while (dbTime < minTime)
	    dbTime += DBSTAMP_INCR;
    }
    return(dbTime);
}

