/*
 * RDBMS/STAMP.H
 *
 * (c)Copyright 2000-2002 Backplane, Inc.  Please refer to the COPYRIGHT
 * file at the base of the distribution tree.
 *
 * $Backplane: rdbms/libsupport/stamp.h,v 1.7 2002/08/20 22:05:55 dillon Exp $
 */

typedef int64_t dbstamp_t;      /* 64 bit, microseconds */


#define DBOFF_MAX	((dboff_t)0x7FFFFFFFFFFFFFFFLL)
#define DBSTAMP_MAX	((dbstamp_t)0x7FFFFFFFFFFFFFFFLL)

/*
 * Each PEER embeds a unique id in generated MinCTs timestamps in
 * order to prevent collisions from simultanious phase-1 commits on clients
 */
#define DBSTAMP_INCR	((dbstamp_t)256)
#define DBSTAMP_ID_MASK	(DBSTAMP_INCR - 1)


static inline
dbstamp_t 
timetodbstamp(time_t t)
{
    return((dbstamp_t)(unsigned long)t * 1000000);
}


static inline
time_t
dbstamptotime(dbstamp_t ts)
{
    return(ts / 1000000);
}
