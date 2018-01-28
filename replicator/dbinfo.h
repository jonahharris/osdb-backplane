/*
 * DBINFO.H
 *
 * (c)Copyright 2000-2002 Backplane, Inc.  Please refer to the COPYRIGHT
 * file at the base of the distribution tree.
 *
 *	d_Routes identifies routes to this database, grouped by host
 */

struct RouteHostInfo;

typedef struct DBInfo {
    struct DBInfo	*d_Next;
    char		*d_DBName;	/* name.createts of database */
    struct RouteHostInfo *d_RHInfoBase;	/* routes to database, by host */
    List		d_CIList;
    List		d_CDList;	/* list of (local) CLDataBases */
    int			d_CDCount;	/* count of CLDataBases */
    notify_t		d_Synchronize;	/* notify synchronizer */
    int			d_Refs;
    int			d_UseCount;
    int			d_Flags;
    int			d_BlockSize;	/* propogated by spanning tree */
    dbstamp_t		d_RepGroupTrigTs;
    dbstamp_t		d_MinCTs;	/* best minimum commit ts received */
    dbstamp_t		d_SyncTs;	/* best synchronization ts received */
} DBInfo;

#define DF_NOTIFIED	0x0001
#define DF_SHUTDOWN	0x0002
#define DF_RESTART	0x0004
#define DF_PEERCHANGE	0x0008
#define DF_LOCALRUNNING 0x0010	/* local copy of database is operational */

#define FDBERR_DUPLICATE	-1

#define RefDBInfo(d)	++d->d_Refs
#define UseDBInfo(d)	++d->d_UseCount

