/*
 * LIBDBCORE/DATAMAP.C -	Datamap cache support routines
 *
 * (c)Copyright 2000-2002 Backplane, Inc.  Please refer to the COPYRIGHT
 * file at the base of the distribution tree.
 *
 * $Backplane: rdbms/libdbcore/datamap.c,v 1.10 2002/10/03 18:17:31 dillon Exp $
 *
 *    Most of the datamap caching is handled in dbmem.c and dbfile.c directly,
 *    this file contains global variables and routines to support the datamap
 *    file ops.
 *
 *    At the moment our garbage collection code is rather poor, but it 
 *    accomplishes our main goal of not mmap()ing too much data.  Keep in
 *    mind that the kernel will cache file data whether we mmap() it or not,
 *    and unmapping it here should not effect the kernel filesystem buffer
 *    cache much at all.
 */

#include "defs.h"

Export u_long	BCMemoryUsed;
Export u_long	BCMemoryLimit;

Prototype DataMap *BCHash[DM_HSIZE];
Prototype void DataMapGarbageCollect(void);

DataMap	*BCHash[DM_HSIZE];
u_long	BCMemoryUsed;
u_long	BCMemoryLimit = MAX_DATAMAP_CACHE;	/* typically 1G */

/*
 * DataMapGarbageCollect() - free up non-persitent datamap blocks
 */
void
DataMapGarbageCollect(void)
{
    static int DMapPurgeIndex;
    int maxscan = DM_HSIZE / 4;

    printf("*** DataMapGarbageCollect ***\n");

    while (BCMemoryUsed > BCMemoryLimit / 4 * 3 && maxscan) {
	DataMap *dm;
	int hv = --DMapPurgeIndex & DM_HMASK;

restart:
	for (dm = BCHash[hv]; dm; dm = dm->dm_HNext) {
	    if (dm->dm_Refs == 0) {
		++dm->dm_Refs;
		dm->dm_Table->ta_RelDataMap(&dm, 1);
		goto restart;
	    }
	}
	--maxscan;
    }
}

