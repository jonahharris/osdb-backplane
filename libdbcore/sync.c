/*
 * LIBDBCORE/SYNC.C	- Implements core table synchronization functions
 *
 * (c)Copyright 1999-2002 Backplane, Inc.  Please refer to the COPYRIGHT
 * file at the base of the distribution tree.
 *
 * $Backplane: rdbms/libdbcore/sync.c,v 1.37 2002/10/02 21:31:33 dillon Exp $
 *
 *	The replicator utilizes low level synchronization functions to
 *	synchronize databases.  These functions access the raw database
 *	and do minimal raw-record translation.  Basically we scan sys.tables
 *	to locate virtual table id translations and then scan their 
 *	associated physical table files to retrieve the data.
 */

#include "defs.h"
#include <dirent.h>

Export dbstamp_t RawScanByTimeStamps(DataBase *db, const char *fileName, dbstamp_t bts, dbstamp_t ets, void (*func)(void *data, RawData *rd), void *data);
Export RSTable *RawScanAllocRSTable(DataBase *db);
Export void RawScanFreeRSTable(RSTable *rl);
Export void RawScanAdd(RSTable *rl, RecHead *rh);
Export int RawMergeByTimeStamps(RSTable *rl, const char *tabName, int blockSize, dbstamp_t bts, dbstamp_t ets);

typedef struct RSTermInfo {
    void	(*ri_Func)(void *data, RawData *rd);
    TableI	*ri_TableI;
    void	*ri_Data;
    RawData	*ri_Rd;
    dbstamp_t	ri_ETs;
    dbstamp_t	ri_LTs;
    int		ri_Count;
} RSTermInfo;

static void RawScanDelete(void *data, RawData *rd);
static int RawScanTermRange(RangeArg ra);

/*
 * RawScanByTimeStamps() - scan raw table between specified time stamps.
 *
 *	We return raw records.  XXX we must translate VTId's to schema &
 *	table names still?
 *
 *	We can records non-inclusive of the ending timestamp.  We return
 *	ets unless we had to cut-off early, in which case we return the
 *	'next' timestamp after the last returned record.
 */
dbstamp_t
RawScanByTimeStamps(DataBase *db, const char *fileName, dbstamp_t bts, dbstamp_t ets, void (*func)(void *data, RawData *rd), void *data)
{
    Table *tab;
    int error;

    if ((tab = OpenTable(db, fileName, "dt0", NULL, &error)) != NULL) {
	Range *r1;
	Range *r2;
	static col_t cols[] = { CID_RAW_TIMESTAMP, CID_RAW_TIMESTAMP };
	RawData *rd = AllocRawData(tab, cols, 2);
	ColData ts1 = { NULL, 0, sizeof(dbstamp_t), (void *)&bts, 0 };
	ColData ts2 = { NULL, 0, sizeof(dbstamp_t), (void *)&ets, 0 };
	ColData *cd1 = rd->rd_ColBase;
	ColData *cd2 = cd1->cd_Next;
	RangeArg ra;
	TableI *ti = AllocPrivateTableI(rd);
	RSTermInfo ri = { func, ti, data, rd, ets, 0, 0 };

	/*
	 * note: ta_Append will be updated by OpenTable().
	 */
#if 0
	/* removed */ SyncTableAppend(tab);
#endif

	ti->ti_ScanOneOnly = -1;
	r1 = HLAddClause(NULL, NULL, ti, cd1, &ts1, ROP_STAMP_GTEQ, ROP_CONST);
	r2 = HLAddClause(NULL, r1, ti, cd2, &ts2, ROP_STAMP_LT, ROP_CONST);
	r2->r_Next.ra_VoidPtr = &ri;
	r2->r_RunRange = RawScanTermRange;

	ra.ra_RangePtr = r1;
	r1->r_RunRange(ra);

	FreeRangeList(ra.ra_RangePtr);
	LLFreeTableI(&ti);
	CloseTable(tab, 0);
	ets = ri.ri_ETs;
    }
    return(ets);
}

static int
RawScanTermRange(RangeArg ra)
{
    RSTermInfo *ri = ra.ra_VoidPtr;

    /*
     * Normal operation
     */
    if (ri->ri_Count < SYNCMERGE_COUNT - 2) {
	++ri->ri_Count;
	ri->ri_Func(ri->ri_Data, ri->ri_Rd);
	return(0);
    }

    /*
     * We returned too many records, we have to find a place
     * to stop.  Returning -1 will terminate the scan.
     */
    if (ri->ri_LTs == 0)
	ri->ri_LTs = ri->ri_Rd->rd_Rh->rh_Stamp;
    if (ri->ri_LTs < ri->ri_Rd->rd_Rh->rh_Stamp) {
	ri->ri_ETs = ri->ri_LTs + 1;
	return(-1);
    }

    /*
     * We have not found a place to stop yet, continue
     */
    ri->ri_Func(ri->ri_Data, ri->ri_Rd);
    return(0);
}

RSTable *
RawScanAllocRSTable(DataBase *db)
{
    RSTable *rl = zalloc(sizeof(RSTable));
    initList(&rl->rl_List);
    rl->rl_Db = db;
    return(rl);
}

void 
RawScanFreeRSTable(RSTable *rl)
{
    RSNode *rn;

    rl->rl_Cache = NULL;
    while ((rn = remHead(&rl->rl_List)) != NULL)
	zfree(rn, sizeof(RSNode));
    zfree(rl, sizeof(RSTable));
}

/*
 * RawScanAdd() - add record to merge table during raw scan
 *
 *	This function adds a raw record received from the remote database
 *	to the RSTable for use in later merge code.  We sort the records
 *	in order to optimize record searches when merging with the local
 *	database.
 */

void 
RawScanAdd(RSTable *rl, RecHead *rh)
{
    RSNode *rn = zalloc(sizeof(RSNode));
    RSNode *scan;

    rn->rn_Rh = rh;

    /*
     * Degenerate case, nothing in the (circular) queue yet
     */
    if ((scan = rl->rl_Cache) == NULL) {
	addTail(&rl->rl_List, &rn->rn_Node);
	rl->rl_Cache = rn;
	return;
    }

    /*
     * Sort by timestamp.
     */
    while (scan->rn_Rh->rh_Stamp > rh->rh_Stamp) {
	scan = getPred(&scan->rn_Node);
	if (&scan->rn_Node == &rl->rl_List.li_Node) {
	    insertNodeAfter(&scan->rn_Node, &rn->rn_Node);
	    rl->rl_Cache = rn;
	    return;
	}
    }
    while (scan->rn_Rh->rh_Stamp <= rh->rh_Stamp) {
	scan = getSucc(&scan->rn_Node);
	if (&scan->rn_Node == &rl->rl_List.li_Node)
	    break;
    }
    insertNodeBefore(&scan->rn_Node, &rn->rn_Node);
    rl->rl_Cache = rn;
}

static void 
RawScanDelete(void *data, RawData *rd)
{
    RSNode *scan;
    RSTable *rl = data;
    const RecHead *rh = rd->rd_Rh;

    /*
     * Degenerate case, nothing in the (circular) queue yet
     */
#if 0
    printf("RAWSCANDELETE %016qx\n", rh->rh_Stamp);
#endif
    if ((scan = rl->rl_Cache) == NULL)
	return;

    /*
     * Look for matching record.
     *
     * First locate the start of search iterating forwards
     */
    while (scan->rn_Rh->rh_Stamp < rh->rh_Stamp) {
	scan = getSucc(&scan->rn_Node);
	if (&scan->rn_Node == &rl->rl_List.li_Node)
	    return;
    }

    /*
     * Then locate the start of search iterating backwards
     */
    while (scan->rn_Rh->rh_Stamp >= rh->rh_Stamp) {
	scan = getPred(&scan->rn_Node);
	if (&scan->rn_Node == &rl->rl_List.li_Node)
	    break;
    }
    scan = getSucc(&scan->rn_Node);

    while (scan->rn_Rh->rh_Stamp == rh->rh_Stamp) {
	RSNode *next;
	RecHead *srh = scan->rn_Rh;

	next = getSucc(&scan->rn_Node);

	DBASSERT(srh->rh_Size >= sizeof(RecHead));

	if (srh->rh_VTableId == rh->rh_VTableId &&
	    srh->rh_Size == rh->rh_Size &&
	    srh->rh_NCols == rh->rh_NCols &&
	    bcmp(&srh->rh_Cols[0], &rh->rh_Cols[0], sizeof(ColHead)) == 0 &&
	    bcmp(srh + 1, rh + 1, rh->rh_Size - sizeof(RecHead)) == 0
	) {
	    int matchFlags = RHF_DELETE | RHF_UPDATE | RHF_INSERT;

	    if (((rh->rh_Flags ^ srh->rh_Flags) & matchFlags) == 0) {
#if 0
		printf("RAWSCANDELETE %016qx (FOUND MATCH)\n", rh->rh_Stamp);
#endif
		removeNode(&scan->rn_Node);
		zfree(scan, sizeof(RSNode));
		if (&next->rn_Node == &rl->rl_List.li_Node) {
		    next = getPred(&next->rn_Node);
		    if (&next->rn_Node == &rl->rl_List.li_Node)
			next = NULL;
		}
		rl->rl_Cache = next;
		break;
	    }
	}
	if (&next->rn_Node == &rl->rl_List.li_Node)
	    break;
	scan = next;
    }
}

/*
 * RawMergeByTimeStamps() -	Merge received raw data with physical table
 *
 *	Once we have received all physical records we must merge them into
 *	the physical table(s) in question.  We must lock-out other writers
 *	during the operation (i.e. lock the entire database) to prevent
 *	corruption.
 */

int 
RawMergeByTimeStamps(RSTable *rl, const char *tabName, int blockSize, dbstamp_t bts, dbstamp_t ets)
{
    Table *tab;
    int error;
    DBCreateOptions opts;

    bzero(&opts, sizeof(opts));
    if (blockSize) {
	opts.c_BlockSize = blockSize;
	opts.c_Flags = DBC_OPT_BLKSIZE;
    }

    if ((tab = OpenTable(rl->rl_Db, tabName, "dt0", &opts, &error)) != NULL) {
	RSNode *rn;
	Range *r1;
	Range *r2;
	static col_t cols[] = { CID_RAW_TIMESTAMP, CID_RAW_TIMESTAMP };
	RawData *rd = AllocRawData(tab, cols, 2);
	ColData ts1 = { NULL, 0, sizeof(dbstamp_t), (void *)&bts, 0 };
	ColData ts2 = { NULL, 0, sizeof(dbstamp_t), (void *)&ets, 0 };
	ColData *cd1 = rd->rd_ColBase;
	ColData *cd2 = cd1->cd_Next;
	RangeArg ra;
	TableI *ti = AllocPrivateTableI(rd);
	RSTermInfo ri = { RawScanDelete, ti, rl, rd };
	dbstamp_t sts;

	LockDatabase(rl->rl_Db);

	/*
	 * Note: ta_Append will be updated by OpenTable().
	 */
#if 0
	/* removed */ SyncTableAppend(tab);
	printf("SYNCHRONIZE >=%016qx to <%016qx @ %016qx\n", bts, ets, tab->ta_Append);
#endif

	ti->ti_ScanOneOnly = -1;
	r1 = HLAddClause(NULL, NULL, ti, cd1, &ts1, ROP_STAMP_GTEQ, ROP_CONST);
	r2 = HLAddClause(NULL, r1, ti, cd2, &ts2, ROP_STAMP_LT, ROP_CONST);
	ra.ra_RangePtr = r1;
	r2->r_Next.ra_VoidPtr = &ri;
	r2->r_RunRange = RawScanTermRange;
	r1->r_RunRange(ra);

	FreeRangeList(ra.ra_RangePtr);
	LLFreeTableI(&ti);

	/*
	 * Before unlocking the table flush out the merge.  Set the 
	 * 'highest time stamp in database' field first, then flush.
	 */
	if ((rn = getTail(&rl->rl_List)) != NULL) {
	    sts = rn->rn_Rh->rh_Stamp;
	    SetNextStamp(rl->rl_Db, sts + 1);
	} else {
	    sts = 0;
	}

	error = 0;

	while ((rn = remHead(&rl->rl_List)) != NULL) {
	    RecHead *rh = rn->rn_Rh;

#if 0
	    printf("***** MERGING %016qx\n", rn->rn_Rh->rh_Stamp);
#endif
	    WriteDataRecord(tab, NULL, rh, rh->rh_VTableId, rh->rh_Stamp, rh->rh_UserId, rh->rh_Flags | RHF_REPLICATED);
	    zfree(rn, sizeof(RSNode));
	    ++error;
	}

	/*
	 * Synchronize the changes made to the table.  This updates the
	 * physical table file's tf_Append and does all logging necessary
	 * to ensure proper recovery.
	 */
	SynchronizeTable(tab);
	SynchronizeDatabase(rl->rl_Db, sts);
	UnLockDatabase(rl->rl_Db);
	CloseTable(tab, 0);
    } else {
	RSNode *rn;
	while ((rn = remHead(&rl->rl_List)) != NULL)
	    zfree(rn, sizeof(RSNode));
    }
    rl->rl_Cache = NULL;
    return(error);
}

