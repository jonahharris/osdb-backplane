/*
 * DATABASE/INSTANCE.C - Implement packet protocol to support an instance of 
 *			the database under management.
 *
 * (c)Copyright 2000-2002 Backplane, Inc.  Please refer to the COPYRIGHT 
 * file at the base of the distribution tree.
 *
 * $Backplane: rdbms/database/instance.c,v 1.70 2002/08/20 22:05:47 dillon Exp $
 */

#include "defs.h"

Prototype void DatabaseInstanceThread(CLDataBase *cd);

static dbstamp_t DoCLRawRead(CLDataBase *cd, dbstamp_t bts, dbstamp_t ets);
static int RSTermRange(Query *q);
static void RawScanCallBack(void *vcd, RawData *rd);
static int SendResultMessage(CLDataBase *cd, Query *q, CLAnyMsg *msg);

static void RequestClientSort(CLDataBase *cd, Query *q);
static void RequestClientLimit(CLDataBase *cd, Query *q);

static void AddSortedResults(Query *q);
static void SortAndLimitResults(Query *q);
static int SortResultsCompare(const void *vr1, const void *vr2);
static int SendSortedResults(CLDataBase *cd, Query *q);

static int ActiveQueries;

/*
 * DatabaseInstanceThread() -	A single instance of a database
 *
 *	Represents a single instance of a database on which transactions
 *	may be run.
 *
 *	We send a single HELLO indicating our ability to open the database
 *	prior to entering the packet loop.
 */

void
DatabaseInstanceThread(CLDataBase *cd)
{
    CLAnyMsg *msg;
    int error;

    dbinfo("drd_database: start instance %s\n", cd->cd_DBName);

    cd->cd_Db = OpenDatabase(DefaultDBDir(), cd->cd_DBName, 0, NULL, &error);
    cd->cd_NotifyInt = allocNotify();
    SetDatabaseId(cd->cd_Db, cd->cd_StampId);
    dbinfo("drd_database: setting ID %qd for %s\n", 
	cd->cd_StampId,
	cd->cd_DBName
    );

    if (error == 0) {
	/*
	 * Successful open of the database, return success along with
	 * the best known synchronization timestamp and the best known
	 * minimum commit time stamp.  MinCTs is not used in the
	 * client<->replicator protocol but is used in replicator<->database
	 * protocol.
	 */
	DBASSERT(cd->cd_Db != NULL);
	msg = BuildCLHelloMsgStr("");
	msg->a_HelloMsg.hm_BlockSize = GetSysBlockSize(cd->cd_Db);
	msg->a_HelloMsg.hm_SyncTs = GetSyncTs(cd->cd_Db);
	msg->a_HelloMsg.hm_MinCTs = GetMinCTs(cd->cd_Db);
	WriteCLMsg(cd->cd_Iow, msg, 1);

	while ((msg = MReadCLMsg(cd->cd_Ior)) != NULL) {
	    switch(msg->cma_Pkt.cp_Cmd) {
	    case CLCMD_CONTINUE:
	    case CLCMD_BREAK_QUERY:
		/* extranious CLCMD_CONTINUE */
		/* extranious CLCMD_BREAK_QUERY */
		/* allow the message to be freed */
		break;
	    case CLCMD_CLOSE_INSTANCE:
		WriteCLMsg(cd->cd_Iow, msg, 1);
		msg = NULL;
		break;
	    case CLCMD_BEGIN_TRAN:
		/*
		 * Begin a transaction using the supplied freeze point.  The
		 * freeze point is ignored for sub transactions.
		 */
		{
		    int flags = 0;
		    if (msg->cma_Pkt.cp_Flags & CPF_READONLY)
			flags = DBF_READONLY;
		    cd->cd_Db = PushDatabase(
				    cd->cd_Db, 
				    msg->a_BeginMsg.bg_FreezeTs,
				    &error,
				    flags
				);
		}
		++cd->cd_Level;
		break;
	    case CLCMD_RUN_QUERY_TRAN:
		/*
		 * Run a query within a transaction
		 */
		{
		    Query *q = GetQuery(cd->cd_Db);
		    int type;
		    token_t t;

		    type = SqlInit(
				&t, 
				msg->cma_Pkt.cp_Data, 
				msg->cma_Pkt.cp_Bytes - sizeof(msg->cma_Pkt)
			    );
		    type = ParseSql(&t, q, type);
		    q->q_QryCopy = safe_strndup(msg->cma_Pkt.cp_Data, msg->cma_Pkt.cp_Bytes - sizeof(msg->cma_Pkt));
		    if (type & TOKF_ERROR) {
			msg->cma_Pkt.cp_Error = -(int)(type & TOKF_ERRMASK);
			WriteCLMsg(cd->cd_Iow, msg, 1);
			msg = NULL;
			FreeQuery(q);
		    } else {
			/*
			 * Issue the query, then return the error/count
			 */
			cd->cd_DSTerm = q->q_TableIQBase;
			q->q_TermFunc = RSTermRange;
			q->q_TermInfo = cd;
			q->q_StallCount = 0;
			++ActiveQueries;
			error = RunQuery(q);	/* error or record count */

			if (!listIsEmpty(&q->q_ResultBuffer)) {
			    SortAndLimitResults(q);
			    error = SendSortedResults(cd, q);
			    q->q_Flags &= ~(QF_CLIENT_ORDER|QF_CLIENT_LIMIT);
			}
			msg->cma_Pkt.cp_Error = error;

			--ActiveQueries;
			DBASSERT(ActiveQueries >= 0);

			if (error >= 0 &&
			    (q->q_Flags & (QF_CLIENT_ORDER|QF_WITH_ORDER)) ==
				(QF_CLIENT_ORDER|QF_WITH_ORDER))
			    RequestClientSort(cd, q);
			if (error >= 0 &&
			    (q->q_Flags & (QF_CLIENT_LIMIT|QF_WITH_LIMIT)) ==
				(QF_CLIENT_LIMIT|QF_WITH_LIMIT))
			    RequestClientLimit(cd, q);

			WriteCLMsg(cd->cd_Iow, msg, 1);
			msg = NULL;
			RelQuery(q);
		    }
		}
		break;
	    case CLCMD_REC_QUERY_TRAN:
		/*
		 * Record a query within a transaction.
		 *
		 * We have to run all non-selects in order to populate the
		 * temporary tables that will then be copied to the conflict
		 * resolution table in commit phase-1, but we do not return
		 * anything.
		 */
		{
		    Query *q = GetQuery(cd->cd_Db);
		    int type;
		    token_t t;

		    type = SqlInit(
				&t, 
				msg->cma_Pkt.cp_Data, 
				msg->cma_Pkt.cp_Bytes - sizeof(msg->cma_Pkt)
			    );
		    type = ParseSql(&t, q, type);
		    if (type & TOKF_ERROR) {
			/* XXX Rel? I think guards are separated out, */
			/* we should be ok to Free */
			FreeQuery(q);
		    } else {
			if (q->q_TermOp != QOP_SELECT) {
			    q->q_TermFunc = NULL;
			    ++ActiveQueries;
			    RunQuery(q);	/* error or record count */
			    --ActiveQueries;
			}
			RelQuery(q);
		    }
		}
		break;
	    case CLCMD_ABORT_TRAN:
		/*
		 * Abort a transaction
		 */
		if (cd->cd_Level) {
		    --cd->cd_Level;
		    if (cd->cd_Flags & CDF_COMMIT1) {
			cd->cd_Flags &= ~CDF_COMMIT1;
			Abort1(cd->cd_Db);
		    }
		    cd->cd_Db = PopDatabase(cd->cd_Db, &error);
		}
		break;
	    case CLCMD_UNCOMMIT1_TRAN:
		/*
		 * Undoes the phase-1 commit state
		 */
		error = -1;
		if (cd->cd_Level) {
		    error = -2;
		    if ((cd->cd_Flags & CDF_COMMIT1) != 0) {
			error = 0;
			cd->cd_Flags &= ~CDF_COMMIT1;
			UnCommit1(cd->cd_Db);
		    }
		}
		msg->cma_Pkt.cp_Error = error;
		WriteCLMsg(cd->cd_Iow, msg, 1);
		msg = NULL;
		break;
	    case CLCMD_COMMIT1_TRAN:
		/*
		 * Phase-1 commit.  If no error occured we return the
		 * minimum commit timestamp.  A phase-2 commit must use
		 * this timestamp or a higher timestamp.
		 */
		error = -1;
		if (cd->cd_Level) {
		    error = -2;

		    if ((cd->cd_Flags & CDF_COMMIT1) == 0) {
			/*
			 * Deal with potential deadlocks (note: thread atomicy
			 * between check and Commit1() return XXX)  XXX
			 */
			/*
			 * Issue the phase-1 commit
			 *
			 * The returned MinCTs will be the minimum commit
			 * timestamp on success, and will be the conflicting
			 * timestamp on failure.
			 *
			 * XXX callback sequencing
			 */
			error = Commit1(
				    cd->cd_Db, 
				    &msg->a_Commit1Msg.c1_MinCTs, 
				    NULL, 
				    NULL,
				    0
				);
			if (error == 0) {
			    cd->cd_Flags |= CDF_COMMIT1;
			}
		    }
		}
		msg->cma_Pkt.cp_Error = error;
		WriteCLMsg(cd->cd_Iow, msg, 1);
		msg = NULL;
		break;
	    case CLCMD_COMMIT2_TRAN:
		error = -1;
		if (cd->cd_Level) {
		    error = -2;
		    if (cd->cd_Flags & CDF_COMMIT1) {
			int dummy;

			error = Commit2(
				    cd->cd_Db,
				    msg->a_Commit2Msg.c2_MinCTs,
				    msg->a_Commit2Msg.c2_UserId,
				    0
				);
			cd->cd_Db = PopDatabase(cd->cd_Db, &dummy);
			cd->cd_Flags &= ~CDF_COMMIT1;
			--cd->cd_Level;
		    }
		}
		msg->cma_Pkt.cp_Error = error;
		WriteCLMsg(cd->cd_Iow, msg, 1);
		msg = NULL;
		break;
	    case CLCMD_RAWREAD:
		/*
		 * XXX error handling
		 */
		msg->a_RawReadMsg.rr_EndTs = DoCLRawRead(cd, msg->a_RawReadMsg.rr_StartTs, msg->a_RawReadMsg.rr_EndTs);
		WriteCLMsg(cd->cd_Iow, msg, 1);
		msg = NULL;
		break;
	    case CLCMD_RAWWRITE:
		/*
		 * Write/Merge raw records for synchronization purposes
		 */
		{
		    CLAnyMsg *smsg;

		    smsg = MReadCLMsg(cd->cd_Ior);

		    while (smsg) {
			List list;
			CLAnyMsg *tmsg;
			RSTable *rst;
			dbstamp_t bts = 0;
			dbstamp_t ets = 0;
			int count = 0;
			int bytes = 0;
			rhflags_t lastOp = 0;
			CLRawDataFileMsg *rdfMsg;

			initList(&list);
			rst = RawScanAllocRSTable(cd->cd_Db);

			if (smsg->cma_Pkt.cp_Cmd == CLCMD_RAWWRITE_END) {
			    RawScanFreeRSTable(rst);
			    WriteCLMsg(cd->cd_Iow, smsg, 1);
			    break;
			}
			DBASSERT(smsg->cma_Pkt.cp_Cmd == CLCMD_RAWDATAFILE);
			rdfMsg = &smsg->a_RawDataFileMsg;

			/*
			 * Retrieve CLCMD_RAWDATA records for this file
			 */
			while ((tmsg = MReadCLMsg(cd->cd_Ior)) != NULL) {
			    RecHead *rh;

			    if (tmsg->cma_Pkt.cp_Cmd != CLCMD_RAWDATA)
				break;
			    rh = (RecHead *)(&tmsg->cma_Pkt.cp_Data[0]);

			    /*
			     * Assert that the packet is well formed
			     */
			    DBASSERT(tmsg->cma_Pkt.cp_Bytes >= offsetof(CLPkt, cp_Data[rh->rh_Size]));

			    if (rh->rh_Flags & RHF_UPDATE)
				DBASSERT(lastOp & RHF_DELETE);
			    lastOp = rh->rh_Flags;

			    /*
			     * Dont let too much accumulate, but always merge
			     * all records associated with a single transaction
			     * all together.  XXX error handling.
			     */
			    if ((count >= MAXMERGE_COUNT ||
				bytes >= MAXMERGE_BYTES) &&
				rh->rh_Stamp != ets
			    ) {
				CLAnyMsg *jmsg;

				dbinfo2("DUMP1 %d/%d\n", count, bytes);
				RawMergeByTimeStamps(
				    rst, 
				    rdfMsg->rdf_FileName,
				    rdfMsg->rdf_BlockSize,
				    bts,
				    ets + 1
				);
				while ((jmsg = remHead(&list)) != NULL)
				    FreeCLMsg(jmsg);
				count = 0;
				bytes = 0;
			    }
			    /*
			     * Accumulate record.  Assert that the timestamps
			     * are ordered as expected (if not we may be
			     * improperly doing a reverse-index-scan when we 
			     * should be doing a forward scan).
			     */
			    DBASSERT(ets <= rh->rh_Stamp);
			    if (count == 0)
				bts = rh->rh_Stamp;
			    ++count;
			    bytes += rh->rh_Size;
			    ets = rh->rh_Stamp;
			    addTail(&list, &tmsg->a_Msg.cm_Node);
			    RawScanAdd(rst, rh);
			}
			if (tmsg != NULL &&
			    (tmsg->cma_Pkt.cp_Cmd == CLCMD_RAWDATAFILE ||
			    tmsg->cma_Pkt.cp_Cmd == CLCMD_RAWWRITE_END)
			) {
			    if (count) {
				dbinfo2("DUMP2 %d/%d FILE %s BLK %d %08llx-%08llx\n", count, bytes, rdfMsg->rdf_FileName, rdfMsg->rdf_BlockSize, bts, ets + 1);
				RawMergeByTimeStamps(
				    rst, 
				    rdfMsg->rdf_FileName,
				    rdfMsg->rdf_BlockSize,
				    bts,
				    ets + 1
				);
			    }
			} else {
			    DBASSERT(tmsg == NULL);
			}
			FreeCLMsg(smsg);
			smsg = tmsg;
			while ((tmsg = remHead(&list)) != NULL)
			    FreeCLMsg(tmsg);
			RawScanFreeRSTable(rst);
		    }
		}
		break;
	    default:
		DBASSERT(0);
		msg->cma_Pkt.cp_Error = -2;
		WriteCLMsg(cd->cd_Iow, msg, 1);
		msg = NULL;
	    }
	    if (msg)
		FreeCLMsg(msg);
	    taskQuantum();
	}
    } else {
	/*
	 * Couldn't open the database, return a failure.  XXX not formatted
	 * as a CLHelloMsg.
	 */
	msg = BuildCLHelloMsgStr(cd->cd_DBName);
	msg->cma_Pkt.cp_Error = -1;
	WriteCLMsg(cd->cd_Iow, msg, 1);
    }
    if (cd->cd_Db) {
	while (cd->cd_Level) {
	    if (cd->cd_Flags & CDF_COMMIT1) {
		cd->cd_Flags &= ~CDF_COMMIT1;
		Abort1(cd->cd_Db);
	    }
	    cd->cd_Db = PopDatabase(cd->cd_Db, &error);
	    --cd->cd_Level;
	}
	CloseDatabase(cd->cd_Db, 0);
    }
    freeNotify(cd->cd_NotifyInt);
    cd->cd_NotifyInt = NULL;
    dbinfo("drd_database: stop instance %s\n", cd->cd_DBName);
    CloseCLInstance(cd);
}


static dbstamp_t
DoCLRawRead(CLDataBase *cd, dbstamp_t bts, dbstamp_t ets)
{
    dbstamp_t rts = ets;
    TableI *tiBase;
    TableI *ti;
    int error;

    /*
     * Push transaction based on ending timestamp
     */
    cd->cd_Db = PushDatabase(cd->cd_Db, ets, &error, DBF_READONLY);
    ++cd->cd_Level;

    /*
     * Obtain list of physical files by accessing TableFile in sys.tables
     */
    tiBase = LLGetTableI(cd->cd_Db, NULL, NULL, 0);

    for (ti = tiBase; ti; ti = ti->ti_Next) {
	TableI *tis;
	dbstamp_t ts;

	/*
	 * Bleh O(N^2)
	 */
	for (tis = tiBase; tis != ti; tis = tis->ti_Next) {
	    if (strcmp(tis->ti_TableFile, ti->ti_TableFile) == 0)
		break;
	}
	if (tis != ti)
	    continue;
	cd->cd_Flags &= ~CDF_DIDRAWDATA;
	ts = RawScanByTimeStamps(
	    cd->cd_Db,
	    ti->ti_TableFile,
	    bts,
	    ets,
	    RawScanCallBack,
	    cd
	);

	/*
	 * The table scan may terminate early due to hitting its record
	 * limit (typically MAXMERGE_COUNT or MAXMERGE_BYTES).  We can only
	 * return the smallest scan timestamp.
	 */
	if (rts > ts)
	    rts = ts;
    }
    LLFreeTableI(&tiBase);
    cd->cd_Db = PopDatabase(cd->cd_Db, &error);
    --cd->cd_Level;
    cd->cd_Flags &= ~CDF_DIDRAWDATA;
    return(rts);
}


static void
AddSortedResults(Query *q)
{
    ResultRow *row = NULL;
    ColI *ci;
    int i;

    row = zalloc(sizeof(ResultRow));
    initNode(&row->rr_Node);
    row->rr_NumCols = q->q_OrderCount;
    row->rr_NumSortCols = q->q_IQSortCount;
    if (row->rr_NumCols) {
	row->rr_Data = zalloc(sizeof(char *) * row->rr_NumCols);
	row->rr_DataLen = zalloc(sizeof(int) * row->rr_NumCols);
    }
    if (row->rr_NumSortCols) {
	row->rr_SortData = zalloc(sizeof(char *) * row->rr_NumSortCols);
	row->rr_SortDataLen = zalloc(sizeof(int) * row->rr_NumSortCols);
    }

    for (ci = q->q_ColIQBase, i = 0; ci != NULL; ci = ci->ci_QNext, i++) {
	ColData *ccd = ci->ci_CData;
	char *data = NULL;

	if (ccd->cd_Data != NULL) {
	    if (ccd->cd_Bytes) {
		data = zalloc(ccd->cd_Bytes);
		bcopy(
		    ccd->cd_Data,
		    data,
		    ccd->cd_Bytes
		);
	    } else {
		data = "";
	    }
	} else {
	    DBASSERT(ccd->cd_Bytes == 0);
	}
	row->rr_Data[i] = data;
	row->rr_DataLen[i] = ccd->cd_Bytes;
    }
    DBASSERT(i == row->rr_NumCols);

    for (ci = q->q_ColIQSortBase, i = 0; ci != NULL; ci = ci->ci_QSortNext, i++) {
	ColData *ccd = ci->ci_CData;
	char *data = NULL;

	if (ccd->cd_Data != NULL) {
	    if (ccd->cd_Bytes) {
		data = zalloc(ccd->cd_Bytes);
		bcopy(
		    ccd->cd_Data,
		    data,
		    ccd->cd_Bytes
		);
	    } else {
		data = "";
	    }
	} else {
	    DBASSERT(ccd->cd_Bytes == 0);
	}
	row->rr_SortData[i] = data;
	row->rr_SortDataLen[i] = ccd->cd_Bytes;
	if (ci->ci_Flags & CIF_SORTDESC)
	    row->rr_SortDataLen[i] |= RR_SORTDESC_MASK;
	if (ci->ci_Flags & CIF_ORDER)
	    row->rr_SortDataLen[i] |= RR_SORTSHOW_MASK;
    }
    DBASSERT(i == row->rr_NumSortCols);

    if ((q->q_Flags & QF_WITH_LIMIT) &&
	 q->q_StartRow == 0 &&
	 q->q_MaxRows == 1) {
	/* Special case for handling common LIMIT 1 queries. */
	ResultRow *head = getHead(&q->q_ResultBuffer);
	if (head != NULL) {
	    q->q_CountRows = 1;
	    if (SortResultsCompare(&row, &head) > 0) {
		FreeResultRow(row);
		return;
	    }
	    else {
		removeNode(&head->rr_Node);
		FreeResultRow(head);
	    }
	}
    }
    addTail(&q->q_ResultBuffer, &row->rr_Node);
}

static void
SortAndLimitResults(Query *q)
{
    ResultRow **sorttable;
    ResultRow *row;
    int i = 0;

    sorttable = malloc(sizeof(ResultRow *) * q->q_CountRows);
    while ((row = remHead(&q->q_ResultBuffer)) != NULL) {
	sorttable[i] = row;
	i++;
    }

    DBASSERT(i == q->q_CountRows);
    if (q->q_CountRows > 0) {
	qsort(sorttable, q->q_CountRows, sizeof(ResultRow *),
	      SortResultsCompare);
    }

    for (i = 0; i < q->q_CountRows; i++) {
	if ((q->q_Flags & QF_WITH_LIMIT) &&
	    (i < q->q_StartRow || i >= q->q_StartRow + q->q_MaxRows)) {
	    FreeResultRow(sorttable[i]);
	    continue;
	}
	addTail(&q->q_ResultBuffer, &sorttable[i]->rr_Node);
    }

    free(sorttable);
}

static int
SortResultsCompare(const void *vr1, const void *vr2)
{
    const ResultRow *r1 = *(void **)vr1;
    const ResultRow *r2 = *(void **)vr2;
    int r = 0;
    int i;

    for (i = 0; i < r1->rr_NumSortCols; i++) {
	int minlen;

	if (r1->rr_SortData[i] == NULL) {
	    if (r2->rr_SortData[i] == NULL)
		continue;
	    r = -1;
	    break;
	}
	if (r2->rr_SortData[i] == NULL) {
	    r = 1;
	    break;
	}

	minlen = MIN(r1->rr_SortDataLen[i] & RR_SORTDATALEN_MASK,
		     r2->rr_SortDataLen[i] & RR_SORTDATALEN_MASK);
	if ((r = memcmp(r1->rr_SortData[i], r2->rr_SortData[i], minlen)) != 0) {
	    break;
	}

	r = (r1->rr_SortDataLen[i] & RR_SORTDATALEN_MASK) -
	    (r2->rr_SortDataLen[i] & RR_SORTDATALEN_MASK);
	if (r != 0)
	    break;
    }

    if (r != 0 && (r1->rr_SortDataLen[i] & RR_SORTDESC_MASK))
	r = -r;

    return(r);
}


static int
SendSortedResults(CLDataBase *cd, Query *q)
{
    ResultRow *row = NULL;
    CLAnyMsg *msg;
    int bytes;
    int off;
    int cols;
    int i;
    int sr;
    int r = 0;

    while ((row = remHead(&q->q_ResultBuffer)) != NULL) {
	cols = 0;
	bytes = offsetof(CLRowMsg, rm_Offsets[1]);

	for (i = 0; i < row->rr_NumCols; i++) {
	    if (row->rr_Data[i] != NULL)
		bytes += ((row->rr_DataLen[i] + 1) + 3) & ~3;
	    bytes += sizeof(int);	/* rm_Offsets entry */
	    cols++;
	}

	for (i = 0; i < row->rr_NumSortCols; i++) {
	    if (row->rr_SortDataLen[i] & RR_SORTSHOW_MASK)
		continue;	/* row already included in display list */
	    if (row->rr_SortData[i] != NULL)
		bytes += (((row->rr_SortDataLen[i] & RR_SORTDATALEN_MASK) + 1) + 3) & ~3;
	    bytes += sizeof(int);	/* rm_Offsets entry */
	    cols++;
	}

	/*
	 * Build the next record.
	 */
	msg = BuildCLMsg(CLCMD_RESULT, bytes);
	off = offsetof(CLRowMsg, rm_Offsets[cols+1]) -
		offsetof(CLRowMsg, rm_Msg.cm_Pkt.cp_Data[0]);
	cols = 0;

	for (i = 0; i < row->rr_NumCols; i++) {
	    msg->a_RowMsg.rm_Offsets[cols] = off;
	    if (row->rr_Data[i] != NULL) {
		bcopy(
		    row->rr_Data[i],
		    msg->cma_Pkt.cp_Data + off,
		    row->rr_DataLen[i]
		);
		off = ((off + row->rr_DataLen[i] + 1) + 3) & ~3;
	    }
	    cols++;
	}

	msg->a_RowMsg.rm_ShowCount = row->rr_NumCols;

	for (i = 0; i < row->rr_NumSortCols; i++) {
	    if (row->rr_SortDataLen[i] & RR_SORTSHOW_MASK)
		continue;	/* row already included in display list */
	    msg->a_RowMsg.rm_Offsets[cols] = off;
	    if (row->rr_SortData[i] != NULL) {
		bcopy(
		    row->rr_SortData[i],
		    msg->cma_Pkt.cp_Data + off,
		    row->rr_SortDataLen[i] & RR_SORTDATALEN_MASK
		);
		off = ((off + (row->rr_SortDataLen[i] & RR_SORTDATALEN_MASK) + 1) + 3) & ~3;
	    }
	    cols++;
	}

	msg->a_RowMsg.rm_Offsets[cols] = off;
	msg->a_RowMsg.rm_Count = cols;

	DBASSERT(off == bytes - offsetof(CLMsg, cm_Pkt.cp_Data[0]));

	FreeResultRow(row);

	sr = SendResultMessage(cd, q, msg);
	if (sr < 0)
	    break;
	r += sr;
    }

    FreeResultBuffer(q);
    return(r);
}


/*
 * RSTermRange()
 *
 *	Note that each termination range falls within a single physical table
 */
static int
RSTermRange(Query *q)
{
    CLDataBase *cd = q->q_TermInfo;
    /*TableI *ti = cd->cd_DSTerm;*/	/* XXX q_TableIQBase */
    CLAnyMsg *msg;
    ColI *ci;
    int cols;
    int bytes;
    int off;
    int sr;
    int r = 0;

    if (q->q_Flags & QF_WITH_ORDER) {
	if (q->q_CountRows <= 500) {
	    AddSortedResults(q);
	    return(1); 
	}
	else if (!listIsEmpty(&q->q_ResultBuffer)) {
	    r = SendSortedResults(cd, q);
	    if (r < 0)
		return(r);
	}
    }

    /*
     * Figure out how much space we need in the cp_Data[] portion of
     * the CLMsg packet.  We need to reserve space for the rm_Offsets
     * array (including one extra to terminate the last entry), and
     * for the row data.
     *
     * Row data includes selected data and any data required for
     * sorting.  Sorting is a client-side operation, not a server-side
     * operation.
     */
    cols = 0;
    bytes = offsetof(CLRowMsg, rm_Offsets[1]);

    for (ci = q->q_ColIQBase; ci; ci = ci->ci_QNext) {
	ColData *ccd = ci->ci_CData;

	if (ccd->cd_Data)
	    bytes += ((ccd->cd_Bytes + 1) + 3) & ~3;
	bytes += sizeof(int);	/* rm_Offsets entry */
	++cols;
    }
    for (ci = q->q_ColIQSortBase; ci; ci = ci->ci_QSortNext) {
	ColData *ccd;

	if (ci->ci_Flags & CIF_ORDER)	/* already included */
	    continue;
	ccd = ci->ci_CData;
	if (ccd->cd_Data)
	    bytes += ((ccd->cd_Bytes + 1) + 3) & ~3;
	bytes += sizeof(int);	/* rm_Offsets entry */
	++cols;
    }

    /*
     * Build the next record.
     */
    msg = BuildCLMsg(CLCMD_RESULT, bytes);

    off = offsetof(CLRowMsg, rm_Offsets[cols+1]) -
	    offsetof(CLRowMsg, rm_Msg.cm_Pkt.cp_Data[0]);
    cols = 0;

    for (ci = q->q_ColIQBase; ci; ci = ci->ci_QNext) {
	ColData *ccd = ci->ci_CData;

	msg->a_RowMsg.rm_Offsets[cols] = off;
	++cols;
	if (ccd->cd_Data) {
	    bcopy(
		ccd->cd_Data, 
		msg->cma_Pkt.cp_Data + off, 
		ccd->cd_Bytes
	    );
	    off = ((off + ccd->cd_Bytes + 1) + 3) & ~3;
	}
    }

    msg->a_RowMsg.rm_ShowCount = cols;

    for (ci = q->q_ColIQSortBase; ci; ci = ci->ci_QSortNext) {
	ColData *ccd;

	if (ci->ci_Flags & CIF_ORDER)	/* already included */
	    continue;
	msg->a_RowMsg.rm_Offsets[cols] = off;
	++cols;
	ccd = ci->ci_CData;
	if (ccd->cd_Data) {
	    bcopy(
		ccd->cd_Data, 
		msg->cma_Pkt.cp_Data + off, 
		ccd->cd_Bytes
	    );
	    off = ((off + ccd->cd_Bytes + 1) + 3) & ~3;
	}
    }
    msg->a_RowMsg.rm_Offsets[cols] = off;
    msg->a_RowMsg.rm_Count = cols;

    sr = SendResultMessage(cd, q, msg);
    if (sr < 0)
	return(sr);
    return(r + sr);
}


static int
SendResultMessage(CLDataBase *cd, Query *q, CLAnyMsg *msg)
{
    int r = 1;

    WriteCLMsg(cd->cd_Iow, msg, 0);

    /*
     * Adjust the stall count for data written.
     */
    q->q_StallCount += msg->cma_Pkt.cp_Bytes;

    /*
     * This is the core callback function returning query results.  A
     * negative return here will abort the query.  Make sure we flush
     * all output before waiting for the unstall.
     */
    while (q->q_StallCount > CL_STALL_COUNT) {
	CLAnyMsg *clMsg;

	WriteCLMsg(cd->cd_Iow, NULL, 1);
	if ((clMsg = MReadCLMsg(cd->cd_Ior)) != NULL) {
	    switch(clMsg->cma_Pkt.cp_Cmd) {
	    case CLCMD_CONTINUE:
		q->q_StallCount -= CL_STALL_COUNT / 2;
		break;
	    case CLCMD_BREAK_QUERY:
		r = DBERR_SELECT_BREAK;
		q->q_StallCount = 0;
		break;
	    default:
		DBASSERT(0);
	    }
	    FreeCLMsg(clMsg);
	} else {
	    DBASSERT(0);
	}
    }

    /*
     * Cooperative threading
     */
    taskQuantum();
    return(r);
}



static void
RawScanCallBack(void *vcd, RawData *rd)
{
    CLDataBase *cd = vcd;
    CLAnyMsg *msg;
    const RecHead *rh = rd->rd_Rh;

    if ((cd->cd_Flags & CDF_DIDRAWDATA) == 0) {
	const char *tabName = rd->rd_Table->ta_Name;
	int tabNameLen = strlen(tabName) + 1;

	cd->cd_Flags |= CDF_DIDRAWDATA;
	msg = BuildCLMsg(CLCMD_RAWDATAFILE, offsetof(CLRawDataFileMsg, rdf_FileName[tabNameLen]));
	msg->a_RawDataFileMsg.rdf_BlockSize = rd->rd_Table->ta_Meta->tf_BlockSize;
	bcopy(tabName, msg->a_RawDataFileMsg.rdf_FileName, tabNameLen);
	WriteCLMsg(cd->cd_Iow, msg, 1);
    }

    msg = BuildCLMsg(CLCMD_RAWDATA, sizeof(CLMsg) + rh->rh_Size);
    bcopy(rh, &msg->cma_Pkt.cp_Data[0], rh->rh_Size);
    WriteCLMsg(cd->cd_Iow, msg, 0);

    taskQuantum();
}


static void
RequestClientSort(CLDataBase *cd, Query *q)
{
    ColI *ci;
    CLAnyMsg *msg;
    int bytes;
    int count = 0;

    if (q->q_ColIQSortBase == NULL)
	return;

    /*
     * Dump column orderings for ORDER BY.  The client
     * is responsible for sorting the results.
     */

    bytes = offsetof(CLOrderMsg, om_Order[q->q_IQSortCount]);
    msg = BuildCLMsg(CLCMD_RESULT_ORDER, bytes);
    msg->a_OrderMsg.om_NumOrder = q->q_IQSortCount;

    for (ci = q->q_ColIQSortBase; 
	 ci; 
	 ci = ci->ci_QSortNext
    ) {
	msg->a_OrderMsg.om_Order[count] = ((ci->ci_Flags & CIF_SORTDESC) ?
				ORDER_STRING_REV :
				ORDER_STRING_FWD) |
				ci->ci_OrderIndex;
	++count;
    }
    DBASSERT(q->q_IQSortCount == count);
    WriteCLMsg(cd->cd_Iow, msg, 0);
}


static void
RequestClientLimit(CLDataBase *cd, Query *q)
{
    CLAnyMsg *msg;

    if (q->q_MaxRows < 0)
	return;

    msg = BuildCLMsg(CLCMD_RESULT_LIMIT, sizeof(CLLimitMsg));
    msg->a_LimitMsg.lm_MaxRows = q->q_MaxRows;
    msg->a_LimitMsg.lm_StartRow = q->q_StartRow;
    WriteCLMsg(cd->cd_Iow, msg, 0);
}

