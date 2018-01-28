/*
 * LIBCLIENT/USER.C	- User-SQL API
 *
 * (c)Copyright 2000-2002 Backplane, Inc.  Please refer to the COPYRIGHT
 * file at the base of the distribution tree.
 *
 * $Backplane: rdbms/libclient/user.c,v 1.64 2003/04/27 20:28:08 dillon Exp $
 */

#include "defs.h"

Export database_t OpenCLDataBase(const char *dbName, int *error);
Export database_t OpenCLInstance(database_t cd, dbstamp_t *syncTs, int type);
Export void CloseCLDataBase(database_t cd);
Export void CloseCLInstance(database_t cd);
Export int ReopenCLDataBase(database_t cd);
Export void PushCLTrans(database_t cd, dbstamp_t fts, int cpFlags);
Export void AbortCLTrans(database_t cd);
Export int UnCommit1CLTrans(database_t cd);
Export void SetCLId(database_t cd, dbstamp_t dbid);
Export int SyncCLInstance(database_t cd, dbstamp_t *ofts);
Export int Commit1CLTrans(database_t cd, dbstamp_t *minCTs);
Export int Commit2CLTrans(database_t cd, dbstamp_t cts);
Export res_t QueryCLTrans(database_t cd, const char *qry, int *perror);
Export void RecordQueryCLTrans(database_t cd, const char *qry, int *perror);
Export int ResCount(res_t res);
Export int ResColumns(res_t res);
Export void SetCLAuditTrail(database_t cd, cluser_t (*func)(void *info), void *info);
Export int ResSeek(res_t res, int index);
Export const char **ResFirstRow(res_t res);
Export const char **ResFirstRowL(res_t res, int **len);
Export const char **ResNextRow(res_t res);
Export const char **ResNextRowL(res_t res, int **len);
Export int ResStreamingError(res_t res);
Export void FreeCLRes(res_t res);
Export void CommitRetryDelay(void);

Prototype tlock_t OCLLock;

static int sayHello(database_t cd, const char *dbName);
static CLAnyMsg *readCLMsgSkipData(database_t cd);
static CLRow *genRowRecord(CLRes *res, CLAnyMsg *msg, CLRow *row);
static void sortCLRows(CLRes *res);
static int sortCLCompare(const void *r1, const void *r2);

tlock_t OCLLock;

/*
 * OpenCLDataBase() -	Open a client connection to the replication server 
 *			to access a particular database.
 */
CLDataBase *
OpenCLDataBase(const char *dbName, int *error)
{
    CLDataBase *cd;
    const char *emsg;
    char *udpath;
    int fd;

    *error = 0;

    if (OCLLock == NULL)
	OCLLock = allocTLock();

    /*
     * We have to hold OCLLock while checking the global database
     * handle list.  Note that the ref count will be bumped on success.
     */
    getTLock(OCLLock);
    if ((cd = FindCLDataBase(dbName, 0)) != NULL) {
	relTLock(OCLLock);
	return(cd);
    }

    /*
     * Obtain a connection to the local replication server
     */
    safe_asprintf(&udpath, "%s/.crd_socket", DefaultDBDir());

    if ((fd = ConnectUDomSocket(udpath, &emsg)) < 0)
	*error = DBERR_CANT_CONNECT;
    free(udpath);

    /*
     * On success allocate the structure and run through the
     * hello sequence.  We have to lock the separately lock
     * the database to handle a race against another thread
     * OpenCLDataBase()'ing (which would succeed instantly)
     * and then attempting to OpenCLInstance() before we've
     * had a chance to complete the main database hello
     * sequence.
     */
    if (*error == 0) {
	cd = AllocCLDataBase(dbName, allocIo(fd));
	getTLock(cd->cd_Lock);
	if (!sayHello(cd, dbName)) {
	    *error = DBERR_CANT_OPEN;
	    relTLock(cd->cd_Lock);
	    CloseCLDataBase(cd);
	    cd = NULL;
	} else {
	    relTLock(cd->cd_Lock);
	}
    }
    relTLock(OCLLock);
    return(cd);
}

/*
 * sayHello() - Perform new database connection handshake.
 *
 *	Returns boolean indicating success.
 */
static
int
sayHello(CLDataBase *cd, const char *dbName)
{
    CLAnyMsg *msg;

    /*
     * Exchange HELLOs.  We start by issuing one, then expect to get
     * one in return.
     */
    msg = BuildCLHelloMsgStr(dbName);
    WriteCLMsg(cd->cd_Iow, msg, 1);

    msg = MReadCLMsg(cd->cd_Ior);

    if (msg == NULL) {
	dberror("Client connection for %s broken unexpedtedly\n", dbName);
	return(0);
    } else if (msg->cma_Pkt.cp_Cmd != CLCMD_HELLO) {
	dberror("Unexpected response %d to HELLO packet\n",
	    msg->cma_Pkt.cp_Cmd);
	return(0);
    } else if (msg->cma_Pkt.cp_Error != 0) {
	dberror("Client connect %s:%s, error %d\n",
	    msg->a_HelloMsg.hm_DBName, dbName, msg->cma_Pkt.cp_Error
	);
	return(0);
    }

    cd->cd_HelloHost = strdup(msg->a_HelloMsg.hm_DBName);
    dbinfo("CLDB %p Client connect, HELLO from %s\n", cd, cd->cd_HelloHost);

    if (msg)
	FreeCLMsg(msg);
    return(1);
}


/*
 * ReopenCLDatabase() - Reopen a database connection.
 *
 *	Allows a database to be reconnected without closing all open instances.
 *	This is primarilly useful for recovering from SIGPIPE's caused when by
 *	temporary loss of connectivity with the database.
 */
int
ReopenCLDataBase(CLDataBase *cd)
{
    char *udpath;
    const char *emsg;
    int fd;

    /*
     * Obtain a new connection to the local replication server
     */
    if (asprintf(&udpath, "%s/.crd_socket", DefaultDBDir()) < 0)
	fatalmem();
    if ((fd = ConnectUDomSocket(udpath, &emsg)) < 0)
	fatalsys("Unable to connect to %s (%s)", udpath, emsg);
    free(udpath);

    closeIo(cd->cd_Ior);
    closeIo(cd->cd_Iow);
    cd->cd_Ior = allocIo(fd);
    cd->cd_Iow = dupIo(cd->cd_Ior);

    return(sayHello(cd, cd->cd_DBName)? 0 : -1);
}


/*
 * OpenCLInstance() -	Open instance of database, return best syncTs
 */
CLDataBase *
OpenCLInstance(CLDataBase *parCd, dbstamp_t *syncTs, int type)
{
    CLAnyMsg *msg;
    CLDataBase *cd;
    iofd_t xfd = IOFD_NULL;

    /*
     * Multiple threads may attempt to OpenCLInstance on a database at the
     * same time.
     */
#if 0
    while (parCd->cd_Flags & CDF_OWNED)
	taskWaitOnList(&parCd->cd_ReqList);
    parCd->cd_Flags |= CDF_OWNED;
#endif

    msg = BuildCLMsg(CLCMD_OPEN_INSTANCE, sizeof(CLMsg));
    getTLock(parCd->cd_Lock);
    WriteCLMsg(parCd->cd_Iow, msg, 1);
    msg = RecvCLMsg(parCd->cd_Ior, &xfd);

#if 0
    parCd->cd_Flags &= ~CDF_OWNED;
    taskWakeupList1(&parCd->cd_ReqList);
#endif
    cd = NULL;

    if (msg && msg->cma_Pkt.cp_Error) {
	FreeCLMsg(msg);
	msg = NULL;
	DBASSERT(xfd == IOFD_NULL);
    }
    if (msg != NULL) {
	cd = AllocCLInstance(parCd, xfd);
	FreeCLMsg(msg);

	if ((msg = MReadCLMsg(cd->cd_Ior)) == NULL) {
	    CloseCLInstance(cd);
	    dbinfo("CLDB %p Lost connection\n", cd);
	    cd = NULL;
	} else {
	    DBASSERT(msg->cma_Pkt.cp_Cmd == CLCMD_HELLO);
	    if (msg->cma_Pkt.cp_Error < 0) {
		CloseCLInstance(cd);
		cd = NULL;
		dberror(
		    "Error %d %s\n",
		    msg->cma_Pkt.cp_Error,
		    msg->a_HelloMsg.hm_DBName
		);
	    } else {
		if (syncTs)
		    *syncTs = msg->a_HelloMsg.hm_SyncTs;
		/* ignore msg->cm_Pkt.cp_MinCTs, we don't return it */
	    }
	    FreeCLMsg(msg);
	}
    }
    relTLock(parCd->cd_Lock);
    return(cd);
}

void
CloseCLDataBase(database_t cd)
{
    getTLock(OCLLock);
    DBASSERT(cd->cd_Refs > 0);
    if (--cd->cd_Refs == 0)
	FreeCLDataBase(cd);
    relTLock(OCLLock);
}

void
CloseCLInstance(database_t cd)
{
    database_t par = cd->cd_Parent;

    getTLock(par->cd_Lock);
    DBASSERT(cd->cd_Refs > 0);
    if (--cd->cd_Refs == 0)
	FreeCLInstance(cd);
    relTLock(par->cd_Lock);
}

/*
 * SyncCLInstance() -	update to the latest (semi synchronous) freeze
 *			timestamp.
 *
 *	Returns -1 on failure, 0 if the database has not gone past the 
 *	supplied *ofts, and 1 if the database has (and a new *ofts is
 *	updated).
 */
int
SyncCLInstance(database_t cd, dbstamp_t *ofts)
{
    CLAnyMsg *msg;
    int r = -1;

    DBASSERT((cd->cd_Flags & CDF_COMMIT1) == 0);

    msg = BuildCLMsg(CLCMD_SYNC_STAMP, sizeof(CLStampMsg));
    if (ofts)
	msg->a_StampMsg.ts_Stamp = *ofts;
    else
	msg->a_StampMsg.ts_Stamp = 0;
    WriteCLMsg(cd->cd_Iow, msg, 1);

    if ((msg = readCLMsgSkipData(cd)) != NULL) {
	DBASSERT(msg->cma_Pkt.cp_Cmd == CLCMD_SYNC_STAMP);
	r = msg->cma_Pkt.cp_Error;
	if (r > 0)
	    *ofts = msg->a_StampMsg.ts_Stamp;
	FreeCLMsg(msg);
    }
    return(r);
}

/*
 * PushCLTrans() - Push new transaction.  There is no acknowlegement to this
 *		   packet.  The freeze time stamp to use must be passed
 *		   (only applies to the first transaction level).
 *
 *		   Note that the freeze ts is used to determine which hosts
 *		   may be used to form the quorum and should either be 
 *		   the timestamp returned by the OpenCLInstance, or the 
 *		   timestamp used in the last phase-2 commit.
 */
void
PushCLTrans(CLDataBase *cd, dbstamp_t fts, int cpFlags)
{
    CLAnyMsg *msg;

    DBASSERT((cd->cd_Flags & CDF_COMMIT1) == 0);

    if (DebugOpt)
	LogWrite(DEBUGPRI, "%p %d BEGIN TRANS %016qx %04x", cd, cd->cd_Level, fts, cpFlags);

    if ((cpFlags & CPF_STREAM) || cd->cd_StreamLevel) {
	cd->cd_Flags |= CDF_STREAMMODE;
	cpFlags |= CPF_STREAM;
	++cd->cd_StreamLevel;
    }

    msg = BuildCLMsg(CLCMD_BEGIN_TRAN, sizeof(CLBeginMsg));
    msg->a_BeginMsg.bg_FreezeTs = fts;
    msg->a_BeginMsg.bg_Msg.cm_Pkt.cp_Flags |= (cpFlags & (CPF_READONLY|CPF_RWSYNC|CPF_STREAM));
    WriteCLMsg(cd->cd_Iow, msg, 0);
    if (cd->cd_Level++ == 0)
	cd->cd_StampId = fts;
}

/*
 * AbortCLTrans() - Abort current transaction (and pop it).  There is no
 *		    acknowledgement to this packet.
 */
void
AbortCLTrans(CLDataBase *cd)
{
    CLAnyMsg *msg;

    DBASSERT(cd->cd_Level != 0);

    if (DebugOpt)
	LogWrite(DEBUGPRI, "%p %d ABORT TRANS", cd, cd->cd_Level - 1);

    cd->cd_Flags &= ~CDF_COMMIT1;
    msg = BuildCLMsg(CLCMD_ABORT_TRAN, sizeof(CLMsg));
    WriteCLMsg(cd->cd_Iow, msg, 0);
    --cd->cd_Level;
    if (cd->cd_StreamLevel) {
	if (--cd->cd_StreamLevel == 0) {
	    cd->cd_Flags &= ~CDF_STREAMMODE;
	}
    }
}

/*
 * UnCommit1CLTrans() - Undoes a previous commit1
 *
 *	Note that deadlock packets are not visible in the
 *	client protocol, so we do not have to look for them.
 */
int 
UnCommit1CLTrans(database_t cd)
{
    int r = -1;
    CLAnyMsg *msg;

    DBASSERT((cd->cd_Flags & CDF_COMMIT1) != 0);
    DBASSERT(cd->cd_Level != 0);

    cd->cd_Flags &= ~CDF_COMMIT1;

    msg = BuildCLMsg(CLCMD_UNCOMMIT1_TRAN, sizeof(CLMsg));
    WriteCLMsg(cd->cd_Iow, msg, 1);
    if ((msg = readCLMsgSkipData(cd)) != NULL) {
	DBASSERT(msg->cma_Pkt.cp_Cmd == CLCMD_UNCOMMIT1_TRAN);
	r = msg->cma_Pkt.cp_Error;
	FreeCLMsg(msg);
    }
    return(r);
}

/*
 * SetCLId() -	Set database id for timestamp allocation
 *
 *	This function is typically only used by the replicator in order
 *	to guarentee that timestamps allocated from databases are unique
 *	on a host-by-host basis.
 */

void 
SetCLId(database_t cd, dbstamp_t dbid)
{
    CLAnyMsg *msg;

    cd->cd_StampId = dbid;
    msg = BuildCLMsg(CLCMD_UPDATE_STAMPID, sizeof(CLStampMsg));
    msg->a_StampMsg.ts_Stamp = dbid;
    WriteCLMsg(cd->cd_Iow, msg, 0);
}

#if 0
/*
 * WaitCLTrans() - wait for the database synchronization timestamp to
 *		   change to a value greater then syncTs.
 *
 *	The new syncTs is returned and may be used in queries.  The syncTs
 *	returned may be less then the last commit timestamp the caller
 *	tracked, due to the commit not having yet synchronized.  The caller
 *	must handle this case and use the higher timestamp (which will cause
 *	his queries to stall until at least one database has synchronized
 *	sufficiently).
 *
 *	XXX timeout
 */
void
WaitCLTrans(database_t cd, dbstamp_t *syncTs, int *to)
{
    CLAnyMsg *msg;
    
    msg = BuildCLMsg(CLCMD_WAIT_TRAN, sizeof(CLStampMsg));
    msg->a_StampMsg.ts_Stamp = *syncTs + 1;
    WriteCLMsg(cd->cd_Iow, msg, 1);

    if ((msg = readCLMsgSkipData(cd)) != NULL) {
	DBASSERT(msg->cma_Pkt.cp_Cmd == CLCMD_WAIT_TRAN);
	*syncTs = msg->a_StampMsg.ts_Stamp;
	FreeCLMsg(msg);
    }
}

#endif

/*
 * Commit1CLTrans() - Issue phase-1 commit of the current transaction
 *
 *	Returns 0 on success, -error on failure
 *
 *	Sets *minCTs to the proposed commit timestamp on success, and
 *	to the highest conflicting timestamp on failure.  You must 
 *	pre-initialize *minCTs to the lowest minCTs you want to allow
 *	(you can initialize it to 0 to have the system assign its own
 *	best minCTs).  Only the replicator really needs to use the
 *	minimum minCTs feature.
 */
int
Commit1CLTrans(CLDataBase *cd, dbstamp_t *minCTs)
{
    int r = -1;
    CLAnyMsg *msg;

    DBASSERT((cd->cd_Flags & CDF_COMMIT1) == 0);
    DBASSERT(cd->cd_Level != 0);

    /*
     * If committing at the top level try to resolve the audit trail
     */
    if (cd->cd_Level == 1 &&
	cd->cd_LLAuditCallBack &&
	(cd->cd_Flags & CDF_AUDITCOMMITGOOD) == 0
    ) {
	cd->cd_LLUserId = cd->cd_LLAuditCallBack(cd->cd_LLAuditInfo);
    }

    /*
     * Issue commit1
     */
    msg = BuildCLMsg(CLCMD_COMMIT1_TRAN, sizeof(CLCommit1Msg));
    if (*minCTs < cd->cd_StampId)
	*minCTs = cd->cd_StampId;
    msg->a_Commit1Msg.c1_MinCTs = *minCTs;
    WriteCLMsg(cd->cd_Iow, msg, 1);

    if ((msg = readCLMsgSkipData(cd)) != NULL) {
	DBASSERT(msg->cma_Pkt.cp_Cmd == CLCMD_COMMIT1_TRAN);
	r = msg->cma_Pkt.cp_Error;
	*minCTs = msg->a_Commit1Msg.c1_MinCTs;
	if (r == 0)
	    cd->cd_Flags |= CDF_COMMIT1;
	FreeCLMsg(msg);
    }
    if (DebugOpt)
	LogWrite(DEBUGPRI, "%p %d COMMIT1 TRANS %d %016qx", cd, cd->cd_Level, r, *minCTs);
    return(r);
}

/*
 * Commit2CLTrans() - Issue phase-2 commit of the current transaction
 *
 *	Returns 0 on success, -error on failure.  The transaction is popped
 *	either way.
 */
int
Commit2CLTrans(CLDataBase *cd, dbstamp_t cts)
{
    int r = -1;
    CLAnyMsg *msg;

    DBASSERT(cd->cd_Level != 0);
    if ((cd->cd_Flags & CDF_COMMIT1) == 0)
	return(DBERR_COMMIT2_WITHOUT_COMMIT1);
#if 0
    DBASSERT((cd->cd_Flags & CDF_COMMIT1) != 0);
#endif

    cd->cd_Flags &= ~CDF_COMMIT1;
    msg = BuildCLMsg(CLCMD_COMMIT2_TRAN, sizeof(CLCommit2Msg));
    msg->a_Commit2Msg.c2_MinCTs = cts;
    msg->a_Commit2Msg.c2_UserId = cd->cd_LLUserId;
    WriteCLMsg(cd->cd_Iow, msg, 1);

    if ((msg = readCLMsgSkipData(cd)) != NULL) {
	DBASSERT(msg->cma_Pkt.cp_Cmd == CLCMD_COMMIT2_TRAN);
	r = msg->cma_Pkt.cp_Error;
	FreeCLMsg(msg);
    }
    --cd->cd_Level;
    if (cd->cd_StreamLevel) {
	if (--cd->cd_StreamLevel == 0) {
	    cd->cd_Flags &= ~CDF_STREAMMODE;
	}
    }

    /*
     * If we have a valid LLUserId and the commit succeeded, set 
     * CDF_AUDITCOMMITGOOD.  This 'caches' the audit trail id.
     */
    if (r == 0 && cd->cd_LLUserId)
	cd->cd_Flags |= CDF_AUDITCOMMITGOOD;
    return(r);
}

/*
 * QueryCLTrans() - Execute Query, return results
 *
 *	Run the specified query and return the results.  *perror is negative
 *	if an error occured, 0 or positive (record count) on success.
 */
CLRes *
QueryCLTrans(CLDataBase *cd, const char *qry, int *perror)
{
    CLRes *res = NULL;
    CLAnyMsg *msg;

    DBASSERT(cd->cd_Level != 0);

    *perror = -1;

    /*
     * XXX wait for previous query to terminate
     */
    if (DebugOpt)
	LogWrite(DEBUGPRI, "%p %d QUERY %s", cd, cd->cd_Level, qry);

    msg = BuildCLMsgStr(CLCMD_RUN_QUERY_TRAN, qry);
    WriteCLMsg(cd->cd_Iow, msg, 1);

    while ((msg = readCLMsgSkipData(cd)) != NULL) {
	/*
	 * Terminator
	 */
	if (msg->cma_Pkt.cp_Cmd == CLCMD_RUN_QUERY_TRAN) {
	    /* Adjust returned row count to reflect limiting. */
	    if ((*perror = msg->cma_Pkt.cp_Error) >= 0 &&
		 res != NULL && res->cr_MaxRows >= 0) {
		*perror = res->cr_MaxRows;
		if (*perror > res->cr_NumRows - res->cr_StartRow)
		    *perror = res->cr_NumRows - res->cr_StartRow;
	    }
	    break;
	}

	/*
	 * Reset result list (occurs if the host we are getting our results
	 * from fails and the replicator is forced to restart the query
	 * (typically a select)) on some other host.
	 */
	if (msg->cma_Pkt.cp_Cmd == CLCMD_RESULT_RESET) {
	    dbinfo("CLDB %p msg %p ********** CLIENT RESET RESULTS ***********\n", cd, msg);
	    FreeCLMsg(msg);
	    if (res) {
		FreeCLRes(res);
		res = NULL;
	    }
	    continue;
	}

	/*
	 * Allocate a result structure if we dont have one
	 */

	if (res == NULL) {
	    res = zalloc(sizeof(CLRes));
	    res->cr_Instance = cd;
	    res->cr_MaxRows = -1;
	    initList(&res->cr_RowList);
	}

	/*
	 * An ORDER BY packet
	 *
	 * (illegal if streaming)
	 */
	if (msg->cma_Pkt.cp_Cmd == CLCMD_RESULT_ORDER) {
	    DBASSERT(res->cr_NumOrder == 0);
	    DBASSERT((cd->cd_Flags & CDF_STREAMMODE) == 0);

	    if ((res->cr_NumOrder = msg->a_OrderMsg.om_NumOrder) != 0) {
		int bytes;

		bytes = sizeof(*res->cr_Order) * res->cr_NumOrder;
		res->cr_Order = zalloc(bytes);
		bcopy(msg->a_OrderMsg.om_Order, res->cr_Order, bytes);
	    }
	    FreeCLMsg(msg);
	    continue;
	}
	/*
	 * A LIMIT packet
	 */
	if (msg->cma_Pkt.cp_Cmd == CLCMD_RESULT_LIMIT) {
	    res->cr_StartRow = msg->a_LimitMsg.lm_StartRow;
	    res->cr_MaxRows = msg->a_LimitMsg.lm_MaxRows;
	    FreeCLMsg(msg);
	    continue;
	}

	/*
	 * The only other allowed return value is CLCMD_RESULT.  
	 * XXX should allow CLOSE_INSTANCE as an indication of an
	 * unrecoverable error XXX.
	 *
	 * Generate the row record
	 */
	DBASSERT(msg->cma_Pkt.cp_Cmd == CLCMD_RESULT);
	(void)genRowRecord(res, msg, NULL);

	/*
	 * If in streaming mode, return immediately
	 */
	if (cd->cd_Flags & CDF_STREAMMODE) {
	    cd->cd_Flags |= CDF_STREAMQRY;
	    *perror = 0;
	    if (DebugOpt)
		LogWrite(DEBUGPRI, "%p %d QUERY RESULTS (STREAMING) %d %p", cd, cd->cd_Level, *perror, res);
	    return(res);
	}
    }

    /*
     * If we lost the link in the middle, do not return a partial result
     */
    if (msg == NULL) {
	*perror = -1;
	if (res) {
	    FreeCLRes(res);
	    res = NULL;
	}
    } else {
	if (res && res->cr_NumOrder)
	    sortCLRows(res);
	FreeCLMsg(msg);
    }
    if (DebugOpt)
	LogWrite(DEBUGPRI, "%p %d QUERY RESULTS %d %p", cd, cd->cd_Level, *perror, res);
    return(res);
}

static CLRow *
genRowRecord(CLRes *res, CLAnyMsg *msg, CLRow *row)
{
    int bytes;
    int roff;
    int i;

    /*
     * Generate the row record
     *
     * rm_ShowCount - first N columns are visible
     * rm_Count	- total number of columns returned (extras may be
     *		  necessary for invisible sort columns)
     */

    if (res->cr_TotalCols == 0) {
	res->cr_TotalCols = msg->a_RowMsg.rm_Count;
	res->cr_NumCols = msg->a_RowMsg.rm_ShowCount;
    }
    DBASSERT(res->cr_NumCols == msg->a_RowMsg.rm_ShowCount);
    DBASSERT(msg->a_RowMsg.rm_Offsets[msg->a_RowMsg.rm_Count] +
	(offsetof(CLMsg, cm_Pkt.cp_Data[0]) - offsetof(CLMsg, cm_Pkt)) <=
	msg->cma_Pkt.cp_Bytes);

    bytes = msg->a_RowMsg.rm_Offsets[msg->a_RowMsg.rm_Count] - msg->a_RowMsg.rm_Offsets[0];
    bytes += offsetof(CLRow, co_Data[msg->a_RowMsg.rm_Count]);
    roff = bytes;
    bytes += sizeof(int) * msg->a_RowMsg.rm_Count;

    /*
     * If we are passed a (prior) row to free, which
     * occurs in streaming mode, we may be able
     * to reuse it instead.
     */
    if (row && row->co_Bytes < bytes) {
	removeNode(&res->cr_CurRow->co_Node);
	zfree(row, row->co_Bytes);
	row = NULL;
    }
    if (row == NULL) {
	row = zalloc(bytes);
	row->co_Res = res;
	row->co_Bytes = bytes;
	row->co_Lens = (int *)((char *)row + roff);
	addTail(&res->cr_RowList, &row->co_Node);
    }

    roff = offsetof(CLRow, co_Data[msg->a_RowMsg.rm_Count]);

    for (i = 0; i < msg->a_RowMsg.rm_Count; ++i) {
	int off = msg->a_RowMsg.rm_Offsets[i];
	int len = msg->a_RowMsg.rm_Offsets[i+1] - off;

	DBASSERT(len >= 0);
	DBASSERT(off + len + (offsetof(CLMsg, cm_Pkt.cp_Data[0]) - offsetof(CLMsg, cm_Pkt)) <= msg->cma_Pkt.cp_Bytes);

	if (len) {
	    row->co_Data[i] = (char *)row + roff;
	    row->co_Lens[i] = len;
	    bcopy(
		&msg->cma_Pkt.cp_Data[off],
		(char *)row + roff, 
		len
	    );
	} else {
	    row->co_Data[i] = NULL;
	    row->co_Lens[i] = 0;
	}
	roff += len;
    }
    FreeCLMsg(msg);
    ++res->cr_NumRows;
    return(row);
}

static CLRes *SortRes;

static void
sortCLRows(CLRes *res)
{
    if (res->cr_NumRows) {
	CLRow **ary = safe_malloc(sizeof(CLRow *) * res->cr_NumRows);
	CLRow *row;
	int i = 0;

	while ((row = remHead(&res->cr_RowList)) != NULL) {
	    DBASSERT(i < res->cr_NumRows);
	    ary[i++] = row;
	}
	DBASSERT(i == res->cr_NumRows);
	DBASSERT(SortRes == NULL);
	SortRes = res;
	qsort(ary, i, sizeof(CLRow *), sortCLCompare);
	SortRes = NULL;
	for (i = 0; i < res->cr_NumRows; ++i) {
	    addTail(&res->cr_RowList, &ary[i]->co_Node);
	}
	free(ary);
    }
}

static int
sortCLCompare(const void *vr1, const void *vr2)
{
    int i;
    int r = 0;
    const CLRow *r1 = *(void **)vr1;
    const CLRow *r2 = *(void **)vr2;
    CLRes *res = SortRes;

    for (i = 0; i < res->cr_NumOrder; ++i) {
	int col = res->cr_Order[i] & ORDER_STRING_COLMASK;
	DBASSERT(col < res->cr_TotalCols);
	if (r1->co_Data[col] == NULL) {
	    if (r2->co_Data[col] == NULL)
		continue;
	    r = -1;
	    break;
	}
	if (r2->co_Data[col] == NULL) {
	    r = 1;
	    break;
	}
	if ((r = strcmp(r1->co_Data[col], r2->co_Data[col])) != 0)
	    break;
    }
    if (r) {
	if (res->cr_Order[i] & ORDER_STRING_REV)
	    r = -r;
    }
    return(r);
}

/*
 * QueryCLTrans() - Execute Query, erturn results
 *
 *	Record the specified query
 */

void
RecordQueryCLTrans(CLDataBase *cd, const char *qry, int *perror)
{
    CLAnyMsg *msg;

    DBASSERT(cd->cd_Level != 0);

    *perror = -1;

    msg = BuildCLMsgStr(CLCMD_REC_QUERY_TRAN, qry);
    WriteCLMsg(cd->cd_Iow, msg, 0);
}

int 
ResCount(CLRes *res)
{
    return(res->cr_NumRows);
}

int 
ResColumns(CLRes *res)
{
    return(res->cr_NumCols);
}

/*
 * Seek to a particular row (non-streaming queries only).  Use
 * ResNextRow() to retrieve it.
 *
 * Returns 0 on success, -1 on failure.
 */
int
ResSeek(CLRes *res, int index)
{
    CLRow *row = res->cr_CurRow;
    CLDataBase *cd = res->cr_Instance;

    if (index == 0) {
	res->cr_CurRowNum = 0;
	for (row = getHead(&res->cr_RowList);
	     row != NULL;
	     row = getListSucc(&res->cr_RowList, &row->co_Node)
	 ) {
	    if (cd->cd_Flags & CDF_STREAMQRY)
		break;
	    if (res->cr_CurRowNum >= res->cr_StartRow)
		break;
	    ++res->cr_CurRowNum;
	}	
    } else if (row && index < res->cr_CurRowNum - res->cr_StartRow) {
	while (index < res->cr_CurRowNum - res->cr_StartRow) {
	    --res->cr_CurRowNum;
	    if ((row = getListPred(&res->cr_RowList, &row->co_Node)) == NULL)
		break;
	}
    } else if (row && index > res->cr_CurRowNum - res->cr_StartRow) {
	while (index > res->cr_CurRowNum - res->cr_StartRow) {
	    ++res->cr_CurRowNum;
	    if ((row = getListSucc(&res->cr_RowList, &row->co_Node)) == NULL)
		break;
	}
    }
    if ((res->cr_CurRow = row) != NULL)
	return(0);
    else
	return(-1);
}

/*
 * Retrieve Results
 */
const char **
ResFirstRow(CLRes *res)
{
    CLRow *row;

    ResSeek(res, 0);
    if ((row = res->cr_CurRow) != NULL)
	return(&row->co_Data[0]);
    return(NULL);
}

const char **
ResFirstRowL(CLRes *res, int **lens)
{
    CLRow *row;

    ResSeek(res, 0);
    if ((row = res->cr_CurRow) != NULL) {
	*lens = row->co_Lens;
	return(&row->co_Data[0]);
    }
    *lens = NULL;
    return(NULL);
}

const char **
ResNextRow(CLRes *res)
{
    CLRow *row;

    if ((row = res->cr_CurRow) != NULL) {
	/*
	 * Retrieve the next row given the current row.
	 *
	 * If we are in streaming mode we pull the results in
	 * real time, replaceing the last row on the fly.
	 */
	CLDataBase *cd = res->cr_Instance;

	if (cd->cd_Flags & CDF_STREAMQRY) {
	    CLAnyMsg *msg = MReadCLMsg(cd->cd_Ior);

	    if (msg->cma_Pkt.cp_Cmd == CLCMD_RUN_QUERY_TRAN) {
		res->cr_Error = msg->cma_Pkt.cp_Error;
		cd->cd_Flags &= ~CDF_STREAMQRY;
		FreeCLMsg(msg);
		row = NULL;
	    } else {
		DBASSERT(msg->cma_Pkt.cp_Cmd == CLCMD_RESULT);
		row = genRowRecord(res, msg, row);
	    }
	} else {
	    row = getListSucc(&res->cr_RowList, &row->co_Node);
	}
	res->cr_CurRow = row;
	++res->cr_CurRowNum;
	if (row) {
	    if (res->cr_MaxRows >= 0 &&
		res->cr_CurRowNum >= res->cr_StartRow + res->cr_MaxRows
	    ) {
		return(NULL);
	    }
	    return(&row->co_Data[0]);
	}
    }
    return(NULL);
}

const char **
ResNextRowL(CLRes *res, int **lens)
{
    CLRow *row;

    if ((row = res->cr_CurRow) != NULL) {
	/*
	 * Retrieve the next row given the current row.
	 *
	 * If we are in streaming mode we pull the results in
	 * real time, replacing the last row on the fly.
	 */
	CLDataBase *cd = res->cr_Instance;

	if (cd->cd_Flags & CDF_STREAMQRY) {
	    CLAnyMsg *msg = MReadCLMsg(cd->cd_Ior);

	    if (msg == NULL) {
		res->cr_Error = DBERR_LOST_LINK;
		cd->cd_Flags &= ~CDF_STREAMQRY;
		row = NULL;
	    } else if (msg->cma_Pkt.cp_Cmd == CLCMD_RUN_QUERY_TRAN) {
		res->cr_Error = msg->cma_Pkt.cp_Error;
		cd->cd_Flags &= ~CDF_STREAMQRY;
		FreeCLMsg(msg);
		row = NULL;
	    } else {
		DBASSERT(msg->cma_Pkt.cp_Cmd == CLCMD_RESULT);
		row = genRowRecord(res, msg, row);
	    }
	} else {
	    row = getListSucc(&res->cr_RowList, &row->co_Node);
	}
	res->cr_CurRow = row;
	++res->cr_CurRowNum;
	if (row) {
	    if (res->cr_MaxRows >= 0 &&
		res->cr_CurRowNum >= res->cr_StartRow + res->cr_MaxRows
	    ) {
		return(NULL);
	    }
	    *lens = row->co_Lens;
	    return(&row->co_Data[0]);
	}
    }
    *lens = NULL;
    return(NULL);
}

/*
 * After ResNextRow() returns NULL in a streaming query,
 * this function will return a negative error if the stream
 * was interrupted, >= 0 if it completed ok.  This is the
 * count / error field that would normally have been returned
 * by Query.
 */
int
ResStreamingError(CLRes *res)
{
    return(res->cr_Error);
}

void
FreeCLRes(CLRes *res)
{
    CLRow *row;

    res->cr_CurRow = NULL;
    while ((row = remHead(&res->cr_RowList)) != NULL) {
	zfree(row, row->co_Bytes);
    }
    if (res->cr_NumOrder) {
	zfree(res->cr_Order, sizeof(*res->cr_Order) * res->cr_NumOrder);
	res->cr_NumOrder = 0;
	res->cr_Order = NULL;
    }
    if (res->cr_RHSize) {
	zfree(res->cr_RHash, res->cr_RHSize * sizeof(CLRow *));
	res->cr_RHash = NULL;
	res->cr_RHSize = 0;
    }
    zfree(res, sizeof(CLRes));
}

/*
 * Poor man's random commit-fail-retry delay
 */
void
CommitRetryDelay(void)
{
    static int DidInitRandom;

    if (DidInitRandom == 0) {
	DidInitRandom = 1;
	randominit();
    }
    taskSleep(random() % 500 + 20);
}

static
CLAnyMsg *
readCLMsgSkipData(database_t cd)
{
    CLAnyMsg *msg;

    while ((msg = MReadCLMsg(cd->cd_Ior)) != NULL) {
	if (cd->cd_Flags & CDF_STREAMQRY) {
	    /*
	     * A result record from the query, which we must
	     * skip because we've gone on to another query.
	     */
	    if (msg->cma_Pkt.cp_Cmd == CLCMD_RESULT) {
		FreeCLMsg(msg);
		continue;
	    }

	    /*
	     * End of query, whew!
	     */
	    if (msg->cma_Pkt.cp_Cmd == CLCMD_RUN_QUERY_TRAN) {
		cd->cd_Flags &= ~CDF_STREAMQRY;
		FreeCLMsg(msg);
		continue;
	    }
	    DBASSERT(msg->cma_Pkt.cp_Cmd != CLCMD_RESULT_RESET);
	}
	break;
    }
    return(msg);
}

/*
 * SetCLUserId() -	Set the audit trail id generator callback
 *
 *	We clear CDF_AUDITCOMMITGOOD in order to force a call to 
 *	the callback, which may insert a new audit trail record just
 *	before the top-level phase-1 commit.  userid is typically passed
 *	as 0 to this function.
 */
void
SetCLAuditTrail(database_t cd, cluser_t (*func)(void *info), void *info)
{
    cd->cd_LLUserId = 0;
    cd->cd_LLAuditInfo = info;
    cd->cd_LLAuditCallBack = func;
    cd->cd_Flags &= ~CDF_AUDITCOMMITGOOD;
}

