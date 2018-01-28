/*
 * REPLICATOR/VCINSTANCE.C -
 *
 * (c)Copyright 2000-2002 Backplane, Inc.  Please refer to the COPYRIGHT
 * file at the base of the distribution tree.
 *
 * $Backplane: rdbms/replicator/vcinstance.c,v 1.66 2002/08/20 22:06:00 dillon Exp $
 */

#include "defs.h"

Prototype void VCInstanceSlave(InstInfo *ii);
Prototype void UpdateRouteMinCTs(CLDataBase *parCd);

static void VCSlavePacketLoop(InstInfo *ii, CLDataBase *cd, CLDataBase *parCd);
static int VCSlaveQueryTrans(CLDataBase *cd, const char *qry, InstInfo *ii);
static int VCSlaveRawRead(CLDataBase *cd, RPRawReadMsg *rpMsg, InstInfo *ii);

/*
 * VCInstanceSlave() -	Receive commands over the VC from remote hosts and
 *			execute them, returning the results as appropriate.
 */
void
VCInstanceSlave(InstInfo *ii)
{
    RPAnyMsg *rpMsg;
    CLDataBase *cdp;
    CLDataBase *cd;
    dbstamp_t syncTs = 0;

    if ((cdp = getHead(&ii->i_RHSlave->rh_DBInfo->d_CDList)) == NULL) {
	dberror("SlaveOpenInstance: Could not find db %s\n", 
	    ii->i_RHSlave->rh_DBInfo->d_DBName
	);
	rpMsg = BuildVCPkt(ii, RPCMD_OPEN_INSTANCE|RPCMDF_REPLY, sizeof(RPOpenInstanceMsg));
	rpMsg->a_OpenInstanceMsg.oi_Msg.rp_Pkt.pk_RCode = -1;
	WriteVCPkt(rpMsg);
    } else {
	RefCLDataBase(cdp);
	if ((cd = OpenCLInstance(cdp, &syncTs, CLTYPE_RW)) == NULL) {
	    dberror("SlaveOpenInstance: failed to open instance of %s\n",
		ii->i_RHSlave->rh_DBInfo->d_DBName
	    );
	    rpMsg = BuildVCPkt(ii, RPCMD_OPEN_INSTANCE|RPCMDF_REPLY, sizeof(RPOpenInstanceMsg));
	    rpMsg->rpa_Pkt.pk_RCode = -2;
	    WriteVCPkt(rpMsg);
	} else {
	    /*
	     * Return the best synchronization timestamp we can that will not
	     * stall queries.
	     */
	    rpMsg = BuildVCPkt(ii, RPCMD_OPEN_INSTANCE|RPCMDF_REPLY, sizeof(RPOpenInstanceMsg));
	    rpMsg->a_OpenInstanceMsg.oi_SyncTs = syncTs;
	    WriteVCPkt(rpMsg);
	    VCSlavePacketLoop(ii, cd, cdp);
	    CloseCLInstance(cd);
	}
	CloseCLDataBase(cdp);
    }
    zfree(ii->i_RXPktList, sizeof(List));
    ii->i_RXPktList = NULL;
    freeNotify(ii->i_NotifyInt);
    ii->i_NotifyInt = NULL;
    FreeInstInfo(ii);
}

static void
VCSlavePacketLoop(InstInfo *ii, CLDataBase *cd, CLDataBase *parCd)
{
    ii->i_VCILevel = 0;
    ii->i_VCILCmd = 0;

    while ((ii->i_Flags & IIF_FAILED) == 0) {
	RPAnyMsg *rpMsg;

	waitNotify(ii->i_NotifyInt, 0);

	while ((rpMsg = remHead(ii->i_RXPktList)) != NULL) {
	    int error = -1;
	    int respond = 0;

	    dbinfo4("INST %p RPMSG RECV INST %s %04x:%04x %02x (%d)\n", ii, ii->i_MasterHost->h_HostName, ii->i_VCId, ii->i_InstId, rpMsg->rpa_Pkt.pk_Cmd, rpMsg->rpa_Pkt.pk_SeqNo);
	    ii->i_VCILCmd = rpMsg->rpa_Pkt.pk_Cmd;

	    switch(rpMsg->rpa_Pkt.pk_Cmd) {
	    case RPCMD_CLOSE_INSTANCE:
		ii->i_Flags |= IIF_FAILED;	/* force loop exit */
		respond = sizeof(RPMsg);
		break;
	    case RPCMD_BEGIN_TRAN:
		/*
		 * The freeze point is passed to us in tr_FreezeTs.  This
		 * only applies to the first level transaction.
		 */
		dbinfo("%p: BEGIN(%d) %016qx\n", ii, ii->i_VCILevel, rpMsg->a_BeginMsg.bg_FreezeTs);
		if (rpMsg->rpa_Pkt.pk_TFlags & RPTF_READONLY)
		    PushCLTrans(cd, rpMsg->a_BeginMsg.bg_FreezeTs, CPF_READONLY);
		else
		    PushCLTrans(cd, rpMsg->a_BeginMsg.bg_FreezeTs, 0);
		++ii->i_VCILevel;
		break;
	    case RPCMD_ABORT_TRAN:
		if (ii->i_VCILevel) {
		    AbortCLTrans(cd);
		    --ii->i_VCILevel;
		    if (ii->i_VCILevel == 0) {
			cd->cd_ActiveMinCTs = 0;
			UpdateRouteMinCTs(parCd);
		    }
		} else {
		    respond = sizeof(RPMsg);	/* respond (w/ error) */
		    error = DBERR_ABORT_OUTSIDE_TRANS;
		}
		dbinfo("%p: ABORT(%d)\n", ii, ii->i_VCILevel);
		break;
	    case RPCMD_RUN_QUERY_TRAN:
		dbinfo("%p: RUNQUERY(%d): %s\n", ii, ii->i_VCILevel, rpMsg->rpa_Pkt.pk_Data);
		if (ii->i_VCILevel) {
		    error = VCSlaveQueryTrans(cd, rpMsg->rpa_Pkt.pk_Data, ii);
		    respond = sizeof(RPMsg);
		} else {
		    respond = sizeof(RPMsg);	/* respond (w/ error) */
		}
		break;
	    case RPCMD_REC_QUERY_TRAN:
		dbinfo("%p: RECQUERY(%d): %s\n", ii, ii->i_VCILevel, rpMsg->rpa_Pkt.pk_Data);
		if (ii->i_VCILevel) {
		    RecordQueryCLTrans(cd, rpMsg->rpa_Pkt.pk_Data, &error);
		} else {
		    respond = sizeof(RPMsg);	/* respond (w/ error) */
		}
		break;
	    case RPCMD_COMMIT1_TRAN:
		respond = sizeof(RPCommit1Msg);
		/* fall through */
	    case RPCMD_ICOMMIT1_TRAN:
		/*
		 * The returned MinCTs is set to the proposed commit timestamp
		 * on success, and the highest conflicting timestamp on 
		 * failure.  We must also manage cd_ActiveMinCTs which is 
		 * reported to other peers for synchronization purposes.  It
		 * may never be higher then the lowest pending (potential)
		 * commit timestamp.  To avoid a race during the
		 * Commit1CLTrans() call we temporarily force it
		 * to the current r_MinCTs, then set it to the real minCTs on
		 * return.  We preinitialize minCTs to r_MinCTs for the
		 * call to guarentee an allocated minCTs >= r_MinCTs.
		 */
		dbinfo("%p: COMMIT1(%d)\n", ii, ii->i_VCILevel);
		if (ii->i_VCILevel) {
		    RouteInfo *r = parCd->cd_Route;
		    dbstamp_t minCTs = r->r_MinCTs;

		    if (ii->i_VCILevel == 1 && r)
			cd->cd_ActiveMinCTs = r->r_MinCTs;
		    error = Commit1CLTrans(cd, &minCTs);
		    rpMsg->a_Commit1Msg.c1_MinCTs = minCTs;
		    if (error == 0 && ii->i_VCILevel == 1 && r) {
			cd->cd_ActiveMinCTs = minCTs;
			DBASSERT(minCTs >= r->r_MinCTs);
		    } else {
			cd->cd_ActiveMinCTs = 0;
			UpdateRouteMinCTs(parCd);
		    }
		} else {
		    /* error, not in transaction */
		}
		break;
	    case RPCMD_UNCOMMIT1_TRAN:
		/*
		 * A high level UNCOMMIT1 usually occurs inside the COMMIT1
		 * code during the deadlock loop, but since the lock detection
		 * code can race against a phase-2 commit (if the client gets
		 * a quorum despite the deadlock) it is possible for the
		 * client to issue an UNCOMMIT1_TRAN even if it's COMMIT1
		 * succeeds.
		 */
		dbinfo("%p: UNCOMMIT1(%d)\n", ii, ii->i_VCILevel);
		error = UnCommit1CLTrans(cd);
		respond = sizeof(RPMsg);
		if (ii->i_VCILevel == 1) {
		    cd->cd_ActiveMinCTs = 0;
		    UpdateRouteMinCTs(parCd);
		}
		break;
	    case RPCMD_COMMIT2_TRAN:
		/*
		 * note: c2_MincTs and c2_UserId are not returned
		 */
		respond = offsetof(RPCommit2Msg, c2_MinCTs);
		/* fall through */
	    case RPCMD_ICOMMIT2_TRAN:
		/*
		 * The actual commit timestamp we must use is passed in the
		 * commit phase 2 message, and we have to update the route
		 * we broadcast to other replicatoin hosts. 
		 */
		dbinfo("%p: COMMIT2(%d)\n", ii, ii->i_VCILevel);
		if (ii->i_VCILevel) {
		    dbstamp_t commitTs = rpMsg->a_Commit2Msg.c2_MinCTs;
		    --ii->i_VCILevel;
		    cd->cd_LLUserId = rpMsg->a_Commit2Msg.c2_UserId;
		    error = Commit2CLTrans(cd, commitTs);
		    if (error == 0 && ii->i_VCILevel == 0) {
			RouteInfo *r;
			if ((r = parCd->cd_Route) != NULL) {
			    /*
			     * note: UpdateRouteMinCTs() is called just below
			     */
			    if (r->r_SaveCTs <= commitTs)
				r->r_SaveCTs = commitTs + 1;
			}
		    }
		} else {
		    /* respond with error */
		    /* XXX client side not expecting a respond */
		}

		/*
		 * If at the top transaction level, clear this instance's
		 * limitiation on the reported MinCTs.  NOTE!  Once we 
		 * do this our newly committed record can be copied to
		 * other replicators.  To avoid data duplication the
		 * other replicators limit the copy to their own C1 
		 * MinCTs which will always be <= this phase-2 commit
		 * MinCTs.
		 */
		if (ii->i_VCILevel == 0) {
		    cd->cd_ActiveMinCTs = 0;
		    UpdateRouteMinCTs(parCd);
		}
		break;
	    case RPCMD_RAWREAD:
		/*
		 * Raw data read (usually issued without being in a 
		 * transaction).  Read all records between SyncTs (which
		 * doesn't really mean sync in this case) and MinCTs.
		 */
		error = VCSlaveRawRead(cd, &rpMsg->a_RawReadMsg, ii);
		respond = sizeof(RPRawReadMsg);
		break;
	    case RPCMD_CONTINUE:
	    case RPCMD_BREAK_QUERY:
		/* extranious flow control */
		error = 0;
		break;
	    default:
		respond = sizeof(RPMsg);	/* respond (w/ error) */
		break;
	    }
	    if (respond) {
		if (ii->i_Flags & IIF_FAILED) {
		    if ((ii->i_Flags & IIF_CLOSED) == 0) {
			ii->i_Flags |= IIF_CLOSED;
			rpMsg->rpa_Pkt.pk_Cmd = RPCMD_CLOSE_INSTANCE;
			ReturnVCPkt(ii, rpMsg, respond, error);
		    } else {
			FreeRPMsg(rpMsg);
		    }
		} else {
		    ReturnVCPkt(ii, rpMsg, respond, error);
		}
	    } else {
		FreeRPMsg(rpMsg);
	    }
	    if (error == DBERR_ABORT_OUTSIDE_TRANS) {
		taskSleep(4000);
		DBASSERT(0);
	    }
	}
	if (ii->i_VCILevel)
	    ii->i_VCILCmd |= 0x80;	/* just used for a status indicator */
	else
	    ii->i_VCILCmd = 0;
    }
    ii->i_VCILCmd = 0;

    /*
     * Cleanup
     */
    if (ii->i_VCILevel) {
	while (ii->i_VCILevel) {
	    --ii->i_VCILevel;
	    AbortCLTrans(cd);
	}
	cd->cd_ActiveMinCTs = 0;
	UpdateRouteMinCTs(parCd);
    }
}

/*
 * VCSlaveQueryTrans() -	Slave runs query returning responses
 *
 *	The run query code is complicated by two facts:
 *
 *	First, we need to know if we loose the link.  We shouldn't receive
 *	anything from remote for this instance except RPCMD_CONTINUE
 *	packets.  If we do we need to abort the selection by forwarding
 *	an abort request to the database.
 *
 *	Second, we need to check for RPCMD_CONTINUE (end-to-end flow control)
 *	packets and forward CLCMD_CONTINUE's to the database core.
 */

static int
VCSlaveQueryTrans(CLDataBase *cd, const char *qry, InstInfo *ii)
{
    CLAnyMsg *msg;
    int error = -1;
    int failed = 1;
    int rep_stallCount = 0;
    int drd_stallCount = 0;
    int i;

    msg = BuildCLMsgStr(CLCMD_RUN_QUERY_TRAN, qry);
    WriteCLMsg(cd->cd_Iow, msg, 1);

    /*
     * Retriieve data from the database core and send over the VC
     * to the remote replicator.
     */
    while ((msg = MReadCLMsg(cd->cd_Ior)) != NULL) {
	RPAnyMsg *rpMsg;

	if (msg->cma_Pkt.cp_Cmd == CLCMD_RUN_QUERY_TRAN) {
	    error = msg->cma_Pkt.cp_Error;
	    FreeCLMsg(msg);
	    failed = 0;
	    break;
	}

	DBASSERT((ii->i_Flags & IIF_IDLE) == 0);

	switch(msg->cma_Pkt.cp_Cmd) {
	case CLCMD_RESULT_ORDER:
	    rpMsg = BuildVCPkt(ii, RPCMD_RESULT_ORDER_TRAN|RPCMDF_REPLY, 
			offsetof(RPOrderMsg, om_Order[msg->a_OrderMsg.om_NumOrder]));
	    rpMsg->a_OrderMsg.om_NumOrder = msg->a_OrderMsg.om_NumOrder;
	    for (i = 0; i < msg->a_OrderMsg.om_NumOrder; ++i)
		rpMsg->a_OrderMsg.om_Order[i] = msg->a_OrderMsg.om_Order[i];
	    rpMsg->rpa_Pkt.pk_TFlags |= RPTF_NOPUSH;
	    WriteVCPkt(rpMsg);
	    break;
	case CLCMD_RESULT_LIMIT:
	    rpMsg = BuildVCPkt(ii, RPCMD_RESULT_LIMIT_TRAN|RPCMDF_REPLY, 
			sizeof(RPLimitMsg));
	    rpMsg->a_LimitMsg.lm_StartRow = msg->a_LimitMsg.lm_StartRow;
	    rpMsg->a_LimitMsg.lm_MaxRows = msg->a_LimitMsg.lm_MaxRows;
	    rpMsg->rpa_Pkt.pk_TFlags |= RPTF_NOPUSH;
	    WriteVCPkt(rpMsg);
	    break;
	case CLCMD_RESULT:
	    i = msg->a_RowMsg.rm_Offsets[msg->a_RowMsg.rm_Count];
	    rpMsg = BuildVCPkt(ii, RPCMD_RESULT_TRAN|RPCMDF_REPLY, 
			offsetof(RPMsg, rp_Pkt.pk_Data[i])
		    );
	    rpMsg->a_RowMsg.rm_ShowCount = msg->a_RowMsg.rm_ShowCount;
	    rpMsg->a_RowMsg.rm_Count = msg->a_RowMsg.rm_Count;
	    for (i = 0; i <= msg->a_RowMsg.rm_Count; ++i)
		rpMsg->a_RowMsg.rm_Offsets[i] = msg->a_RowMsg.rm_Offsets[i];
	    /* i = start offset of copy in packet data */
	    i = offsetof(CLRowMsg, rm_Offsets[msg->a_RowMsg.rm_Count+1]) - offsetof(CLRowMsg, rm_Msg.cm_Pkt.cp_Data[0]);
	    /* i = bytes to copy */
	    i = msg->a_RowMsg.rm_Offsets[msg->a_RowMsg.rm_Count] - i;
	    bcopy(
		&msg->a_RowMsg.rm_Offsets[msg->a_RowMsg.rm_Count+1],
		&rpMsg->a_RowMsg.rm_Offsets[msg->a_RowMsg.rm_Count+1],
		i
	    );

	    /*
	     * Queue the packet
	     */
	    rpMsg->rpa_Pkt.pk_TFlags |= RPTF_NOPUSH;
	    WriteVCPkt(rpMsg);

	    /*
	     * The stallcount is the size of the physical packet
	     */
	    rep_stallCount += rpMsg->rpa_Pkt.pk_Bytes;
	    drd_stallCount += msg->cma_Pkt.cp_Bytes;
	    break;
	default:
	    DBASSERT(0);
	    break;
	} 
	FreeCLMsg(msg);

	/*
	 * If we have sent CL_STALL_COUNT bytes to the remote replicator,
	 * wait for a RPCMD_CONTINUE (end-to-end flow control) before
	 * continuing.
	 */
	while (rep_stallCount > CL_STALL_COUNT) {
	    while ((rpMsg = remHead(ii->i_RXPktList)) != NULL) {
		/* XXX what if we get a CLOSE here? */
		switch(rpMsg->rpa_Pkt.pk_Cmd) {
		case RPCMD_CONTINUE:
		    rep_stallCount -= CL_STALL_COUNT / 2;
		    break;
		case RPCMD_BREAK_QUERY:
		    drd_stallCount = 0;
		    msg = BuildCLMsg(CLCMD_BREAK_QUERY, sizeof(CLMsg));
		    WriteCLMsg(cd->cd_Iow, msg, 1);
		    break;
		default:
		    DBASSERT(0);	/* XXX YYY HANDLE CLOSE!!! */
		}
		FreeRPMsg(rpMsg);
	    }

	    /*
	     * If the remote replicator still hasn't cleared enough
	     * out, wait until we get a response (another RPCMD_CONTINUE).
	     */
	    if (rep_stallCount > CL_STALL_COUNT) {
		ii->i_Flags |= IIF_STALLED;
		FlushLinkByInfo(ii);
		if (ii->i_Flags & IIF_FAILED)
		    break;
		waitNotify(ii->i_NotifyInt, 0);
		ii->i_Flags &= ~IIF_STALLED;
	    }
	}
	/* XXX IIF_FAILED */

	/*
	 * If we have received >= CL_STALL_COUNT / 2 bytes from our local
	 * database, send a CLCMD_CONTINUE
	 */
	if (drd_stallCount > CL_STALL_COUNT / 2) {
	    msg = BuildCLMsg(CLCMD_CONTINUE, sizeof(CLMsg));
	    WriteCLMsg(cd->cd_Iow, msg, 1);
	    drd_stallCount -= CL_STALL_COUNT / 2;
	}
    }
    if (failed) {
	ii->i_Flags |= IIF_FAILED;
    }
    return(error);
}

/*
 * VCSlaveRawRead() - issue raw read to slave
 */

static int
VCSlaveRawRead(CLDataBase *cd, RPRawReadMsg *rpMsg, InstInfo *ii)
{
    CLAnyMsg *msg;
    int error = -1;

    msg = BuildCLMsg(CLCMD_RAWREAD, sizeof(CLRawReadMsg));
    msg->a_RawReadMsg.rr_StartTs = rpMsg->rr_StartTs;
    msg->a_RawReadMsg.rr_EndTs = rpMsg->rr_EndTs;
    WriteCLMsg(cd->cd_Iow, msg, 1);

    while ((msg = MReadCLMsg(cd->cd_Ior)) != NULL) {
	RPAnyMsg *nrpMsg;
	int len;

	if (msg->cma_Pkt.cp_Cmd == CLCMD_RAWREAD) {
	    error = msg->cma_Pkt.cp_Error;
	    rpMsg->rr_StartTs = msg->a_RawReadMsg.rr_StartTs;
	    rpMsg->rr_EndTs = msg->a_RawReadMsg.rr_EndTs;
	    FreeCLMsg(msg);
	    break;
	}

	/*
	 * Note: we are passing the contents of CLRawDataFileMsg
	 * (e.g. like rdf_BlockSize) directly to the RPPkt.
	 */
	switch(msg->cma_Pkt.cp_Cmd) {
	case CLCMD_RAWDATAFILE:
	    len = strlen(msg->a_RawDataFileMsg.rdf_FileName) + 1;
	    nrpMsg = BuildVCPkt(ii, RPCMD_RAWDATAFILE|RPCMDF_REPLY, offsetof(RPRawDataFileMsg, rdf_FileName[len]));
	    nrpMsg->a_RawDataFileMsg.rdf_BlockSize = msg->a_RawDataFileMsg.rdf_BlockSize;
	    bcopy(msg->a_RawDataFileMsg.rdf_FileName, nrpMsg->a_RawDataFileMsg.rdf_FileName, len);
	    break;
	case CLCMD_RAWDATA:
	    len = msg->cma_Pkt.cp_Bytes - offsetof(CLPkt, cp_Data[0]);
	    nrpMsg = BuildVCPkt(ii, RPCMD_RAWDATA|RPCMDF_REPLY, offsetof(RPMsg, rp_Pkt.pk_Data[0]) + len);
	    bcopy(msg->cma_Pkt.cp_Data, nrpMsg->rpa_Pkt.pk_Data, len);
	    /*
	     * note: the caller of this procedure will eventually return
	     * a reply, flushing anything that hasn't been pushed out yet.
	     * We don't implement stall code here so we do not have to
	     * worry about it.
	     */
	    nrpMsg->rpa_Pkt.pk_TFlags |= RPTF_NOPUSH;
	    break;
	default:
	    nrpMsg = NULL;	/* avoid compiler warning */
	    DBASSERT(0);
	    break;
	}
	WriteVCPkt(nrpMsg);
	FreeCLMsg(msg);
    }
    return(error);
}

/*
 * UpdateRouteMinCTs() -	Update the MinCTs field in our database route
 *
 *	This field is used by other peers to determine when they must
 *	synchronize to our database.  The timestamp reported must guarentee
 *	that no *NEW* commits into our local database will be made prior to 
 *	the reported timestamp.  This does not count record updates retrieved
 *	via synchronization to other hosts.
 *
 *	We normally report the timestamp of the most recent commit to
 *	our database but may have to report a lower timestamp if any
 *	commits are pending.
 */

void
UpdateRouteMinCTs(CLDataBase *parCd)
{
    RouteInfo *r = parCd->cd_Route;
    CLDataBase *cd;
    dbstamp_t minCTs = r->r_SaveCTs;
    const char *from = "saveCTs";

    DBASSERT(r->r_MinCTs <= minCTs);

    for (
	cd = getHead(&parCd->cd_List);
	cd;
	cd = getListSucc(&parCd->cd_List, &cd->cd_Node)
    ) {
	if (cd->cd_ActiveMinCTs && cd->cd_ActiveMinCTs < minCTs) {
	    minCTs = cd->cd_ActiveMinCTs;
	    from = "activeMinCTs";
	}
    }

    if (r->r_MinCTs != minCTs) {
	dbinfo2("UPDATE ROUTE MINCTS %016qx FROM %s\n", minCTs, from);
	r->r_MinCTs = minCTs;
	NotifyAllLinks(r, r->r_Hop);
    }
}

