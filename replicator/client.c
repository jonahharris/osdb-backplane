/*
 * REPLICATOR/CLIENT.C	- Server side of the client/server SQL protocol
 *
 * (c)Copyright 2000-2002 Backplane, Inc.  Please refer to the COPYRIGHT
 * file at the base of the distribution tree.
 *
 * $Backplane: rdbms/replicator/client.c,v 1.116 2002/08/20 22:06:00 dillon Exp $
 *
 *	This code provides replicative support to the connecting client.
 */

#include "defs.h"

Prototype void MainClientMsgAcceptor(DBManageNode *dbm);
Prototype void StartClientThreadIdleClose(InstInfo *ii);
Prototype void RouteCheckRemoteClosingDB(ClientInfo *cinfo);
Prototype List ClientInfoList;

#define RET_PASSIVE	0x0001
#define RET_ACTIVE	0x0002
#define RET_RUN		0x0004
#define RET_CABORT	0x0008	/* Special commit abort operation */

#if 0
#define CLIENT_DEBUG	1
#endif

List ClientInfoList = INITLIST(ClientInfoList);

static void ClientThreadReplicated(CLAnyMsg *msg);
static void ClientThreadRepMaster(CLDataBase *cd);
static void clientRepNotified(ClientInfo *cinfo);
static void ClientThreadRepMasterIdleClose(ClientInfo *ci);
static void RouteUpdateAllInstances(ClientInfo *cinfo, DBInfo *db);
static void TryToOpenHostInstance(ClientInfo *cinfo, RouteHostInfo *rh, int rType);
static void CloseAllInstances(ClientInfo *cinfo, int forceClose);
/*static void RepClientCheckHello(ClientInfo *cinfo);*/
static void RepClientStartCLCmd(ClientInfo *cinfo, CLAnyMsg *msg);
static void RepClientExecuteCLCmd(ClientInfo *cinfo, InstInfo *ii);
static void RepClientInstancePkt(ClientInfo *cinfo, RPAnyMsg *rpMsg);
static void RepClientRestart(ClientInfo *cinfo, InstInfo *ii, int isCounted);
static void RepClientReturn(ClientInfo *cinfo, InstInfo *ii, int flags, int error);
static void RepClientDereferenceAll(ClientInfo *cinfo, InstInfo *ii);
static void RepRemoveUnneededMessages(ClientInfo *cinfo, CLAnyMsg *msg, CLAnyMsg *endMsg);

static int bhc(DBInfo *db, HostInfo *h);
static RouteHostInfo *selectBetterRoute(ClientInfo *cinfo, RouteHostInfo *bestRh, RouteHostInfo *rh, RouteInfo **pbestR, RouteInfo *r);

/*
 * IIFClear() - clear instance flags and keep track of aggregate status
 */
static __inline void
IIFClear(ClientInfo *cinfo, InstInfo *ii, int flags)
{
    if (flags & IIF_READY) {
	if ((ii->i_Flags & (IIF_READY|IIF_IDLE)) == IIF_READY)
	    --cinfo->ci_Ready;
    }
    if (flags & IIF_CLOSED) {
	if ((ii->i_Flags & (IIF_CLOSED|IIF_IDLE)) == IIF_CLOSED)
	    --cinfo->ci_Closed;
    }
    ii->i_Flags &= ~flags;
}

/*
 * IIFSet() - set instance flags and keep track of aggregate status
 */
static __inline void
IIFSet(ClientInfo *cinfo, InstInfo *ii, int flags)
{
    if (flags & IIF_READY) {
	if ((ii->i_Flags & (IIF_READY|IIF_IDLE)) == 0)
	    ++cinfo->ci_Ready;
    }
    if (flags & IIF_CLOSED) {
	if ((ii->i_Flags & (IIF_CLOSED|IIF_IDLE)) == 0)
	    ++cinfo->ci_Closed;
    }
    ii->i_Flags |= flags;
}

/*
 * MainClientMsgAcceptor()
 *
 *	We are a slaved replicator for a particular database.  Retrieve
 *	descriptors and initial hello messages from the master replicator
 *	via the UDOM socket it exec'd us with.  Start a thread for each
 *	connection.
 */

void
MainClientMsgAcceptor(DBManageNode *dbm)
{
    /*
     * Accept connections (descriptors) from client
     */
    for (;;) {
	CLAnyMsg *msg;
	iofd_t passedio = NULL;

	msg = RecvCLMsg(dbm->dm_ClientIo, &passedio);
	if (msg == NULL)
	    break;
	if (passedio) {
	    msg->a_Msg.cm_AuxInfo = passedio;
	    taskCreate(ClientThreadReplicated, msg);
	} else {
	    FreeCLMsg(msg);
	}
    }

    /*
     * The master replicator process went away, the best thing to do
     * here is exit
     */
    exit(1);
}

/*
 * StartClientThreadIdleClose() - start a thread to close the specified
 *				  instance.
 *
 *    This occurs when a remote database is trying to shutdown.  It 
 *    will stop advertising its database route which will trigger us
 *    to close any idle instances.  The remote will (normally) wait
 *    until all instances have been closed.
 *
 *    The caller will unlink the instance from any list and clear
 *    IIF_IDLE (if it was set), before calling us.
 */
void 
StartClientThreadIdleClose(InstInfo *ii)
{
    ClientInfo *cinfo = zalloc(sizeof(ClientInfo));

    initList(&cinfo->ci_List);
    initList(&cinfo->ci_CLMsgList);
    cinfo->ci_Notify = allocNotify();
    addTail(&ii->i_RHSlave->rh_DBInfo->d_CIList, &cinfo->ci_Node);

    ii->i_RXPktList = &cinfo->ci_List;
    ii->i_NotifyInt = cinfo->ci_Notify;
    ii->i_Next = cinfo->ci_IIBase;
    cinfo->ci_IIBase = ii;
    cinfo->ci_Count = 1;
    cinfo->ci_NumPeers = 1;
    if (ii->i_Type == CTYPE_SNAP)
	cinfo->ci_Snaps = 1;

    taskCreate(ClientThreadRepMasterIdleClose, cinfo);
}

/*
 * This implements the client interface with replication.  This is the
 * default.
 *
 * XXX these should  be hello messages YYY
 */

static void
ClientThreadReplicated(CLAnyMsg *msg)
{
    CLDataBase *cd = AllocCLDataBase(NULL, msg->a_Msg.cm_AuxInfo);
    DBInfo *db;
    int error;

    /*
     * The client hello was passed to us.  Cleanup AuxInfo and deal with
     * it.  The database name may not be empty.
     */
    msg->a_Msg.cm_AuxInfo = NULL;

    if (ValidCLMsgHelloWithDB(&msg->a_HelloMsg, 1) < 0) {
	if (msg)
	    FreeCLMsg(msg);
	CloseCLDataBase(cd);
	return;
    }

    /*
     * Locate the DBInfo record, which contains the aggregate state of
     * the database for the entire spanning tree (including any local
     * copies).
     */

    if ((db = FindDBInfo(msg->a_HelloMsg.hm_DBName, 1, &error)) == NULL ||
	db->d_Refs == 0
    ) {
	if (db)
	    DoneDBInfo(db);
	FreeCLMsg(msg);
	if (error == FDBERR_DUPLICATE)
	    msg = BuildCLHelloMsgStr("Cannot Connect, Unassociated Duplicate Databases found in replication graph");
	else
	    msg = BuildCLHelloMsgStr("Database not found");
	msg->cma_Pkt.cp_Error = -1;
	WriteCLMsg(cd->cd_Iow, msg, 1);
	CloseCLDataBase(cd);
	return;
    }
    FreeCLMsg(msg);

    /*
     * Respond with a HELLO packet containing our hostname.  This is not
     * an instance open so don't bother XXX returning the sync point.
     * XXX because we don't have it at this point anyway.
     */

    msg = BuildCLHelloMsgStr(MyHost.h_HostName);
    WriteCLMsg(cd->cd_Iow, msg, 1);

    dbinfo2("Starting Client Link (%s)\n", cd->cd_DBName);

    /*
     * The only command we recognize is CLCMD_OPEN_INSTANCE.  
     */
    while ((msg = MReadCLMsg(cd->cd_Ior)) != NULL) {
	iofd_t iofd;
	CLDataBase *dcd;
	int fds[2];

	if (msg->cma_Pkt.cp_Cmd != CLCMD_OPEN_INSTANCE) {
	    dberror("Illegal client packet received %d\n", msg->cma_Pkt.cp_Cmd);
	    break;
	}

	/*
	 * Create descriptor pair to hold instance, assigning one to a new
	 * thread called ClientThreadRepMaster, and returning the other to the
	 * requesting client.
	 */

	if (socketpair(PF_LOCAL, SOCK_STREAM, 0, fds) < 0) {
	    dberror("socketpair failed %s\n", strerror(errno));
	    break;
	}
	dcd = AllocCLDataBase(NULL, allocIo(fds[0]));
	dcd->cd_DBInfo = db;
	UseDBInfo(db);
	iofd = allocIo(fds[1]);
	SendCLMsg(cd->cd_Iow, msg, iofd);
	closeIo(iofd);
	taskCreate(ClientThreadRepMaster, dcd);
	taskQuantum();
    }
    if (msg)
	FreeCLMsg(msg);
    dbinfo2("stopping Client Link (%s)\n", cd->cd_DBName);
    DoneDBInfo(db);
    CloseCLDataBase(cd);
}

/*
 * ClientThreadRepMaster() -	Accept client commands (CLMsg packets) and
 *				execute them in the replicated environment.
 *
 *	This thread is responsible for distributing SQL requests from 
 *	the client to the available replicated databases.  Nothing happens
 *	until we push the first transaction level.
 */
static void
ClientThreadRepMaster(CLDataBase *cd)
{
    ClientInfo cinfo;

    bzero(&cinfo, sizeof(cinfo));
    initList(&cinfo.ci_List);
    initList(&cinfo.ci_CLMsgList);
    cinfo.ci_Notify = allocNotify();
    cinfo.ci_NumPeers = 1;
    cinfo.ci_Cd = cd;

    /*
     * Send our HELLO response now, returning the highest available 
     *
     * XXX support queries to snapshot hosts or nearest snapshot host when 
     * in read-only mode (but snapshot hosts cannot count as part of the
     * quorum). XXX
     */
    {
	CLAnyMsg *msg;
	RouteHostInfo *rh;

	msg = BuildCLHelloMsgStr("");
	msg->a_HelloMsg.hm_SyncTs = 0;
	msg->a_HelloMsg.hm_MinCTs = DBSTAMP_MAX;	/* unused */

	/*
	 * Figure out SyncTs (best ts for read-only trans), and calculate 
	 * the count for the quorum test.  Only PEER databases coming
	 * from PEER hosts count for the quorum test.
	 */
	for (rh = cd->cd_DBInfo->d_RHInfoBase; rh; rh = rh->rh_DNext) {
	    RouteInfo *r;

	    DBASSERT(rh->rh_Host->h_Type == CTYPE_REPLIC);

	    for (r = rh->rh_Routes; r; r = r->r_RHNext) {
		if (r->r_Refs == 0)
		    continue;
		if (cinfo.ci_NumPeers < r->r_RepCount)	/* total for quorum */
		    cinfo.ci_NumPeers = r->r_RepCount;
							/* best sync stamp */
		if (msg->a_HelloMsg.hm_SyncTs < r->r_SyncTs)
		    msg->a_HelloMsg.hm_SyncTs = r->r_SyncTs;
	    }
	}

	WriteCLMsg(cd->cd_Iow, msg, 1);
    }

    /*
     * Starting state is to wait for a command.
     */
    cinfo.ci_Flags |= CIF_ACCEPTOK;

    /*
     * Use the CD's cd_Node structure to associate it with the DBInfo, which
     * will allow routing updates effecting the DBInfo to notify this cd. 
     */
    addTail(&cd->cd_DBInfo->d_CIList, &cinfo.ci_Node);

    /*
     * Command processing loop.  There are three event sources:
     *
     *	CIF_CHECKNEW	- new routes, timestamp updates, link state changes
     *			  (cinfo.ci_Notify)
     *	CIF_ACCEPTOK	- Means we can safely accept a new command
     *			  (but not necessarily that a command is avialable)
     *	cinfo.ci_List	- Incoming responses (cinfo.ci_Notify)
     */
    for (;;) {
	/*
	 * If we are ready to accept a new command from the client, do so.
	 * Note that we may process return packets while we are blocked
	 * waiting for the new command.
	 */

	if (cinfo.ci_Flags & CIF_ACCEPTOK) {
	    CLAnyMsg *msg;

	    setNotifyDispatch(cinfo.ci_Notify, clientRepNotified, &cinfo, 1);
	    msg = MReadCLMsg(cd->cd_Ior);
	    setNotifyDispatch(cinfo.ci_Notify, NULL, NULL, 0);

	    if (msg == NULL)
		break;
	    cinfo.ci_Flags &= ~CIF_ACCEPTOK;
	    RepClientStartCLCmd(&cinfo, msg);
	} else {
	    waitNotify(cinfo.ci_Notify, 0);
	}
	clientRepNotified(&cinfo);
	taskQuantum();
    }

    /*
     * Cleanup
     */
    removeNode(&cinfo.ci_Node);
    CloseAllInstances(&cinfo, 0);
    freeNotify(cinfo.ci_Notify);
    DBASSERT(cinfo.ci_List.li_Node.no_Next == &cinfo.ci_List.li_Node);
    DBASSERT(cd->cd_DBInfo != NULL);
    DoneDBInfo(cd->cd_DBInfo);
    cd->cd_DBInfo = NULL;
    CloseCLDataBase(cd);
}

/*
 * Handle route, timestamp, link state, and reply packet events
 */
void
clientRepNotified(ClientInfo *cinfo)
{
    RPAnyMsg *rpMsg;
    CLDataBase *cd = cinfo->ci_Cd;

    dbinfo2("clientRepNotified\n");

    /*
     * Deal with route, timestamp, and link state changes.  We only
     * bother when there are client commands in-process.  This also
     * gives us a nice optimization in that if the next transaction
     * happens to be read-only we need only open a single instance, not
     * one to each peer.
     *
     * XXX we need to close or idle-out open instances from a prior 
     * transaction that are not useable for the current transaction
     * (e.g. readwrite->readonly).
     */
    if ((cinfo->ci_Flags & CIF_CHECKNEW) && getHead(&cinfo->ci_CLMsgList)) {
	cinfo->ci_Flags &= ~CIF_CHECKNEW;
	RouteUpdateAllInstances(cinfo, cd->cd_DBInfo);
    }

    /*
     * Process any RPCMD packets received for the instances under
     * our management.
     */
    while ((rpMsg = remHead(&cinfo->ci_List)) != NULL) {
	RepClientInstancePkt(cinfo, rpMsg);
    }
}

/*
 * ClientThreadRepMasterIdleClose()
 */

static void
ClientThreadRepMasterIdleClose(ClientInfo *ci)
{
    /*
     * Close the linked-in IDLE instances
     */
    dbinfo2("CINFO %p *************** START CLOSE IDLE \n", ci);
    removeNode(&ci->ci_Node);
    CloseAllInstances(ci, 1);
    dbinfo2("CINFO %p *************** END CLOSE IDLE \n", ci);

    /*
     * Cleanup
     */
    freeNotify(ci->ci_Notify);
    DBASSERT(ci->ci_List.li_Node.no_Next == &ci->ci_List.li_Node);
    zfree(ci, sizeof(ClientInfo));
}

/*
 * RouteUpdateAllInstances()
 *
 *	Scan available routes looking for hosts to open or close.  Only
 *	active routes whos databases have achieved the minimum synchronization
 *	time stamp will be opened, and open databases that have not achieved
 *	the minCTs will be taken out of the quorum.
 *
 *	Read-only transactions require only one active database.  Read-write
 *	transactions require a quorum.  If we are not able to open a
 *	sufficient number of instances (due to the freeze point not being
 *	achieved yet), the state machine will stall until the system has
 *	had a chance to catch up.
 *
 *	We optimize read-only queries by opening up only one host, but
 *	due to chasing timestamps we can end up in a situation where we
 *	have turned off a host with pending transactions and must turn it
 *	back on.
 */

static void
RouteUpdateAllInstances(ClientInfo *cinfo, DBInfo *db)
{
    RouteHostInfo *rh;
    RouteHostInfo *bestRh = NULL;
    RouteInfo *bestR = NULL;

    /*
     * Scan available routes for this database.  Routes will be grouped
     * by host.
     */
    for (rh = db->d_RHInfoBase; rh; rh = rh->rh_DNext) {
	RouteInfo *r;
	int openThisHost = 0;

	DBASSERT(rh->rh_Host->h_Type == CTYPE_REPLIC);

	for (r = rh->rh_Routes; r; r = r->r_RHNext) {
	    if (r->r_Refs == 0)
		continue;

	    /*
	     * Allow only PEER and SNAPshot hosts.
	     *
	     * Only use information from peers to update the replication count
	     * (though snapshots should have reasonable data here).
	     *
	     * Only use snapshot hosts with a hop of 0 (local to machine),
	     * or 1 (nearby) - this is a bad optimization that needs to be
	     * fixed XXX.
	     */
	    switch(r->r_Type) {
	    case CTYPE_PEER:
		if (cinfo->ci_NumPeers < r->r_RepCount)
		    cinfo->ci_NumPeers = r->r_RepCount;
		break;
	    case CTYPE_SNAP:
		break;
	    default:
		continue;
	    }

	    /*
	     * Routes are grouped by host.  
	     */
	    if (r->r_SyncTs >= cinfo->ci_MinSyncTs) {
		if ((cinfo->ci_Flags & CIF_READONLY) == 0)
		    openThisHost = 1;
		else
		    bestRh = selectBetterRoute(cinfo, bestRh, rh, &bestR, r);
	    }
	}
	if (openThisHost)
	    TryToOpenHostInstance(cinfo, rh, rh->rh_Routes->r_Type);
    }
    if (bestRh)
	TryToOpenHostInstance(cinfo, bestRh, bestRh->rh_Routes->r_Type);

    /*
     * Look for things to unblock due to sync stamp changes (YUCH).
     *
     * Look for things to close due to the remote attempting to close
     * a database.
     *
     * Check for peerage typing changes (SNAP->PEER, PEER->SNAP) XXX
     */
    {
	InstInfo *ii;

	for (ii = cinfo->ci_IIBase; ii; ii = ii->i_Next) {
	    RouteInfo *r;
	    int trySyncWait = 0;

	    rh = ii->i_RHSlave;

	    /*
	     * Test all available routes, not just the first one that
	     * triggers a syncwait retry.
	     */
	    for (r = rh->rh_Routes; r; r = r->r_RHNext) {
		if (r->r_Refs == 0)
		    continue;
		if (ii->i_SyncTs < r->r_SyncTs) {
		    ii->i_SyncTs = r->r_SyncTs;
		    trySyncWait = 1;
		}
	    }
	    if (trySyncWait && (ii->i_Flags & IIF_SYNCWAIT)) {
		DBASSERT(ii->i_CLMsg != NULL);
		DBASSERT(ii->i_CLMsg->cma_Pkt.cp_Cmd == CLCMD_BEGIN_TRAN);
		RepClientExecuteCLCmd(cinfo, ii);
	    }
	}
    }

    if (cinfo->ci_NumPeers < cinfo->ci_Count - cinfo->ci_Snaps)
	cinfo->ci_NumPeers = cinfo->ci_Count - cinfo->ci_Snaps;
}

void
RouteCheckRemoteClosingDB(ClientInfo *cinfo)
{
    InstInfo *ii;

    for (ii = cinfo->ci_IIBase; ii; ii = ii->i_Next) {
	RouteInfo *r;
	RouteHostInfo *rh;
	int goodRoute = 0;

	rh = ii->i_RHSlave;

	for (r = rh->rh_Routes; r; r = r->r_RHNext) {
	    if (r->r_Refs == 0)
		continue;
	    goodRoute = 1;
	    break;
	}
	if (goodRoute == 0 &&
	    ii->i_CLMsg == NULL &&
	    cinfo->ci_Level == 0 &&
	    (ii->i_Flags & (IIF_READY|IIF_FAILED|IIF_CLOSED|IIF_CLOSING)) == IIF_READY
	) {
	    ii->i_Flags |= IIF_CLOSING;
	    dbinfo3("TRANSMIT INST %p CLOSE COMMAND %02x\n", ii, RPCMD_CLOSE_INSTANCE);
	    WriteVCPkt(BuildVCPkt(ii, RPCMD_CLOSE_INSTANCE, sizeof(RPMsg)));
	    IIFClear(cinfo, ii, IIF_READY);
	}
    }
}

/*
 * selectBetterRoute()	- Choose the best route to a database
 *
 *	bestRh and *pbestR may initially be NULL
 */
static RouteHostInfo *
selectBetterRoute(ClientInfo *cinfo, RouteHostInfo *bestRh, RouteHostInfo *rh, RouteInfo **pbestR, RouteInfo *r)
{
    RouteInfo *bestR = *pbestR;

    if (bestR == NULL) {
	/*
	 * Beggar's can't be choosers
	 */
	*pbestR = r;
	bestRh = rh;
    } else if (bestR->r_SyncTs < cinfo->ci_MinSyncTs &&
	r->r_SyncTs >= cinfo->ci_MinSyncTs
    ) {
	/*
	 * A database able to handle our request is
	 * better.
	 */
	*pbestR = r;
	bestRh = rh;
    } else if (bestR->r_SyncTs >= cinfo->ci_MinSyncTs &&
	bestR->r_Hop > r->r_Hop
    ) {
	/*
	 * Closer is better
	 */
	*pbestR = r;
	bestRh = rh;
    }
    return(bestRh);
}

static void
TryToOpenHostInstance(ClientInfo *cinfo, RouteHostInfo *rh, int rType)
{
    HostInfo *h = rh->rh_Host;
    InstInfo *ii = NULL;
    InstInfo *mi;
    RPAnyMsg *rpMsg;
    static int IId = 1;

    /*
     * Check to see if an instance to this host is already opened (whether
     * ready or not).  If we find an instance marked CLOSED, we re-open it
     * later on.
     *
     * Special case:  we also have to compare i_Type, because a replication
     * source may change from a SNAP to a PEER or vise versa at any time.
     * This might result in the same db@host opened as both a snapshot
     * and a peer, which is ok.
     */
    for (ii = cinfo->ci_IIBase; ii; ii = ii->i_Next) {
	if (ii->i_SlaveHost == h && ii->i_Type == rType) {
	    if (ii->i_Flags & IIF_CLOSED)
		break;
	    return;
	    /* NOT REACHED */
	}
    }

    dbinfo2("CINFO %p ---> OPEN %s@%s\n", cinfo, rh->rh_DBInfo->d_DBName, h->h_HostName);

    /*
     * If we could not find an instance at all, we must create a new one.
     * But first try to reuse an idle instance for this host.  Note
     * that the i_Type could change out from under the idle instance,
     * so adjust it.
     */
    if (ii == NULL) {
	if ((ii = rh->rh_IdleInstBase) != NULL) {
	    DBASSERT(ii->i_Flags & IIF_IDLE);
	    rh->rh_IdleInstBase = ii->i_Next;
	    ii->i_Flags &= ~IIF_IDLE;
	    ii->i_RXPktList = &cinfo->ci_List;
	    ii->i_NotifyInt = cinfo->ci_Notify;
	    ii->i_Next = cinfo->ci_IIBase;
	    ii->i_Type = rType;
	    cinfo->ci_IIBase = ii;
	    ++cinfo->ci_Count;
	    if (ii->i_Type == CTYPE_SNAP)
		++cinfo->ci_Snaps;

	    /*
	     * When reactivating an idle instance, we must restart 
	     * commands on the instance.
	     */
	    if (ii->i_Flags & IIF_READY) {
		++cinfo->ci_Ready;
		RepClientRestart(cinfo, ii, 0);
	    }
	    return;
	}
    }

    /*
     * Kick the VC to start the VC manager if necessary
     */
    mi = VCManagerMasterKick(&MyHost, h);
    if (mi->i_Flags & IIF_FAILED)
	return;

    if (ii == NULL) {
	/*
	 * We need to create a new instance
	 */
	while ((ii = FindInstInfo(&MyHost, h, IIF_MASTER, IId, mi->i_VCId)) != NULL) {
	    if ((IId = (IId + 1) & 0x7FFFFFFF) == 0)
		IId = 1;
	}
	ii = AllocInstInfo(mi, &MyHost,
		rh->rh_Host, rh->rh_DBInfo, IIF_MASTER, IId);
	ii->i_RXPktList = &cinfo->ci_List;
	ii->i_NotifyInt = cinfo->ci_Notify;
	ii->i_Next = cinfo->ci_IIBase;
	ii->i_Type = rType;
	cinfo->ci_IIBase = ii;
	/*
	 * Bump the open count.
	 */
	++cinfo->ci_Count;
	if (ii->i_Type == CTYPE_SNAP)
	    ++cinfo->ci_Snaps;
    } else {
	/*
	 * We need to reopen a previously closed instance.  Note: we have
	 * to adjust the i_Type as it may have changed since the instance
	 * was closed.
	 */
	DBASSERT((ii->i_Flags & IIF_CLOSED) != 0);
	ReallocInstInfo(ii);
	IIFClear(cinfo, ii, IIF_CLOSED);
	dbinfo2("CINFO %p INST %p RESTART INSTANCE %d\n", cinfo, ii, ii->i_InstId);
	ii->i_Type = rType;
    }

    {
	int len = strlen(rh->rh_DBInfo->d_DBName) + 1;

	rpMsg = BuildVCPkt(ii, RPCMD_OPEN_INSTANCE, offsetof(RPOpenInstanceMsg, oi_DBName[len]));
	bcopy(rh->rh_DBInfo->d_DBName, rpMsg->a_OpenInstanceMsg.oi_DBName, len);
    }
    dbinfo2(
	"CINFO %p INST %p ************* TRANSMITTING OPEN INSTANCE VC FOR %04x:%04x\n",
	cinfo, ii,
	ii->i_VCId, ii->i_InstId
    );
    WriteVCPkt(rpMsg);

    /*
     * Requeue the command list to the instance at the appropriate place.
     * Since the instance is not ready, it will not actually run yet.
     */
    RepClientRestart(cinfo, ii, 0);
    DBASSERT((ii->i_Flags & IIF_READY) == 0);
    return;
}

/*
 * bhc() - best hop count to host
 */
static int
bhc(DBInfo *db, HostInfo *h)
{
    RouteHostInfo *rh;
    int hop = RP_MAXHOPS;

    for (rh = db->d_RHInfoBase; rh; rh = rh->rh_DNext) {
	RouteInfo *r;

	if (rh->rh_Host != h)
	    continue;
	for (r = rh->rh_Routes; r; r = r->r_RHNext) {
	    if (r->r_Refs == 0)
		continue;
	    if (hop > r->r_Hop)
		hop = r->r_Hop;
	}
    }
    return(hop);
}

/*
 * RepClientStartCLCmd() - issue CL Cmd.
 *
 *	The CLMsg is queued to all existing instances.  We directly execute
 *	it on any ready instances.
 */

static void
RepClientStartCLCmd(ClientInfo *cinfo, CLAnyMsg *msg)
{
    InstInfo *ii;
    InstInfo *bestII = NULL;

    DBASSERT(msg->a_Msg.cm_Refs > 0);

    /*
     * Special immediate commands
     */
    switch(msg->cma_Pkt.cp_Cmd) {
    case CLCMD_SYNC_STAMP:
	{
	    DBInfo *db = cinfo->ci_Cd->cd_DBInfo;

	    msg->cma_Pkt.cp_Error = 0;
	    if (msg->a_StampMsg.ts_Stamp < db->d_SyncTs) {
		msg->a_StampMsg.ts_Stamp = db->d_SyncTs;
		msg->cma_Pkt.cp_Error = 1;
	    }
	    WriteCLMsg(cinfo->ci_Cd->cd_Iow, msg, 1);
	}
	cinfo->ci_Flags |= CIF_ACCEPTOK;
	return;
    default:
	break;
    }

    /*
     * Enqueue.  Certain commands must be special-cased
     */
    addTail(&cinfo->ci_CLMsgList, &msg->a_Msg.cm_Node);
    msg->a_Msg.cm_QCount = cinfo->ci_Count - cinfo->ci_Closed;

    dbinfo3("CINFO %p ENQUEUE NEW MESSAGE %p %02x\n", 
	cinfo, msg, msg->cma_Pkt.cp_Cmd);

    switch(msg->cma_Pkt.cp_Cmd) {
    case CLCMD_BEGIN_TRAN:
	/*
	 * When beginning a new top-level transaction we may still have
	 * tag-ends left over from the previous transaction.  Due to the
	 * freeze timestamp changing we may have to temporarily avoid 
	 * some of our already-open instances.  This is in addition to 
	 * opening new any newly available instances.
	 */
	if (cinfo->ci_StreamLevel || (msg->cma_Pkt.cp_Flags & CPF_STREAM))
	    ++cinfo->ci_StreamLevel;
	if (cinfo->ci_Level++ == 0) {
	    DBASSERT((cinfo->ci_Flags & CIF_READONLY) == 0);
	    cinfo->ci_MinSyncTs = msg->a_BeginMsg.bg_FreezeTs;
	    cinfo->ci_Flags |= CIF_CHECKNEW;
	    cinfo->ci_Flags &= ~CIF_ACCEPTOK;	/* wait for quorum hellos */
	    if (msg->cma_Pkt.cp_Flags & CPF_READONLY) {
		cinfo->ci_Flags |= CIF_READONLY;
		msg->cma_Pkt.cp_Flags |= CPF_TOPREADONLY;
	    }
	    if (msg->cma_Pkt.cp_Flags & CPF_RWSYNC)
		cinfo->ci_Flags |= CIF_RWSYNC;

	    /*
	     * If requested, use the most recent synchronization timestamp
	     * we know about, from any replication source.
	     */
	    if (cinfo->ci_Flags & CIF_RWSYNC) {
		DBInfo *db = cinfo->ci_Cd->cd_DBInfo;

		if (cinfo->ci_MinSyncTs < db->d_SyncTs) {
		    cinfo->ci_MinSyncTs = db->d_SyncTs;
		    msg->a_BeginMsg.bg_FreezeTs = db->d_SyncTs;
		}
	    }
	} else if (cinfo->ci_Flags & CIF_READONLY) {
	    msg->cma_Pkt.cp_Flags |= CPF_TOPREADONLY;
	}
	break;
    case CLCMD_RUN_QUERY_TRAN:
	/*
	 * Select the best Instance Info to run the command on (may be
	 * cached in cinfo).  NOTE: assumes InstInfo's are not Free'd during
	 * the transaction.
	 *
	 * Generally choose the instance with the best hop count, skipping
	 * CLOSED instances, and do our best to skip stalled instances.
	 */
	if ((bestII = cinfo->ci_BestII) == NULL ||
	    (bestII->i_Flags & IIF_CLOSED)
	) {
	    DBInfo *db = cinfo->ci_Cd->cd_DBInfo;

	    bestII = NULL;
	    for (ii = cinfo->ci_IIBase; ii; ii = ii->i_Next) {
		int bbhc;

		if (ii->i_Flags & IIF_CLOSED)
		    continue;
		if (bestII == NULL) {
		    bestII = ii;
		    continue;
		}
		bbhc = (bhc(db,bestII->i_SlaveHost) > bhc(db,ii->i_SlaveHost));
		if ((bestII->i_Flags & IIF_SYNCWAIT) == 0) {
		    if ((ii->i_Flags & IIF_SYNCWAIT) == 0 && bbhc)
			bestII = ii;
		} else if (bbhc) {
		    bestII = ii;
		}
	    }
	    cinfo->ci_BestII = bestII;
	}
	break;
    case CLCMD_COMMIT1_TRAN:
	/*
	 * Setup for commit1 processing.  Initialize the good and bad
	 * MinCTs return responses for aggregation.  Note that commit1
	 * is at the current level.
	 */
	DBASSERT(cinfo->ci_Level != 0);
	cinfo->ci_Flags |= CIF_COMMIT1;
	cinfo->ci_MinCTsGood = msg->a_Commit1Msg.c1_MinCTs - 1;
	cinfo->ci_MinCTsBad = msg->a_Commit1Msg.c1_MinCTs - 1;
	break;
    case CLCMD_COMMIT2_TRAN:	/* must be in commit1 */
	/*
	 * note that commit2 drops through so the cm_Level is
	 * the level we are entering (current level - 1)
	 */
	DBASSERT(cinfo->ci_Flags & CIF_COMMIT1);
	/* fall through */
    case CLCMD_ABORT_TRAN:	/* may be in commit1 */
	/*
	 * note that the cm_Level is set to the level we are 
	 * entering (current level - 1)
	 */
	DBASSERT(cinfo->ci_Level != 0);
	cinfo->ci_Flags &= ~CIF_COMMIT1;
	if (--cinfo->ci_Level == 0) {
	    cinfo->ci_Flags &= ~(CIF_READONLY | CIF_RWSYNC);
	}
	if (cinfo->ci_StreamLevel)
	    --cinfo->ci_StreamLevel;
	break;
    }

    /* note: cm_level represented after any adjustments */
    msg->a_Msg.cm_Level = cinfo->ci_Level;	

    /*
     * Initiate on ready instances which are not already processing some
     * previous CL message.  If bestII is non-NULL, initiate the command
     * it bestII first.
     *
     * Pre-bump cm_AtQueue to prevent a premature freeing since 
     * RepClientExecuteCLCmd() may manipulate the message.
     */
    ++msg->a_Msg.cm_Refs;
    for (ii = cinfo->ci_IIBase; ii; ii = ii->i_Next) {
	if ((ii->i_Flags & IIF_CLOSED) == 0 && ii->i_CLMsg == NULL) {
	    ii->i_CLMsg = msg;
	    ++msg->a_Msg.cm_AtQueue;
	}
    }

    /*
     * Now execute as appropriate
     */
    if (bestII && bestII->i_CLMsg == msg && (bestII->i_Flags & IIF_READY)) {
	RepClientExecuteCLCmd(cinfo, bestII);
    }
    for (ii = cinfo->ci_IIBase; ii; ii = ii->i_Next) {
	if (ii != bestII && ii->i_CLMsg == msg && (ii->i_Flags & IIF_READY))
	    RepClientExecuteCLCmd(cinfo, ii);
    }
    FreeCLMsg(msg);
}

/*
 * RepClientExecuteCLCmd() -	Next command loaded, execute
 *
 *	Note that the command, i_CLMsg, remains loaded until the operation 
 *	is complete, so this routine cannot be called gratuitously or it
 *	might re-execute a command that has already been initiated.
 */

static void
RepClientExecuteCLCmd(ClientInfo *cinfo, InstInfo *ii)
{
    CLAnyMsg *msg;
    int isacked;

again:
    isacked = 0;

    while (isacked == 0 && (msg = ii->i_CLMsg) != NULL) {
	int ackmode = RET_ACTIVE;
	RPAnyMsg *rpMsg = NULL;
	dbstamp_t ts;

	dbinfo3("CINFO %p INST %p CLCMSG %s:%04x %02x msg=%p\n",
	    cinfo, ii, ii->i_SlaveHost->h_HostName, ii->i_InstId,
	    msg->cma_Pkt.cp_Cmd, msg);

	switch(msg->cma_Pkt.cp_Cmd) {
	case CLCMD_CLOSE_INSTANCE:
	    ii->i_Flags |= IIF_CLOSING;
	    rpMsg = BuildVCPkt(ii, RPCMD_CLOSE_INSTANCE, sizeof(RPMsg));
	    isacked = 1;
	    break;
	case CLCMD_BEGIN_TRAN:
	    /*
	     * If the top level transaction was readonly, set
	     * IIF_READONLY even if this lower level trans is not,
	     * then use the flag later to avoid extranious query
	     * recording.
	     */
	    if (msg->cma_Pkt.cp_Flags & CPF_TOPREADONLY)
		ii->i_Flags |= IIF_READONLY;
	    else
		ii->i_Flags &= ~IIF_READONLY;

	    /*
	     * Beginning of a new transaction
	     *
	     * If the entire transaction has already been completed by
	     * another instance or set of instances, and CPF_NORESTART
	     * is set (meaning we are in a read-only transaction or
	     * the entire transaction was a top-level ro or rw transaction),
	     * we can skip the entire transaction.
	     *
	     * If nobody else is actively using messages in this transaction,
	     * we can remove them.
	     *
	     * We must also clear any pending IIF_SYNCWAIT
	     */
	    if (msg->a_Msg.cm_Level == 1 &&
		(msg->cma_Pkt.cp_Flags & CPF_NORESTART)
	    ) {
		int level = 0;
		int count = 0;
		int atq = 0;
		CLAnyMsg *lmsg = msg;

		dbinfo3("INST %p *** ABORT INST SEQUENCE (", ii);

		do {
		    DBASSERT(msg->cma_Pkt.cp_Flags & CPF_NORESTART);
		    dbinfo3(" %02x", msg->cma_Pkt.cp_Cmd);
		    --msg->a_Msg.cm_QCount;
		    --msg->a_Msg.cm_AtQueue;
		    if (atq < msg->a_Msg.cm_AtQueue)
			atq = msg->a_Msg.cm_AtQueue;
		    DBASSERT(msg->a_Msg.cm_QCount >= 0);
		    DBASSERT(msg->a_Msg.cm_AtQueue >= 0);
		    if (msg->cma_Pkt.cp_Cmd == CLCMD_BEGIN_TRAN)
			++level;
		    if (msg->cma_Pkt.cp_Cmd == CLCMD_ABORT_TRAN)
			--level;
		    if (msg->cma_Pkt.cp_Cmd == CLCMD_COMMIT2_TRAN)
			--level;
		    lmsg = msg;
		    msg = getListSucc(&cinfo->ci_CLMsgList, &msg->a_Msg.cm_Node);
		    if (msg)
			++msg->a_Msg.cm_AtQueue;
		    ++count;
		} while (level != 0);

		dbinfo3(") LEVEL %d COUNT %d NEXT STARTMSG %02x\n",
		    level,
		    count,
		    (msg ? msg->cma_Pkt.cp_Cmd : -1)
		);
		DBASSERT(level == 0);
		DBASSERT(count > 1);
		if (atq == 0)
		    RepRemoveUnneededMessages(cinfo, ii->i_CLMsg, lmsg);
		ii->i_Flags &= ~IIF_SYNCWAIT;	/* UNBLOCK */
		ii->i_CLMsg = msg;
		goto again;
	    }
	    /*
	     * If this is a top-level transaction (msg->a_Msg.cm_Level 
	     * is 1 because it has already been incremented), then we have
	     * to hold-off until the host's SyncTs reaches our freeze 
	     * timestamp.
	     */
	    ts = msg->a_BeginMsg.bg_FreezeTs;   /* freeze pt for trans */
	    if (msg->a_Msg.cm_Level == 1) {
		if (ii->i_SyncTs < ts) {
		    ii->i_Flags |= IIF_SYNCWAIT;	/* BLOCK */
		    dbinfo3("INST %p >>> SYNCWAIT %016qx/%016qx %s\n", ii, ts, ii->i_SyncTs, ii->i_SlaveHost->h_HostName);
		    return;
		}
		if (ii->i_Flags & IIF_SYNCWAIT) {
		    DBASSERT(ii->i_SyncTs >= ts);
		    dbinfo3("INST %p <<< SYNCDONE %016qx/%016qx %s\n", ii, ts, ii->i_SyncTs, ii->i_SlaveHost->h_HostName);
		    ii->i_Flags &= ~IIF_SYNCWAIT;	/* UNBLOCK */
		}
	    }
	    rpMsg = BuildVCPkt(ii, RPCMD_BEGIN_TRAN, sizeof(RPBeginMsg));
	    rpMsg->a_BeginMsg.bg_FreezeTs = ts;
	    /*
	     * XXX What about CPF_TOPREADONLY ?
	     */
	    if (msg->cma_Pkt.cp_Flags & CPF_READONLY)
		rpMsg->rpa_Pkt.pk_TFlags |= RPTF_READONLY;
	    break;
	case CLCMD_RUN_QUERY_TRAN:
	    /*
	     * Query is only run on one replication host (snapshot or peer),
	     * and recorded on any remaining peers (but not on any snapshots).
	     */
	    if (msg->cma_Pkt.cp_Flags & CPF_DIDRESPOND) {
		/*
		 * We responded already.  We have to record the query for
		 * peers and snapshots unless we are in read-only (at the
		 * top level only) mode.
		 *
		 * Inner read-only levels must be recorded on PEERs to
		 * participate in conflict detection but do not need to be
		 * recorded on snapshots.
		 *
		 * isacked is left at 0.
		 */
		if ((ii->i_Flags & IIF_READONLY) == 0 &&
		    (ii->i_Type == CTYPE_PEER ||
			(msg->cma_Pkt.cp_Flags & CPF_READONLY) == 0)
		) {
		    int len = msg->cma_Pkt.cp_Bytes - sizeof(msg->cma_Pkt);
		    rpMsg = BuildVCPkt(ii, RPCMD_REC_QUERY_TRAN, offsetof(RPMsg, rp_Pkt.pk_Data[len]));
		    bcopy(msg->cma_Pkt.cp_Data, rpMsg->rpa_Pkt.pk_Data, len);
		}
	    } else if (msg->a_Msg.cm_AuxInfo == NULL) {
		/*
		 * If nobody is primary for the query, assign this host
		 * as primary.  Snapshot hosts can take any query type,
		 * including update/delete/insert, but never commit them.
		 */
		int len = msg->cma_Pkt.cp_Bytes - sizeof(msg->cma_Pkt);

		msg->a_Msg.cm_AuxInfo = ii;
		msg->a_Msg.cm_StallCount = 0;
		rpMsg = BuildVCPkt(ii, RPCMD_RUN_QUERY_TRAN, offsetof(RPMsg, rp_Pkt.pk_Data[len]));
		bcopy(msg->cma_Pkt.cp_Data, rpMsg->rpa_Pkt.pk_Data, len);
		isacked = 1;
	    } else {
		/*
		 * If someone is currently primary we have to stall until
		 * the primary returns.  The command will then be retried
		 * (resulting in a recording or, if the primary connection
		 * was lost, a new primary assignment).
		 */
		isacked = 1;
	    }
	    break;
	case CLCMD_REC_QUERY_TRAN:
	    /*
	     * Asks that the query be recorded.   
	     *
	     * XXX NOTE: PASS-THRU API NOT IMPLEMENTED XXX
	     */
	    {
		int len = msg->cma_Pkt.cp_Bytes - sizeof(msg->cma_Pkt);

		rpMsg = BuildVCPkt(ii, RPCMD_REC_QUERY_TRAN, offsetof(RPMsg, rp_Pkt.pk_Data[len]));
		bcopy(msg->cma_Pkt.cp_Data, rpMsg->rpa_Pkt.pk_Data, len);
	    }
	    break;
	case CLCMD_ABORT_TRAN:
	    /*
	     * Rollback a transaction.  IIF_ABORTCOMMIT only applies to the
	     * top level transaction so only clear it if we are at the top
	     * level.
	     */
	    if (msg->a_Msg.cm_Level == 0)
		ii->i_Flags &= ~IIF_ABORTCOMMIT;
	    rpMsg = BuildVCPkt(ii, RPCMD_ABORT_TRAN, sizeof(RPMsg));
	    break;
	case CLCMD_COMMIT1_TRAN:
	    /*
	     * Commit1 - no timestamps are supplied but we will return one
	     * 		 later.
	     *
	     *		Outside Commit1's are not sent to snapshot hosts.
	     *		Inside commit1's are in order for queries to be
	     *		consistent.
	     *
	     *		We have to use a passive return to an outside 
	     *		snapshot host so it does not count towards the quorum.
	     *
	     *		There are two 'late commit1' cases.  #1: Inside commits
	     *		do not require a quorum, the first one will cause
	     *		CPF_DIDRESPOND to be set.  However, we must still
	     *		forward them to clients in order for further queries
	     *		to be consistent.
	     *
	     *		The second 'late commit1' case is with the outside
	     *		commit.  If we have completed a commit1 on a sufficient
	     *		number of instances (a quorum) and we have started on
	     *		the commit2 for those intances, we CANNOT initiate any
	     *		new commit1's or we could race against the record
	     *		replication and wind up with duplicate records in the
	     *		database.  For this case we set IIF_ABORTCOMMIT to
	     *		alert us when further top-level COMMIT1s are not
	     *		legal.  This will also cause us to abort any further
	     *		inner commits on other instances playing catchup with
	     *		us, since we are going to throw those instances away
	     *		anyway we might as well help them along.
	     */
	    if (msg->a_Msg.cm_Level == 1) {
		if (ii->i_Type == CTYPE_PEER) {
		    /*
		     * If we've responded to our client for a peer, we've
		     * already gone on to the COMMIT2 state.  We cannot
		     * allow a COMMIT1 to late peers to occur so we force
		     * a passive-abort situation.  We do a passive respons
		     * here and abort on the commit2 attempt.
		     */
		    if (msg->cma_Pkt.cp_Flags & CPF_DIDRESPOND) {
			ii->i_Flags |= IIF_ABORTCOMMIT;
			dbinfo3("INST %p *** WARNING, SLAVE BEHIND, ABORTING COMMIT ***\n", ii);
		    }
		} else {
		    /*
		     * Always abort a top-level commit to a snapshot host.
		     * This also forces a passive return.
		     */
		    ii->i_Flags |= IIF_ABORTCOMMIT;
		}
	    } else {
		/*
		 * Inner commit1's can be aborted if we've already completed
		 * the outer commit2's, in which case IIF_ABORTCOMMIT is
		 * already set, but cannot be aborted if we've merely
		 * responded to the inner commit sequence from other instances
		 * already.
		 *
		 * Inner commit1's are RET_ACTIVE - they can participate
		 * in generating an error code.  The reply code will cause the
		 * first inner commit1 response to return a reply to the client
		 * (and set DIDRESPOND), so we don't actually wait for all
		 * respondants before continuing.  We don't have to lift a
		 * finger here for that.
		 */
	    }
	    if (ii->i_Flags & IIF_ABORTCOMMIT) {
		ackmode = RET_PASSIVE | RET_CABORT;
	    } else if (msg->a_Msg.cm_Level == 1) {
		/*
		 * Stall on outer commits waiting for the acknowledgement.
		 */
		rpMsg = BuildVCPkt(ii, RPCMD_COMMIT1_TRAN, sizeof(RPCommit1Msg));
		isacked = 1;
	    } else {
		/*
		 * Use the inner-commit command to avoid waiting for a reply.
		 */
		rpMsg = BuildVCPkt(ii, RPCMD_ICOMMIT1_TRAN, sizeof(RPCommit1Msg));
	    }
	    break;
	case CLCMD_COMMIT2_TRAN:
	    /*
	     * Commit2 - commit timestamp to use in minCTs.
	     *
	     *	If IIF_ABORTCOMMIT is set then a top-level commit1
	     *	has already responded and we have already initiated
	     *  commit2's on other instances.  We can completely abort
	     *  all further ops on this instance, whether it is a 
	     *	peer or not and we must use a passive return.
	     *	
	     *		Inside Commit2's ARE sent to the snapshot host, but
	     *		no quorum is required.  XXX (need to check dbcore on
	     *		return value for inside commit)
	     *
	     *		We have to use a passive return for a snapshot
	     *		host so it does not count towards the quorum.
	     *
	     *		note: cm_Level is set to the post-command level,
	     *		so it's 0 rather then 1
	     */
	    if (ii->i_Flags & IIF_ABORTCOMMIT) {
		if (msg->a_Msg.cm_Level == 0)
		    ii->i_Flags &= ~IIF_ABORTCOMMIT;
		rpMsg = BuildVCPkt(ii, RPCMD_ABORT_TRAN, sizeof(RPMsg));
		ackmode = RET_PASSIVE | RET_CABORT;
		dbinfo3(
		    "INST %p *** Warning, aborting delayed commit"
		    " sequence after quorum previously reached ***\n",
		    ii
		);
	    } else if (msg->a_Msg.cm_Level != 0 || ii->i_Type == CTYPE_PEER) {
		/*
		 * peer or inner-snap, issue the commit, expect an ack.
		 *
		 * Do not stall waiting for *ANY* replies for an inner-commit.
		 * Basically we assume it will succeed, but a response still
		 * comes back
		 */
		if (msg->a_Msg.cm_Level == 0) {
		    rpMsg = BuildVCPkt(ii, RPCMD_COMMIT2_TRAN, sizeof(RPCommit2Msg));
		    isacked = 1;
		} else {
		    rpMsg = BuildVCPkt(ii, RPCMD_ICOMMIT2_TRAN, sizeof(RPCommit2Msg));
		}
		rpMsg->a_Commit2Msg.c2_MinCTs = msg->a_Commit2Msg.c2_MinCTs;
		rpMsg->a_Commit2Msg.c2_UserId = msg->a_Commit2Msg.c2_UserId;
	    } else {
		/*
		 * outer snap always aborts.  Aborts are not acked, and
		 * we must use a passive return so as not be included in
		 * the quorum (though at this point it may not matter).
		 */
		rpMsg = BuildVCPkt(ii, RPCMD_ABORT_TRAN, sizeof(RPMsg));
		ackmode = RET_PASSIVE | RET_CABORT;
	    }
	    break;
	default:
	    DBASSERT(0);	/* XXX remove */
	    msg->cma_Pkt.cp_Error = -1;
	    isacked = 1;
	    break;
	}

	/*
	 * Forward command to instance over VC.  If we expect an ACK leave
	 * the instance indexed to this CLMsg.  
	 *
	 * If we do not expect an ACK then acknowledge the message manually
	 */

	if (rpMsg) {
	    dbinfo3("CINFO %p INST %p CLMSG HANDLED, RPMSG SEND %s %04x:%04x %02x (%d)\n",
		cinfo, ii, ii->i_SlaveHost->h_HostName, ii->i_VCId,
		ii->i_InstId, rpMsg->rpa_Pkt.pk_Cmd, rpMsg->rpa_Pkt.pk_SeqNo
	    );
	    if (isacked == 0)
		rpMsg->rpa_Pkt.pk_TFlags |= RPTF_NOPUSH;
	    WriteVCPkt(rpMsg);
	} else {
	    dbinfo3("CINFO %p INST %p CLCMSG HANDLED (no RPMSG sent)\n",
		cinfo, ii
	    );
	}

	if (isacked == 0)
	    RepClientReturn(cinfo, ii, ackmode, 0);
    }
}

/*
 * Receive a message from the remote replicator responding to a
 * command we queued on behalf of our client.
 *
 * Generally speaking we must dispose of the current message, ii->i_CLMsg.
 * There are certain multi-packet-return-data cases where we do not.
 */

static void
RepClientInstancePkt(ClientInfo *cinfo, RPAnyMsg *rpMsg)
{
    InstInfo *ii = rpMsg->a_Msg.rp_InstInfo;

    /*
     * Once we've sent a CLOSE command, ignore any pending
     * results until we get the CLOSE reply.  Pending results
     * may come in when peers or snapshots get behind and the
     * client closes the connection.
     */
    if ((ii->i_Flags & IIF_CLOSING) &&
	rpMsg->rpa_Pkt.pk_Cmd != (RPCMD_CLOSE_INSTANCE|RPCMDF_REPLY)
    ) {
	FreeRPMsg(rpMsg);
	return;
    }
    dbinfo3("RECEIVED INST %p RESPONSE PACKET %02x %d %s\n", 
	ii,
	rpMsg->rpa_Pkt.pk_Cmd,
	rpMsg->rpa_Pkt.pk_RCode,
	ii->i_SlaveHost->h_HostName
    );

    switch(rpMsg->rpa_Pkt.pk_Cmd) {
    case RPCMD_CLOSE_INSTANCE|RPCMDF_REPLY:
	/*
	 * Clear the ready bit and set the closed bit for the instance,
	 * and derference all its messages.
	 */
	IIFClear(cinfo, ii, IIF_READY|IIF_CLOSING);
	dbinfo3("INST %p CLOSE REPLY OR FAILED INSTANCE, Ready now %d\n", ii, cinfo->ci_Ready);
	DBASSERT((ii->i_Flags & IIF_CLOSED) == 0);
	IIFSet(cinfo, ii, IIF_CLOSED);
	RepClientDereferenceAll(cinfo, ii);
	break;
    case RPCMD_OPEN_INSTANCE|RPCMDF_REPLY:
	/*
	 * New instance open return packet, returns latest known
	 * sync timestamp.  We queued pending messages when we 
	 * initialized the InstInfo, but we didn't start running any 
	 * of them, so start running the list now.
	 */
	DBASSERT((ii->i_Flags & IIF_READY) == 0);
	if (rpMsg->rpa_Pkt.pk_RCode < 0) {
	    IIFSet(cinfo, ii, IIF_CLOSED);
	    RepClientDereferenceAll(cinfo, ii);
	} else {
	    IIFSet(cinfo, ii, IIF_READY);
	    RepClientExecuteCLCmd(cinfo, ii);
	}
	break;
    case RPCMD_UPDATE_INSTANCE|RPCMDF_REPLY:
	/*
	 * Update instance open return packet, returns a new minimum 
	 * commit timestamp and a new sync timestamp in the payload.
	 * This packet is not a response to any command, so do not dispose
	 * of the pending command.
	 */
	DBASSERT((ii->i_Flags & IIF_READY) != 0);
	/* XXX trigger query waiting on update? */
	break;
    case RPCMD_RUN_QUERY_TRAN|RPCMDF_REPLY:
	/*
	 * The last packet in a query response, dispose of the current
	 * pending command.
	 */
	RepClientReturn(cinfo, ii, RET_ACTIVE|RET_RUN, rpMsg->rpa_Pkt.pk_RCode);
	break;
    case RPCMD_COMMIT1_TRAN|RPCMDF_REPLY:
	/*
	 * Commit phase-1 return, returns minimum commit time 
	 * stamp.
	 *
	 * We aggregate the returned MinCTs in our response, taking
	 * the highest one.   We have to aggregate successes verses
	 * failures separately so we do not return a success timestamp
	 * when one of the commit attempts failed.  Commit failures return
	 * the conflicting timestamp while successes return an allocated
	 * timestamp.  Returning the allocated timestamp for a failure 
	 * could stall the client.
	 *
	 * XXX when restarting the wrong MinCTs will be transmitted,
	 * DONT MESS W/ ORIGINAL MSG PACKET!
	 *
	 * XXX make sure assigned MinCTsw/ quorum does not change once quorum
	 * is selected.
	 */

	/*
	 * WE CANNOT AGGREGATE THE RETURNED TIMESTAMP IF WE HAVE ALREADY
	 * REPLIED BACK TO THE CLIENT.  If we were to do this it could
	 * screw up a second commit-in-progress in back-to-back 
	 * transactions by applying a timestamp from a previous
	 * transaction to a later one (which might be pushed down),
	 * causing later queries to use the wrong timestamp.
	 */
	if ((ii->i_CLMsg->cma_Pkt.cp_Flags & CPF_DIDRESPOND) == 0) { 
	    if (rpMsg->rpa_Pkt.pk_RCode == 0) {
		if (cinfo->ci_MinCTsGood < rpMsg->a_Commit1Msg.c1_MinCTs)
		    cinfo->ci_MinCTsGood = rpMsg->a_Commit1Msg.c1_MinCTs;
	    } else {
		if (cinfo->ci_MinCTsBad < rpMsg->a_Commit1Msg.c1_MinCTs)
		    cinfo->ci_MinCTsBad = rpMsg->a_Commit1Msg.c1_MinCTs;
		dbinfo3("CINFO %p >> Commit1 failed %016qx\n", 
			cinfo, rpMsg->a_Commit1Msg.c1_MinCTs);
		DBASSERT(rpMsg->rpa_Pkt.pk_RCode != DBERR_COMMIT2_WITHOUT_COMMIT1);
	    }
	}

	/*
	 * This is a slightly different case then the late commit detection
	 * in the RepClientExecuteCLCmd() code.  This case occurs when we
	 * have sent our commit1 request but initiated commit2's before
	 * receiving a reply to it.  Thus, we do not know if our commit1
	 * request got to the remote node in time to avoid data-duplication,
	 * so we have to set IIF_ABORTCOMMIT for the top-level commit case
	 * here.
	 */
	if (ii->i_CLMsg->cma_Pkt.cp_Flags & CPF_DIDRESPOND) {
	    if (ii->i_CLMsg->a_Msg.cm_Level == 1) {
		ii->i_Flags |= IIF_ABORTCOMMIT;
		dbinfo3("INST %p *** WARNING, OUTER COMMIT1<->2 INTERLOCK, ABORT *** %016qx\n", ii, ii->i_CLMsg->a_Commit1Msg.c1_MinCTs);
	    } else {
		dbinfo3("INST %p *** WARNING, INNER COMMIT1<->2 INTERLOCK, ALLOWING CASE *** %016qx\n", ii, ii->i_CLMsg->a_Commit1Msg.c1_MinCTs);
	    }
	} else if (rpMsg->a_Commit1Msg.c1_MinCTs != DBSTAMP_MAX) {
	    dbinfo3("CINFO %p INST %p *** COMMIT1 AGGREGATE RESPONSE *** GOOD=%016qx BAD=%016qx RC %d\n", cinfo, ii, cinfo->ci_MinCTsGood, cinfo->ci_MinCTsBad, rpMsg->rpa_Pkt.pk_RCode);
	} else {
	    dbinfo3("CINFO %p INST %p *** WARNING, CONFLICT DETECTED, ABORTING *** %016qx\n", cinfo, ii, ii->i_CLMsg->a_Commit1Msg.c1_MinCTs);
	}
	RepClientReturn(cinfo, ii, RET_ACTIVE|RET_RUN, rpMsg->rpa_Pkt.pk_RCode);
	break;
    case RPCMD_COMMIT2_TRAN|RPCMDF_REPLY:
	/*
	 * Commit phase-2 return, returns success or failed (we passed
	 * the actual commit timestamp in our COMMIT2 command, but no
	 * timestamp is returned).
	 */
	RepClientReturn(cinfo, ii, RET_ACTIVE|RET_RUN, rpMsg->rpa_Pkt.pk_RCode);
	break;
    case RPCMD_RESULT_ORDER_TRAN|RPCMDF_REPLY:
	/*
	 * result sort order message, occurs before any result messages
	 * (intermediate response, do not dispose of the current command)
	 */
	DBASSERT(ii->i_CLMsg && ii->i_CLMsg->a_Msg.cm_AuxInfo == (void *)ii);
	{
	    CLAnyMsg *tmp;
	    int i;

	    tmp = BuildCLMsg(CLCMD_RESULT_ORDER, offsetof(CLOrderMsg, om_Order[rpMsg->a_OrderMsg.om_NumOrder]));
	    tmp->a_OrderMsg.om_NumOrder = rpMsg->a_OrderMsg.om_NumOrder;
	    for (i = 0; i < rpMsg->a_OrderMsg.om_NumOrder; ++i)
		tmp->a_OrderMsg.om_Order[i] = rpMsg->a_OrderMsg.om_Order[i];
	    WriteCLMsg(cinfo->ci_Cd->cd_Iow, tmp, 0);
	}
	break;
    case RPCMD_RESULT_LIMIT_TRAN|RPCMDF_REPLY:
	/*
	 * result limit message, occurs before any result messages
	 * (intermediate response, do not dispose of the current command)
	 */
	DBASSERT(ii->i_CLMsg && ii->i_CLMsg->a_Msg.cm_AuxInfo == (void *)ii);
	{
	    CLAnyMsg *tmp;

	    tmp = BuildCLMsg(CLCMD_RESULT_LIMIT, sizeof(CLLimitMsg));
	    tmp->a_LimitMsg.lm_StartRow = rpMsg->a_LimitMsg.lm_StartRow;
	    tmp->a_LimitMsg.lm_MaxRows = rpMsg->a_LimitMsg.lm_MaxRows;
	    WriteCLMsg(cinfo->ci_Cd->cd_Iow, tmp, 0);
	}
	break;
    case RPCMD_RESULT_TRAN|RPCMDF_REPLY:
	/*
	 * result message (one of potentially many), contains a row.
	 * (intermediate response, do not dispose of the current command)
	 *
	 * At this point we are transfering received data directly to
	 * the client.  The pipe to the client may stall that operation.
	 * However, the remote replicator may be queueing immense amounts
	 * of data to us.  The RPCMD_CONTINUE / CL_STALL_COUNT is used
	 * to limit the size of the backup to around 32K.
	 */
	DBASSERT(ii->i_CLMsg && ii->i_CLMsg->a_Msg.cm_AuxInfo == (void *)ii);
	{
	    CLAnyMsg *tmp;

	    /*
	     * The stall count is the size of the physical packet
	     */
	    tmp = ii->i_CLMsg;
	    tmp->a_Msg.cm_StallCount += rpMsg->rpa_Pkt.pk_Bytes;

	    while (tmp->a_Msg.cm_StallCount > CL_STALL_COUNT/2) {
		tmp->a_Msg.cm_StallCount -= CL_STALL_COUNT/2;
		WriteVCPkt(BuildVCPkt(ii, RPCMD_CONTINUE, sizeof(RPMsg)));
	    }

	    if ((cinfo->ci_Flags & CIF_DEADCLIENT) == 0) {
		int bytes;

		bytes = rpMsg->a_RowMsg.rm_Offsets[rpMsg->a_RowMsg.rm_Count];
		tmp = BuildCLMsg(CLCMD_RESULT, offsetof(CLMsg, cm_Pkt.cp_Data[0]) + bytes);
		bcopy(&rpMsg->a_Msg.rp_Pkt.pk_Data, tmp->a_Msg.cm_Pkt.cp_Data, bytes);
		if (WriteCLMsg(cinfo->ci_Cd->cd_Iow, tmp, 0) < 0) {
		    dbinfo3("CINFO %p INST %p *** Client disappeared, aborting query\n", cinfo, ii);
		    WriteVCPkt(BuildVCPkt(ii, RPCMD_BREAK_QUERY, sizeof(RPMsg)));
		    cinfo->ci_Flags |= CIF_DEADCLIENT;
		}
	    }
	}
	break;
    default:
	dberror("Invalid RP packet received cmd %02x error %d\n",
	    rpMsg->rpa_Pkt.pk_Cmd,
	    rpMsg->rpa_Pkt.pk_RCode
	);
	DBASSERT(0); /* XXX */
	break;
    }
    FreeRPMsg(rpMsg);
}

/*
 * RepClientRestart() -	Requeue CLMsg list to newly restarted/formed instance
 *
 *	Requeue CL commands after an instance has been [re]started.  We only
 *	requeue commands that are part of the current transaction.  There
 *	may be hanger-ons from previous transactions (e.g. due to pending
 *	commits that have achieved quorum but all the hosts hadn't
 *	acknowledged yet).  
 *
 *	The CPF_NORESTART flag makes this easy for us.
 */

static void
RepClientRestart(ClientInfo *cinfo, InstInfo *ii, int isCounted)
{
    CLAnyMsg *msg;

    /*
     * Figure out where to restart from.  We can't restart from earlier
     * transactions which have been committed or aborted not only because
     * this is wasteful, but also because a previously successful phase-2
     * commit may NOT be successful when rerun due to the synchronizer 
     * catching the other hosts up.
     *
     * if isCounted is non-zero, queued messages *already* take into account
     * this instance (XXX we should really have taken the instance out of
     * the active  queue completely XXX).  If it is 0, then we are adding a
     * new instance and must bump the ref count for pending queries.
     */
    if (isCounted) {
	msg = ii->i_CLMsg;
	while (msg && (msg->cma_Pkt.cp_Flags & CPF_NORESTART)) {
	    ii->i_Flags &= ~IIF_SYNCWAIT;	/* UNBLOCK */
	    --msg->a_Msg.cm_QCount;
	    --msg->a_Msg.cm_AtQueue;
	    msg = getListSucc(&cinfo->ci_CLMsgList, &msg->a_Msg.cm_Node);
	    if (msg)
		++msg->a_Msg.cm_AtQueue;
	}
	ii->i_CLMsg = msg;
    } else {
	for (msg = getHead(&cinfo->ci_CLMsgList);	
	    msg;
	    msg = getListSucc(&cinfo->ci_CLMsgList, &msg->a_Msg.cm_Node)
	) {
	    if ((msg->cma_Pkt.cp_Flags & CPF_NORESTART) == 0)
		break;
	}
	DBASSERT(ii->i_CLMsg == NULL);
	ii->i_CLMsg = msg;
	if (msg)
	    ++msg->a_Msg.cm_AtQueue;
	while (msg) {
	    ++msg->a_Msg.cm_QCount;
	    msg = getListSucc(&cinfo->ci_CLMsgList, &msg->a_Msg.cm_Node);
	}
    }
    if (ii->i_CLMsg != NULL && (ii->i_Flags & IIF_READY))
	RepClientExecuteCLCmd(cinfo, ii);
}

/*
 * RepClientReturn() -	Process results received from one of our remote
 *			instances.
 *
 *	The message is queued to cm_QCount instances, cm_QCount decrements
 *	as we get responses.  cm_QResponded is the number of answers we got,
 *	good or bad, not counting any failed VC's.
 *
 *	Depending on the command we determine whether we need to send an ack
 *	to the our local client, and whether we need a quorum to complete
 *	the command (whether or not we have one yet).
 *
 *	Flags is typically either RET_ACTIVE or RET_PASSIVE.  RET_PASSIVE is
 *	typically used when other instances have already responded to the
 *	query.  RET_CABORT may be set for a COMMIT1 or COMMIT2, indicating
 *	special abort handling.
 */

static void
RepClientReturn(ClientInfo *cinfo, InstInfo *ii, int flags, int error)
{
    CLAnyMsg *msg;
    int doack = 0;
    int needQuorum = 0;
    int canContinueEarly = 0;
    int lastTran = 0;
    int midLastTran = 0;
    int optRemoveQuery = 0;
    int quorum = (cinfo->ci_NumPeers / 2) + 1;

    /*
     * Extract message and adjust message pointer to the next one for this
     * instance. 
     */
    msg = ii->i_CLMsg;
    DBASSERT(msg != NULL);
    --msg->a_Msg.cm_AtQueue;
    DBASSERT(msg->a_Msg.cm_AtQueue >= 0);
    ii->i_CLMsg = getListSucc(&cinfo->ci_CLMsgList, &msg->a_Msg.cm_Node);
    ii->i_Flags &= ~IIF_SYNCWAIT;	/* UNBLOCK */
    if (ii->i_CLMsg != NULL)
	++ii->i_CLMsg->a_Msg.cm_AtQueue;

    /*
     * deal with passive vs active returns.  A passive return occurs when
     * we lose a VC (ii->i_Flags will have IIF_CLOSED set) and usually 
     * doesn't count.
     */
    if ((flags & RET_ACTIVE) && (msg->cma_Pkt.cp_Flags & CPF_DIDRESPOND) == 0) {
	if (error)
	    msg->cma_Pkt.cp_Error = error;
    }

    /*
     * Figure out what the return requirements are for various CL commands.
     *
     * No quorum is needed when closing instances, beginning a read-only
     * transaction (XXX), recording or aborting a query.  A quorum is
     * needed only when beginning a RW transaction and when committing.
     *
     * No quorum is needed for inside commits and, in fact, we can't use
     * a quorum here because snapshot hosts must be able to participate.
     * Note that msg->cm_Level is adjusted to its post-command state prior
     * entering here.
     */
    switch(msg->cma_Pkt.cp_Cmd) {
    case CLCMD_HELLO:
	DBASSERT(0);
	break;
    case CLCMD_CLOSE_INSTANCE:
	doack = 1;
	break;
    case CLCMD_BEGIN_TRAN:
	/*
	 * Finishing a transaction start command, clear IIF_SYNCWAIT.
	 */
	/* needQuorum = 1; */
	ii->i_Flags &= ~IIF_SYNCWAIT;
	break;
    case CLCMD_RUN_QUERY_TRAN:
	/*
	 * If the master trans is read-only, we can
	 * optimize the removal of this query (i.e. not
	 * save it for restarts)
	 */
	if (ii->i_Flags & IIF_READONLY)
	    optRemoveQuery = 1;
	doack = 1;
	break;
    case CLCMD_REC_QUERY_TRAN:
	break;
    case CLCMD_ABORT_TRAN:
	/*
	 * note: cm_Level is the current level AFTER execution, '0'
	 * would be an abort of a top level transaction.
	 */
	if (msg->a_Msg.cm_Level == 0)
	    lastTran = 1;
	else
	    midLastTran = 1;
	break;
    case CLCMD_COMMIT1_TRAN:
	/*
	 * note: cm_Level is the current level as of the message, level 1
	 * is a commit of the top level transaction.  Set the returned
	 * minCTs based on whether an error occured or not.
	 *
	 * A COMMIT1 normally requires a quorum.  If we have already
	 * performed the commit on a quorum and responded to it the
	 * commit1 is converted into a NOP and the commit2 is converted
	 * into an abort.   This is the CABORT case, and obviously does
	 * not require a quorum.
	 */
	doack = 1;
	if ((flags & RET_CABORT) == 0) {
	    if (msg->a_Msg.cm_Level == 1) {
		needQuorum = 1;
	    } else {
		canContinueEarly = 1;	/* don't have to wait for all respondants */
	    }
	    if (msg->cma_Pkt.cp_Error)
		msg->a_Commit1Msg.c1_MinCTs = cinfo->ci_MinCTsBad;
	    else
		msg->a_Commit1Msg.c1_MinCTs = cinfo->ci_MinCTsGood;
	}
	break;
    case CLCMD_COMMIT2_TRAN:
	/*
	 * note: cm_Level is the current level AFTER the message would
	 * execute. '0' is a commit2 of the top level transaction.
	 *
	 * In the CABORT case the commit sequence is being aborted.  No
	 * quorum is necessary for an abort.
	 */
	doack = 1;
	if (msg->a_Msg.cm_Level == 0) {
	    if ((flags & RET_CABORT) == 0)
		needQuorum = 1;
	    lastTran = 1;
	} else {
	    midLastTran = 1;
	    canContinueEarly = 1;	/* don't have to wait for all respondants */
	}
	break;
    default:
	DBASSERT(0);
	break;
    }

    /*
     * Decrement the QCount, increment QResponded if an active return.
     */
    --msg->a_Msg.cm_QCount;
    DBASSERT(msg->a_Msg.cm_QCount >= 0);
    if (flags & RET_ACTIVE)
	++msg->a_Msg.cm_QResponded;

    dbinfo3("CINFO %p INST %p RepClientReturn %04x:%04x numpeer %d qc %d resp %d\n",
	cinfo, ii,
	ii->i_VCId, ii->i_InstId,
	cinfo->ci_NumPeers, msg->a_Msg.cm_QCount, msg->a_Msg.cm_QResponded
    );

    /*
     * If we need a quorum we stall until we get one.  There had better
     * not be any new commands pending.
     *
     * XXX quorum may change on the fly, then what?
     */
    if (needQuorum && msg->a_Msg.cm_QResponded < quorum) {
	DBASSERT(ii->i_CLMsg == NULL);
	return;
    }

    if (doack) {
	/*
	 * When we have to ack back to our client, we have two choices.
	 * If needQuorum is set we can ack back the *moment* we
	 * get a quorum.  Otherwise we can ack back only after all remotes
	 * have acknowledged the message.
	 */
	if (needQuorum) {
	    if (msg->a_Msg.cm_QResponded >= quorum) {
		if ((msg->cma_Pkt.cp_Flags & CPF_DIDRESPOND) == 0) {
		    cinfo->ci_Flags |= CIF_ACCEPTOK;
		    msg->cma_Pkt.cp_Flags |= CPF_DIDRESPOND;
		    ++msg->a_Msg.cm_Refs;
		    WriteCLMsg(cinfo->ci_Cd->cd_Iow, msg, 1);
		}
	    }
	} else {
	    if (msg->a_Msg.cm_AuxInfo == (void *)ii) {
		/*
		 * Special handling of directed query.  If the query 
		 * succeeded (RET_ACTIVE) we return our response and then
		 * re-execute the query on the remaining instances (which
		 * causes it to be 'recorded' at the other hosts). 
		 *
		 * If the connection failed (RET_PASSIVE) we send a RESET
		 * to the client to reset any partially received results,
		 * clear the directed query, and re-execute on the remaining
		 * instances in order to choose a new directed host for 
		 * the query.  However, if ci_StreamLevel is > 0 we cannot
		 * restart the query.
		 */
		InstInfo *si;

		DBASSERT((msg->cma_Pkt.cp_Flags & CPF_DIDRESPOND) == 0);
		if (flags & RET_ACTIVE) {
		    /*
		     * Normal completion of active query
		     */
		    cinfo->ci_Flags |= CIF_ACCEPTOK;
		    msg->cma_Pkt.cp_Flags |= CPF_DIDRESPOND;
		    ++msg->a_Msg.cm_Refs;
		    WriteCLMsg(cinfo->ci_Cd->cd_Iow, msg, 1);
		    msg->a_Msg.cm_AuxInfo = NULL;
		} else if (cinfo->ci_StreamLevel) {
		    /*
		     * Instance failed, streaming, restart not ok
		     */
		    cinfo->ci_Flags |= CIF_ACCEPTOK;
		    msg->cma_Pkt.cp_Flags |= CPF_DIDRESPOND;
		    msg->cma_Pkt.cp_Error = error;
		    ++msg->a_Msg.cm_Refs;
		    WriteCLMsg(cinfo->ci_Cd->cd_Iow, msg, 1);
		    msg->a_Msg.cm_AuxInfo = NULL;
		} else {
		    /*
		     * Instance failed, non-streaming, restart ok.
		     */
		    CLAnyMsg *tmp;

		    DBASSERT(msg->cma_Pkt.cp_Cmd == CLCMD_RUN_QUERY_TRAN);
		    msg->a_Msg.cm_AuxInfo = NULL;

		    tmp = BuildCLMsg(CLCMD_RESULT_RESET, sizeof(CLMsg));
		    WriteCLMsg(cinfo->ci_Cd->cd_Iow, tmp, 1);
		}
		for (si = cinfo->ci_IIBase; si; si = si->i_Next) {
		    if ((si->i_Flags & IIF_CLOSED) == 0 && 
			si->i_CLMsg == msg
		    ) {
			if (si->i_Flags & IIF_READY)
			    RepClientExecuteCLCmd(cinfo, si);
		    }
		}
	    } else if (msg->a_Msg.cm_QCount == 0) {
		/*
		 * Normal handling, all remotes have acknowledged the
		 * message.  Set the DIDRESPOND bit and return the reply. 
		 * Indicate that a new command can be accepted.
		 */
		if ((msg->cma_Pkt.cp_Flags & CPF_DIDRESPOND) == 0) {
		    cinfo->ci_Flags |= CIF_ACCEPTOK;
		    msg->cma_Pkt.cp_Flags |= CPF_DIDRESPOND;
		    ++msg->a_Msg.cm_Refs;
		    WriteCLMsg(cinfo->ci_Cd->cd_Iow, msg, 1);
		}
	    } else if (canContinueEarly) {
		if ((msg->cma_Pkt.cp_Flags & CPF_DIDRESPOND) == 0) {
		    cinfo->ci_Flags |= CIF_ACCEPTOK;
		    msg->cma_Pkt.cp_Flags |= CPF_DIDRESPOND;
		    ++msg->a_Msg.cm_Refs;
		    WriteCLMsg(cinfo->ci_Cd->cd_Iow, msg, 1);
		}
	    } else {
		/*
		 * XXX optimization?, client may be able to continue early
		 * for things like inner commit1 and commit2's, where we
		 * do not have to wait for the acks per-say.
		 */
	    }
	}
    } else {
	/*
	 * When no ack return is expected we can continue on with the
	 * next command.  msg->a_Msg.cm_QCount may or may not be 0.
	 */
	if ((msg->cma_Pkt.cp_Flags & CPF_DIDRESPOND) == 0) {
	    msg->cma_Pkt.cp_Flags |= CPF_DIDRESPOND;
	    cinfo->ci_Flags |= CIF_ACCEPTOK;
	}
    }

    /*
     * When we have completed the outer (rw or ro) transaction 
     * (lastTran), or completed an inner ro transaction, we must
     * set CPF_NORESTART on all messages associated with the
     * transaction.
     *
     * This will prevent sequence restarts (from link failures) from
     * re-running a completed transaction while at the same time
     * allowing any operations still in progress (not cleared out
     * of other instance's queues) to continue.
     */
    if (optRemoveQuery) {
	if (msg->a_Msg.cm_QCount == 0 &&
	    (msg->cma_Pkt.cp_Flags & CPF_DIDRESPOND)
	) {
	    dbinfo3("INST %p *** msg=%p REMOVE ROQUERY %02x %d %d\n", ii, msg, msg->cma_Pkt.cp_Cmd, msg->a_Msg.cm_QCount, msg->a_Msg.cm_AtQueue);
	    DBASSERT(msg->a_Msg.cm_AtQueue == 0);
	    removeNode(&msg->a_Msg.cm_Node);
	    FreeCLMsg(msg);
	}
    } else if ((lastTran || (midLastTran && (cinfo->ci_Flags & CIF_READONLY)))
	&& (msg->cma_Pkt.cp_Flags & CPF_DIDRESPOND)
    ) {
	CLAnyMsg *endMsg = msg;
	CLAnyMsg *lmsg;
	int count = 1;
	int atq = msg->a_Msg.cm_AtQueue;

	/*
	 * Set CPF_NORESTART on the sequence while locating its
	 * beginning.
	 */
	msg->cma_Pkt.cp_Flags |= CPF_NORESTART;
	lmsg = msg;
	dbinfo3("INST %p MSGCMD msg=%p %02x\n", ii, msg, msg->cma_Pkt.cp_Cmd);
	msg = getListPred(&cinfo->ci_CLMsgList, &msg->a_Msg.cm_Node);
	while (count && msg) {
	    dbinfo3("INST %p MSGCMD msg=%p %02x\n", ii, msg, msg->cma_Pkt.cp_Cmd);
	    switch(msg->cma_Pkt.cp_Cmd) {
	    case CLCMD_BEGIN_TRAN:
		--count;
		break;
	    case CLCMD_COMMIT2_TRAN:
	    case CLCMD_ABORT_TRAN:
		++count;
		break;
	    }
	    if (atq < msg->a_Msg.cm_AtQueue)
		atq = msg->a_Msg.cm_AtQueue;
	    msg->cma_Pkt.cp_Flags |= CPF_NORESTART;
	    lmsg = msg;
	    msg = getListPred(&cinfo->ci_CLMsgList, &msg->a_Msg.cm_Node);
	}
	dbinfo3("INST %p COUNT %d\n", ii, count);
	DBASSERT(lmsg->cma_Pkt.cp_Cmd == CLCMD_BEGIN_TRAN);
	DBASSERT(count == 0);

	/*
	 * If atq is 0, nobody is referencing any of the commands
	 * in this transaction and we can safely remove them.
	 */
	if (atq == 0)
	    RepRemoveUnneededMessages(cinfo, lmsg, endMsg);
	/*
	 * If a remote database is trying to shutdown, close our instance
	 * here.  This is the best time to do it nicely - between
	 * transactions.
	 */
	{
	    InstInfo *is;
	    for (is = cinfo->ci_IIBase; is; is = is->i_Next) {
		if (is->i_CLMsg == NULL &&
		    cinfo->ci_Level == 0 &&
		    (is->i_Flags & (IIF_READY|IIF_FAILED|IIF_CLOSED|IIF_CLOSING)) == IIF_READY &&
		    RemoteClosingDB(is->i_RHSlave)
		) {
		    ii->i_Flags |= IIF_CLOSING;
		    dbinfo3("TRANSMIT INST %p CLOSE COMMAND %02x (Remote Closing DB)\n", ii, RPCMD_CLOSE_INSTANCE);
		    WriteVCPkt(BuildVCPkt(is, RPCMD_CLOSE_INSTANCE, sizeof(RPMsg)));
		    IIFClear(cinfo, is, IIF_READY);
		}
	    }
	}
    }

    /*
     * We can throw away any messages with cm_QCount set to 0 with 
     * CPF_NORESTART set.  The msg we were called with may be thrown
     * away here.   XXX removed.  We had better handle this through
     * the transaction termination handling code.
     */

#if 0
    while ((msg = getHead(&cinfo->ci_CLMsgList)) != NULL) {
	if (msg->cm_QCount != 0 || (msg->cma_Pkt.cp_Flags & CPF_NORESTART) == 0)
	    break;
	removeNode(&msg->a_Msg.cm_Node);
	DBASSERT(msg->a_Msg.cm_AtQueue == 0);
	FreeCLMsg(msg);
    }
#endif

    /*
     * Work on the next command
     */
    if (flags & RET_RUN) {
	if (ii->i_CLMsg != NULL && (ii->i_Flags & IIF_READY)) {
	    RepClientExecuteCLCmd(cinfo, ii);
	}
    }
}

static void
RepRemoveUnneededMessages(ClientInfo *cinfo, CLAnyMsg *lmsg, CLAnyMsg *endMsg)
{
    CLAnyMsg *msg;

    do {
	msg = lmsg;
	lmsg = getListSucc(&cinfo->ci_CLMsgList, &msg->a_Msg.cm_Node);
	DBASSERT(msg != NULL);
	removeNode(&msg->a_Msg.cm_Node);
	dbinfo3("CINFO %p *** msg=%p REMOVE NODE %02x %d %d\n", cinfo, msg, msg->cma_Pkt.cp_Cmd, msg->a_Msg.cm_QCount, msg->a_Msg.cm_AtQueue);
	FreeCLMsg(msg);
    } while (msg != endMsg);
}

static void
RepClientDereferenceAll(ClientInfo *cinfo, InstInfo *ii)
{
    CLAnyMsg *msg;

    while ((msg = ii->i_CLMsg) != NULL) {
	/*
	 * Passive error return (typically just stalls the client,
	 * unless we still have a quorum.
	 */
	RepClientReturn(cinfo, ii, RET_PASSIVE, DBERR_LOST_LINK);
    }
}

/*
 * CloseAllInstances() - open an InstInfo 
 *
 */

static void
CloseAllInstances(ClientInfo *cinfo, int forceClose)
{
    InstInfo **pii;
    InstInfo *ii;

    /*
     * Send the CLOSE_INSTANCE command to instances as appropriate.  Cache
     * (for reuse by another client) any good instances that do not have
     * any pending commands left.  VCManager will handle failures of 
     * idle instances automatically.
     */
    cinfo->ci_Flags |= CIF_SHUTDOWN;
    pii = &cinfo->ci_IIBase;
    while ((ii = *pii) != NULL) {
	if ((ii->i_Flags & (IIF_FAILED|IIF_CLOSED)) == 0) {
	    if (ii->i_CLMsg == NULL &&
		cinfo->ci_Level == 0 &&
		forceClose == 0 &&
		RemoteClosingDB(ii->i_RHSlave) == 0
	    ) {
		*pii = ii->i_Next;
		ii->i_Flags |= IIF_IDLE;
		ii->i_VCILCmd = 0x00;	/* status tracking */
		ii->i_Next = ii->i_RHSlave->rh_IdleInstBase;
		ii->i_RHSlave->rh_IdleInstBase = ii;
		ii->i_RXPktList = NULL;
		ii->i_NotifyInt = NULL;
		--cinfo->ci_Count;
		if (ii->i_Type == CTYPE_SNAP)
		    --cinfo->ci_Snaps;
		if (ii->i_Flags & IIF_READY)
		    --cinfo->ci_Ready;
	    } else if ((ii->i_Flags & IIF_CLOSING) == 0) {
	        ii->i_Flags |= IIF_CLOSING;
		dbinfo3("TRANSMIT INST %p CLOSE COMMAND %02x (Close All Instances)\n", ii, RPCMD_CLOSE_INSTANCE);
		WriteVCPkt(BuildVCPkt(ii, RPCMD_CLOSE_INSTANCE, sizeof(RPMsg)));
		pii = &ii->i_Next;
	    } else {
		pii = &ii->i_Next;
	    }
	} else {
	    pii = &ii->i_Next;
	}
    }

    /*
     * Wait for the instances to officially close.  Do normal processing
     * until all instances are officially closed.
     */
    while (cinfo->ci_Closed < cinfo->ci_Count) {
	RPAnyMsg *rpMsg;

	waitNotify(cinfo->ci_Notify, 0);
	while ((rpMsg = remHead(&cinfo->ci_List)) != NULL)
	    RepClientInstancePkt(cinfo, rpMsg);
    }

    dbinfo2("Shutting down client, freeing structures\n");

    /*
     * Free up the InstInfo structures associated with the now closed
     * instances.
     */
    while ((ii = cinfo->ci_IIBase) != NULL) {
	cinfo->ci_IIBase = ii->i_Next;
	ii->i_Next = NULL;
	ii->i_RXPktList = NULL;
	ii->i_NotifyInt = NULL;
	FreeInstInfo(ii);
    }

    /*
     * Free any CLMsg's left dangling.
     */
    {
	CLAnyMsg *msg;

	while ((msg = getHead(&cinfo->ci_CLMsgList)) != NULL) {
	    removeNode(&msg->a_Msg.cm_Node);
	    dbinfo3("CINFO %p *** %p REMOVE NODE2 %02x %d %d\n", cinfo, msg, msg->cma_Pkt.cp_Cmd, msg->a_Msg.cm_QCount, msg->a_Msg.cm_AtQueue);
	    FreeCLMsg(msg);
	}
    }
}

