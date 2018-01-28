/*
 * REPLICATOR/SYNCHRONIZE.C	- Synchronizer and VC maintainer for a database
 *
 * (c)Copyright 2000-2002 Backplane, Inc.  Please refer to the COPYRIGHT
 * file at the base of the distribution tree.
 *
 * $Backplane: rdbms/replicator/synchronize.c,v 1.78 2002/08/20 22:06:00 dillon Exp $
 *
 *	This code runs in the main replication process, one thread for
 *	each physical database under our control.  We use cd_NotifyInt 
 *	to detect shutdown requests on this database.
 *
 *	This code is responsible for synchronizing this database with
 *	its peers.  When we have synchronized with a quorum's worth
 *	we change our status from 'synchronizing' to 'ready'.
 */

#include "defs.h"

Prototype void StartSynchronizerThread(DBInfo *d);
Prototype void StopSynchronizerThread(DBInfo *d);

static void SynchronizerThreadDirect(DBInfo *d);
static int DoSynchronization(CLDataBase *cd);
static void DoSynchronizationUpdateSyncTs(CLDataBase *cd);
static void StartSynchronizeWith(CLDataBase *parCd, RouteHostInfo *rh);
static void SynchronizeWithThread(CLDataBase *cd);
static rp_type_t GetRepGroupInfo(DBInfo *d, CLDataBase *cd, dbstamp_t *dbid, rp_totalrep_t * rcount);
static int BackgroundSyncing(CLDataBase *parCd);

/*
 * StartSynchronizerThread() - Start the synchronizer and/or notify
 *				it that new databases might have been
 *				forked.
 */
void 
StartSynchronizerThread(DBInfo *d)
{
    UseDBInfo(d);	/* synchronizer thread's hold */
    d->d_Synchronize = allocNotify();
    taskCreate(SynchronizerThreadDirect, d);
}

/*
 * StopSynchronizerThread() - Terminate the synchronizer and the local
 *			      database(s) under its control.
 *
 */
void 
StopSynchronizerThread(DBInfo *d)
{
    if (d->d_Synchronize && (d->d_Flags & DF_SHUTDOWN) == 0) {
	d->d_Flags |= DF_SHUTDOWN;
	issueNotify(d->d_Synchronize);
    }
}

/*
 * SynchronizerThreadDirect() -	master synchronization thread managing the
 *				local database.
 *
 *	This thread creates a subthread for each synchronization source.
 *	'cd' is a reference to our local database.
 */

static void 
SynchronizerThreadDirect(DBInfo *d)
{
    CLDataBase *cd;
    CLAnyMsg *msg;
    RouteInfo *r;
    pid_t pid;

    /*
     * receive HELLO from drd_database process (occurs after drd_database
     * has recovered the database).
     *
     * The HELLO will contain the initial SyncTs (synchronization timestamp)
     * and RepCount (number of replicated hosts we expect to see).
     */
    cd = getHead(&d->d_CDList);
    msg = MReadCLMsg(cd->cd_Ior);

    if (msg != NULL &&
	msg->cma_Pkt.cp_Cmd == CLCMD_HELLO &&
	msg->cma_Pkt.cp_Error == 0
    ) {
	rp_type_t type;
	rp_totalrep_t repCount;
	dbstamp_t dbid;

	/*
	 * Get replication group information from the local database.
	 *
	 * XXX recover d->d_RepGroupTrigTs from prior run to catch
	 * sys.repgroup mods received by the synchronizer but for which
	 * the syncts has not yet caught up.
	 */
	type = GetRepGroupInfo(d, cd, &dbid, &repCount);
	SetCLId(cd, dbid);

	/*
	 * Generate a route to this database (on MyHost), which propogates 
	 * throughout the spanning tree.
	 */
	dbwarning(
	    "Starting Synchronizer thread for %s syncts=%016qx mincts=%16qx blksize=%d\n",
	    d->d_DBName,
	    msg->a_HelloMsg.hm_SyncTs,
	    msg->a_HelloMsg.hm_MinCTs,
	    msg->a_HelloMsg.hm_BlockSize
	);
	UseDBInfo(d);
	r = AllocRouteInfo(NULL, &MyHost, d, 
		0, 1, repCount, type, 
		msg->a_HelloMsg.hm_BlockSize, 
		msg->a_HelloMsg.hm_SyncTs, msg->a_HelloMsg.hm_MinCTs
	    );

	/*
	 * Set initial value for r_SaveCTs
	 */
	r->r_SaveCTs = r->r_MinCTs;
	cd->cd_Route = r;
	DoneDBInfo(d);
	FreeCLMsg(msg);

	/*
	 * Note that other threads will write open-instance commands to
	 * this cd and wait for open-instance responses.  Our ability to
	 * operate on this cd is limited to the same thing.  We will be
	 * woken up when DF_SHUTDOWN is set, or when a route table change
	 * effecting synchronization occurs.
	 *
	 * XXX if unable to sync because of a Local MinCTs change, must
	 *     be notified when the local minCTs updates.
	 *
	 * At this point a USE ref remains on DBInfo.
	 */
	while ((d->d_Flags & DF_SHUTDOWN) == 0) {
	    int timeout;

	    timeout = DoSynchronization(cd);

	    /*
	     * If a peering change occured check to see whether our peering
	     * state has changed and shutdown (& restart later) if it has.
	     *
	     * XXX if database is shutdown during peering after receiving
	     * the raw records but before synchronizing, we have a problem.
	     */
	    if (d->d_Flags & DF_PEERCHANGE) {
		int ntype = GetRepGroupInfo(d, cd, &dbid, &repCount);
		dbinfo2("CHECK PEERAGE %d %d\n", ntype, type);
		if (ntype != type) {
		    dbinfo2("Peerage change %s: %d->%d\n",
			d->d_DBName, type, ntype);
		    d->d_Flags |= DF_SHUTDOWN | DF_RESTART;
		    break;
		}
		if (r->r_SyncTs > d->d_RepGroupTrigTs)
		    d->d_Flags &= ~DF_PEERCHANGE;
	    }
	    waitNotify(d->d_Synchronize, timeout);
	}
	dbwarning("Stopping database %s, waiting for pending syncs to complete\n", d->d_DBName);
	while (BackgroundSyncing(cd) != 0)
	    taskSleep(2000);

	/*
	 * Remove the database route and cleanup.  We drop the Refs to 0,
	 * but we cannot physically destroy the route until all slaves &
	 * synchronizers are through.
	 */
	dbwarning("Stopping database %s, removing route\n", d->d_DBName);
	UseRouteInfo(r);
	FreeRouteInfo(r);

	if (cd->cd_Refs == 1) {
	    dbwarning("Database %p %s quiescent, stopping immediately\n", 
		cd, 
		d->d_DBName
	    );
	} else {
	    dbwarning("Database %p %s is active (%d use), waiting for existing clients to finish\n", cd, d->d_DBName, cd->cd_Refs - 1);
	}
	{
	    int refs = cd->cd_Refs;
	    while (cd->cd_Refs != 1) {
		if (cd->cd_Refs != refs) {
		    refs = cd->cd_Refs;
		}
		dbwarning("%d Database %s is active (%d refs), continuing to wait for existing clients to finish\n", getpid(), d->d_DBName, cd->cd_Refs - 1);
		taskSleep(5000);
	    }
	}
	cd->cd_Route = NULL;
	UnuseRouteInfo(r);

	/*
	 * Clear out rh_ScanTs.  If we do not do this and the user removes and
	 * reinstantiates a peer or snapshot database, we will try to pick up
	 * our scans where we left off w/ the old database, which is wrong.
	 */
	{
	    RouteHostInfo *rh;

	    for (rh = d->d_RHInfoBase; rh; rh = rh->rh_DNext)
		rh->rh_ScanTs = 0;
	}
    } else {
	if (msg)
	    FreeCLMsg(msg);
    }
    if (cd) {
	/*
	 * Shutdown the message request channel, wait for the 
	 * drd_database daemon to terminate.
	 */
	if (cd->cd_Iow)
	    t_shutdown(cd->cd_Iow, SHUT_WR);
	while ((msg = MReadCLMsg(cd->cd_Ior)) != NULL)
	    FreeCLMsg(msg);

	pid = cd->cd_Pid;
	CloseCLDataBase(cd);
	if (--d->d_CDCount == 0)
	    d->d_Flags &= ~DF_LOCALRUNNING;
	while (waitpid(pid, NULL, 0) > 0)
	    ;
	dbwarning("Termination Complete\n");
    }
    d->d_Flags &= ~(DF_NOTIFIED|DF_SHUTDOWN);	/* XXX misplaced */
    DoneDBInfo(d);
}

/*
 * DoSynchronization() -	Synchronize remote databases to local database
 *
 *	'cd' represents a management link to the local database with which
 *	we can obtain database instances.  cd->cd_DBInfo represents the 
 *	database name under replicative management.
 *
 *	Our job is to synchronize zero or more remote copies of this database
 *	to our local copy and to update the local copy's SyncTs and RepCount
 *	as appropriate.  This in turn may indirectly wake up clients waiting
 *	for a SyncTs update in order to proceed after a commit failure.
 *
 *	Each copy in the system has a synchronization point, represented by
 *	SyncTs.  All data before this point is known to be fully up to date
 *	and synchronized, any data after this point requires synchronization.
 *	In order to be able to update our SyncTs we must query at least a
 *	quorum of remote databases to obtain their data up to their current
 *	MinCommitTs point (whatever that happens to be).  NOTE: the quorum
 *	only counts CTYPE_PEER hosts.  CTYPE_SNAP (snapshot) hosts are never
 *	counted in the quorum.
 *
 *	We can also unilaterally synchronize up to the SyncTs of any single
 *	remote database, whether it is a SNAP or a PEER, if our SyncTs is
 *	less then the remote database's SyncTs.  We generally do this if
 *	we have to catch-up to someone and we also run such updates serially
 *	rather then in parallel, based on the closest available host.
 *
 *	There is a special limitation when synchronizing to the MinCTs, and
 *	that is we cannot synchronize past our OWN MinCTs (as adjusted by
 *	pending Commit-1's), since this might result in a synchronization of
 *	another replication host's Commit-2 data followed by our replicator
 *	issuing the same Commit-2, resulting in duplicate data.
 *
 *	We can update our SyncTs when we are able to form a QUORUM of 
 *	databases.  We form a QUORUM out of the databases with the highest
 *	MinCommitTs points and then take lowest MinCommitTs of that quorum.
 *	If it is higher then our SyncTs, we can update our SyncTs to the
 *	new value. 
 *
 *	MinCommitTs is 'not quite the append ts' for any given database.  
 *	What it is is the lowest timestamp a database *MAY* create new 
 *	records with, governed by phase-1 commits that may already be in 
 *	progress but not complete.  Since non-conflicting commits may occur 
 *	out of order, a database may commit data at the last timestamp
 *	and then soon after commit previously negotiated data before the 
 *	last timestamp.
 */

static int
DoSynchronization(CLDataBase *cd)
{
    RouteInfo *ri = cd->cd_Route;
    RouteInfo *rbest;
    DBInfo *d = ri->r_RHInfo->rh_DBInfo;
    RouteHostInfo *rh;
    RouteHostInfo *rhNext;
    int timeout = 0;

    /*
     * Unilateral synchronization.  See if anyone has a better SyncTs
     * and synchronize with that one database to catch up.
     *
     * When synchronizing to SyncTs, we can use both PEERs and SNAPs.
     */
    rbest = NULL;
    for (rh = d->d_RHInfoBase; rh; rh = rhNext) {
	RouteInfo *r;
	dbstamp_t syncTs = 0;
	dbstamp_t minCTs = 0;

	rhNext = rh->rh_DNext;
	if (rh->rh_Host->h_Type != CTYPE_REPLIC) 	/* not rep host */
	    continue;
	if (rh->rh_Host == ri->r_Host) {		/* self route */
	    rh->rh_MinCTs = ri->r_MinCTs;
	    rh->rh_SyncTs = ri->r_SyncTs;
	    continue;
	}
	for (r = rh->rh_Routes; r; r = r->r_RHNext) {
	    if (r->r_Refs == 0) 			/* dead route */
		continue;
	    if (r->r_Type == CTYPE_PEER) {
		if (minCTs < r->r_MinCTs) 		/* best MinCTs */
		    minCTs = r->r_MinCTs;
	    } else {
		if (minCTs < r->r_SyncTs) 		/* use SyncTs */
		    minCTs = r->r_SyncTs;
	    }
	    if (syncTs < r->r_SyncTs)			/* best SyncTs */
		syncTs = r->r_SyncTs;
	    if (r->r_SyncTs <= ri->r_SyncTs)		/* no help */
		continue;
	    if (rbest == NULL || r->r_Hop < rbest->r_Hop ||
		(r->r_Hop == rbest->r_Hop && r->r_SyncTs > rbest->r_SyncTs)
	    ) {
		rbest = r;
	    }
	}
	rh->rh_SyncTs = syncTs;
	rh->rh_MinCTs = minCTs;
    }

    if (rbest) {
	rh = rbest->r_RHInfo;
	/*
	 * If we loose a link and get it back, our ScanTs for the remote DB
	 * will be reset to 0.  We obviously do not need to rescan all the
	 * records from the remote.  We can shortcut the start point to
	 * our own sync point assuming the remote database reaches that far.
	 *
	 * XXX we really need to clean this up
	 */
	if (rh->rh_ScanTs < ri->r_SyncTs) {
	    rh->rh_ScanTs = ri->r_SyncTs;
	    if (rh->rh_ScanTs > rh->rh_MinCTs)
		rh->rh_ScanTs = rh->rh_MinCTs;
	}
	dbinfo2("Unilaterial Synchronization of %s %016llx to %016llx\n", rh->rh_DBInfo->d_DBName, ri->r_SyncTs, rbest->r_SyncTs);
	UseRouteHostInfo(rh);
	StartSynchronizeWith(cd, rh);
	UnuseRouteHostInfo(rh);
	return(timeout);
    }

    /*
     * Look for remote databases with updated MinCTs's.  Make sure to
     * skip our local copy of the database (we don't need to synchronize
     * with ourselves!).
     *
     * Synchronize with all remote databases which have updated since
     * our last scan.
     */
    for (rh = d->d_RHInfoBase; rh; rh = rhNext) {
	RouteInfo *r;
	dbstamp_t minCTs = 0;
	dbstamp_t syncTs = 0;
	int valid = 0;

	rhNext = rh->rh_DNext;

	if (rh->rh_Host->h_Type != CTYPE_REPLIC) 	/* not rep host */
	    continue;
	if (rh->rh_Host == ri->r_Host) {		/* self route */
	    rh->rh_MinCTs = ri->r_MinCTs;
	    rh->rh_SyncTs = ri->r_SyncTs;
	    continue;
	}
	for (r = rh->rh_Routes; r; r = r->r_RHNext) {
	    if (r->r_Refs == 0) 			/* dead route */
		continue;
	    if (syncTs < r->r_SyncTs)			/* best SyncTs */
		syncTs = r->r_SyncTs;
	    if (r->r_Type == CTYPE_PEER) {
		if (minCTs < r->r_MinCTs)		/* best MinCTs */
		    minCTs = r->r_MinCTs;
		valid = 1;				/* valid for quorum */
	    } else {
		if (minCTs < r->r_SyncTs) 		/* use SyncTs */
		    minCTs = r->r_SyncTs;
	    }
	}

	/*
	 * Loop if no valid routes were found
	 */
	if (valid == 0)
	    continue;

	/*
	 * Otherwise update MinCTs and SyncTs (XXX need to do this at a lower
	 * level!), reference rh so it isn't ripped out from under us , and
	 * synchronize if necessary.
	 */
	UseRouteHostInfo(rh);
	rh->rh_MinCTs = minCTs;
	rh->rh_SyncTs = syncTs;

	/*
	 * If we loose a link and get it back, our ScanTs for the remote DB
	 * will be reset to 0.  We obviously do not need to rescan all the
	 * records from the remote.  We can shortcut the start point to
	 * our own sync point assuming the remote database reaches that far.
	 */
	if (rh->rh_ScanTs < ri->r_SyncTs) {
	    rh->rh_ScanTs = ri->r_SyncTs;
	    if (rh->rh_ScanTs > rh->rh_MinCTs)
		rh->rh_ScanTs = rh->rh_MinCTs;
	}

	/*
	 * Special case limitation to minCTs.  We cannot synchronize
	 * beyond our own reported MinCTs.
	 */
	if (minCTs > cd->cd_Route->r_MinCTs) {
	    minCTs = cd->cd_Route->r_MinCTs;
	    timeout = 10000;
	}

	/*
	 * Ok.  rh_ScanTs represents how much we have scanned from the
	 * remote so far.  minCTs represents how much we can scan.  If
	 * the remote has additional unscanned data we synchronize to it.
	 */
	if (minCTs > rh->rh_ScanTs)
	    StartSynchronizeWith(cd, rh);

	/*
	 * We did some complex stuff above, so context switches might have
	 * occured.  We must reload rh_Next before releasing rh.
	 */
	rhNext = rh->rh_DNext;
	UnuseRouteHostInfo(rh);
    }
    DoSynchronizationUpdateSyncTs(cd);
    return(timeout);
}

static void
DoSynchronizationUpdateSyncTs(CLDataBase *cd)
{
    RouteInfo *ri = cd->cd_Route;
    DBInfo *d = ri->r_RHInfo->rh_DBInfo;
    RouteHostInfo *rh;
    int repCount;	/* number of replication peers */
    int quorum;		/* quorum calculation */
    int count;
    dbstamp_t ts;
    dbstamp_t *qts;
    dbstamp_t hits;

    /*
     * Figure out the number of replication PEERs.  Count the ones we
     * see, look at what was returned from sys.repgroup (ri->r_RepCount),
     * take the higher of the two.
     */
    repCount = 0;
    for (rh = d->d_RHInfoBase; rh; rh = rh->rh_DNext) {
	RouteInfo *r;

	if (rh->rh_Host->h_Type != CTYPE_REPLIC)
	    continue;

	for (r = rh->rh_Routes; r; r = r->r_RHNext) {
	    if (r->r_Refs == 0)
		continue;
	    if (r->r_Type == CTYPE_PEER)
		break;
	}
	if (r)
	    ++repCount;
    }
    if (repCount < ri->r_RepCount)
	repCount = ri->r_RepCount;

    /*
     * Attempt to update our synchronization timestamp.  In order to do
     * so we must take the lowest MinCTs we find out of a quorum of 
     * databases (counting our own database as well).  We use the most
     * beneficial quorum that we can find... the ones with the highest
     * MinCTs's.
     *
     * Only PEERs count when calculating the quorum.
     *
     * We can also synchronization up to the SyncTs of ANY database
     * (PEER or SNAP) without requiring a quorum.
     */

    count = 0;
    hits = 0;
    quorum = repCount / 2 + 1;

    qts = malloc(sizeof(dbstamp_t) * quorum);
    bzero(qts, sizeof(dbstamp_t) * quorum);

    for (rh = d->d_RHInfoBase; rh; rh = rh->rh_DNext) {
	RouteInfo *r;
	int addCount = 0;
	
	if (rh->rh_Host->h_Type != CTYPE_REPLIC) 	/* not rep host */
	    continue;
#if 0
	if (rh->rh_Host == ri->r_Host)			/* deal w/ self later */
	    continue;
#endif
	for (r = rh->rh_Routes; r; r = r->r_RHNext) {
	    if (r->r_Refs == 0) 			/* dead route */
		continue;
	    if (r->r_Type == CTYPE_PEER) {
		if (hits < r->r_MinCTs)			/* highest MinCTs */
		    hits = r->r_MinCTs;
		addCount = 1;
	    } else {
		if (hits < r->r_SyncTs)			/* highest SyncTs */
		    hits = r->r_SyncTs;
	    }
	}
	if (addCount) {
	    int i;
	    for (i = 0; i < quorum; ++i) {
		if (rh->rh_Host == ri->r_Host) {
		    if (rh->rh_MinCTs >= qts[i]) {
			bcopy(qts + i, qts + i + 1, (quorum - i - 1) * sizeof(dbstamp_t));
			qts[i] = rh->rh_MinCTs;
			break;
		    }
		} else {
		    if (rh->rh_ScanTs >= qts[i]) {
			bcopy(qts + i, qts + i + 1, (quorum - i - 1) * sizeof(dbstamp_t));
			qts[i] = rh->rh_ScanTs;
			break;
		    }
		}
	    }
	    count += addCount;
	}
    }

    /*
     * ts is the worst of the best quorum.  ts is invalid if count
     * did not reach a quorum.
     */

    ts = qts[quorum-1];

    dbinfo2("CLDB %p Count %d quorum %d sync %016qx/%016qx/%016qx\n", 
	cd, count, quorum, ri->r_SyncTs, ts, hits);

    /*
     * Raise our SaveCTs (the 'real' commit timestamp for new queries) to
     * the highest replicated MinCTs.  Our reported MinCTs 
     * (UpdateRouteMinCTs()) may wind up being lower due to in-progress
     * commits.
     *
     * This will cause peers to stabilize their MinCTs at the highest
     * one set so far which in turn allows the peers to stabilize their
     * SyncTs's.  The MinCTs can be raised due to a commit phase-1 even
     * if no records are actually committed, so this synchronization is
     * important.
     */
    if (ri->r_SaveCTs < hits) {
	ri->r_SaveCTs = hits;
	UpdateRouteMinCTs(cd);
    }

    /*
     * Sanity, the scan should have been limited appropriately
     */
    DBASSERT(ts <= ri->r_MinCTs);

    /*
     * If we have a quorum of responses, we can update the synchronization
     * point.  Even though we may have returned MinCTs's below the new
     * synchronization point, due to the quorum calculation we are 
     * guarenteed that no client will actually commit with a timestamp below
     * the synchronization point.  We have to retest after potentially
     * blocking in getTLock().
     */
    if (quorum && count >= quorum && ri->r_SyncTs < ts) {
	getTLock(cd->cd_Lock);
	if (quorum && count >= quorum && ri->r_SyncTs < ts) {
	    /*
	     * Update the synchronization timestamp.  If sys.repgroup
	     * adjustments might possibly have occured, set CDF_PEERCHANGE
	     * to force a check.
	     */
	    CLAnyMsg *msg;

	    if (ri->r_SyncTs < d->d_RepGroupTrigTs)
		d->d_Flags |= DF_PEERCHANGE;
	    ri->r_SyncTs = ts;
	    msg = BuildCLMsg(CLCMD_UPDATE_SYNCTS, sizeof(CLStampMsg));
	    msg->a_StampMsg.ts_Stamp = ts;
	    WriteCLMsg(cd->cd_Iow, msg, 1);
	    relTLock(cd->cd_Lock);
	    NotifyAllLinks(ri, ri->r_Hop);
	} else {
	    relTLock(cd->cd_Lock);
	}
    }
    free(qts);
}

/*
 * StartSynchronizeWith() -	Setup synchronizing thread
 */
static void
StartSynchronizeWith(CLDataBase *parCd, RouteHostInfo *rh)
{
    CLDataBase *cd;

    /*
     * See if we already have a synchronizer thread running for
     * this guy.  If so, just wake him up.
     */
    for (cd = getHead(&parCd->cd_List);
	 cd;
	 cd = getListSucc(&parCd->cd_List, &cd->cd_Node)
    ) {
	if (cd->cd_SyncRouteHost == rh && (cd->cd_Flags & CDF_SYNCDONE) == 0) {
	    cd->cd_Flags |= CDF_SYNCPEND;
	    issueNotify(cd->cd_NotifyInt);
	    return;
	}
    }

    /*
     * Otherwise create a new synchronizer thread.
     */
    cd = OpenCLInstance(parCd, NULL, CLTYPE_RW);
    if (cd != NULL) {
	cd->cd_SyncRouteHost = rh;
	cd->cd_NotifyInt = allocNotify();
	UseRouteHostInfo(rh);
	taskCreate(SynchronizeWithThread, cd);
    }
}

static int
BackgroundSyncing(CLDataBase *parCd)
{
    CLDataBase *cd;
    int count = 0;

    for (cd = getHead(&parCd->cd_List);
	 cd;
	 cd = getListSucc(&parCd->cd_List, &cd->cd_Node)
    ) {
	if (cd->cd_SyncRouteHost != NULL)
	    ++count;
    }
    return(count);
}

/*
 * SynchronizeWith() -		Synchronize with remote database
 *
 *	cd is an instance of our local database, rs represents
 *	the route to the remote database to synchronize with.
 */
static void
SynchronizeWithThread(CLDataBase *cd)
{
    static int IId = 1;
    InstInfo *ii;
    InstInfo *mi;
    RPAnyMsg *rpMsg;
    int error;
    int closed;
    CLDataBase *parCd = cd->cd_Parent;
    RouteHostInfo *rh = cd->cd_SyncRouteHost;
    const char *dbName = rh->rh_DBInfo->d_DBName;

    /*
     * Get the master VC manager, don't do anything if it has failed
     */
loop:
    error = 0;
    closed = 0;

    /* XXX */
    dbinfo2("RUNNING SYNCHRONIZATION FOR %s with %s\n", dbName, rh->rh_Host->h_HostName);

    mi = VCManagerMasterKick(&MyHost, rh->rh_Host);
    if (mi->i_Flags & IIF_FAILED) {
	dbwarning("Synchronizewith: MASTER MANAGER FOR %s FAILED\n",
		rh->rh_Host->h_HostName
	);
	goto finished;
    }

    /*
     * Allocate a new instance id
     */
    do {
	if ((IId = (IId - 1) & 0x7FFFFFFF) == 0)
	    IId = 0x7FFFFFFF;
    } while ((ii = FindInstInfo(&MyHost, rh->rh_Host, IIF_MASTER, IId, mi->i_VCId)) != NULL);

    ii = AllocInstInfo(mi, &MyHost, 
	    rh->rh_Host, rh->rh_DBInfo, IIF_MASTER | IIF_PRIVATE, IId);

    {
	int len = strlen(dbName) + 1;

	rpMsg = BuildVCPkt(ii, RPCMD_OPEN_INSTANCE, offsetof(RPOpenInstanceMsg, oi_DBName[len]));
	bcopy(dbName, rpMsg->a_OpenInstanceMsg.oi_DBName, len);
    }
    WriteVCPkt(rpMsg);

    /*
     * Process the return code
     */
retry:
    while ((rpMsg = remHead(ii->i_RXPktList)) == NULL) {
	waitNotify(ii->i_NotifyInt, 0);
    }

    switch(rpMsg->rpa_Pkt.pk_Cmd) {
    case RPCMD_CLOSE_INSTANCE|RPCMDF_REPLY:
	error = -1;
	closed = 1;
        break;
    case RPCMD_OPEN_INSTANCE|RPCMDF_REPLY:
	if ((error = rpMsg->rpa_Pkt.pk_RCode) < 0) {
	    closed = 1;
	    break;
	}
	/* fall through */
    case RPCMD_UPDATE_INSTANCE|RPCMDF_REPLY:
        /*
         * New instance open return packet, returns minimum commit
         * timestamp and sync timestamp in the payload.  We queued
         * pending messages when we initialized the InstInfo, but
         * we didn't start running any of them, so start running
         * the list.
         */
	if (rpMsg->rpa_Pkt.pk_Cmd == (RPCMD_UPDATE_INSTANCE|RPCMDF_REPLY))
	    goto retry;
        break;
    }
    FreeRPMsg(rpMsg);

    while (error == 0) {
	CLAnyMsg *clMsg;
	dbstamp_t scanTs = rh->rh_ScanTs;
	dbstamp_t minCTs = rh->rh_MinCTs;
	dbstamp_t rsyncTs = rh->rh_SyncTs;
	dbstamp_t readLTs = 0;
	int done = 0;
	int didwr = 0;
	rhflags_t lastOp = 0;
	int count = 0;
	int continueReading = 0;
	int trackingSys = 0;

	DBASSERT(closed == 0);

	dbinfo2("Synchronize to %s:%s %016qx-%016qx (syncts %016qx)",
	    rh->rh_Host->h_HostName, 
	    rh->rh_DBInfo->d_DBName,
	    scanTs,
	    minCTs,
	    rh->rh_SyncTs
	);

	/*
	 * We cannot safely synchronize beyond the smallest 
	 * in-progress commit timestamp or we might race
	 * against commit phase-2 and generate duplicate
	 * data in the physical database.
	 */
	if (minCTs > parCd->cd_Route->r_MinCTs) {
	    char *alloc = NULL;

	    minCTs = parCd->cd_Route->r_MinCTs;
	    dbstamp_to_ascii_simple(minCTs, 0, &alloc);
	    dbinfo2(" (SPECIAL LIMIT %016qx %s)", minCTs, alloc);
	    free(alloc);
	}
	DBASSERT(minCTs >= scanTs);
	dbinfo2("\n");

	/*
	 * Start rawread on remote from scanTs to minCTs.  On completion
	 * the acknowledgement packet will set minCTs to the scan point
	 * the operation actually ended on.
	 */
	rpMsg = BuildVCPkt(ii, RPCMD_RAWREAD, sizeof(RPRawReadMsg));
	rpMsg->a_RawReadMsg.rr_StartTs = scanTs;
	rpMsg->a_RawReadMsg.rr_EndTs = minCTs;
	WriteVCPkt(rpMsg);

	/*
	 * Read results from remote
	 */
	while (!done) {
	    while ((rpMsg = remHead(ii->i_RXPktList)) == NULL)
		waitNotify(ii->i_NotifyInt, 0);

	    switch(rpMsg->rpa_Pkt.pk_Cmd) {
	    case RPCMD_RAWDATAFILE|RPCMDF_REPLY:
		/*
		 * Initiate a raw-write on our local database
		 * when we encounter the first table file that may
		 * have to be written.
		 */
		if (didwr == 0) {
		    didwr = 1;
		    clMsg = BuildCLMsg(CLCMD_RAWWRITE, sizeof(CLRawWriteMsg));
		    clMsg->a_RawWriteMsg.rw_StartTs = scanTs;
		    clMsg->a_RawWriteMsg.rw_EndTs = minCTs;
		    WriteCLMsg(cd->cd_Iow, clMsg, 1);
		}
		/*
		 * For each table file initiate a rawdatafile command.
		 *
		 * Note: we are passing the contents of CLRawDataFileMsg
		 * (e.g. like rdf_BlockSize) directly from the RPPkt.
		 */
		{
		    int msgLen = rpMsg->rpa_Pkt.pk_Bytes - sizeof(RPPkt);
		    int len;
		    DBASSERT(msgLen > 0);
		    DBASSERT(rpMsg->rpa_Pkt.pk_Data[msgLen-1] == 0);
		    len = strlen(rpMsg->a_RawDataFileMsg.rdf_FileName) + 1;
		    clMsg = BuildCLMsg(CLCMD_RAWDATAFILE, offsetof(CLRawDataFileMsg, rdf_FileName[len]));
		    clMsg->a_RawDataFileMsg.rdf_BlockSize = rpMsg->a_RawDataFileMsg.rdf_BlockSize;
		    bcopy(rpMsg->a_RawDataFileMsg.rdf_FileName, clMsg->a_RawDataFileMsg.rdf_FileName, len);
		    WriteCLMsg(cd->cd_Iow, clMsg, 1);
		}
		/*
		 * Specifically track SYS table changes.
		 */
		if (strcmp(rpMsg->a_RawDataFileMsg.rdf_FileName, "sys") == 0) {
		    trackingSys = 1;
		} else {
		    trackingSys = 0;
		}
		lastOp = 0;
		break;
	    case RPCMD_RAWDATA|RPCMDF_REPLY:
		/*
		 * Data in reply, collect
		 */
		DBASSERT(didwr != 0);
		{
		    RecHead *rec = (RecHead *)(&rpMsg->a_Msg + 1);

		    DBASSERT(rpMsg->rpa_Pkt.pk_Bytes >= sizeof(RPPkt) + sizeof(RecHead));
		    DBASSERT(rpMsg->rpa_Pkt.pk_Bytes >= offsetof(RPPkt, pk_Data[rec->rh_Size]));
		    if (rec->rh_Flags & RHF_UPDATE)
			DBASSERT(lastOp & RHF_DELETE);
		    lastOp = rec->rh_Flags;

		    clMsg = BuildCLMsg(CLCMD_RAWDATA, sizeof(CLMsg) + rec->rh_Size);
		    bcopy(rec, (char *)(&clMsg->a_Msg + 1), rec->rh_Size);
		    WriteCLMsg(cd->cd_Iow, clMsg, 0);
		    ++count;
		    DBASSERT(rec->rh_Stamp >= scanTs && rec->rh_Stamp < minCTs);

		    /*
		     * Track sys.repgroup changes.  This is a two-stage
		     * operation.  If we detect a sys.repgroup change we 
		     * set the (highest) triggering timestamp, but we cannot
		     * actually check until our synchronization point moves
		     * past this point.  To avoid having to do a lot of 
		     * tracking, we will check every time the sync point
		     * moves until it gets ahead of d_RepGroupTrigTs.
		     */
		    if (trackingSys && rec->rh_VTableId == VT_SYS_REPGROUP) {
			if (rh->rh_DBInfo->d_RepGroupTrigTs < rec->rh_Stamp) {
			    rh->rh_DBInfo->d_RepGroupTrigTs = rec->rh_Stamp;
			}
		    }
		}
		break;
	    case RPCMD_RAWREAD|RPCMDF_REPLY:
		/*
		 * End of reply.  If there were no data records
		 * we can update rh_ScanTs now.  If there were
		 * data records we have to wait until our write
		 * termination is acknowledged.
		 */
		done = 1;
		lastOp = 0;
		if (didwr) {
		    didwr = 2;
		    DBASSERT(rpMsg->a_RawReadMsg.rr_EndTs <= minCTs);
		    readLTs = rpMsg->a_RawReadMsg.rr_EndTs;
		} else {
		    DBASSERT(rpMsg->a_RawReadMsg.rr_EndTs <= minCTs);
		    rh->rh_ScanTs = rpMsg->a_RawReadMsg.rr_EndTs;
		    if (rpMsg->a_RawReadMsg.rr_EndTs != minCTs) {
			dbinfo2("CONTINUE READING %016qx/%016qx\n", rpMsg->a_RawReadMsg.rr_EndTs, minCTs);
			continueReading = 1;
		    }
		}
		break;
	    case RPCMD_CLOSE_INSTANCE|RPCMDF_REPLY:
		/*
		 * XXX failure
		 */
		done = 1;
		closed = 1;
		error = -1;
		break;
	    case RPCMD_UPDATE_INSTANCE|RPCMDF_REPLY:
		break;
	    }
	    if (rpMsg)
		FreeRPMsg(rpMsg);
	    taskQuantum();
	}
	if (didwr && error == 0) {
	    /*
	     * Send RAWWRITE_END response, assuming we didn't get an
	     * unexpected close.
	     */
	    clMsg = BuildCLMsg(CLCMD_RAWWRITE_END, sizeof(CLRawWriteEndMsg));
	    if (closed == 0)
		clMsg->a_RawWriteEndMsg.re_EndTs = minCTs;
	    WriteCLMsg(cd->cd_Iow, clMsg, 1);

	    if ((clMsg = MReadCLMsg(cd->cd_Ior)) != NULL) {
		DBASSERT(clMsg->cma_Pkt.cp_Cmd == CLCMD_RAWWRITE_END);
		if (clMsg->cma_Pkt.cp_Error < 0) {
		    dberror("Synchronization failed error %d\n",
			clMsg->cma_Pkt.cp_Error
		    );
		} else {
		    RouteInfo *r = parCd->cd_Route;

		    dbinfo2("Synchronization %d/%d records, to %016qx!\n",
			count,
			clMsg->cma_Pkt.cp_Error,
			clMsg->a_RawWriteEndMsg.re_EndTs
		    );
		    /*
		     * Update scan point
		     */
		    DBASSERT(clMsg->a_RawWriteEndMsg.re_EndTs <= minCTs);
		    DBASSERT(readLTs != 0);
		    rh->rh_ScanTs = readLTs;
		    if (readLTs != minCTs) {
			dbinfo2("CONTINUE READING %016qx/%016qx\n", readLTs, minCTs);
			continueReading = 1;
		    }

		    /*
		     * Update the minimum commit time stamp of our local
		     * database.
		     */
		    if (r->r_SaveCTs < rh->rh_ScanTs) {
			r->r_SaveCTs = rh->rh_ScanTs;
			UpdateRouteMinCTs(parCd);
		    }
		    /*
		     * rh->rh_ScanTs represents how far we've scanned the
		     * remote host, capped at the remote host's syncTs.
		     */
		    if (rsyncTs > rh->rh_ScanTs)
			rsyncTs = rh->rh_ScanTs;

		    /*
		     * If we have data from the remote host that
		     * exceeds our current sync point and is within the
		     * bounds of the remote host's own sync point, we can 
		     * 'skip' our sync point to the newScanTs.  Normally
		     * we can only sync to the lowest ScanTs in the best
		     * quorum of hosts we can find.
		     *
		     * We have to retest after the getTLock().
		     */
		    if (r->r_SyncTs < rsyncTs) {
			CLAnyMsg *clMsg2;

			getTLock(parCd->cd_Lock);
			if (r->r_SyncTs < rsyncTs) {
			    /*
			     * Update the synchronization timestamp.  If
			     * sys.repgroup adjustments might possibly have
			     * occured, set CDF_PEERCHANGE to force a check.
			     */
			    dbinfo2("QUICK UPDATE SYNC POINT TO %016qx\n",
				rsyncTs);
			    if (r->r_SyncTs < rh->rh_DBInfo->d_RepGroupTrigTs)
				rh->rh_DBInfo->d_Flags |= DF_PEERCHANGE;
			    r->r_SyncTs = rsyncTs;
			    if (r->r_MinCTs < r->r_SyncTs)
				UpdateRouteMinCTs(parCd);
			    clMsg2 = BuildCLMsg(CLCMD_UPDATE_SYNCTS, sizeof(CLStampMsg));
			    clMsg2->a_StampMsg.ts_Stamp = rsyncTs;

			    /*
			     * parCd is the main db open, thus several threads
			     * may be operating on it.  One-way commands are ok
			     * but we have to lock.
			     */
			    WriteCLMsg(parCd->cd_Iow, clMsg2, 1);
			    relTLock(parCd->cd_Lock);
			    NotifyAllLinks(r, r->r_Hop);
			} else {
			    relTLock(parCd->cd_Lock);
			}
		    }
		}
		FreeCLMsg(clMsg);
	    }
	}

	DoSynchronizationUpdateSyncTs(parCd);
	/*
	 * If no error wait for a wakeup
	 */
	if (error == 0 && !continueReading) {
	    cd->cd_Flags |= CDF_SYNCWAITING;
	    waitNotify(cd->cd_NotifyInt, 10000);
	    cd->cd_Flags &= ~CDF_SYNCWAITING;
	    if ((cd->cd_Flags & CDF_SYNCPEND) == 0)
		break;
	    cd->cd_Flags &= ~CDF_SYNCPEND;
	}
    }

    /*
     * XXX close the instance
     */
    if (closed == 0) {
	rpMsg = BuildVCPkt(ii, RPCMD_CLOSE_INSTANCE, sizeof(RPMsg));
	WriteVCPkt(rpMsg);
	while (closed == 0) {
	    while ((rpMsg = remHead(ii->i_RXPktList)) == NULL)
		waitNotify(ii->i_NotifyInt, 0);
	    if (rpMsg->rpa_Pkt.pk_Cmd == (RPCMD_CLOSE_INSTANCE|RPCMDF_REPLY))
		closed = 1;
	    FreeRPMsg(rpMsg);
	}
    }
    FreeInstInfo(ii);
finished:
    DoSynchronizationUpdateSyncTs(parCd);
    if (cd->cd_Flags & CDF_SYNCPEND) {
	cd->cd_Flags &= ~CDF_SYNCPEND;
	goto loop;
    }
    cd->cd_Flags |= CDF_SYNCDONE;
    dbwarning("*** TASK EXIT FOR %s\n", cd->cd_SyncRouteHost->rh_Host->h_HostName);
    UnuseRouteHostInfo(cd->cd_SyncRouteHost);
    cd->cd_SyncRouteHost = NULL;
    CloseCLInstance(cd);
}

/*
 * GetRepGroupInfo() -	Obtain replication group info for this host/database
 *
 *	Extract the database id and peering arrange (snapshot or peer)
 *	from the database.  Note that using 'roSyncTs' translates roughly
 *	into 'use the state of the database as of the most recent 
 *	synchronization point'.
 */

rp_type_t
GetRepGroupInfo(DBInfo *d, CLDataBase *cd, dbstamp_t *dbid, rp_totalrep_t *rcount)
{
    CLDataBase *dcd;
    dbstamp_t syncTs = 0;
    rp_type_t type = CTYPE_SNAP;

    *dbid = 0;
    if ((dcd = OpenCLInstance(cd, &syncTs, CLTYPE_RW)) != NULL) {
	res_t res;
	char *qry;
	int error = 0;

	/*
	 * Deal with degenerate condition if no synchronization has yet
	 * occured.
	 */
	PushCLTrans(dcd, syncTs, CPF_READONLY);
	safe_asprintf(
	    &qry, 
	    "SELECT HostId, HostType FROM sys.repgroup WHERE HostName = '%s'",
	    MyHost.h_HostName
	);
	res = QueryCLTrans(dcd, qry, &error);
	if (res) {
	    const char **row;
	    if ((row = ResFirstRow(res)) != NULL) {
		if (row[0])
		    *dbid = strtol(row[0], NULL, 0);
		if (row[1]) {
		    if (strcmp(row[1], "PEER") == 0) {
			type = CTYPE_PEER;
		    } else if (strcmp(row[1], "SNAP") == 0) {
			type = CTYPE_SNAP;
		    } else {
			dberror(
			    "GetRepGroupInfo: Unknown type %s\n",
			    row[1]
			);
		    }
		}
	    }
	    FreeCLRes(res);
	}
	free(qry);

	safe_asprintf(&qry, "COUNT FROM sys.repgroup WHERE HostType = 'PEER'");
	res = QueryCLTrans(dcd, qry, &error);
	if (res)
	    FreeCLRes(res);
	free(qry);
	if (error >= 0)
	    *rcount = error;
	else
	    *rcount = 0;
	dbwarning("Replicator %s count %d\n", d->d_DBName, (int)*rcount);
	AbortCLTrans(dcd);
	CloseCLInstance(dcd);
    }
    return(type);
}

