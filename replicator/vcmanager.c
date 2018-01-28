/*
 * REPLICATOR/VCMANAGER.C - Manage the VC (virtual circuit) to another host
 *
 * (c)Copyright 2000-2002 Backplane, Inc.  Please refer to the COPYRIGHT
 * file at the base of the distribution tree.
 *
 * $Backplane: rdbms/replicator/vcmanager.c,v 1.41 2002/08/20 22:06:00 dillon Exp $
 */

#include "defs.h"

Prototype InstInfo *VCManagerMasterKick(HostInfo *master, HostInfo *slave);
Prototype int VCManagerMasterCheck(HostInfo *master, HostInfo *slave, rp_vcid_t vcid);
Prototype void VCManagerSlaveThread(InstInfo *ii);
Prototype InstInfo *VCManagerSlaveKick(HostInfo *master, HostInfo *slave, rp_vcid_t vcid);
Prototype void VCNotifyClientsByRH(RouteHostInfo *rh, int remoteGoingAway);

static void VCManagerMasterThread(InstInfo *ii);
static void VCNotifyFailedClients(InstInfo *mi);
static void VCNotifyHostUp(HostInfo *h);
static void VCShutdownMasters(HostInfo *h);
static void VCShutdownSlaves(HostInfo *h);
static rp_vcid_t BumpSeq(InstInfo *ii);
static void VCResetVCId(HostInfo *mh, HostInfo *sh);

#define MAXMISS		5	/* slots missed before VC reset */
#define MISSTS		5000	/* miss time slot time, milliseconds */

/*
 * VCManagerMasterThread() - Manage the master side of a VC from MyHost
 *			     to some slave host.
 *
 *	The master sticks around as long as we have instances that are
 *	trying to talk to the remote slave.  Keepalives are exchanged 
 *	over a special instance.
 */

static void
VCManagerMasterThread(InstInfo *ii)
{
    int missCount = 0;
    HostInfo *h = ii->i_SlaveHost;
    struct timeval tv1;

    dbinfo2("Starting VC MASTER Manager for %s\n", h->h_HostName);

    gettimeofday(&tv1, NULL);
    tv1.tv_sec -= MISSTS / 1000 + 1;	/* force initial ping */

    while (ii->i_Refs > 1 || h->h_Refs) {
	RPAnyMsg *rpMsg;
	int gotPing = 0;
	int sentPing = 0;
	int ms;
	struct timeval tv2;

	/*
	 * Build and transmit a keepalive packet every
	 * MISSTS milliseconds.
	 */
	gettimeofday(&tv2, NULL);
	ms = (tv2.tv_usec + 1000000 - tv1.tv_usec) / 1000 + (tv2.tv_sec - tv1.tv_sec - 1) * 1000;
	if (ms < -MISSTS || ms >= MISSTS) {
	    rpMsg = BuildVCPkt(ii, RPCMD_KEEPALIVE, sizeof(RPMsg));
	    WriteVCPkt(rpMsg);
#ifdef PINGDEBUG
	    printf("*** PING %s\n", ii->i_RHSlave->rh_Host->h_HostName);
#endif
	    tv1 = tv2;
	    ms = 0;
	    sentPing = 1;
	} else if (ms < 0) {
	    ms = 0;
	}

	/*
	 * Wait for activity
	 */
	waitNotify(ii->i_NotifyInt, MISSTS - ms);

	/*
	 * Receive a packet either to our master instance (ii), which
	 * will always be a keepalive, or to an idle instance that we
	 * have attempted to close.
	 */
	while ((rpMsg = remHead(ii->i_RXPktList)) != NULL) {
	    if (rpMsg->a_Msg.rp_InstInfo == ii) {
		/*
		 * ping packet to our master ii
		 */
		FreeRPMsg(rpMsg);
		gotPing = 1;
#ifdef PINGDEBUG
		printf("*** PONG1 %s\n", ii->i_RHSlave->rh_Host->h_HostName);
#endif
	    } else {
		/*
		 * close response to our close request.  IIF_IDLE should
		 * no longer be set.
		 */
		InstInfo *idle = rpMsg->a_Msg.rp_InstInfo;
		DBASSERT((idle->i_Flags & IIF_IDLE) == 0);
#ifdef PINGDEBUG
		printf("*** PONG2 %s\n", ii->i_RHSlave->rh_Host->h_HostName);
#endif
	    }
	}
	if (gotPing) {
	    missCount = 0;
	} else if (sentPing) {
	    ++missCount;
	    if (missCount > MAXMISS)
		missCount = MAXMISS;
	}

	/*
	 * Handle VC failure / reforging based on missCount.  We special
	 * case the situation where we lose all routes to the host in order
	 * to optimize fast-recovery of any ongoing transactions.
	 */
	if (missCount == 0) {
	    /*
	     * Good ping pong
	     */
	    if (ii->i_Flags & IIF_FAILED) {
		dbinfo2("MASTER Manager REFORGED for %s\n", h->h_HostName);
		ii->i_Flags &= ~IIF_FAILED;
		VCNotifyFailedClients(ii);
		VCNotifyHostUp(h);
	    }
	} else if (h->h_Refs == 0) {
	    /*
	     * Lost all routes to host, implement fast-fail
	     */
	    if ((ii->i_Flags & IIF_FAILED) == 0) {
		ii->i_Flags |= IIF_FAILED;
		VCShutdownMasters(ii->i_SlaveHost);
		ii->i_VCId = BumpSeq(ii);
		dberror("MASTER Manager FAILED1 for %s\n", h->h_HostName);
	    }
	    missCount = 0;
	} else if (missCount == MAXMISS) {
	    /*
	     * Maximum retries, fail host and restart counter
	     */
	    if (ii->i_Flags & IIF_FAILED) {
		missCount = 0;
		ii->i_VCId = BumpSeq(ii);
		dberror("Retry Master VC to %s to VCID %04x\n", h->h_HostName, (int)ii->i_VCId);
	    } else {
		ii->i_Flags |= IIF_FAILED;
		VCShutdownMasters(ii->i_SlaveHost);
		ii->i_VCId = BumpSeq(ii);
		dberror("MASTER Manager FAILED2 for %s, new VCID %04x\n", h->h_HostName, (int)ii->i_VCId);
	    }
	    missCount = 0;
	} 
    }

    /*
     * Reset VC so next connection doesn't get confused XXX
     */
    ii->i_Flags |= IIF_FAILED;
    BumpSeq(ii);

    dbinfo2("INST %p Stopping VC MASTER Manager for %s (%d:%d)\n", 
	ii,
	h->h_HostName,
	ii->i_Refs, ii->i_UseCount
    );
    FreeInstInfo(ii);
}

InstInfo *
VCManagerMasterKick(HostInfo *master, HostInfo *slave)
{
    InstInfo *ii;

    DBASSERT(master == &MyHost);

    /*
     * Find special management instance for MyHost->slave,
     * starting one if none could be found.
     */
    if ((ii = FindInstInfo(master, slave, IIF_MASTER, 0, 0)) == NULL) {
	ii = AllocInstInfo(NULL, master, slave, 
		NULL, IIF_MASTER | IIF_PRIVATE, 0);
	ii->i_Flags |= IIF_FAILED;
	taskCreate(VCManagerMasterThread, ii);
	ii->i_VCId = BumpSeq(ii);
    }
    return(ii);
}

static rp_vcid_t
BumpSeq(InstInfo *ii)
{
    SeqInfo *s;
    rp_vcid_t vcid;
    RPAnyMsg *rpMsg;

    /*
     * Clear any pending packets
     */
    while ((rpMsg = remHead(ii->i_RXPktList)) != NULL)
	FreeRPMsg(rpMsg);

    /*
     * Bump the VCID.
     */
    s = AllocSeqInfo(ii->i_MasterHost, ii->i_SlaveHost, 0);
    vcid = (rp_vcid_t)time(NULL);
    if (vcid == s->s_VCId)
	++vcid;
    s->s_VCId = vcid;
    s->s_Other->s_VCId = vcid;
    s->s_SeqNo = 0;
    s->s_Other->s_SeqNo = (rp_seq_t)-1;
    FreeSeqInfo(s);
    return(vcid);
}

static void
VCResetVCId(HostInfo *mh, HostInfo *sh)
{
    SeqInfo *s;

    s = AllocSeqInfo(mh, sh, 0);
    s->s_VCId = 0;
    s->s_Other->s_VCId = 0;
    FreeSeqInfo(s);
}

/*
 * VCManagerMasterCheck() - check incoming VCId against current master state
 *
 *	Returns -1 on failure, 0 on success.
 */

int 
VCManagerMasterCheck(HostInfo *master, HostInfo *slave, rp_vcid_t vcid)
{
    InstInfo *ii;

    DBASSERT(master == &MyHost);
    if ((ii = FindInstInfo(master, slave, IIF_MASTER, 0, 0)) == NULL)
	return(-1);
    if (ii->i_Flags & IIF_FAILED)
	return(-1);
    return(0);
}

/*
 * VCManagerSlaveThread() - Implements the slave side manager for a VC.
 *
 *	The slave sticks around as long as there is a host route to the
 *	master or a slave instance still operating.
 */

void
VCManagerSlaveThread(InstInfo *ii)
{
    int missCount = 0;
    HostInfo *h = ii->i_MasterHost;

    /*
     * As long as there are live instances or (route) references to
     * the host, stick around.
     */
    dbinfo2("INST %p Starting VC SLAVE Manager for %s\n",
	ii, h->h_HostName);

    while (h->h_Refs > 0 || ii->i_Refs > 1) {
	RPAnyMsg *rpMsg;

	/*
	 * Ignore the link if its marked failed -- wait for a 
	 * VCMangerSlaveCheck() call to bring it back up.  This state
	 * may occur if we are still receiving packets on a failed
	 * VC.  We do not want to return the ping, because the link
	 * is dead.  We have to wait for the master to reforge it.
	 */
	if (ii->i_Flags & IIF_FAILED) {
	    waitNotify(ii->i_NotifyInt, 0);
	    continue;
	}

	waitNotify(ii->i_NotifyInt, MISSTS + 1000);

	if (ii->i_Flags & IIF_RESET) {
	    ii->i_Flags &= ~IIF_RESET;
	    missCount = 0;
	} else if (missCount != MAXMISS) {
	    ++missCount;
	}

	while ((rpMsg = remHead(ii->i_RXPktList)) != NULL) {
	    missCount = 0;
	    /* XXX assert ping packet? */
	    ReturnVCPkt(ii, rpMsg, sizeof(RPMsg), 0);
#ifdef PINGDEBUG
	    printf("PING RETURN TO %s\n", h->h_HostName);
#endif
	}
	if (missCount == MAXMISS) {
	    if ((ii->i_Flags & IIF_FAILED) == 0) {
		ii->i_Flags |= IIF_FAILED;
		VCShutdownSlaves(ii->i_MasterHost);
		VCResetVCId(ii->i_MasterHost, ii->i_SlaveHost);
		missCount = 0;
	    }
	}
    }
    dbinfo2("INST %p Stopping VC SLAVE Manager for %s (%d:%d)\n",
	ii,
	h->h_HostName,
	ii->i_Refs, ii->i_UseCount
    );
    FreeInstInfo(ii);
}

/*
 * VCManagerSlaveCheck() -	Start up slave VC manager if possible,
 *				return slave management ii or NULL
 *
 *	NULL is returned in the special case where the slave has failed
 *	due to a timeout but is still receiving packets on the same VC.
 *	In this case we have to remain failed and wait for the master to
 *	reforge.
 */

InstInfo * 
VCManagerSlaveKick(HostInfo *master, HostInfo *slave, rp_vcid_t vcid)
{
    InstInfo *ii;

    DBASSERT(slave == &MyHost);

    if ((ii = FindInstInfo(master, slave, IIF_SLAVE, 0, 0)) != NULL) {
	if (ii->i_Flags & IIF_FAILED) {
	    if (ii->i_VCId == vcid) {
		ii = NULL;
	    } else {
		ii->i_Flags &= ~IIF_FAILED;
		ii->i_VCId = vcid;
		issueNotify(ii->i_NotifyInt);
	    }
	} else if (ii->i_VCId != vcid) {
	    VCShutdownSlaves(ii->i_MasterHost);
	    ii->i_Flags |= IIF_RESET;
	    ii->i_VCId = vcid;
	    issueNotify(ii->i_NotifyInt);
	}
    } else {
	ii = AllocInstInfo(NULL, master, slave, 
		NULL, IIF_SLAVE | IIF_PRIVATE, 0);
	ii->i_VCId = vcid;
	taskCreate(VCManagerSlaveThread, ii);
    }
    return(ii);
}

/*
 * VCShutdownMasters() - shutdown masterside connections to a given host
 */

static void
VCShutdownMasters(HostInfo *h)
{
    InstInfo *ii;
    InstInfo **pii;

    dbinfo2("Shutting down masters for %s\n", h->h_HostName);

    pii = &h->h_SlaveInstBase;
    while ((ii = *pii) != NULL) {
	if (ii->i_Refs == 0 || (ii->i_Flags & IIF_MASTER) == 0) {
	    pii = &ii->i_SlaveNext;
	    continue;
	}
	DBASSERT(ii->i_MasterHost == &MyHost);
	if ((ii->i_Flags & IIF_FAILED) == 0) {
	    RPAnyMsg *rpMsg;

	    if (ii->i_RHSlave->rh_DBInfo) {
		dbinfo2("   Master DB %s\n", ii->i_RHSlave->rh_DBInfo->d_DBName);
		ii->i_Flags |= IIF_FAILED;
		if (ii->i_Flags & IIF_IDLE) {
		    RouteHostInfo *rh = ii->i_RHSlave;
		    InstInfo **idp = &rh->rh_IdleInstBase;

		    while (*idp && ii != *idp) {	/* XXX */
			idp = &(*idp)->i_Next;
		    }
		    DBASSERT(ii == *idp);
		    *idp = ii->i_Next;
		    ii->i_Flags &= ~IIF_IDLE;
		    printf("****** FREE IDLE AFTER FAILURE\n");
		    FreeInstInfo(ii);
		    continue;
		}
		rpMsg = SimulateReceivedVCPkt(ii, 
			RPCMD_CLOSE_INSTANCE|RPCMDF_REPLY);
		addTail(ii->i_RXPktList, &rpMsg->a_Msg.rp_Node);
		issueNotify(ii->i_NotifyInt);
	    }
	}
	pii = &ii->i_SlaveNext;
    }
    dbinfo2("End Shutting down masters for %s\n", h->h_HostName);
}

/*
 * VCNotifyClientsByRH() - Notify clients of a database routing change
 */

void
VCNotifyClientsByRH(RouteHostInfo *rh, int remoteGoingAway)
{
    ClientInfo *ci;
    DBInfo *db = rh->rh_DBInfo;

    for (
	ci = getHead(&db->d_CIList);
	ci;
	ci = getListSucc(&db->d_CIList, &ci->ci_Node)
    ) {
	if (ci->ci_Notify) {
	    ci->ci_Flags |= CIF_CHECKNEW;
	    issueNotify(ci->ci_Notify);
	    if (remoteGoingAway)
		RouteCheckRemoteClosingDB(ci);
	}
    }
}

/*
 * VCNotifyFailedClients() - Notify clients holding failed instances
 */

static void
VCNotifyFailedClients(InstInfo *mi)
{
    InstInfo *ii;

    for (ii = mi->i_SlaveHost->h_SlaveInstBase; ii; ii = ii->i_SlaveNext) {
	if (ii->i_Refs == 0 || (ii->i_Flags & IIF_MASTER) == 0)
	    continue;
	DBASSERT(ii->i_MasterHost == &MyHost);
	if ((ii->i_Flags & IIF_FAILED) && ii->i_RHSlave->rh_DBInfo)
	    issueNotify(ii->i_NotifyInt);
    }
}

static void
VCNotifyHostUp(HostInfo *h)
{
    RouteHostInfo *rh;

    for (rh = h->h_RHInfoBase; rh; rh = rh->rh_HNext) {
	DBInfo *db;

	if ((db = rh->rh_DBInfo) != NULL && db->d_Refs != 0) {
	    /*
	     * If we are hosting this database, notify our synchronizer
	     * synchronize to the host's copy.
	     */
	    if (db->d_Synchronize)
		issueNotify(db->d_Synchronize);
		 
	    /*
	     * Notify any clients using this database that a new
	     * peer or snap is up.
	     */
	    VCNotifyClientsByRH(rh, 0);
	}
    }
}

/*
 * VCShutdownSlaves() - shutdown slave instances handling traffic for h
 *
 *	h represents the master (some remote host).  Scan his master list
 *	for instances.  (All instances will point to &MyHost as the slave).
 */

static void
VCShutdownSlaves(HostInfo *h)
{
    InstInfo *ii;

    dbinfo2("Shutting down slaves for %s\n", h->h_HostName);
    for (ii = h->h_MasterInstBase; ii; ii = ii->i_MasterNext) {
	if (ii->i_Refs == 0 || (ii->i_Flags & IIF_SLAVE) == 0)
	    continue;
	DBASSERT(ii->i_SlaveHost == &MyHost);
	if (ii->i_RHSlave->rh_DBInfo) {
	    dbinfo2("   Slave DB %s\n", ii->i_RHSlave->rh_DBInfo->d_DBName);
	    ii->i_Flags |= IIF_FAILED;
	}
	if (ii->i_NotifyInt)
	    issueNotify(ii->i_NotifyInt);
    }
}


