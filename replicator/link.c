/*
 * REPLICATOR/LINK.C
 *
 * (c)Copyright 2000-2002 Backplane, Inc.  Please refer to the COPYRIGHT
 * file at the base of the distribution tree.
 *
 * $Backplane: rdbms/replicator/link.c,v 1.57 2003/04/02 05:27:17 dillon Exp $
 *
 *	Alloc*() - 	Allocate structure & bump ref count
 *	Free*() - 	Decrement ref count, free structure as appropriate
 *	Destroy*() -	Physically free structure after all users go away
 *
 *	Find/Search routines do not automatically bump ref or use count.
 *
 *	Routines that take structures as an argument and keep a reference
 *	to them persistently will bump their ref and/or use count as
 *	appropriate.
 */

#include "defs.h"
#include <pwd.h>

Prototype void InitMyHost(int type);
Prototype HostInfo *AllocHostInfo(const char *hostName, rp_type_t type, rp_addr_t addr);
Prototype void FreeHostInfo(HostInfo *h);
Prototype void DestroyHostInfo(HostInfo *h);
Prototype void FreeRouteHostInfo(RouteHostInfo *rh);
Prototype void DestroyRouteHostInfo(RouteHostInfo *rh);
Prototype InstInfo *AllocInstInfo(InstInfo *mi, HostInfo *m, HostInfo *s, DBInfo *db, int flags, int32_t instId);
Prototype void ReallocInstInfo(InstInfo *ii);
Prototype InstInfo *FindInstInfo(HostInfo *m, HostInfo *s, int flags, int instId, rp_vcid_t vcid);
Prototype void FreeInstInfo(InstInfo *ii);
Prototype void DestroyInstInfo(InstInfo *ii);
Prototype SeqInfo *AllocSeqInfo(HostInfo *src, HostInfo *dst, int cmd);
Prototype void FreeSeqInfo(SeqInfo *s);
Prototype void DestroySeqInfo(SeqInfo *s);
Prototype LinkInfo *AllocLinkInfo(const char *lcmd, rp_type_t type, iofd_t fd);
Prototype void SetLinkStatus(LinkInfo *l, const char *ctl, ...);

Prototype void NotifyLinksStartup(LinkInfo *l);
Prototype void NotifyAllLinks(RouteInfo *r, rp_hop_t oldHop);
Prototype void NotifyRouteLink(LinkInfo *l, RouteInfo *r, rp_cmd_t cmd);
Prototype void NotifyLink(LinkInfo *l, void *ptr, rp_cmd_t cmd, int now);
Prototype void FlushLink(LinkInfo *l);
Prototype void FlushLinkByInfo(InstInfo *ii);
Prototype LinkNotify *DrainLinkNotify(LinkInfo *l);
Prototype void FreeLinkNotify(LinkNotify *ln);

Prototype void FreeLinkInfo(LinkInfo *l);
Prototype void DestroyLinkInfo(LinkInfo *l);

Prototype RouteInfo *AllocRouteInfo(LinkInfo *l, HostInfo *h, DBInfo *d, rp_hop_t hop, rp_latency_t latency, rp_totalrep_t repCount, rp_type_t type, int blockSize, dbstamp_t syncTs, dbstamp_t minCTs);
Prototype RouteInfo *FindRouteInfo(LinkInfo *l, DBInfo *d, rp_addr_t addr);
Prototype void FreeRouteInfo(RouteInfo *r);
Prototype void DestroyRouteInfo(RouteInfo *r);
Prototype int RemoteClosingDB(RouteHostInfo *rh);

static RouteHostInfo *AllocRouteHostInfo(HostInfo *h, DBInfo *db);

Prototype HostInfo	MyHost;
Prototype LinkInfo	*LinkBase;

HostInfo	MyHost;
LinkInfo	*LinkBase;
rp_addr_t	NextSrcA = 2;

void 
InitMyHost(int type)
{
    char hostBuf[MAXHOSTNAME];
    u_int64_t digest[2];
    const char *vname;

    DBASSERT(sizeof(digest) == 16);

    bzero(hostBuf, sizeof(hostBuf));
    gethostname(hostBuf, sizeof(hostBuf) - 1);

    if (VirtualHostName == NULL) {
	struct passwd *pw;
	pw = getpwuid(getuid());
	if (pw == NULL)
	    fatalsys("getpwuid: failed");
	vname = strdup(pw->pw_name);
	endpwent();
    } else {
	vname = VirtualHostName;
    }

    switch(type) {
    case CTYPE_PEER:
    case CTYPE_SNAP:
    case CTYPE_REPLIC:
	safe_asprintf(&MyHost.h_HostName, "%s.%s", hostBuf, vname);
	break;
    default:
	safe_asprintf(&MyHost.h_HostName, "%s.PID%d", hostBuf, (int)getpid());
	break;
    }
    /*
     * Generate a unique host address identifier based on the hostname.  We
     * prefer using MD5Data() but if not we fake it with crypt().  The idea
     * is for the digest space to be so large that no collision will ever
     * occur.
     */
#if HAS_MD5DATA
    MD5Data(MyHost.h_HostName, strlen(MyHost.h_HostName), (void *)digest);
#else
    vname = crypt(MyHost.h_HostName, "az");
    bzero(digest, sizeof(digest));
    bcopy(vname + 2, digest, 11);
#endif
    MyHost.h_Addr = digest[0] ^ digest[1];
    MyHost.h_Type = type;
    MyHost.h_Refs = 1;
    if (DebugOpt)
	printf("MyHost %s addr %016qx\n", MyHost.h_HostName, MyHost.h_Addr);
}

/*
 * AllocHostInfo() -	Allocate host info structure (searched)
 *
 *	The ref count reflects the number of routes referencing the
 *	host.  The use count reflects other references to the
 *	host.
 */

HostInfo *
AllocHostInfo(const char *hostName, rp_type_t type, rp_addr_t addr)
{
    HostInfo *h = NULL;

    /*
     * Only the CTYPE_REPLIC type can share a host structure, e.g. for
     * routes.  Clients (and other ytpes) do not, since we might be managing
     * many clients with the same host id.
     */
    if (type == CTYPE_REPLIC) {
	for (h = &MyHost; h; h = h->h_Next) {
	    /*
	     * Only match active hosts.  Hosts with a refcount of 0 are 
	     * in the process of being deleted.
	     */
	    if (h->h_Type == type &&
		strcmp(hostName, h->h_HostName) == 0
	    ) {
		break;
	    }
	}
    }

    if (h == NULL) {
	h = zalloc(sizeof(HostInfo));
	h->h_HostName = strdup(hostName);
	h->h_Next = MyHost.h_Next;
	h->h_Type = type;
	h->h_Addr = addr;
	MyHost.h_Next = h;
    } else if (h->h_Refs == 0) {
	/*
	 * Allow physical address to change if h_Refs was 0
	 */
	h->h_Addr = addr;
    } else {
	/*
	 * Otherwise we are in trouble if the address has changed.  It
	 * may be an indication that two different replication daemons
	 * are reporting the same host name.
	 */
	DBASSERT(h->h_Addr == addr);
    }
    ++h->h_Refs;
    return(h);
}

/*
 * FreeHostInfo() -	Dereference a HostInfo
 *
 *	When the reference count goes to 0 we can no longer route to the host
 *	and must notify our links of the case.
 *
 *	With the reference count at 0, there can be no routes for this host.
 */

void
FreeHostInfo(HostInfo *h)
{
    DBASSERT(h->h_Refs > 0);

    if (--h->h_Refs == 0) {
	/*
	 * Catch a really bad thing
	 */
	if (h == &MyHost)
	    fatal("Attempt to free MyHost");

	if (h->h_UseCount == 0)
	    DestroyHostInfo(h);
    }
}

/*
 * DestroyHostInfo() -	Called when the ref and use counts reach 0
 */

void
DestroyHostInfo(HostInfo *h)
{
    HostInfo **ph;

    DBASSERT(h->h_RHInfoBase == NULL);
    DBASSERT(h->h_MasterInstBase == NULL);
    DBASSERT(h->h_SlaveInstBase == NULL);
    /*
     * Host linkage
     */
    for (ph = &MyHost.h_Next; *ph != h; ph = &(*ph)->h_Next)
	;
    *ph = h->h_Next;
    h->h_Next = NULL;

    /*
     * Misc allocated tags
     */
    safe_free(&h->h_HostName);
    zfree(h, sizeof(HostInfo));
}

/*
 * AllocRouteHostInfo() - allocate/reference an RHI
 */
static
RouteHostInfo *
AllocRouteHostInfo(HostInfo *h, DBInfo *db)
{
    RouteHostInfo *rh;

    for (rh = h->h_RHInfoBase; rh; rh = rh->rh_HNext) {
	if (rh->rh_DBInfo == db)
	    break;
    }
    if (rh == NULL) {
	rh = zalloc(sizeof(RouteHostInfo));

	rh->rh_Host = h;
	rh->rh_HNext = h->h_RHInfoBase;
	h->h_RHInfoBase = rh;
	UseHostInfo(h);

	if ((rh->rh_DBInfo = db) != NULL) {
	    rh->rh_DNext = db->d_RHInfoBase;
	    db->d_RHInfoBase = rh;
	    UseDBInfo(db);
	}
    }

    ++rh->rh_Refs;
    if (rh->rh_Refs == 1 && rh->rh_DBInfo)
	VCNotifyClientsByRH(rh, 0);
    return(rh);
}

void
FreeRouteHostInfo(RouteHostInfo *rh)
{
    if (--rh->rh_Refs == 0 && rh->rh_UseCount == 0) {
	if (rh->rh_DBInfo)
	    VCNotifyClientsByRH(rh, 1);
	DestroyRouteHostInfo(rh);
    }
}

void
DestroyRouteHostInfo(RouteHostInfo *rh)
{
    RouteHostInfo **prh;

    DBASSERT(rh->rh_Routes == NULL);

    for (
	prh = &rh->rh_Host->h_RHInfoBase;
	rh != (*prh);
	prh = &(*prh)->rh_HNext
    ) {
	DBASSERT(*prh != NULL);
    }
    *prh = rh->rh_HNext;

    if (rh->rh_DBInfo != NULL) {
	for (
	    prh = &rh->rh_DBInfo->d_RHInfoBase;
	    rh != (*prh);
	    prh = &(*prh)->rh_DNext
	) {
	    DBASSERT(*prh != NULL);
	}
	*prh = rh->rh_DNext;
	if (rh->rh_DBInfo->d_RHInfoBase == NULL)
	    rh->rh_DBInfo->d_BlockSize = 0;	/* reset */
	DoneDBInfo(rh->rh_DBInfo);
	rh->rh_DBInfo = NULL;
    }
    UnuseHostInfo(rh->rh_Host);
    rh->rh_Host = NULL;
    zfree(rh, sizeof(RouteHostInfo));
}

/*
 * AllocInstInfo() -	Allocate a database instance tracking information
 *			structure (not searched).
 *
 *	The passed hostinfos represent the master and slave side of the
 *	connection irregardless of whether we are the master or the slave.
 *	What we are is passed in flags.
 */
InstInfo *
AllocInstInfo(InstInfo *mi, HostInfo *master, HostInfo *slave, DBInfo *db, int flags, int32_t instId)
{
    InstInfo *ii = zalloc(sizeof(InstInfo));
    SeqInfo *s;

    /*
     * Locate/create sequence number info structure.  We need this to record
     * the VCId.  The SeqInfo for both directions of a VC will have the
     * same VCId.
     */
    s = AllocSeqInfo(master, slave, 0);

    if (mi)
	ii->i_VCId = mi->i_VCId;
    else
	ii->i_VCId = s->s_VCId;
    ii->i_MasterHost = master;
    ii->i_MasterNext = master->h_MasterInstBase;
    master->h_MasterInstBase = ii;
    UseHostInfo(master);

    ii->i_RHSlave = AllocRouteHostInfo(slave, db);
    ii->i_SlaveNext = slave->h_SlaveInstBase;
    slave->h_SlaveInstBase = ii;
    UseHostInfo(slave);

    if ((ii->i_VCManager = mi) != NULL)
	++mi->i_Refs;

    ii->i_InstId = instId;
    ii->i_Flags = flags;
    ii->i_Refs = 1;	/* represents linkages */

    if (flags & IIF_PRIVATE) {
        ii->i_NotifyInt = allocNotify();
	ii->i_RXPktList = zalloc(sizeof(List));
	initList(ii->i_RXPktList);
    }
    FreeSeqInfo(s);

    return(ii);
}

void 
ReallocInstInfo(InstInfo *ii)
{
    DBASSERT(ii->i_VCManager != NULL);
    ii->i_VCId = ii->i_VCManager->i_VCId;
    ii->i_Flags &= ~IIF_FAILED;
}

InstInfo *
FindInstInfo(HostInfo *m, HostInfo *s, int flags, int instId, rp_vcid_t vcid)
{
    InstInfo *ii;

    for (ii = m->h_MasterInstBase; ii; ii = ii->i_MasterNext) {
	if (ii->i_Refs &&
	    ii->i_RHSlave->rh_Host == s && 
	    ii->i_InstId == instId &&
	    (instId == 0 || ii->i_VCId == vcid) &&
	    (ii->i_Flags & flags) == flags
	) {
	    break;
	}
    }
    return(ii);
}

void
FreeInstInfo(InstInfo *ii)
{
    DBASSERT(ii->i_Refs > 0);
    if (--ii->i_Refs == 0) {
	if (ii->i_VCManager) {
	    FreeInstInfo(ii->i_VCManager);
	    ii->i_VCManager = NULL;
	}
	if (ii->i_UseCount == 0)
	    DestroyInstInfo(ii);
    }
}

void
DestroyInstInfo(InstInfo *ii)
{
    InstInfo **pi;

    pi = &ii->i_MasterHost->h_MasterInstBase;
    while (*pi != ii)
	pi = &(*pi)->i_MasterNext;
    *pi = ii->i_MasterNext;
    ii->i_MasterNext = NULL;

    pi = &ii->i_RHSlave->rh_Host->h_SlaveInstBase;
    while (*pi != ii)
	pi = &(*pi)->i_SlaveNext;
    *pi = ii->i_SlaveNext;
    ii->i_SlaveNext = NULL;

    DBASSERT((ii->i_Flags & IIF_IDLE) == 0);

    if (ii->i_Flags & IIF_PRIVATE) {
	if (ii->i_NotifyInt) {
	    freeNotify(ii->i_NotifyInt);
	    ii->i_NotifyInt = NULL;
	}
	if (ii->i_RXPktList) {
	    RPAnyMsg *rpMsg;

	    while ((rpMsg = remHead(ii->i_RXPktList)) != NULL)
		FreeRPMsg(rpMsg);
	    zfree(ii->i_RXPktList, sizeof(List));
	    ii->i_RXPktList = NULL;
	}
    }
    DBASSERT(ii->i_NotifyInt == NULL);
    DBASSERT(ii->i_RXPktList == NULL);

    UnuseHostInfo(ii->i_MasterHost);
    ii->i_MasterHost = NULL;
    FreeRouteHostInfo(ii->i_RHSlave);
    ii->i_RHSlave = NULL;

    zfree(ii, sizeof(InstInfo));
}

SeqInfo *
AllocSeqInfo(HostInfo *src, HostInfo *dst, int cmd)
{
    SeqInfo *s;

    if ((cmd & RPCMDF_REPLY) == 0) {
	for (s = src->h_Seqs; s; s = s->s_Next) {
	    if (s->s_SlaveHost == dst && !((cmd ^ s->s_Cmd) & RPCMDF_REPLY))
		break;
	}
    } else {
	for (s = dst->h_Seqs; s; s = s->s_Next) {
	    if (s->s_SlaveHost == src && !((cmd ^ s->s_Cmd) & RPCMDF_REPLY))
		break;
	}
    }
    if (s == NULL) {
	SeqInfo *so;

	s = zalloc(sizeof(SeqInfo));
	if ((cmd & RPCMDF_REPLY) == 0) {
	    s->s_MasterHost = src;
	    s->s_SlaveHost = dst;
	} else {
	    s->s_MasterHost = dst;
	    s->s_SlaveHost = src;
	}
	s->s_Cmd = cmd;

	so = zalloc(sizeof(SeqInfo));
	so->s_Cmd = cmd ^ RPCMDF_REPLY;
	so->s_MasterHost = s->s_MasterHost;
	so->s_SlaveHost = s->s_SlaveHost;
	s->s_Other = so;
	so->s_Other = s;

	s->s_Next = s->s_MasterHost->h_Seqs;
	s->s_MasterHost->h_Seqs = s;

	so->s_Next = so->s_MasterHost->h_Seqs;
	so->s_MasterHost->h_Seqs = so;

	if (src == &MyHost && (cmd & RPCMDF_REPLY) == 0) {
	    /*
	     * s represents our outgoing sequencing, so our incoming.
	     */
	    so->s_SeqNo = (rp_seq_t)-1;
	    s->s_VCId = 1;
	    so->s_VCId = 1;
	}

	/*
	 * Bump use count for hosts for s and so
	 */
	UseHostInfo(src);
	UseHostInfo(src);
	UseHostInfo(dst);
	UseHostInfo(dst);

	/*
	 * Self referential between s and so.  This also prevents
	 * the structures from being implicitly deleted.  We will
	 * explicitly delete them only when the HostInfo ref count
	 * goes to 0 XXX
	 */
	s->s_Refs = 1;
	so->s_Refs = 1;
    }
    ++s->s_Refs;
    return(s);
}

void 
FreeSeqInfo(SeqInfo *s)
{
    DBASSERT(s->s_Refs > 0);
    if (--s->s_Refs == 0) {
	if (s->s_UseCount == 0)
	    DestroySeqInfo(s);
    }
}

void
DestroySeqInfo(SeqInfo *s)
{
    SeqInfo *so;
    SeqInfo **ps;

    if ((so = s->s_Other) != NULL) {
	s->s_Other = NULL;
	so->s_Other = NULL;
	FreeSeqInfo(so);
    }
    for (ps = &s->s_MasterHost->h_Seqs; *ps != s; ps = &(*ps)->s_Next)
	;
    *ps = s->s_Next;
    s->s_Next = NULL;
    UnuseHostInfo(s->s_MasterHost);
    UnuseHostInfo(s->s_SlaveHost);
    zfree(s, sizeof(SeqInfo));
}

/*
 * AllocLinkInfo() -	Allocate a link info structure
 *
 *	Allocate a link information structure for a file descriptor.
 */

LinkInfo *
AllocLinkInfo(const char *lcmd, rp_type_t type, iofd_t fd)
{
    LinkInfo *l = zalloc(sizeof(LinkInfo));

    l->l_Ior = fd;
    if (fd)
	l->l_Iow = dupIo(fd);
    l->l_Refs = 1;
    l->l_NotifyApp = &l->l_Notifies;
    l->l_NotifyCount = 0;
    l->l_Next = LinkBase;
    DBASSERT(l->l_Next != (LinkInfo *)0x72);
    l->l_Type = type;
    l->l_Pid = -1;
    l->l_NotifyInt = allocNotify();
    safe_asprintf(&l->l_StatusMsg, "Unknown");
    if (lcmd)
	l->l_LinkCmdName = strdup(lcmd);
    LinkBase = l;

    return(l);
}

void
SetLinkStatus(LinkInfo *l, const char *ctl, ...)
{
    va_list va;
    char *str;

    va_start(va, ctl);
    safe_vasprintf(&str, ctl, va);
    va_end(va);
    safe_free(&l->l_StatusMsg);
    l->l_StatusMsg = str;
}

/*
 * NotifyLinksStartup() - send notifications to synchronize with all hosts.
 *
 *	After the initial HELLO exchange we need to queue up notifications
 *	for links to destinations.
 *
 *	Routes are linked by destination host and sorted by the relay
 *	host.  There can be only one (relay,dest) per LinkInfo (each node
 *	aggregates all of its (relay,dest)'s into a single one when
 *	forwarding routing information.
 *
 *	We do not send a notification for the HELLO we've already exchanged
 *	in the bootstrap... which is the one for ourselves with a NULL
 *	DBInfo.  We do have to send notifications for all our local
 *	database's, however.
 *
 *	Unfortunately, we need to scan routes by relay
 */

void
NotifyLinksStartup(LinkInfo *l)
{
    HostInfo *h;

    /*
     * Drain any garbage that might have built up while we were exchanging
     * initial HELLOs.
     */
    {
	LinkNotify *ln;

	while ((ln = DrainLinkNotify(l)) != NULL) {
	    if (ln->n_Cmd) {
		UnuseRouteInfo((RouteInfo *)ln->n_Data);
	    } else {
		FreeRPMsg(ln->n_Data);
	    }
	    FreeLinkNotify(ln);
	}
    }

    /*
     * These sorts of local links do not produce notifications
     * to other replicators.
     */
    if (l->l_Type == CTYPE_ACCEPTOR || l->l_Type == CTYPE_CONTROL)
	return;

    /*
     * Figure out the best route for each "host:database" and propogate it
     */
    for (h = &MyHost; h; h = h->h_Next) {
	RouteHostInfo *rh;

	if (h->h_Refs == 0)
	    continue;

	for (rh = h->h_RHInfoBase; rh; rh = rh->rh_HNext) {
	    RouteInfo *r;
	    RouteInfo *best = NULL;

	    if (rh->rh_Host == &MyHost && rh->rh_DBInfo == NULL)
		continue;
	    for (r = rh->rh_Routes; r; r = r->r_RHNext) {
		if (r->r_Refs == 0)
		    continue;
		if (best == NULL || best->r_Hop > r->r_Hop)
		    best = r;
	    }
	    if (best)
		NotifyRouteLink(l, best, RPCMD_HELLO);
	}
    }
}

/*
 * NotifyLinks() -	Notify links of a state change in host h
 *
 *	The given route table entry has just been created, modified,
 *	or deleted (r_Refs == 0 if deleted).  Notify all physical links
 *	of the change.
 *
 *	A hop count representitive of the route prior to the change, oldHop,
 *	must be passed.  A new route being added should have a
 *	'previous' hop count of RP_MAXHOPS to indicate that it effectively
 *	did not exist before.
 *
 *	Note that we propogate a route received from host X back to host X.
 *	This cannot be easily optimized out due to the number of ways a loop
 *	can be generated in the graph.
 */

void
NotifyAllLinks(RouteInfo *r, rp_hop_t oldHop)
{
    LinkInfo *l;
    RouteInfo *scan;
    RouteInfo *best;
    RouteHostInfo *rh = r->r_RHInfo;
    int count;
    int updatedTs = 0;

    /*
     * Only PEER or SNAP routes to REPLIC hosts are propogated.
     */
    if (rh->rh_Host->h_Type != CTYPE_REPLIC)
	return;
    if (r->r_Type != CTYPE_PEER && r->r_Type != CTYPE_SNAP && r->r_Type != CTYPE_REPLIC)
	return;

    /*
     * Do not re-propogate our bootstrap route when we add it,
     * since we already exchanged it in the bootstrap.
     */
    if (rh->rh_Host == &MyHost && rh->rh_DBInfo == NULL)
	return;

    /*
     * If this is a database route then update the aggregate MinCTs and SyncTs
     */
    if (rh->rh_DBInfo) {
	DBInfo *db = rh->rh_DBInfo;
	if (db->d_MinCTs < r->r_MinCTs) {
	    db->d_MinCTs = r->r_MinCTs;
	    updatedTs = 1;
	}
	if (db->d_SyncTs < r->r_SyncTs) {
	    db->d_SyncTs = r->r_SyncTs;
	    updatedTs = 1;
	}
    }

    /*
     * Look at all routes to this route's host and choose the best one.
     *
     * Due to the way the spanning tree algorithm works, and to avoid having
     * to maintain a ridiculous amount of state, if a route becomes isolated
     * there will be nothing anchoring it in the graph and this will result
     * in an update loop in the graph that continuously increments the 
     * hop count.
     *
     * We detect this situation when the hop count reaches RP_MAXHOPS and
     * use it to invalidate the route.
     */
    best = NULL;
    count = 0;

    for (scan = rh->rh_Routes; scan; scan = scan->r_RHNext) {
	if (scan->r_Refs == 0)			/* undergoing deletion */
	    continue;
	if (scan->r_Hop == RP_MAXHOPS)		/* dead route */
	    continue;
	++count;
	if (best == NULL || best->r_Hop > scan->r_Hop)
	     best = scan;
    }

    /*
     * 'best' is the best available ACTIVE route (not deleted and not maxed 
     * out on hops).  'best' will be NULL if no active routes could be found.
     *
     * If no active routes could be found then 'r' will obviously point to 
     * a deleted route or one with a maxed-out ref count.
     *
     * If best is non-NULL and not the route that was modified, and has at 
     * least the same hop count as the route being modified, AND none of the
     * timestamps were updated, then we do not have to do anything.
     */

    if (best && best != r && best->r_Hop <= oldHop && !updatedTs)
	return;

    /*
     * Database specific route updates may trigger local events, waking up
     * clients waiting for synchronization events, notifying the synchronizer
     * of a new host or a deleted host, or clients of a new host or a deleted
     * host.
     *
     * XXX need to reduce the overhead of this!!
     */
    if (rh->rh_DBInfo) {
	RouteInfo *r;

	/*
	 * Notify the synchronizer that something has changed
	 */
	if (rh->rh_DBInfo->d_Synchronize)
	    issueNotify(rh->rh_DBInfo->d_Synchronize);

	/*
	 * If all routes to this database@host have gone away, it may be
	 * due to a 'normal' shutdown.  We have to close any idle instances
	 * we are holding to the remote database being shut down.
	 */
	for (r = rh->rh_Routes; r; r = r->r_RHNext) {
	    if (r->r_Refs != 0)
		break;
	}
	if (r == NULL) {
	    InstInfo *ii;

	    while ((ii = rh->rh_IdleInstBase) != NULL) {
		DBASSERT(ii->i_Flags & IIF_IDLE);
		rh->rh_IdleInstBase = ii->i_Next;
		ii->i_Next = NULL;
		ii->i_Flags &= ~IIF_IDLE;
		StartClientThreadIdleClose(ii);
	    }
	    VCNotifyClientsByRH(rh, 1);
	} else {
	    VCNotifyClientsByRH(rh, 0);
	}
    }

    /*
     * Notify all links of the routing If the 'best' route we have
     *
     * We treat routes which have reached RP_MAXHOPS as being dead 
     * (effectively deleted).
     */
    for (l = LinkBase; l; l = l->l_Next) {
	/*
	 * Don't notify dead links
	 */
	if (l->l_Refs == 0)
	    continue;

	/*
	 * Only certain types of links require notification.
	 */
	switch(l->l_Type) {
	case CTYPE_REPLIC:
	    break;
	default:
	    continue;
	}

	if (best == NULL) {
	    /*
	     * No active routes (count is 0 in this case).  The route 'r' o
	     * represents a route that was just deleted, a new route with a 
	     * maxed-out hop count (effectively dead), or a route adjustment
	     * with a maxed-out hop count (also effectively dead).
	     *
	     * If the old hop count was RP_MAXHOPS we have *already* sent
	     * the GOODBYE and should not send it again.
	     */
	    if (oldHop != RP_MAXHOPS)
		NotifyRouteLink(l, r, RPCMD_GOODBYE);
	} else if (count == 1) {
	    /*
	     * Only one active route, either adding a new route or changing
	     * an existing route, as determined by the passed 'cmd' and
	     * whether
	     */
	    if (oldHop == RP_MAXHOPS)
		NotifyRouteLink(l, best, RPCMD_HELLO);
	    else
		NotifyRouteLink(l, best, RPCMD_ADJUST);
	} else {
	    /*
	     * More then one active route, the notification must be an 
	     * ADJUSTMENT whether this route is new or not.
	     */
	    NotifyRouteLink(l, best, RPCMD_ADJUST);
	}
    }
}

/*
 * RemoteClosingDB() -	Check if the database@host is being shutdown by the
 *			remote replicator.
 */

int
RemoteClosingDB(RouteHostInfo *rh)
{
    int res = 1;
    RouteInfo *r;

    for (r = rh->rh_Routes; r; r = r->r_RHNext) {
	if (r->r_Refs != 0) {
	    res = 0;
	    break;
	}
    }
    return(res);
}

/*
 * NotifyRouteLink() -	Notify a link with route data
 */

void
NotifyRouteLink(LinkInfo *l, RouteInfo *r, rp_cmd_t cmd)
{
    /*
     * Only routes represented by PEER or SNAP hosts are propogated.  Other
     * types of routes (such as the route to a client) are local to the 
     * machine the client connected to.
     */
    if (r->r_Host->h_Type != CTYPE_REPLIC)
	return;
    if (r->r_Type != CTYPE_PEER && r->r_Type != CTYPE_SNAP && r->r_Type != CTYPE_REPLIC)
	return;
    UseRouteInfo(r);
    NotifyLink(l, r, cmd, 1);
}

/*
 * NotifyLink() -	Notify a link with data
 *
 *	This is typically used to queue an outgoing packet to a
 *	physical network pipe.
 */

void
NotifyLink(LinkInfo *l, void *data, rp_cmd_t cmd, int now)
{
    LinkNotify *ln;

    ln = zalloc(sizeof(LinkNotify));
    ln->n_Data = data;
    ln->n_Cmd = cmd;
    *l->l_NotifyApp = ln;
    l->l_NotifyApp = &ln->n_Next;
    ++l->l_NotifyCount;
    if (l->l_NotifyInt && (now || l->l_NotifyCount > 32))
	issueNotify(l->l_NotifyInt);
}

/*
 * FlushLink() - signal the link to flush out any queued packets
 */
void
FlushLink(LinkInfo *l)
{
    if (l->l_Notifies && l->l_NotifyInt)
	issueNotify(l->l_NotifyInt);
}

void
FlushLinkByInfo(InstInfo *ii)
{
    HostInfo *h;
    RouteHostInfo *rh;

    if (ii->i_Flags & IIF_SLAVE)
	h = ii->i_MasterHost;
    else
	h = ii->i_SlaveHost;

    for (rh = h->h_RHInfoBase; rh; rh = rh->rh_HNext) {
	RouteInfo *r;

	if (rh->rh_DBInfo)
	    continue;

	for (r = rh->rh_Routes; r; r = r->r_RHNext) {
	    if (r->r_Refs == 0)
		continue;
	    FlushLink(r->r_Phys);
	}
    }
}

/*
 * Drain the next link notification event and return the route with
 * the use-count already bumped, and set *pcmd to the event type.
 *
 * The caller must unuse the returned route.  NULL is returned if no
 * events are pending.  If this route is being deleted r_Refs will be 0.
 */

LinkNotify * 
DrainLinkNotify(LinkInfo *l)
{
    LinkNotify *ln;

    if ((ln = l->l_Notifies) != NULL) {
	if ((l->l_Notifies = ln->n_Next) == NULL)
	    l->l_NotifyApp = &l->l_Notifies;
	--l->l_NotifyCount;
    }
    return(ln);
}

void
FreeLinkNotify(LinkNotify *ln)
{
    zfree(ln, sizeof(LinkNotify));
}

/*
 * FreeLinkInfo() -	Dereference link information structure
 *
 *	When the last reference is removed, all routes for this link
 *	are freed.
 */

void
FreeLinkInfo(LinkInfo *l)
{
    DBASSERT(l->l_Refs > 0);

    if (--l->l_Refs == 0) {
	RouteInfo *r;
	LinkNotify *ln;

	/*
	 * Close descriptor immediately
	 */
	if (l->l_Ior) {
	    closeIo(l->l_Ior);
	    l->l_Ior = NULL;
	}
	if (l->l_Iow) {
	    closeIo(l->l_Iow);
	    l->l_Iow = NULL;
	}
	safe_free(&l->l_LinkCmdName);
	safe_free(&l->l_DBName);
	safe_free(&l->l_StatusMsg);
	freeNotify(l->l_NotifyInt);
	l->l_NotifyInt = NULL;

	/*
	 * Prevent premature destruction
	 */
	UseLinkInfo(l);

	/*
	 * Dereference route information.  r_Refs may be 0 or 1 only and
	 * implies a LinkInfo linkage.  Only call Free if the route has
	 * not already been freed (for example, by a GOODBYE)
	 */
	r = l->l_Routes;
	if (r != NULL)
	    UseRouteInfo(r);
	while (r) {
	    RouteInfo *rnext;

	    if ((rnext = r->r_LNext) != NULL)
		UseRouteInfo(rnext);
	    UnuseRouteInfo(r);
	    if (r->r_Refs)	/* XXX */
		FreeRouteInfo(r);
	    r = rnext;
	}

	/*
	 * Free up any pending notifies for this link.  With the link down
	 * there isn't anywhere to send them :-)  The notifies may have
	 * already been present or may have been added due to freeing the
	 * routes.
	 */
	while ((ln = l->l_Notifies) != NULL) {
	    l->l_Notifies = ln->n_Next;
	    if (ln->n_Cmd) {
		UnuseRouteInfo((RouteInfo *)ln->n_Data);
	    } else {
		FreeRPMsg(ln->n_Data);
	    }
	    zfree(ln, sizeof(LinkNotify));
	}
	l->l_NotifyApp = &l->l_Notifies;
	l->l_NotifyCount = 0;

	if (l->l_Pid > 0) {
	    kill(l->l_Pid, SIGTERM);
	    while (waitpid(l->l_Pid, NULL, 0) < 0) {
		if (errno != EINTR)
		    break;
	    }
	}

	UnuseLinkInfo(l);
	/* l may be invalid at this point */
    }
}

/*
 * DestroyLinkInfo
 */

void
DestroyLinkInfo(LinkInfo *l)
{
    LinkInfo **pl;

    DBASSERT(l->l_Routes == NULL);

    for (pl = &LinkBase; *pl != l; pl = &(*pl)->l_Next)
	;
    DBASSERT(l->l_Next != (LinkInfo *)0x72);
    *pl = l->l_Next;
    l->l_Next = NULL;

    zfree(l, sizeof(LinkInfo));
}

/*
 * AllocRouteInfo() -	Allocate a route information structure
 *
 *	Allocate a route for the (relay,destination) pair.  dstA is the
 *	remote's address representation of the destination host.
 *
 *	LinkInfo may be NULL, indicating a route to a database on MyHost.
 *
 *	DBInfo may be NULL, indicating a general spanning tree route rather
 *	then a database-specific route.
 *
 *	This routine is responsible for adjusting ref/use counts as 
 *	appropriate, not the caller.
 */

RouteInfo *
AllocRouteInfo(LinkInfo *l, HostInfo *h, DBInfo *d, rp_hop_t hop, rp_latency_t latency, rp_totalrep_t repCount, rp_type_t type, int blockSize, dbstamp_t syncTs, dbstamp_t minCTs)
{
    RouteInfo *r;
    RouteHostInfo *rh;

    /*
     * When allocating a route, each physical interface may have only
     * one route for a given (relay, dst) pair so if we find one already
     * in there, we are in trouble.
     */

    r = zalloc(sizeof(RouteInfo));

    dbinfo3("ROUTE %p AllocRoute to %s%s%s (hop %d latency %d type %d syncTs %016qx minCTs %016qx)\n",
	r,
	((d) ? d->d_DBName : ""),
	((d) ? "@" : ""),
	h->h_HostName,
	hop,
	latency,
	type,
	syncTs,
	minCTs
    );

    rh = AllocRouteHostInfo(h, d);

    /*
     * Aggregate blocksize to rh.
     */
    if (rh->rh_Routes == NULL || rh->rh_BlockSize < blockSize)
	rh->rh_BlockSize = blockSize;
    if (d && d->d_BlockSize < rh->rh_BlockSize)
	d->d_BlockSize = rh->rh_BlockSize;

    r->r_RHInfo = rh;
    r->r_RHNext = rh->rh_Routes;
    rh->rh_Routes = r;

    r->r_Phys = l;		/* we are responsible for bumping use */
    r->r_Refs = 1;
    r->r_Hop = hop;
    r->r_Type = type;
    r->r_Latency = latency;
    r->r_RepCount = repCount;
    r->r_SyncTs = syncTs;
    r->r_MinCTs = minCTs;
    r->r_BlockSize = blockSize;

    /*
     * Bump the active ref count for the host and database, indicating
     * an active route.
     */
    ++h->h_Refs;
    if (d)
	RefDBInfo(d);	/* active route reference */

    /*
     * Insert into LinkInfo's list of routes.  There are no grouping
     * requirements.
     */
    if (l) {
	r->r_LNext = l->l_Routes;
	l->l_Routes = r;
	UseLinkInfo(l);		/* bump use on link */
    }

    NotifyAllLinks(r, RP_MAXHOPS);

    return(r);
}

/*
 * FindRouteInfo()
 */

RouteInfo *
FindRouteInfo(LinkInfo *l, DBInfo *d, rp_addr_t addr)
{
    RouteInfo *r;

    for (r = l->l_Routes; r; r = r->r_LNext) {
	if (r->r_Refs &&
	    r->r_RHInfo->rh_DBInfo == d &&
	    r->r_Host->h_Addr == addr
	) {
	    break;
	}
    }
    return(r);
}

void
FreeRouteInfo(RouteInfo *r)
{
    DBASSERT(r->r_Refs > 0);

    if (--r->r_Refs == 0) {
	NotifyAllLinks(r, r->r_Hop);
	if (r->r_UseCount == 0)
	    DestroyRouteInfo(r);
    }
}

void
DestroyRouteInfo(RouteInfo *r)
{
    RouteHostInfo *rh;

    /*
     * Dereference the physical link, if any
     */
    {
	LinkInfo *l;

	if ((l = r->r_Phys) != NULL) {
	    RouteInfo **pr;

	    if (l->l_PeerHost == r->r_Host)
		l->l_PeerHost = NULL;
	    for (pr = &l->l_Routes; *pr != r; pr = &(*pr)->r_LNext)
		;
	    *pr = r->r_LNext;
	    r->r_LNext = NULL;
	    UnuseLinkInfo(r->r_Phys);
	    r->r_Phys = NULL;
	}
    }

    rh = r->r_RHInfo;
    r->r_RHInfo = NULL;

    /*
     * Remove the route from the RouteHost list
     */
    {
	RouteInfo **pr;

	for (pr = &rh->rh_Routes; *pr != r; pr = &(*pr)->r_RHNext)
	    ;
	*pr = r->r_RHNext;
	r->r_RHNext = NULL;
	if (rh->rh_Routes == NULL)
	    rh->rh_BlockSize = 0;	/* reset block size */
    }

    /*
     * Active dereference of the underlying database and host.  The
     * RouteHost still has Use references on both the host and database.
     */
    FreeHostInfo(rh->rh_Host);
    if (rh->rh_DBInfo)
	PutDBInfo(rh->rh_DBInfo);

    /*
     * Free the reference to the RHInfo
     */
    FreeRouteHostInfo(rh);

    zfree(r, sizeof(RouteInfo));
}

