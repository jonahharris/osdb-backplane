/*
 * REPLICATOR/HELLO.C -	Hello packet processing, misc control/termination
 *
 * (c)Copyright 2000-2002 Backplane, Inc.  Please refer to the COPYRIGHT
 * file at the base of the distribution tree.
 *
 * $Backplane: rdbms/replicator/hello.c,v 1.27 2002/08/20 22:06:00 dillon Exp $
 */

#include "defs.h"

Prototype int CmdStartupHello(LinkInfo *l);
Prototype int CmdNotifyHello(LinkInfo *l, RouteInfo *r, rp_cmd_t cmd);
Prototype int RecStartupHello(LinkInfo *l);
Prototype int RecNotifyHello(LinkInfo *l, RPAnyMsg *rpMsg);
Prototype int CmdControl(LinkInfo *l, const char *id, rp_cmd_t ctlOp, char **emsg);

/*
 * CmdStartupHello() -	Transmit the startup HELLO packet over a link
 *
 */

int
CmdStartupHello(LinkInfo *l)
{
    int bytes;
    int hlen = strlen(MyHost.h_HostName) + 1;
    RPAnyMsg *rpMsg;

    bytes = offsetof(RPHelloMsg, he_HostName[hlen]);

    rpMsg = AllocLLPkt(RPCMD_HELLO, bytes);
    rpMsg->a_HelloMsg.he_Type = MyHost.h_Type;
    rpMsg->a_HelloMsg.he_Latency = 1;
    rpMsg->a_HelloMsg.he_Addr = MyHost.h_Addr;
    strcpy(rpMsg->a_HelloMsg.he_HostName, MyHost.h_HostName);

    return (WriteLLPkt(l, rpMsg, 1));
}

/*
 * CmdNotifyHello() - Transmit a route notification over a link
 *
 *	Transmit a notification HELLO/GOODBYE/ADJUST packet for route r
 *	over link l.
 */

#define LINKLATENCY	1	/* XXX fixme */

int
CmdNotifyHello(LinkInfo *l, RouteInfo *r, rp_cmd_t cmd)
{
    RPAnyMsg *rpMsg;
    HostInfo *h = r->r_Host;
    DBInfo *db = r->r_RHInfo->rh_DBInfo;
    int hlen = strlen(h->h_HostName) + 1;
    int bytes;

    bytes = offsetof(RPHelloMsg, he_HostName[hlen]);
    if (db != NULL)
	bytes += strlen(db->d_DBName) + 1;

    rpMsg = AllocLLPkt(cmd, bytes);
    rpMsg->a_HelloMsg.he_Type = r->r_Type;	/* note: may be database or host type */
    rpMsg->a_HelloMsg.he_Latency = r->r_Latency + LINKLATENCY;
    rpMsg->a_HelloMsg.he_State = r->r_State;
    rpMsg->a_HelloMsg.he_BlockShift = ffs(r->r_BlockSize) - 1;
    rpMsg->a_HelloMsg.he_RepCount = r->r_RepCount;
    rpMsg->a_HelloMsg.he_SyncTs = r->r_SyncTs;
    rpMsg->a_HelloMsg.he_MinCTs = r->r_MinCTs;
    rpMsg->a_HelloMsg.he_Addr = h->h_Addr;
    strcpy(rpMsg->a_HelloMsg.he_HostName, h->h_HostName);
    if (db) {
	rpMsg->a_HelloMsg.he_DBNameOff = strlen(h->h_HostName) + 1;
	strcpy(rpMsg->a_HelloMsg.he_HostName + rpMsg->a_HelloMsg.he_DBNameOff, db->d_DBName);
    }

    /*
     * When forwarding a HELLO packet for the spanning tree, the
     * hop count in the new packet is the best case route (r)
     * plus 1.
     */
    if ((rpMsg->rpa_Pkt.pk_Hop = r->r_Hop) != RP_MAXHOPS)
	++rpMsg->rpa_Pkt.pk_Hop;

    return (WriteLLPkt(l, rpMsg, 1));
}

/*
 * RecStartupHello() -	Receive the startup HELLO packet
 */

int
RecStartupHello(LinkInfo *l)
{
    RPAnyMsg *rpMsg;
    int rv = -1;

    if ((rpMsg = ReadPkt(l)) != NULL) {
	int maxOff = offsetof(RPHelloMsg, he_Pkt) + rpMsg->rpa_Pkt.pk_Bytes;
	int strBase = offsetof(RPHelloMsg, he_HostName[0]);

	if (
	    rpMsg->rpa_Pkt.pk_Cmd == RPCMD_HELLO &&
	    rpMsg->a_HelloMsg.he_HostName[maxOff - strBase - 1] == 0
	) {
	    HostInfo *h;

	    h = AllocHostInfo(
		    rpMsg->a_HelloMsg.he_HostName,
		    rpMsg->a_HelloMsg.he_Type, 
		    rpMsg->a_HelloMsg.he_Addr
		);

	    l->l_PeerHost = h;
	    l->l_Type = rpMsg->a_HelloMsg.he_Type;

	    /*
	     * Allocate the host route (hop should be 0, latency usually 1).
	     * This will result in a kickback to all links, including this 
	     * one.  db is NULL (and the replication count and syncts are
	     * irrelevant), indicating that this is a host route rather
	     * then a database availability route.
	     *
	     * If the peer host is ourself, however, the link represents
	     * a local loopback and we do not generate a route.  The packet
	     * code will assert (on purpose) if it sees a packet from itself.
	     */
	    if (h == &MyHost)  {
		fprintf(stderr, "Warning, loopback link detected.\n");
	    } else {
		AllocRouteInfo(l, h, NULL, 
		    rpMsg->rpa_Pkt.pk_Hop, rpMsg->a_HelloMsg.he_Latency, 0, CTYPE_REPLIC,
		    0, 0, 0
		);
	    }
	    FreeHostInfo(h);
	    rv = 0;
	}
	FreeRPMsg(rpMsg);
    }
    return(rv);
}

/*
 * Receive a notification RPCMD_HELLO, GOODBYE, or ADJUST packet
 */

int
RecNotifyHello(LinkInfo *l, RPAnyMsg *rpMsg)
{
    HostInfo *h;
    RouteInfo *r;
    DBInfo *d;
    int rv = -1;
    int blockSize;

    switch(rpMsg->a_HelloMsg.he_Pkt.pk_Cmd) {
    case RPCMD_HELLO:
	/*
	 * Note: AllocRouteInfo expects to use the ref count we got
	 * by calling AllocHostInfo() or GetHostByLinkAddr().
	 */
	if (rpMsg->a_HelloMsg.he_DBNameOff) {
	    h = AllocHostInfo(rpMsg->a_HelloMsg.he_HostName, CTYPE_REPLIC, rpMsg->a_HelloMsg.he_Addr);
	    d = AllocDBInfo(rpMsg->a_HelloMsg.he_HostName + rpMsg->a_HelloMsg.he_DBNameOff);
	} else {
	    DBASSERT(rpMsg->a_HelloMsg.he_Type == CTYPE_REPLIC);
	    h = AllocHostInfo(rpMsg->a_HelloMsg.he_HostName, rpMsg->a_HelloMsg.he_Type, rpMsg->a_HelloMsg.he_Addr);
	    d = NULL;
	}
	r = FindRouteInfo(l, d, rpMsg->a_HelloMsg.he_Addr);
	DBASSERT(r == NULL);
	blockSize = (rpMsg->a_HelloMsg.he_BlockShift < 0) ? 0 : 1 << rpMsg->a_HelloMsg.he_BlockShift;
	AllocRouteInfo(l, h, d, 
	    rpMsg->rpa_Pkt.pk_Hop, rpMsg->a_HelloMsg.he_Latency, rpMsg->a_HelloMsg.he_RepCount,
	    rpMsg->a_HelloMsg.he_Type, blockSize,
	    rpMsg->a_HelloMsg.he_SyncTs, rpMsg->a_HelloMsg.he_MinCTs
	);
	if (d)
	    DoneDBInfo(d);
	FreeHostInfo(h);
	rv = 0;
	break;
    case RPCMD_GOODBYE:
	/*
	 * note: it is important not to update the hop count when deleting 
	 * a route so the link code knows what the last valid hop count was
	 * at the time of deletion.
	 */
	if (rpMsg->a_HelloMsg.he_DBNameOff)
	    d = AllocDBInfo(rpMsg->a_HelloMsg.he_HostName + rpMsg->a_HelloMsg.he_DBNameOff);
	else
	    d = NULL;
	r = FindRouteInfo(l, d, rpMsg->a_HelloMsg.he_Addr);
	if (d)
	    DoneDBInfo(d);
	DBASSERT(r != NULL);
	FreeRouteInfo(r);
	rv = 0;
	break;
    case RPCMD_ADJUST:
	if (rpMsg->a_HelloMsg.he_DBNameOff)
	    d = AllocDBInfo(rpMsg->a_HelloMsg.he_HostName + rpMsg->a_HelloMsg.he_DBNameOff);
	else
	    d = NULL;
	r = FindRouteInfo(l, d, rpMsg->a_HelloMsg.he_Addr);
	if (d)
	    DoneDBInfo(d);
	DBASSERT(r != NULL);
	UseRouteInfo(r);
	if (r->r_Latency != rpMsg->a_HelloMsg.he_Latency || 
	    r->r_Hop != rpMsg->rpa_Pkt.pk_Hop ||
	    r->r_Type != rpMsg->a_HelloMsg.he_Type ||
	    r->r_State != rpMsg->a_HelloMsg.he_State ||
	    r->r_BlockSize != (1 << rpMsg->a_HelloMsg.he_BlockShift) ||
	    r->r_RepCount != rpMsg->a_HelloMsg.he_RepCount ||
	    r->r_SyncTs != rpMsg->a_HelloMsg.he_SyncTs ||
	    r->r_MinCTs != rpMsg->a_HelloMsg.he_MinCTs
	) {
	    rp_hop_t oldHop = r->r_Hop;

	    dbinfo3("ROUTE %p ADJUST %s:%s:%08qx latency %d->%d, hop %d->%d state %d->%d blksize %d repcnt %d->%d type %d->%d sync=%016qx (%qd) minct=%016qx (%qd)\n",
		r,
		r->r_Host->h_HostName, 
		(r->r_RHInfo->rh_DBInfo ? r->r_RHInfo->rh_DBInfo->d_DBName : "?"),
		rpMsg->a_HelloMsg.he_Addr,
		r->r_Latency, rpMsg->a_HelloMsg.he_Latency,
		r->r_Hop, rpMsg->rpa_Pkt.pk_Hop,
		r->r_State, rpMsg->a_HelloMsg.he_State,
		1 << rpMsg->a_HelloMsg.he_BlockShift,
		r->r_RepCount, rpMsg->a_HelloMsg.he_RepCount,
		r->r_Type, rpMsg->a_HelloMsg.he_Type,
		rpMsg->a_HelloMsg.he_SyncTs,
		rpMsg->a_HelloMsg.he_SyncTs - r->r_SyncTs,
		rpMsg->a_HelloMsg.he_MinCTs,
		rpMsg->a_HelloMsg.he_MinCTs - r->r_MinCTs
	    );

	    r->r_Latency = rpMsg->a_HelloMsg.he_Latency;
	    r->r_Hop = rpMsg->rpa_Pkt.pk_Hop;
	    r->r_State = rpMsg->a_HelloMsg.he_State;
	    r->r_Type = rpMsg->a_HelloMsg.he_Type;
	    r->r_RepCount = rpMsg->a_HelloMsg.he_RepCount;
	    r->r_SyncTs = rpMsg->a_HelloMsg.he_SyncTs;
	    r->r_MinCTs = rpMsg->a_HelloMsg.he_MinCTs;
	    r->r_BlockSize = 1 << rpMsg->a_HelloMsg.he_BlockShift;
	    /* XXX what about address changes? */
	    NotifyAllLinks(r, oldHop);
	}
	UnuseRouteInfo(r);
	rv = 0;
	break;
    }
    FreeRPMsg(rpMsg);
    return(rv);
}

/*
 * Send link-level control
 */

int
CmdControl(LinkInfo *l, const char *id, rp_cmd_t ctlOp, char **emsg)
{
    int r;
    RPAnyMsg *rpMsg;
    int hlen = strlen(id) + 1;

    rpMsg = AllocLLPkt(ctlOp, offsetof(RPSysctlMsg, sc_Buffer[hlen]));
    bcopy(id, rpMsg->a_SysctlMsg.sc_Buffer, hlen);

    if ((r = WriteLLPkt(l, rpMsg, 1)) == 0) {
	r = -1;
	while ((rpMsg = ReadPkt(l)) != NULL) {
	    if ((rpMsg->rpa_Pkt.pk_Cmd & RPCMDF_REPLY) == 0) {
		switch(rpMsg->rpa_Pkt.pk_Cmd) {
		case RPCMD_HELLO:
		case RPCMD_GOODBYE:
		case RPCMD_ADJUST:
		    RecNotifyHello(l, rpMsg);
		    continue;
		default:
		    dberror("CmdControl: Unsupported command received %02x\n",
			rpMsg->rpa_Pkt.pk_Cmd
		    );
		    break;
		}
		break;
	    }

	    /*
	     * We are expecting a synchronous reply to our command.
	     */
	    if (rpMsg->rpa_Pkt.pk_Cmd == (ctlOp | RPCMDF_REPLY)) {
		r = rpMsg->rpa_Pkt.pk_RCode;
		if (rpMsg->rpa_Pkt.pk_Bytes > RPPKTSIZE(RPSysctlMsg) &&
		    ((char *)&rpMsg->rpa_Pkt)[rpMsg->rpa_Pkt.pk_Bytes - 1] == 0
		) {
		    int len;

		    len = strlen(rpMsg->a_SysctlMsg.sc_Buffer);
		    *emsg = malloc(len + 1);
		    bcopy(&rpMsg->a_SysctlMsg.sc_Buffer, *emsg, len);
		    (*emsg)[len] = 0;
		}
	    }
	    FreeRPMsg(rpMsg);
	    break;
	}
    }
    return(r);
}

