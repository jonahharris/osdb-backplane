/*
 * REPLICATOR/PKT.C -	Replication packet (encoded on a stream)
 *
 * (c)Copyright 2000-2002 Backplane, Inc.  Please refer to the COPYRIGHT
 * file at the base of the distribution tree.
 *
 * $Backplane: rdbms/replicator/pkt.c,v 1.40 2002/08/20 22:06:00 dillon Exp $
 */

#include "defs.h"

Prototype RPAnyMsg *AllocLLPkt(rp_cmd_t cmd, int len);
Prototype RPAnyMsg *AllocPkt(HostInfo *h, rp_cmd_t cmd, int len);
Prototype RPAnyMsg *DupPkt(RPAnyMsg *rpMsg);
Prototype RPAnyMsg *BuildVCPkt(InstInfo *ii, rp_cmd_t cmd, int len);
Prototype RPAnyMsg *BuildVCPktStr(InstInfo *ii, rp_cmd_t cmd, const char *data);
Prototype RPAnyMsg *SimulateReceivedVCPkt(InstInfo *ii, rp_cmd_t cmd);
Prototype void WriteVCPkt(RPAnyMsg *rpMsg);
Prototype void ForwardPkt(LinkInfo *l, RPAnyMsg *rpMsg);
Prototype void FreeRPMsg(RPAnyMsg *rpMsg);
Prototype void *ReadPkt(LinkInfo *li);
Prototype void ReturnVCPkt(InstInfo *ii, RPAnyMsg *rrpMsg, int len, int error);
Prototype int ReturnLLPkt(LinkInfo *l, RPAnyMsg *rrpMsg, int error, const char *msg);
Prototype void QueuePkt(LinkInfo *l, RPAnyMsg *rpMsg, int force);
Prototype int WriteLLPkt(LinkInfo *l, RPAnyMsg *rpMsg, int flushMe);
Prototype int FlushLLPkt(LinkInfo *l);

static void ReceiveVCPkt(RPAnyMsg *rpMsg);

/*
 * AllocLLPkt() - Allocate a link level packet
 *
 *	Link level packets have SrcA == 0 and DstA == 0
 */

RPAnyMsg *
AllocLLPkt(rp_cmd_t cmd, int len)
{
    RPAnyMsg *rpMsg;
    int n;

    DBASSERT(len >= sizeof(RPMsg));
    n = (len + 3) & ~3;

    rpMsg = zalloc(n);
    rpMsg->a_Msg.rp_Refs = 1;
    rpMsg->a_Msg.rp_Pkt.pk_Magic = RP_MAGIC;
    rpMsg->a_Msg.rp_Pkt.pk_Cmd = cmd;
    rpMsg->a_Msg.rp_Pkt.pk_Bytes = n - offsetof(RPMsg, rp_Pkt);
    rpMsg->a_Msg.rp_Pkt.pk_Hop = 0;

    return(rpMsg);
}

/*
 * AllocPkt() -	Allocate a packet to be sent to host h.
 */

RPAnyMsg *
AllocPkt(HostInfo *h, rp_cmd_t cmd, int len)
{
    RPAnyMsg *rpMsg = AllocLLPkt(cmd, len);
    SeqInfo *s;

    rpMsg->a_Msg.rp_SHost = &MyHost;
    rpMsg->a_Msg.rp_DHost = h;
    rpMsg->rpa_Pkt.pk_SAddr = MyHost.h_Addr;
    rpMsg->rpa_Pkt.pk_DAddr = h->h_Addr;

    UseHostInfo(rpMsg->a_Msg.rp_SHost);
    UseHostInfo(rpMsg->a_Msg.rp_DHost);

    s = AllocSeqInfo(&MyHost, h, cmd);
    rpMsg->rpa_Pkt.pk_SeqNo = s->s_SeqNo;
    rpMsg->rpa_Pkt.pk_VCId = s->s_VCId;
    ++s->s_SeqNo;
    FreeSeqInfo(s);

    return(rpMsg);
}

RPAnyMsg *
BuildVCPkt(InstInfo *ii, rp_cmd_t cmd, int len)
{
    RPAnyMsg *rpMsg;
    HostInfo *h;
    SeqInfo *s;

    /*
     * Calculate destination host - depends on whether this packet is a
     * command or a reply.   The source host is always &MyHost.
     */
    if (cmd & RPCMDF_REPLY) {
	h = ii->i_MasterHost;
	DBASSERT(ii->i_Flags & IIF_SLAVE);
    } else {
	h = ii->i_SlaveHost;
	DBASSERT(ii->i_Flags & IIF_MASTER);
    }

    rpMsg = AllocLLPkt(cmd, len);
    rpMsg->a_Msg.rp_SHost = &MyHost;
    rpMsg->a_Msg.rp_DHost = h;
    rpMsg->rpa_Pkt.pk_SAddr = MyHost.h_Addr;
    rpMsg->rpa_Pkt.pk_DAddr = h->h_Addr;

    UseHostInfo(rpMsg->a_Msg.rp_SHost);
    UseHostInfo(rpMsg->a_Msg.rp_DHost);

    rpMsg->rpa_Pkt.pk_InstId = ii->i_InstId;
    ii->i_VCILCmd = cmd;	/* status tracking */

    /*
     * Don't allow old failed instances send data on the current VC.
     */
    s = AllocSeqInfo(&MyHost, h, cmd);
    if (s->s_VCId == ii->i_VCId) {
	rpMsg->rpa_Pkt.pk_SeqNo = s->s_SeqNo;
	rpMsg->rpa_Pkt.pk_VCId = s->s_VCId;
	++s->s_SeqNo;
    } else {
	rpMsg->rpa_Pkt.pk_SeqNo = 0;
	rpMsg->rpa_Pkt.pk_VCId = (rp_vcid_t)-1;
    }
    FreeSeqInfo(s);

    return(rpMsg);
}

RPAnyMsg *
BuildVCPktStr(InstInfo *ii, rp_cmd_t cmd, const char *str)
{
    int len;
    RPAnyMsg *rpMsg;

    len = strlen(str) + 1;
    rpMsg = BuildVCPkt(ii, cmd, offsetof(RPMsg, rp_Pkt.pk_Data[len]));
    bcopy(str, rpMsg->rpa_Pkt.pk_Data, len);

    return(rpMsg);
}

/*
 *  SimulateReceivedVCPkt()
 *
 *  Simulate a packet received over a VC.  ii represents the master/slave
 *  instance the packet was received on.  If the packet is a reply then
 *  DHost is the master, else the packet is a command and DHost is the slave.
 */

RPAnyMsg *
SimulateReceivedVCPkt(InstInfo *ii, rp_cmd_t cmd)
{
    RPAnyMsg *rpMsg;

    rpMsg = AllocLLPkt(cmd, 0);

    if (cmd & RPCMDF_REPLY) {
	rpMsg->a_Msg.rp_SHost = ii->i_SlaveHost;
	rpMsg->a_Msg.rp_DHost = ii->i_MasterHost;
    } else {
	rpMsg->a_Msg.rp_SHost = ii->i_MasterHost;
	rpMsg->a_Msg.rp_DHost = ii->i_SlaveHost;
    }
    rpMsg->a_Msg.rp_InstInfo = ii;
    UseInstInfo(ii);
    UseHostInfo(rpMsg->a_Msg.rp_SHost);
    UseHostInfo(rpMsg->a_Msg.rp_DHost);
    return(rpMsg);
}

/*
 * WriteVCPkt()
 *
 *	note: the 'now' flag indicates that the data should be pushed out now.
 *	This is typically set when a response is expected.  Otherwise the
 *	data is queued for batch transmission later on.
 *
 *	XXX do not write data if instance has failed or VCId mismatch
 */

void
WriteVCPkt(RPAnyMsg *rpMsg)
{
    /*
     * Handle local loopback efficiently, else return
     * over the spanning tree.
     */
    if (rpMsg->rpa_Pkt.pk_VCId != (rp_vcid_t)-1) {
	if (rpMsg->a_Msg.rp_DHost == &MyHost)
	    ReceiveVCPkt(rpMsg);
	else
	    QueuePkt(NULL, rpMsg, 0);
    } else {
	FreeRPMsg(rpMsg);
    }
}

void
FreeRPMsg(RPAnyMsg *rpMsg)
{
    DBASSERT(rpMsg->rpa_Pkt.pk_Magic == RP_MAGIC);
    if (--rpMsg->a_Msg.rp_Refs == 0) {
	int rpSize;

	if (rpMsg->a_Msg.rp_SHost) {
	    UnuseHostInfo(rpMsg->a_Msg.rp_SHost);
	    rpMsg->a_Msg.rp_SHost = NULL;
	}
	if (rpMsg->a_Msg.rp_DHost) {
	    UnuseHostInfo(rpMsg->a_Msg.rp_DHost);
	    rpMsg->a_Msg.rp_DHost = NULL;
	}
	if (rpMsg->a_Msg.rp_InstInfo) {
	    UnuseInstInfo(rpMsg->a_Msg.rp_InstInfo);
	    rpMsg->a_Msg.rp_InstInfo = NULL;
	}
	rpMsg->rpa_Pkt.pk_Magic = 0;

	rpSize = sizeof(RPMsg) - sizeof(RPPkt) +
	    ((rpMsg->rpa_Pkt.pk_Bytes + 3) & ~3);
	zfree(rpMsg, rpSize);
    } 
}

/*
 * ReadPkt() - read a packet from the network
 *
 *	The next ready packet is read from the network
 *
 *	XXX emplace timeout on link (20 seconds?)
 */
void *
ReadPkt(LinkInfo *l)
{
    RPPkt pk;
    RPAnyMsg *rpMsg;
    int msgSize;

retry:
    /*
     * Read packet header
     */
    {
	int n;

	n = t_read(l->l_Ior, &pk, sizeof(pk), 0);
	if (n != sizeof(pk))
	    return(NULL);
    }
    if (pk.pk_Magic != RP_MAGIC) {
	dberror("no magic %02x\n", pk.pk_Magic);
	return(NULL);
    }
    if (pk.pk_Bytes < sizeof(pk)) {
	dberror("bytes too low\n");
	return(NULL);
    }

    /*
     * Allocate a packet buffer, copy the header, and read the remainder.
     */
    msgSize = sizeof(RPMsg) - sizeof(pk) + ((pk.pk_Bytes + 3) & ~3);
    rpMsg = zalloc(msgSize);
    rpMsg->a_Msg.rp_Refs = 1;
    bcopy(&pk, &rpMsg->rpa_Pkt, sizeof(pk));

    {
	int n;
	int left;

	left = msgSize - sizeof(RPMsg);
	if (left)
	    n = t_read(l->l_Ior, &rpMsg->rpa_Pkt.pk_Data[0], left, 0);
	else
	    n = 0;
	if (n != left) {
	    FreeRPMsg(rpMsg);
	    return(NULL);
	}
    }

    /*
     * Figure out where packet came from and where it is destined to.
     */
    if (rpMsg->rpa_Pkt.pk_SAddr != 0) {
	RouteInfo *r;

	if ((r = FindRouteInfo(l, NULL, rpMsg->rpa_Pkt.pk_SAddr)) != NULL) {
	    rpMsg->a_Msg.rp_SHost = r->r_Host;
	    UseHostInfo(r->r_Host);
	}

	if (rpMsg->a_Msg.rp_SHost == NULL) {
	    dberror("%s: Link failed, bad src address %08x cmd %02x\n",
		(l->l_PeerHost ? l->l_PeerHost->h_HostName : "?"),
		rpMsg->rpa_Pkt.pk_SAddr,
		rpMsg->rpa_Pkt.pk_Cmd
	    );
	    FreeRPMsg(rpMsg);
	    return(NULL);
	}
    }

    /*
     * pk_DstA, if not 0, represents an address from our point of view.
     */
    if (rpMsg->rpa_Pkt.pk_DAddr != 0) {
	RouteInfo *r;

	if ((r = FindRouteInfo(l, NULL, rpMsg->rpa_Pkt.pk_DAddr)) != NULL) {
	    rpMsg->a_Msg.rp_DHost = r->r_Host;
	    UseHostInfo(r->r_Host);
	}

	if (rpMsg->a_Msg.rp_DHost == NULL) {
	    dberror("%s: Link failed, bad dest address %08x cmd %02x\n",
		(l->l_PeerHost ? l->l_PeerHost->h_HostName : "?"),
		rpMsg->rpa_Pkt.pk_DAddr,
		rpMsg->rpa_Pkt.pk_Cmd
	    );
	    FreeRPMsg(rpMsg);
	    return(NULL);
	}
    } else {
	rpMsg->a_Msg.rp_DHost = &MyHost;
	UseHostInfo(&MyHost);
    }

    /*
     * Packets sent over VCs (virtual circuits) have pk_SAddr and pk_DAddr
     * set.
     *
     * Due to the saturation (or semi-saturation) model used by the spanning
     * tree to propogate packets, we have to track packets running over VCs
     * in order to discard (and not re-propogate) duplicate packets.
     */
    if (rpMsg->rpa_Pkt.pk_SAddr && rpMsg->rpa_Pkt.pk_DAddr) {
	SeqInfo *s;

	s = AllocSeqInfo(rpMsg->a_Msg.rp_SHost, rpMsg->a_Msg.rp_DHost, rpMsg->rpa_Pkt.pk_Cmd);

#if 0
	if ((rp_seq_t)(s->s_SeqNo + 1) != rpMsg->rpa_Pkt.pk_SeqNo) {
	    printf("RXPKT FAILED seq %d/%d vc %04x/%04x\n",
		rpMsg->rpa_Pkt.pk_SeqNo, s->s_SeqNo,
		rpMsg->rpa_Pkt.pk_VCId, s->s_VCId
	    );
	}
#endif

	if (rpMsg->rpa_Pkt.pk_VCId != s->s_VCId) {
	    /*
	     * Connection reset.  Connections that go through us can be
	     * picked up in the middle, connections that go to us must
	     * be picked up at the beginning.
	     */
	    if (rpMsg->a_Msg.rp_DHost == &MyHost && s->s_SlaveHost == &MyHost) {
		s->s_SeqNo = (rp_seq_t)-1;
		s->s_VCId = rpMsg->rpa_Pkt.pk_VCId;
		s->s_Other->s_SeqNo = 0;
		s->s_Other->s_VCId = rpMsg->rpa_Pkt.pk_VCId;
	    } else {
		DBASSERT(rpMsg->a_Msg.rp_SHost != &MyHost);
		s->s_SeqNo = rpMsg->rpa_Pkt.pk_SeqNo - 1;
		s->s_VCId = rpMsg->rpa_Pkt.pk_VCId;
	    }
	} else if ((rp_seq_signed_t)(rpMsg->rpa_Pkt.pk_SeqNo - s->s_SeqNo) <= 0) {
	    /*
	     * Connect good, check packet sequence number and ignore packets
	     * that we have already seen.
	     */
	    FreeSeqInfo(s);
	    FreeRPMsg(rpMsg);
	    goto retry;
	}

	/*
	 * Ignore packets that are out of sequence.  This can occur due
	 * to links going up and down.  We do not want to skip because it
	 * may cause us to ignore a crucial packet with a smaller sequence
	 * number.
	 */
	if (rpMsg->rpa_Pkt.pk_SeqNo != (rp_seq_t)(s->s_SeqNo + 1)) {
	    FreeSeqInfo(s);
	    FreeRPMsg(rpMsg);
	    printf("WARNING, RXPKT FAILED seq %d/%d vc %04x/%04x\n", rpMsg->rpa_Pkt.pk_SeqNo, s->s_SeqNo,
		    rpMsg->rpa_Pkt.pk_VCId, s->s_VCId);
	    goto retry;
	}
	s->s_SeqNo = rpMsg->rpa_Pkt.pk_SeqNo;
	FreeSeqInfo(s);
    }

    /*
     * If the packet is not destined for us, forward it.
     */
    if (rpMsg->a_Msg.rp_DHost != &MyHost) {
	ForwardPkt(l, rpMsg);
	goto retry;
    }

    /*
     * If the packet came over a VC then forward the packet to the appropriate
     * VC instance depending on SHost/DHost and whether the packet is a command
     * or a reply.  The VC manager is responsible for handling sequence number
     * restarts and such.
     */
    if (rpMsg->a_Msg.rp_SHost != NULL) {
	ReceiveVCPkt(rpMsg);
	goto retry;
    }

    return(rpMsg);
}

static void
ReceiveVCPkt(RPAnyMsg *rpMsg)
{
    InstInfo *ii;
    InstInfo *mi = NULL;
    HostInfo *s = rpMsg->a_Msg.rp_SHost;
    HostInfo *d = rpMsg->a_Msg.rp_DHost;

    /*
     * Incoming packet is either a command or a reply, effecting which 
     * type of instance structure to look for (master or slave).
     *
     * We must ignore packets related to a failed master or slave VC link.
     */
    if (rpMsg->rpa_Pkt.pk_Cmd & RPCMDF_REPLY) {
	ii = FindInstInfo(d, s, IIF_MASTER, rpMsg->rpa_Pkt.pk_InstId, rpMsg->rpa_Pkt.pk_VCId);
    } else {
	mi = VCManagerSlaveKick(s, d, rpMsg->rpa_Pkt.pk_VCId);
	ii = FindInstInfo(s, d, IIF_SLAVE, rpMsg->rpa_Pkt.pk_InstId, rpMsg->rpa_Pkt.pk_VCId);
    }

    if (rpMsg->rpa_Pkt.pk_Cmd == RPCMD_OPEN_INSTANCE) {
	DBInfo *db = AllocDBInfo(rpMsg->a_OpenInstanceMsg.oi_DBName);

	DBASSERT(ii == NULL);
	if (mi) {
	    ii = AllocInstInfo(mi, s, d, db, 
		IIF_SLAVE|IIF_PRIVATE, rpMsg->rpa_Pkt.pk_InstId);
	    taskCreate(VCInstanceSlave, ii);
	}
	DoneDBInfo(db);
	FreeRPMsg(rpMsg);
    } else if (ii) {
	if ((ii->i_Flags & IIF_FAILED) == 0 || ii->i_InstId == 0) {
	    rpMsg->a_Msg.rp_InstInfo = ii;
	    UseInstInfo(ii);

	    addTail(ii->i_RXPktList, &rpMsg->a_Msg.rp_Node);
	    issueNotify(ii->i_NotifyInt);
	} else {
	    dberror(
		"RECEIVED PACKET %02x INSTID %d - instance marked failed\n", 
		rpMsg->rpa_Pkt.pk_Cmd,
		rpMsg->rpa_Pkt.pk_InstId
	    );
	    FreeRPMsg(rpMsg);
	}
    } else {
	dberror(
	    "RECEIVED PACKET %02x INSTID %d VCId %04x - instance not found\n", 
	    rpMsg->rpa_Pkt.pk_Cmd,
	    rpMsg->rpa_Pkt.pk_InstId,
	    rpMsg->rpa_Pkt.pk_VCId
	);
	FreeRPMsg(rpMsg);
    }
}

/*
 * ReturnVCPkt() - return packet to sender with optional error.
 *
 *	This function returns a VC (virtual circuit) packet to the sender.
 *	It also flushes the link.
 */

void
ReturnVCPkt(InstInfo *ii, RPAnyMsg *rrpMsg, int len, int error)
{
    RPAnyMsg *wrpMsg;
    SeqInfo *s;

    DBASSERT(rrpMsg->rpa_Pkt.pk_SAddr != 0);
    DBASSERT(len >= sizeof(RPMsg));

    s = AllocSeqInfo(&MyHost, rrpMsg->a_Msg.rp_SHost, RPCMDF_REPLY);

    if (ii->i_VCId == s->s_VCId) {
	wrpMsg = AllocPkt(
		    rrpMsg->a_Msg.rp_SHost, 
		    rrpMsg->rpa_Pkt.pk_Cmd | RPCMDF_REPLY, 
		    len
		);
	wrpMsg->rpa_Pkt.pk_RCode = error;
	wrpMsg->rpa_Pkt.pk_InstId = rrpMsg->rpa_Pkt.pk_InstId;

	/*
	 * note: TFlags left cleared on return, which means nopush
	 * will not be set so the link will flush.
	 */

	dbinfo4("INST %p ReturnVCPkt rc=%d iid=%04x\n",
	    ii, error, wrpMsg->rpa_Pkt.pk_InstId
	);

	/*
	 * Additional data
	 */
	if (len > sizeof(RPMsg)) {
	    bcopy(
		&rrpMsg->rpa_Pkt.pk_Data[0],
		&wrpMsg->rpa_Pkt.pk_Data[0],
		len - sizeof(RPMsg)
	    );
	}

	/*
	 * Optimize the local loopback path, else return over the
	 * spanning tree.
	 */
	if (wrpMsg->a_Msg.rp_DHost == &MyHost)
	    ReceiveVCPkt(wrpMsg);
	else
	    QueuePkt(NULL, wrpMsg, 1);
    }
    FreeSeqInfo(s);
    FreeRPMsg(rrpMsg);
}

/*
 * ReturnLLPkt() - return link level packet to sender
 */

int
ReturnLLPkt(LinkInfo *l, RPAnyMsg *rrpMsg, int error, const char *msg)
{
    RPAnyMsg *wrpMsg;
    int msgLen = msg ? strlen(msg) + 1 : 0;

    wrpMsg = AllocLLPkt(rrpMsg->rpa_Pkt.pk_Cmd | RPCMDF_REPLY, sizeof(RPMsg) + msgLen);
    wrpMsg->rpa_Pkt.pk_RCode = error;
    if (msgLen)
	bcopy(msg, &wrpMsg->rpa_Pkt.pk_Data[0], msgLen);
    FreeRPMsg(rrpMsg);
    return(WriteLLPkt(l, wrpMsg, 1));
}

/*
 * ForwardPkt() -	Forward the packet if we are not the destination.
 *			The link the packet came in on must be provided.
 *
 *	Note that we force flush the forwarded packet.  We depend on
 *	buffering at the originator.  The output will still be somewhat
 *	buffered due to the threading (which is what we want) XXX.
 *
 *	We do not want to buffer per-say because it introduces serious
 *	latency and RPTF_NOPUSH doesn't work for intermediate nodes.
 */
void
ForwardPkt(LinkInfo *l, RPAnyMsg *rpMsg)
{
    if (rpMsg->rpa_Pkt.pk_Hop == RP_MAXHOPS) {
	FreeRPMsg(rpMsg);
    } else {
	++rpMsg->rpa_Pkt.pk_Hop;
	QueuePkt(l, rpMsg, 1);
    }
}

/*
 * QueuePkt() - Queue outgoing packet to multiple interfaces and free the 
 *		packet.
 *
 *	If l is non-NULL it implies a packet that was recieved on the
 *	interface and is now being forwarded, and use to avoid an unneeded
 *	transmission of the packet.
 */

void
QueuePkt(LinkInfo *l, RPAnyMsg *rpMsg, int force)
{
    RouteHostInfo *rh;

    for (rh = rpMsg->a_Msg.rp_DHost->h_RHInfoBase; rh; rh = rh->rh_HNext) {
	RouteInfo *r;
	RouteInfo *best = NULL;

	/*
	 * We want the host route (rh_DBInfo == NULL), not all the
	 * individual database routes.
	 */
	if (rh->rh_DBInfo)
	    continue;

	/*
	 * Figure out the best route
	 */
	for (r = rh->rh_Routes; r; r = r->r_RHNext) {
	    if (r->r_Refs == 0 || r->r_Phys == l)
		continue;
	    if (best == NULL || best->r_Hop > r->r_Hop)
		best = r;
	}

	/*
	 * Forward the packet
	 */
	for (r = rh->rh_Routes; r; r = r->r_RHNext) {
	    /*
	     * Do not route through an inactive route that is in the
	     * process of being deleted (Refs == 0).
	     *
	     * Do not bounce the packet back to the sending replicator,
	     * since it already obviously has it (r->r_Phys == l).
	     *
	     * XXX shutdown/free releases host/routes out from under us.
	     */
	    if (r->r_Refs == 0 || r->r_Phys == l)
		continue;
	    if (r == best) {
		int now = force || !(rpMsg->rpa_Pkt.pk_TFlags & RPTF_NOPUSH);
		++rpMsg->a_Msg.rp_Refs;
		NotifyLink(r->r_Phys, rpMsg, 0, now);
	    } else {
#if 0
		++rpMsg->a_Msg.rp_Refs;
		NotifyLink(r->r_Phys, rpMsg, 0, 0);
#endif
	    }
	}
    }
    FreeRPMsg(rpMsg);
}

/*
 * WriteLLPkt() - write a constructed packet to the destination link, then
 *		free it.
 *
 *	This routine must be used to write link-level packets which do
 *	not have a source address, destination address, or sequence number.
 */

int
WriteLLPkt(LinkInfo *l, RPAnyMsg *rpMsg, int flushMe)
{
    int bytes = (rpMsg->rpa_Pkt.pk_Bytes + 3) & ~3;
    int r = -1;

    if (t_mwrite(l->l_Iow, &rpMsg->rpa_Pkt, bytes, 0) == bytes)
	r = 0;
    if (flushMe) {
	if (t_mflush(l->l_Iow, 0) < 0)
	    r = -1;
    }
    FreeRPMsg(rpMsg);
    return(r);
}

/*
 * FlushLLPkt() - flush buffered link level packets to their socket
 */
int
FlushLLPkt(LinkInfo *l)
{
    return(t_mflush(l->l_Iow, 0));
}

