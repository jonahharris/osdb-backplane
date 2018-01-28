/*
 * REPLICATOR/REPLICATE.C	- The replication protocol.
 *
 * (c)Copyright 2000-2002 Backplane, Inc.  Please refer to the COPYRIGHT
 * file at the base of the distribution tree.
 *
 * $Backplane: rdbms/replicator/replicate.c,v 1.26 2002/08/20 22:06:00 dillon Exp $
 *
 *	The slave side of a replication link is responsible for receiving
 *	commands from a remote master, executing, and replying to them.
 */

#include "defs.h"

Prototype void MainReplicationMsgAcceptor(DBManageNode *dbm);

Prototype void ReplicationReaderThread(LinkInfo *l);
static void ReplicationWriterThread(LinkInfo *l);

/*
 * MainReplicationMsgAcceptor()
 *
 *	The slaved replicator accepts HELLO messages with descriptors from
 *	the master replicator process and hands them off to their own
 *	replication protocol threads.
 */

void
MainReplicationMsgAcceptor(DBManageNode *dbm)
{
    for (;;) {
        CLAnyMsg *msg;
        iofd_t passedio = NULL;
 
        msg = RecvCLMsg(dbm->dm_ControlIo, &passedio);
	if (msg == NULL)
	    break;

        if (passedio) {
	    LinkInfo *l;

	    DBASSERT(msg->cma_Pkt.cp_Cmd = CLCMD_HELLO);
	    l = AllocLinkInfo(NULL, CTYPE_UNKNOWN, passedio);
	    l->l_DBName = safe_strdup(msg->a_HelloMsg.hm_DBName);
            taskCreate(ReplicationReaderThread, l);
        } 
	FreeCLMsg(msg);
    }

    /*
     * Control link lost from master replicator, the best thing to do here
     * is to simply exit.  The database processes will clean up after 
     * themselves.
     */
    exit(1);
}

/*
 * ReplicationReaderThread()	- master and slave side link connection
 *
 *	This thread deals with outgoing replication links as well
 *	as incoming connections to the child replicator (after the
 *	master replicator has routed the descriptor to the child).
 */
void
ReplicationReaderThread(LinkInfo *l)
{
    RPAnyMsg *rpMsg;

    /*
     * Exchange HELLO packets.  A slave receives a hello packet from
     * the connecting source first, then sends one in response.  The
     * master starts by sending a HELLO packet and then receives one.
     */

    if (CmdStartupHello(l) < 0) {
	dberror("Failed to transmit HELLO stage 1\n");
	SetLinkStatus(l, "TxHello: failed");
	FreeLinkInfo(l);
	return;
    }
    SetLinkStatus(l, "Transmit Hello: sent");
    if (RecStartupHello(l) < 0) {
	dberror("Failed to receive HELLO stage 2\n");
	SetLinkStatus(l, "RxHello: failed");
	FreeLinkInfo(l);
	return;
    }
    SetLinkStatus(l, "ONLINE");

    /*
     * Queueing up HELLOs to the new link to bring it up to date with
     * our current host state.  After this we take notification ints 
     * to keep things in sync.
     */
    ++l->l_Refs;
    taskCreate(ReplicationWriterThread, l);
    NotifyLinksStartup(l);

    while ((rpMsg = ReadPkt(l)) != NULL) {
	int sipl;

	sipl = raiseSIpl(1);

	switch(rpMsg->rpa_Pkt.pk_Cmd) {
	case RPCMD_HELLO:
	case RPCMD_GOODBYE:
	case RPCMD_ADJUST:
	    DBASSERT(l->l_DBName != NULL);
	    RecNotifyHello(l, rpMsg);
	    rpMsg = NULL;
	    break;
	case RPCMD_STOP_REPLICATOR:
	    /*
	     * The command is asking us to terminate the replicator
	     */
	    {
		fprintf(stderr, "Stopping replicator\n");
		ReturnLLPkt(l, rpMsg, 0, "Terminated");
		exit(1);
	    }
	    break;
	case RPCMD_SETOPT:
	case RPCMD_INFO:
	case RPCMD_START:
	case RPCMD_START_REPLICATOR:
	case RPCMD_STOP:
	case RPCMD_ADDLINK:
	case RPCMD_REMLINK:
	case RPCMD_CREATEDB:
	case RPCMD_CREATEPEER:
	case RPCMD_CREATESNAP:
	case RPCMD_DESTROYDB:
	case RPCMD_DOWNGRADEDB:
	case RPCMD_UPGRADEDB:
	    DBASSERT(l->l_DBName != NULL);
	    {
		int error;
		char *emsg = NULL;

		error = SysControl(l->l_DBName, rpMsg->a_SysctlMsg.sc_Buffer, rpMsg->rpa_Pkt.pk_Cmd, &emsg);
		ReturnLLPkt(l, rpMsg, error, emsg); /* emsg may be NULL*/
		rpMsg = NULL;
		if (emsg)
		    free(emsg);
	    }
	    break;
	default:
	    dberror("Unrecognized link-level command %d\n", 
		rpMsg->rpa_Pkt.pk_Cmd);
	    /* XXX */
	    break;
	}
	if (rpMsg)
	    FreeRPMsg(rpMsg);
	setSIpl(sipl);
    }

    SetLinkStatus(l, "LINKDEAD");
    /*
     * Make sure we do not get any spurious notifications prior to 
     * exiting the thread.
     */
    l->l_Flags |= LIF_EOF;
    issueNotify(l->l_NotifyInt);
    dbinfo2("LINK %p Link %s Dead\n",
	l,
	(l->l_PeerHost ? l->l_PeerHost->h_HostName : "?")
    );
    FreeLinkInfo(l);
}

/*
 * ReplicationWriterThread() - 	Notification of data or routes to write out
 *				to link.
 *
 *	Messages may be queued to the link without waking it up.  We must
 *	wakeup periodically to drain such messages.
 */

static void
ReplicationWriterThread(LinkInfo *l)
{
    for (;;) {
	LinkNotify *ln;

	while ((ln = DrainLinkNotify(l)) != NULL) {
	    if (ln->n_Cmd) {
		CmdNotifyHello(l, ln->n_Data, ln->n_Cmd);
		UnuseRouteInfo((RouteInfo *)ln->n_Data);
	    } else {
		WriteLLPkt(l, ln->n_Data, 0);
	    }
	    FreeLinkNotify(ln);
	}
	FlushLLPkt(l);
	if (l->l_Flags & LIF_EOF)
	    break;
	waitNotify(l->l_NotifyInt, 5000);
    }
    FreeLinkInfo(l);
}

