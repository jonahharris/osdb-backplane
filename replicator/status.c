/*
 * REPLICATOR/STATUS.C	- Execute replicator system control function
 *
 * (c)Copyright 2000-2002 Backplane, Inc.  Please refer to the COPYRIGHT
 * file at the base of the distribution tree.
 *
 * $Backplane: rdbms/replicator/status.c,v 1.33 2002/08/20 22:06:00 dillon Exp $
 */

#include "defs.h"

Prototype void GenStatus(const char *cmd, char **emsg);

static char *GenLinkStatus(void);
static char *GenPeerStatus(void);
static char *GenClientStatus(void);
static char *GenRDBMStatus(void);
static char *GenRouteStatus(void);
static char *GenHostStatus(void);

static const char *instFlagsToStr(int flags, char **alloc);
static const char *rpCmdToStr(rp_cmd_t cmd);

#define GS_LINKS	0x0001
#define GS_PEERS	0x0002
#define GS_CLIENTS	0x0004
#define GS_RDBMS	0x0008
#define GS_ROUTES	0x0010
#define GS_HOSTS	0x0020

#define GS_ALL	(GS_LINKS|GS_PEERS|GS_CLIENTS|GS_RDBMS|GS_ROUTES|GS_HOSTS)

void
GenStatus(const char *cmd, char **emsg)
{
    int flags = 0;
    char *lmsg = NULL;
    char *pmsg = NULL;
    char *cmsg = NULL;
    char *rmsg = NULL;
    char *rtmsg = NULL;

    if (cmd[0] == 0)
	flags = GS_ALL;
    else if (strcmp(cmd, "links") == 0)
	flags = GS_LINKS;
    else if (strcmp(cmd, "peers") == 0)
	flags = GS_PEERS;
    else if (strcmp(cmd, "clients") == 0)
	flags = GS_CLIENTS;
    else if (strcmp(cmd, "rdbms") == 0)
	flags = GS_RDBMS;
    else if (strcmp(cmd, "hosts") == 0)
	flags = GS_RDBMS;

    if (flags & GS_LINKS)
	lmsg = GenLinkStatus();
    if (flags & GS_PEERS)
	pmsg = GenPeerStatus();
    if (flags & GS_CLIENTS)
	cmsg = GenClientStatus();
    if (flags & GS_RDBMS)
	rmsg = GenRDBMStatus();
    if (flags & GS_ROUTES)
	rtmsg = GenRouteStatus();
    if (flags & GS_HOSTS)
	rtmsg = GenHostStatus();

    asprintf(emsg, "\n%s%s%s%s%s", 
	(lmsg ? lmsg : ""), 
	(pmsg ? pmsg : ""), 
	(cmsg ? cmsg : ""), 
	(rmsg ? rmsg : ""),
	(rtmsg ? rtmsg : "")
    );

    safe_free(&lmsg);
    safe_free(&pmsg);
    safe_free(&cmsg);
    safe_free(&rmsg);
    safe_free(&rtmsg);
}

static char *
GenLinkStatus(void)
{
    LinkMaintain *lm;
    char *msg = NULL;

    for (lm = LinkMaintainBase; lm; lm = lm->m_Next) {
	safe_appendf(&msg, "Link %04x \"%s\"\n", lm->m_Flags, lm->m_LinkCmd);
    }
    return(msg);
}

static char *
GenPeerStatus(void)
{
    LinkInfo *l;
    char *msg = NULL;

    for (l = LinkBase; l; l = l->l_Next) {
	const char *tname = NULL;

	switch(l->l_Type) {
	case CTYPE_PEER:
	    tname = "Peer";
	    break;
	case CTYPE_SNAP:
	    tname = "Snap";
	    break;
	case CTYPE_REPLIC:
	    tname = "Repl";
	    break;
	}
	if (tname) {
	    RouteInfo *r;

	    safe_appendf(&msg, "%s %04x \"%s\" Routes", 
		tname,
		l->l_Flags, 
		(l->l_PeerHost ? l->l_PeerHost->h_HostName : "(link lost)")
	    );
	    for (r = l->l_Routes; r; r = r->r_LNext) {
		if (r->r_RHInfo->rh_DBInfo)
		    continue;
		safe_appendf(&msg, " (%s:H%d:L%d:R%d)", 
		    r->r_Host->h_HostName, 
		    (int)r->r_Hop,
		    (int)r->r_Latency,
		    (int)r->r_RepCount
		);
	    }
	    safe_appendf(&msg, "\n");
	}
    }
    return(msg);
}

static char *
GenClientStatus(void)
{
    LinkInfo *li;
    DBInfo *d;
    char *msg = NULL;

    for (li = LinkBase; li; li = li->l_Next) {
	if (li->l_Type != CTYPE_PEER && li->l_Type != CTYPE_SNAP) {
	    const char *tname = "Unknown";

	    switch(li->l_Type) {
	    case CTYPE_ACCEPTOR:
		tname = "Accept ";
		break;
	    case CTYPE_CONTROL:
		tname = "Control";
		break;
	    case CTYPE_CLIENT:
		tname = "Client ";
		break;
	    case CTYPE_RELAY:
		tname = "Relay  ";
		break;
	    case CTYPE_REPLIC:
		tname = "Replic ";
		break;
	    }
	    safe_appendf(&msg, "%s %04x \"%s\" status %s queue %d\n", 
		tname, li->l_Flags, 
		(li->l_PeerHost ? li->l_PeerHost->h_HostName : "(lost)"),
		li->l_StatusMsg,
		li->l_NotifyCount
	    );
	}
    }

    /*
     * Scan connected clients by database
     */
    for (d = DBInfoBase; d; d = d->d_Next) {
	ClientInfo *cinfo;
	char *alloc1 = NULL;
	char *alloc2 = NULL;

	safe_appendf(&msg, "DB %s refs=%d use=%d\n", d->d_DBName, 
	    d->d_Refs, d->d_UseCount);
	for (
	    cinfo = getHead(&d->d_CIList); 
	    cinfo; 
	    cinfo = getListSucc(&d->d_CIList, &cinfo->ci_Node)
	) {
	    CLAnyMsg *clMsg;
	    InstInfo *ii;
	    int count = 0;
	    int icount = 0;

	    safe_appendf(&msg, "   cinfo %p rdy/cnt/ttl/QRM %d/%d/%d/%d"
			    " snps %d clsd %d"
			    " flgs %04x psh %d\n\tfrz %s ACTMIN %s\n",
		cinfo,
		cinfo->ci_Ready,
		cinfo->ci_Count,
		cinfo->ci_NumPeers,
		(cinfo->ci_NumPeers / 2) + 1,
		cinfo->ci_Snaps,
		cinfo->ci_Closed,
		cinfo->ci_Flags,
		cinfo->ci_Level,
		dbstamp_to_ascii_simple(cinfo->ci_MinSyncTs, 0, &alloc1),
		(cinfo->ci_Cd ? dbstamp_to_ascii_simple(cinfo->ci_Cd->cd_ActiveMinCTs, 0, &alloc2) : "?")
	    );

	    for (ii = cinfo->ci_IIBase; ii; ii = ii->i_Next) {
		char good = 'B';
		clMsg = ii->i_CLMsg;

		if (ii->i_RHSlave->rh_DBInfo &&
		    ii->i_RHSlave->rh_DBInfo->d_SyncTs >= cinfo->ci_MinSyncTs
		) {
		    good = 'G';
		}
		safe_appendf(&msg, "\tinst %p (%04x:%04x) %04x-%c QC%d QR%d\n",
		    ii,
		    ii->i_VCId,
		    ii->i_InstId,
		    ii->i_Flags , good,
		    (clMsg ? clMsg->a_Msg.cm_QCount : -1),
		    (clMsg ? clMsg->a_Msg.cm_QResponded : -1)
		);
		++icount;
	    }
	    for (
		clMsg = getHead(&cinfo->ci_CLMsgList);
		clMsg; 
		clMsg = getListSucc(&cinfo->ci_CLMsgList, &clMsg->a_Msg.cm_Node)
	    ) {
		int n;
		safe_appendf(&msg, "\tmsg %p cpcmd=%02x cpflg=%04x l=%d QC=%d QR=%d",
		    clMsg,
		    clMsg->cma_Pkt.cp_Cmd,
		    clMsg->cma_Pkt.cp_Flags,
		    clMsg->a_Msg.cm_Level,
		    clMsg->a_Msg.cm_QCount,
		    clMsg->a_Msg.cm_QResponded
		);
		n = 0;
		for (ii = cinfo->ci_IIBase; ii; ii = ii->i_Next) {
		    if (clMsg == ii->i_CLMsg)
			safe_appendf(&msg, " I%d", n);
		    ++n;
		}
		++count;
		safe_appendf(&msg, "\n");
	    }
	}
	safe_free(&alloc1);
	safe_free(&alloc2);
    }
    return(msg);
}

static char *
GenRDBMStatus(void)
{
    DBInfo *d;
    char *msg = NULL;
    char *alloc1 = NULL;
    char *alloc2 = NULL;
    char *alloc3 = NULL;
    char *alloc4 = NULL;

    for (d = DBInfoBase; d; d = d->d_Next) {
	RouteHostInfo *rh;
	CLDataBase *cd;

	safe_appendf(&msg, "%p Database %s blksize=%d\n", d, d->d_DBName, d->d_BlockSize);
	safe_appendf(&msg, "\t  LocalRepositories:");
	if (getHead(&d->d_CDList) == NULL)
	    safe_appendf(&msg, " [NO LOCAL REPOSITORIES RUNNING]");
	for (
	    cd = getHead(&d->d_CDList);
	    cd;
	    cd = getListSucc(&d->d_CDList, &cd->cd_Node)
	) {
	    safe_appendf(&msg, " CD=%p Refs=%d", cd, cd->cd_Refs);
	}
	safe_appendf(&msg, "\n", d->d_DBName);

	for (rh = d->d_RHInfoBase; rh; rh = rh->rh_DNext) {
	    RouteInfo *r;

	    for (r = rh->rh_Routes; r; r = r->r_RHNext) {
		const char *rType = "????";
		const char *via;

		switch(r->r_Type) {
		case CTYPE_PEER:
		    rType = "PEER";
		    break;
		case CTYPE_SNAP:
		    rType = "SNAP";
		    break;
		}

		if (r->r_Hop == 0) {
		    via = "[LOCAL REPOSITORY]";
		} else if (r->r_Hop == 1) {
		    via = "[DIRECT CONNECT]";
		} else if (r->r_Phys && r->r_Phys->l_PeerHost) {
		    via = r->r_Phys->l_PeerHost->h_HostName;
		} else {
		    via = "[UNKNOWN]";
		}
		safe_appendf(
		    &msg,
		    "  ROUTE hop=%d repcnt=%d type=%s to %s\n"
		    "\tBlkSize=%d via %s\n"
		    "\tSyncTs= %s\n"
		    "\tMinCTs= %s\n"
		    "\tScanTs= %s\n"
		    "\tSveCTs= %s\n",
		    (int)r->r_Hop,
		    (int)r->r_RepCount,
		    rType,
		    r->r_Host->h_HostName, 
		    r->r_BlockSize,
		    via,
		    dbstamp_to_ascii_simple(r->r_SyncTs, 0, &alloc1),
		    dbstamp_to_ascii_simple(r->r_MinCTs, 0, &alloc2),
		    dbstamp_to_ascii_simple(rh->rh_ScanTs, 0, &alloc3),
		    dbstamp_to_ascii_simple(r->r_SaveCTs, 0, &alloc4)
		);
	    }
	}
    }
    safe_free(&alloc1);
    safe_free(&alloc2);
    safe_free(&alloc3);
    safe_free(&alloc4);
    return(msg);
}

static char *
GenRouteStatus(void)
{
    return(NULL);
}

static char *
GenHostStatus(void)
{
    char *msg = NULL;
    char *alloc = NULL;
    InstInfo *ii;
    int type;

    for (type = 0; type < 2; ++type) {
	switch(type) {
	case 0:
	    safe_appendf(&msg, "\nMaster Instance Terminations\n");
	    break;
	case 1:
	    safe_appendf(&msg, "\nSlave Instance Terminations\n");
	    break;
	}
	for (ii = MyHost.h_SlaveInstBase; ii; ii = ii->i_SlaveNext) {
	    SeqInfo *si;

	    if (type == 0 && (ii->i_Flags & IIF_MASTER) == 0)
		continue;
	    if (type == 1 && (ii->i_Flags & IIF_MASTER) != 0)
		continue;

	    si = AllocSeqInfo(ii->i_MasterHost, &MyHost, 0);
	    safe_appendf(
		&msg, 
		"  INST  DBNAME=%-25s Remote=%s\n"
		"\tpsh=%d vcid=%04x iid=%04x cmd=%02x [%s,%s]\n"
		"\tadr=%p flags=%04x (%s)\n"
		"\tSequencer Local=%04x/%d Remote=%04x/%d\n",
		(ii->i_RHSlave->rh_DBInfo ? 
		    ii->i_RHSlave->rh_DBInfo->d_DBName : "[HOST-MANAGER]"),
		((ii->i_Flags & IIF_MASTER) ? ii->i_SlaveHost->h_HostName :
		    ii->i_MasterHost->h_HostName),
		ii->i_VCILevel,
		ii->i_VCId,
		ii->i_InstId,
		ii->i_VCILCmd & 0x7F,
		rpCmdToStr(ii->i_VCILCmd),
		((ii->i_VCILCmd & 0x80) ? "DONE" : 
		 ((ii->i_VCILCmd == 0x00) ? "----" : "ACTV")),
		ii,
		ii->i_Flags,
		instFlagsToStr(ii->i_Flags, &alloc),
		si->s_VCId,
		si->s_SeqNo,
		si->s_Other->s_VCId,
		si->s_Other->s_SeqNo
	    );
	    FreeSeqInfo(si);
	}
    }
    safe_free(&alloc);
    return(msg);
}

const char *
instFlagsToStr(int flags, char **alloc)
{
    safe_free(alloc);
    if (flags & IIF_MASTER)
	safe_appendf(alloc, " MASTER");
    if (flags & IIF_SLAVE)
	safe_appendf(alloc, " SLAVE");
    if (flags & IIF_READY)
	safe_appendf(alloc, " READY");
    if (flags & IIF_CLOSED)
	safe_appendf(alloc, " CLOSED");
    if (flags & IIF_FAILED)
	safe_appendf(alloc, " FAILED");
    if (flags & IIF_PRIVATE)
	safe_appendf(alloc, " PRIVATE");
    if (flags & IIF_RESET)
	safe_appendf(alloc, " RESET");
    if (flags & IIF_IDLE)
	safe_appendf(alloc, " IDLE");
    if (flags & IIF_SYNCWAIT)
	safe_appendf(alloc, " SYNCWAIT");
    if (flags & IIF_ABORTCOMMIT)
	safe_appendf(alloc, " ABORTCOMMIT");
    if (flags & IIF_STALLED)
	safe_appendf(alloc, " STALLED");
    return(*alloc);
}

static const char *
rpCmdToStr(rp_cmd_t cmd)
{
    const char *str;

    cmd &= 0x7F;

    switch(cmd) {
    case 0x00:
	str = "<IDLE>";
	break;
    case RPCMD_HELLO:
	str = "HELLO";
	break;
    case RPCMD_GOODBYE:
	str = "GOODBYE";
	break;
    case RPCMD_ADJUST:
	str = "ADJUST";
	break;
    case RPCMD_SYNC:
	str = "SYNC";
	break;
    case RPCMD_START:
	str = "START";
	break;
    case RPCMD_STOP:
	str = "STOP";
	break;
    case RPCMD_RESTART:
	str = "RESTART";
	break;
    case RPCMD_INFO:
	str = "INFO";
	break;
    case RPCMD_STOP_REPLICATOR:
	str = "STOP_REPLICATOR";
	break;
    case RPCMD_SETOPT:
	str = "SETOPT";
	break;
    case RPCMD_START_REPLICATOR:
	str = "START_REPLICATOR";
	break;
    case RPCMD_INITIAL_START:
	str = "INITIAL_START";
	break;
    case RPCMD_ADDLINK:
	str = "ADDLINK";
	break;
    case RPCMD_REMLINK:
	str = "REMLINK";
	break;
    case RPCMD_CREATEDB:
	str = "CREATEDB";
	break;
    case RPCMD_CREATESNAP:
	str = "CREATESNAP";
	break;
    case RPCMD_CREATEPEER:
	str = "CREATEPEER";
	break;
    case RPCMD_DESTROYDB:
	str = "DESTROYDB";
	break;
    case RPCMD_DOWNGRADEDB:
	str = "DOWNGRADEDB";
	break;
    case RPCMD_UPGRADEDB:
	str = "UPGRADEDB";
	break;
    case RPCMD_OPEN_INSTANCE:
	str = "OPEN_INSTANCE";
	break;
    case RPCMD_CLOSE_INSTANCE:
	str = "CLOSE_INSTANCE";
	break;
    case RPCMD_BEGIN_TRAN:
	str = "BEGIN";
	break;
    case RPCMD_RUN_QUERY_TRAN:
	str = "RUN_QUERY";
	break;
    case RPCMD_REC_QUERY_TRAN:
	str = "REC_QUERY";
	break;
    case RPCMD_ABORT_TRAN:
	str = "ABORT";
	break;
    case RPCMD_COMMIT1_TRAN:
	str = "COMMIT1";
	break;
    case RPCMD_COMMIT2_TRAN:
	str = "COMMIT2";
	break;
    case RPCMD_RESULT_TRAN:
	str = "RESULT_ROW";
	break;
    case RPCMD_UPDATE_INSTANCE:
	str = "UPDATE";
	break;
    case RPCMD_UNCOMMIT1_TRAN:
	str = "UNCOMMIT1";
	break;
    case RPCMD_DEADLOCK_TRAN:
	str = "DEADLOCK";
	break;
    case RPCMD_RESULT_ORDER_TRAN:
	str = "ORDER";
	break;
    case RPCMD_RESULT_LIMIT_TRAN:
	str = "LIMIT";
	break;
    case RPCMD_CONTINUE:
	str = "CONTINUE";
	break;
    case RPCMD_KEEPALIVE:
	str = "KEEPALIVE";
	break;
    case RPCMD_RAWREAD:
	str = "RAWREAD";
	break;
    case RPCMD_RAWDATA:
	str = "RAWDATA";
	break;
    case RPCMD_RAWDATAFILE:
	str = "RAWDATAFILE";
	break;
    case RPCMD_BREAK_QUERY:
	str = "BREAK";
	break;
    case RPCMD_ICOMMIT1_TRAN:
	str = "ICOMMIT1";
	break;
    case RPCMD_ICOMMIT2_TRAN:
	str = "ICOMMIT2";
	break;
    default:
	str = "UNKNOWN";
	break;
    }
    return(str);
}

