/*
 * LIBCLIENT/CLIENTMSG.C -	Send and receives messages to replication server
 *
 * (c)Copyright 2000-2002 Backplane, Inc.  Please refer to the COPYRIGHT
 * file at the base of the distribution tree.
 *
 * $Backplane: rdbms/libclient/clientmsg.c,v 1.19 2003/04/02 05:27:01 dillon Exp $
 */

#include "defs.h"

Export CLAnyMsg *ReadCLMsg(iofd_t iofd);
Export CLAnyMsg *MReadCLMsg(iofd_t iofd);
Export CLAnyMsg *RecvCLMsg(iofd_t iofd, iofd_t *pxfd);
Export int WriteCLMsg(iofd_t iofd, CLAnyMsg *msg, int flushMe);
Export void SendCLMsg(iofd_t iofd, CLAnyMsg *msg, iofd_t xfd);
Export CLAnyMsg *BuildCLMsg(cm_cmd_t cmd, int len);
Export CLAnyMsg *BuildCLMsgStr(cm_cmd_t cmd, const char *str);
Export CLAnyMsg *BuildCLHelloMsgStr(const char *str);
Export void FreeCLMsg(CLAnyMsg *cm);

static CLAnyMsg *CLFreeMsg;

/*
 * clHeaderFixup() - Valid the packet header and fixup the byte ordering
 */
static
CLAnyMsg *
clHeaderFixup(CLPkt *pkt)
{
    CLAnyMsg *cm;
    int order;
    int clmsgSize;
    int n;

    /*
     * Verify the magic number and figure out the byte ordering.  Convert
     * the packet header to host-normal form.  The caller(s) are responsible
     * for converting any extended fields to host-normal form by
     * checking whether cm_ByteOrder == BYTE_ORDER or not.
     */
    switch(pkt->cp_Magic) {
    case CLMSG_MAGIC_LSB:
	order = LITTLE_ENDIAN;
	break;
    case CLMSG_MAGIC_MSB:
	order = BIG_ENDIAN;
	break;
    default:
	return(NULL);
    }
    if (order != BYTE_ORDER) {
	SWAP16(pkt->cp_Flags);
	SWAP32(pkt->cp_Bytes);
	SWAP32(pkt->cp_Error);
#if BYTE_ORDER == LITTLE_ENDIAN
	pkt->cp_Magic = CLMSG_MAGIC_LSB;
#else
	pkt->cp_Magic = CLMSG_MAGIC_MSB;
#endif
    }

    /*
     * Figure out the packet size.  The over-the-wire packet is always
     * 8-byte aligned to avoid having to move any data when reading
     * multiple packets from a stream or to allow for direct socket
     * data mapping in the future.
     */
    DBASSERT(pkt->cp_Bytes >= sizeof(*pkt));
    n = ALIGN8(pkt->cp_Bytes);

    /*
     * Allocate the CLMsg.  Reuse a previously freed CLMsg if
     * possible.
     */
    clmsgSize = sizeof(CLMsg) + (n - sizeof(*pkt));
    if ((cm = CLFreeMsg) == NULL || clmsgSize > cm->a_Msg.cm_CLMsgSize) {
	cm = zalloc(clmsgSize);
	cm->a_Msg.cm_CLMsgSize = clmsgSize;
    } else {
	CLFreeMsg = NULL;
	bzero(cm, offsetof(CLMsg, cm_CLMsgSize));
    }
    bcopy(pkt, &cm->cma_Pkt, sizeof(*pkt));
    cm->a_Msg.cm_ByteOrder = order;
    cm->a_Msg.cm_Refs = 1;

    return(cm);
}

/*
 * clHeaderFixup() - Fixup the byte ordering for command-specific data
 */
static
void
clContentFixup(CLAnyMsg *msg)
{
    int i;

    switch(msg->cma_Pkt.cp_Cmd) {
    case CLCMD_HELLO:		/* a_HelloMsg */
	SWAP64(msg->a_HelloMsg.hm_SyncTs);
	SWAP64(msg->a_HelloMsg.hm_MinCTs);
	SWAP32(msg->a_HelloMsg.hm_BlockSize);
	break;
    case CLCMD_OPEN_INSTANCE:	/* a_Msg */
	break;
    case CLCMD_CLOSE_INSTANCE:	/* a_Msg */
	break;
    case CLCMD_BEGIN_TRAN:	/* a_BeginMsg */
	SWAP64(msg->a_BeginMsg.bg_FreezeTs);
	break;
    case CLCMD_RUN_QUERY_TRAN:	/* a_Msg + text query */
	break;
    case CLCMD_REC_QUERY_TRAN:	/* a_Msg + text query */
	break;
    case CLCMD_ABORT_TRAN:	/* a_Msg */
	break;
    case CLCMD_COMMIT1_TRAN:	/* a_Commit1Msg */
	SWAP64(msg->a_Commit1Msg.c1_MinCTs);
	break;
    case CLCMD_COMMIT2_TRAN:	/* a_Commit2Msg */
	SWAP64(msg->a_Commit2Msg.c2_MinCTs);
	SWAP32(msg->a_Commit2Msg.c2_UserId);
	break;
    case CLCMD_RESULT:		/* a_RowMsg */
	SWAP32(msg->a_RowMsg.rm_ShowCount);
	SWAP32(msg->a_RowMsg.rm_Count);
	DBASSERT(msg->a_RowMsg.rm_Count >= 0 && offsetof(CLAnyMsg, a_RowMsg.rm_Offsets[msg->a_RowMsg.rm_Count]) <= offsetof(CLAnyMsg, a_Msg.cm_Pkt.cp_Data[msg->a_Msg.cm_Pkt.cp_Bytes]));
	for (i = 0; i < msg->a_RowMsg.rm_Count; ++i)
	    SWAP32(msg->a_RowMsg.rm_Offsets[i]);
	break;
    case CLCMD_RESULT_RESET:	/* a_Msg */
	break;
    case CLCMD_SYNC_STAMP:	/* a_StampMsg */
    case CLCMD_UPDATE_SYNCTS:	/* a_StampMsg */
    case CLCMD_UPDATE_STAMPID:	/* a_StampMsg */
    case CLCMD_WAIT_TRAN:	/* a_StampMsg	*/
	SWAP64(msg->a_StampMsg.ts_Stamp);
	break;
    case CLCMD_UNCOMMIT1_TRAN:	/* a_Msg */
	break;
    case CLCMD_RAWREAD:		/* a_RawReadMsg */
	SWAP64(msg->a_RawReadMsg.rr_StartTs);
	SWAP64(msg->a_RawReadMsg.rr_EndTs);
	break;
    case CLCMD_RAWDATA:		/* a_Msg + phys DB formatted data */
	dberror("Attempt to synchronize databases w/ different byte orderings\n"
		"I need to translate the physical DB record XXX TODO\n");
	DBASSERT(0);
	break;
    case CLCMD_RAWWRITE:	/* a_RawWriteMsg */
	SWAP64(msg->a_RawWriteMsg.rw_StartTs);
	SWAP64(msg->a_RawWriteMsg.rw_EndTs);
	break;
    case CLCMD_RAWWRITE_END:	/* a_RawWriteEndMsg */
	SWAP64(msg->a_RawWriteEndMsg.re_EndTs);
	break;
    case CLCMD_RAWDATAFILE:	/* a_RawDataFileMsg + filename */
	SWAP32(msg->a_RawDataFileMsg.rdf_BlockSize);
	break;
    case CLCMD_CONTINUE:	/* a_Msg */
	break;
    case CLCMD_BREAK_QUERY:	/* a_Msg */
	break;
    case CLCMD_DBMANAGER_START:	/* a_Msg */
	break;
    case CLCMD_DEADLOCK_TRUE:	/* a_Msg */
	break;
    case CLCMD_DEADLOCK_FALSE:	/* a_Msg */
	break;
    case CLCMD_RESULT_ORDER:	/* a_OrderMsg */
	SWAP32(msg->a_OrderMsg.om_NumOrder);
	DBASSERT(msg->a_OrderMsg.om_NumOrder >= 0 && offsetof(CLAnyMsg, a_OrderMsg.om_Order[msg->a_OrderMsg.om_NumOrder]) <= offsetof(CLAnyMsg, a_Msg.cm_Pkt.cp_Data[msg->a_Msg.cm_Pkt.cp_Bytes]));
	for (i = 0; i < msg->a_OrderMsg.om_NumOrder; ++i)
	    SWAP32(msg->a_OrderMsg.om_Order[i]);
	break;
    case CLCMD_RESULT_LIMIT:	/* a_LimitMsg */
	SWAP32(msg->a_LimitMsg.lm_StartRow);
	SWAP32(msg->a_LimitMsg.lm_MaxRows);
	break;
    default:
	DBASSERT(0);
    }
}

CLAnyMsg *
ReadCLMsg(iofd_t iofd)
{
    CLPkt pkt;
    CLAnyMsg *cm;
    int n;

    if (iofd == IOFD_NULL)
	return(NULL);

    if (t_read(iofd, &pkt, sizeof(pkt), 0) != sizeof(pkt))
	return(NULL);
    if ((cm = clHeaderFixup(&pkt)) == NULL)
	return(NULL);
    n = ALIGN8(pkt.cp_Bytes);

    if (t_read(iofd, &cm->cma_Pkt.cp_Data[0], n - sizeof(pkt), 0) != n - sizeof(pkt)) {
	FreeCLMsg(cm);
	return(NULL);
    }
    if (cm->a_Msg.cm_ByteOrder != BYTE_ORDER)
	clContentFixup(cm);
    return(cm);
}

CLAnyMsg *
MReadCLMsg(iofd_t iofd)
{
    CLPkt pkt;
    CLAnyMsg *cm;
    int n;

    if (iofd == IOFD_NULL)
	return(NULL);

    if (t_mread(iofd, &pkt, sizeof(pkt), 0) != sizeof(pkt))
	return(NULL);
    if ((cm = clHeaderFixup(&pkt)) == NULL)
	return(NULL);
    n = ALIGN8(pkt.cp_Bytes);

    if (t_mread(iofd, &cm->cma_Pkt.cp_Data[0], n - sizeof(pkt), 0) != n - sizeof(pkt)) {
	FreeCLMsg(cm);
	return(NULL);
    }
    if (cm->a_Msg.cm_ByteOrder != BYTE_ORDER)
	clContentFixup(cm);
    return(cm);
}

CLAnyMsg *
RecvCLMsg(iofd_t iofd, iofd_t *pxfd)
{
    union {
	CLPkt pkt;
	char buf[offsetof(CLPkt, cp_Data[CLMSG_MAXDBNAME])];
    } umsg;
    CLAnyMsg *cm;
    int n;

    if (iofd == IOFD_NULL)
	return(NULL);
    n = t_recvmsg_fd(iofd, &umsg.pkt, sizeof(umsg), pxfd, 0);
    if (n <= 0)
	return(NULL);
    DBASSERT(n >= sizeof(umsg.pkt));

    if ((cm = clHeaderFixup(&umsg.pkt)) == NULL)
	return(NULL);

    if (n != ALIGN8(umsg.pkt.cp_Bytes)) {
	dberror(
	    "****BAD RECEIVE %d/%d/%d cmd %02x\n",
	    n, umsg.pkt.cp_Bytes, ALIGN8(umsg.pkt.cp_Bytes),
	    umsg.pkt.cp_Cmd
	);
	if (pxfd && *pxfd) {
	    closeIo(*pxfd);
	    *pxfd = IOFD_NULL;
	}
	FreeCLMsg(cm);
	return(NULL);
    }
    if (n != sizeof(cm->cma_Pkt))
	bcopy(&umsg.pkt + 1, &cm->cma_Pkt + 1, n - sizeof(cm->cma_Pkt));
    if (cm->a_Msg.cm_ByteOrder != BYTE_ORDER)
	clContentFixup(cm);
    return(cm);
}

/*
 * WriteCLMsg() -	write CLMsg and free it
 */
int
WriteCLMsg(iofd_t iofd, CLAnyMsg *msg, int flushMe)
{
    int r = 0;

    if (msg) {
	CLPkt *pkt = &msg->cma_Pkt;
	int n = ALIGN8(pkt->cp_Bytes);

	if (iofd) {
	    if (t_mwrite(iofd, pkt, n, 0) != n)
		r = -1;
	}
	FreeCLMsg(msg);
    }
    if (flushMe && r == 0 && iofd != IOFD_NULL)
	r = t_mflush(iofd, 0);
    return(r);
}

/*
 * SendCLMsg() -	send CLMsg with descriptor and free it
 */
void 
SendCLMsg(iofd_t iofd, CLAnyMsg *msg, iofd_t xfd)
{
    int n = ALIGN8(msg->cma_Pkt.cp_Bytes);

    if (iofd != IOFD_NULL) {
	if (xfd)
	    t_sendmsg_fd(iofd, &msg->cma_Pkt, n, xfd, 0);
	else
	    t_write(iofd, &msg->cma_Pkt, n, 0);
    }
    FreeCLMsg(msg);
}

CLAnyMsg *
BuildCLMsg(cm_cmd_t cmd, int len)
{
    CLAnyMsg *msg;
    int n;

    DBASSERT(len >= sizeof(CLMsg));

    n = ALIGN8(len);
    msg = zalloc(n);
    msg->a_Msg.cm_CLMsgSize = n;
    msg->a_Msg.cm_ByteOrder = BYTE_ORDER;
#if BYTE_ORDER == LITTLE_ENDIAN
    msg->a_Msg.cm_Pkt.cp_Magic = CLMSG_MAGIC_LSB;
#else
    msg->a_Msg.cm_Pkt.cp_Magic = CLMSG_MAGIC_MSB;
#endif
    msg->a_Msg.cm_Pkt.cp_Cmd = cmd;
    msg->a_Msg.cm_Pkt.cp_Bytes = len - offsetof(CLMsg, cm_Pkt);
    msg->a_Msg.cm_Refs = 1;

    return(msg);
}

CLAnyMsg *
BuildCLMsgStr(cm_cmd_t cmd, const char *str)
{
    int len = strlen(str) + 1;
    CLAnyMsg *msg;

    msg = BuildCLMsg(cmd, offsetof(CLMsg, cm_Pkt.cp_Data[len]));
    bcopy(str, msg->cma_Pkt.cp_Data, len);

    return(msg);
}

CLAnyMsg *
BuildCLHelloMsgStr(const char *str)
{
    CLAnyMsg *msg;
    int zlen = (str) ? strlen(str) : 0;
    
    msg = BuildCLMsg(CLCMD_HELLO, offsetof(CLHelloMsg, hm_DBName[zlen+1]));
    if (zlen)
	bcopy(str, msg->a_HelloMsg.hm_DBName, zlen);
    msg->a_HelloMsg.hm_DBName[zlen] = 0;
        
    return(msg);
}

void 
FreeCLMsg(CLAnyMsg *msg)
{
    if (msg) {
	DBASSERT(msg->a_Msg.cm_Refs > 0);
	DBASSERT(msg->a_Msg.cm_CLMsgSize > 0);
	if (--msg->a_Msg.cm_Refs == 0) {
	    if (CLFreeMsg == NULL) {
		CLFreeMsg = msg;
	    } else if (msg->a_Msg.cm_CLMsgSize > CLFreeMsg->a_Msg.cm_CLMsgSize) {
		zfree(CLFreeMsg, CLFreeMsg->a_Msg.cm_CLMsgSize);
		CLFreeMsg = msg;
	    } else {
		zfree(msg, msg->a_Msg.cm_CLMsgSize);
	    }
	}
    }
}

