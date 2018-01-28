/*
 * LIBCLIENT/CLIENTMSG.H	- Client/Server Messaging Structures
 *
 * (c)Copyright 2000-2002 Backplane, Inc.  Please refer to the COPYRIGHT
 * file at the base of the distribution tree.
 *
 * $Backplane: rdbms/libclient/clientmsg.h,v 1.36 2002/09/20 19:56:00 dillon Exp $
 *
 *	Unix-domain messages sent/received to replication server.  Please note
 *	that all over-the-wire fields are in host order, and thus machine
 *	dependant.  The cp_Magic code is used to determine the actual
 *	byte ordering in order to determine what kind of conversion is
 *	required.
 */

typedef u_int8_t	cm_cmd_t;
typedef u_int8_t	cm_magic_t;

typedef struct CLPKt {
    cm_magic_t	cp_Magic;	/* wire, magic number / byte ordering */
    cm_cmd_t	cp_Cmd;		/* wire, command code */
    u_int16_t	cp_Flags;	/* wire, misc flagfs */
    int32_t	cp_Bytes;	/* wire, total bytes in message */
    int32_t	cp_Error;	/* wire, returned error code */
    char	cp_Data[0];	/* wire, data (if any) */
} CLPkt;

#define CPF_READONLY	0x0001	/* for CLCMD_BEGIN_TRAN */
#define CPF_DIDRESPOND	0x0002	/* (replicator internal) */
#define CPF_NORESTART	0x0004	/* (replicator internal) */
#define CPF_RWSYNC	0x0008	/* use higher timestamp if avail */
#define CPF_STREAM	0x0010	/* streaming query */
#define CPF_TOPREADONLY	0x0020	/* for overall trans, not just this level */

typedef struct CLMsg {
    Node	cm_Node;	/* link node (replicator only) */
    void	*cm_AuxInfo;	/* aux info */
    int		cm_StallCount;	/* for SELECTS (CLCMD_RUN_QUERY_TRAN)*/
    int		cm_QCount;	/* distrib. count (replicator only) */
    int		cm_QResponded;	/* valid replies */
    int		cm_Level;	/* transactional level of msg */
    int		cm_Refs;
    int		cm_AtQueue;	/* msg is qpoint for instance (refs) */
    int		cm_ByteOrder;	/* byte ordering of packet */

    /*
     * note: structural order is important here.  For CLMsg reuse we bzero
     * up (not including) cm_CLMsgSize.
     */
    int		cm_CLMsgSize;	/* reserved space for entire msg */
    CLPkt	cm_Pkt;		/* wire */
} CLMsg;

/*
 * CLRowMsg() -	CLMsg containing row data
 */

typedef struct CLRowMsg {
    CLMsg	rm_Msg;
    int32_t	rm_ShowCount;	/* wire, columns to display */
    int32_t	rm_Count;	/* wire, total columns */
    int32_t	rm_Offsets[0];	/* wire, offset array */
    /* data follows */
} CLRowMsg;

/*
 * CLOrderMsg() - CLMsg containing sorting control info
 */

typedef struct CLOrderMsg {
    CLMsg	om_Msg;
    int32_t	om_NumOrder;	/* wire, total ordering fields */
    int32_t	om_Order[0];	/* wire, ordering array */
} CLOrderMsg;

/*
 * CLOrderMsg() - CLMsg containing sorting control info
 */

typedef struct CLLimitMsg {
    CLMsg	lm_Msg;
    int32_t	lm_StartRow;	/* wire, starting row */
    int32_t	lm_MaxRows;	/* wire, maximum number of rows wanted */
} CLLimitMsg;

#define CLMSG_MAGIC_LSB	((cm_magic_t)0xAF)	/* lsb-first ordering */
#define CLMSG_MAGIC_MSB	((cm_magic_t)0xAE)	/* msb-first ordering */
#define CLMSG_MAXDBNAME	64

/*
 * CLHelloMsg - CLMsg containing hello data
 */
typedef struct CLHelloMsg {
    CLMsg	hm_Msg;
    dbstamp_t	hm_SyncTs;	/* wire */
    dbstamp_t	hm_MinCTs;	/* wire */
    int32_t	hm_BlockSize;	/* wire */
    char	hm_DBName[0];	/* wire */
} CLHelloMsg;

#define CLHELLOSIZE(slen)	(offsetof(CLHelloMsg, hm_DBName[(slen)+1]))

/*
 * CLBeginMsg - Begin transaction
 */
typedef struct CLBeginMsg {
    CLMsg	bg_Msg;
    dbstamp_t	bg_FreezeTs;	/* wire */
} CLBeginMsg;

/*
 * CLCommit1Msg - Commit phase 1
 */
typedef struct CLCommit1Msg {
    CLMsg	c1_Msg;
    dbstamp_t	c1_MinCTs;	/* wire */
} CLCommit1Msg;

/*
 * CLCommit1Msg - Commit phase 2
 */
typedef struct CLCommit2Msg {
    CLMsg	c2_Msg;
    dbstamp_t	c2_MinCTs;	/* wire */
    rhuser_t	c2_UserId;	/* wire */
    /* pad */
} CLCommit2Msg;

/*
 * CLRawDataFileMsg - Synchronizer packet data-file mark
 */
typedef struct CLRawDataFileMsg {
    CLMsg	rdf_Msg;
    int32_t	rdf_BlockSize;	/* wire */
    char	rdf_FileName[0];/* wire */
} CLRawDataFileMsg;

/*
 * CLRawReadMsg
 */
typedef struct CLRawReadMsg {
    CLMsg	rr_Msg;
    dbstamp_t	rr_StartTs;	/* wire */
    dbstamp_t	rr_EndTs;	/* wire */
} CLRawReadMsg;

/*
 * CLRawWriteMsg
 */
typedef struct CLRawWriteMsg {
    CLMsg	rw_Msg;
    dbstamp_t	rw_StartTs;	/* wire */
    dbstamp_t	rw_EndTs;	/* wire */
} CLRawWriteMsg;

/*
 * CLRawWriteEndMsg
 */
typedef struct CLRawWriteEndMsg {
    CLMsg	re_Msg;
    dbstamp_t	re_EndTs;	/* wire */
} CLRawWriteEndMsg;

/*
 * CLUpdateSyncMsg
 */
typedef struct CLStampMsg {
    CLMsg	ts_Msg;
    dbstamp_t	ts_Stamp;	/* wire */
} CLStampMsg;

/*
 * CLAnyMsg	- union representing any CLMsg
 */
typedef union CLAnyMsg {
    CLMsg		a_Msg;
    CLRowMsg		a_RowMsg;
    CLOrderMsg		a_OrderMsg;
    CLLimitMsg		a_LimitMsg;
    CLHelloMsg		a_HelloMsg;
    CLBeginMsg		a_BeginMsg;
    CLCommit1Msg	a_Commit1Msg;
    CLCommit2Msg	a_Commit2Msg;
    CLRawDataFileMsg	a_RawDataFileMsg;
    CLRawReadMsg	a_RawReadMsg;
    CLRawWriteMsg	a_RawWriteMsg;
    CLRawWriteEndMsg	a_RawWriteEndMsg;
    CLStampMsg		a_StampMsg;
} CLAnyMsg;

#define cma_Pkt		a_Msg.cm_Pkt

/*
 * CLMsg packet commands intentionally mimic replication packet commands,
 * though the command id's may be different.
 */
#define CLCMD_HELLO		0x01	/* a_HelloMsg		*/
#define CLCMD_OPEN_INSTANCE	0x02	/* a_Msg		*/
#define CLCMD_CLOSE_INSTANCE	0x03	/* a_Msg		*/
#define CLCMD_BEGIN_TRAN	0x04	/* a_BeginMsg		*/
#define CLCMD_RUN_QUERY_TRAN	0x05	/* a_Msg + query	*/
#define CLCMD_REC_QUERY_TRAN	0x06	/* a_Msg + query	*/
#define CLCMD_ABORT_TRAN	0x07	/* a_Msg		*/
#define CLCMD_COMMIT1_TRAN	0x08	/* a_Commit1Msg		*/
#define CLCMD_COMMIT2_TRAN	0x09	/* a_Commit2Msg		*/
#define CLCMD_RESULT		0x0A	/* a_RowMsg		*/
#define CLCMD_RESULT_RESET	0x0B	/* a_Msg		*/
#define CLCMD_SYNC_STAMP	0x0C	/* a_StampMsg		*/
#define CLCMD_UPDATE_SYNCTS	0x0D	/* a_StampMsg		*/
#define CLCMD_UPDATE_STAMPID	0x0E	/* a_StampMsg		*/
#define CLCMD_UNCOMMIT1_TRAN	0x0F	/* a_Msg		*/

#define CLCMD_RAWREAD		0x10	/* a_RawReadMsg		*/
#define CLCMD_RAWDATA		0x11	/* a_Msg + data		*/
#define CLCMD_RAWWRITE		0x12	/* a_RawWriteMsg	*/
#define CLCMD_RAWWRITE_END	0x13	/* a_RawWriteEndMsg	*/
#define CLCMD_RAWDATAFILE	0x14	/* a_RawDataFileMsg	*/
#define CLCMD_WAIT_TRAN		0x15	/* a_StampMsg		*/
#define CLCMD_CONTINUE		0x16	/* a_Msg		*/
#define CLCMD_BREAK_QUERY	0x17	/* a_Msg		*/

#define CLCMD_DBMANAGER_START	0x20	/* a_Msg		*/

#define CLCMD_DEADLOCK_TRUE	0x30	/* unused 		*/
#define CLCMD_DEADLOCK_FALSE	0x31	/* unused 		*/

#define CLCMD_RESULT_ORDER	0x40	/* a_OrderMsg		*/
#define CLCMD_RESULT_LIMIT	0x41	/* a_LimitMsg		*/

