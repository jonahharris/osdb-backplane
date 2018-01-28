/*
 * REPLICATOR/PKT.H	- Replication support structures
 *
 * (c)Copyright 2000-2002 Backplane, Inc.  Please refer to the COPYRIGHT
 * file at the base of the distribution tree.
 *
 * $Backplane: rdbms/replicator/pkt.h,v 1.34 2002/08/20 22:06:00 dillon Exp $
 */

typedef u_int8_t	rp_magic_t;
typedef u_int8_t	rp_cmd_t;
typedef u_int8_t	rp_hop_t;
typedef u_int8_t	rp_tflags_t;
typedef u_int64_t	rp_addr_t;

typedef int16_t		rp_totalrep_t;
typedef int8_t		rp_state_t;
typedef int8_t		rp_blkshift_t;
typedef u_int8_t	rp_latency_t;
typedef u_int8_t	rp_type_t;
typedef u_int16_t	rp_vcid_t;
typedef int16_t		rp_vcid_signed_t;
typedef u_int32_t	rp_seq_t;
typedef int32_t		rp_seq_signed_t;
typedef u_int32_t	rp_inst_t;

#define MAX_LATENCY	32767
#define MAXHOSTNAME	64

/*
 * Connection types.  The type of connection determines which subset of
 * the packet protocol is to be used and the level of exposure the 
 * connecting client has to it. 
 *
 * A PEER is responsible for maintaining the spanning tree, propogating 
 * packets, and maintaining the virtual circuits between hosts.
 * A CLIENT on the otherhand usually only deals with databse instances and
 * transactions.  The PEER or SNAP SERVER the client connects to is
 * responsible for distributing the transaction across available peers,
 * reissuing the transaction when VC's fail, and so forth.  A RELAY
 * server is simply treated as a client by peer and snap servers.
 */

#define CTYPE_UNKNOWN	0	/* (host route) unknown/special */
#define CTYPE_ACCEPTOR	1	/* (link) special control connection */
#define CTYPE_CONTROL	2	/* (link) special control connection */
#define CTYPE_CLIENT	3	/* (?) client connection */
#define CTYPE_RELAY	4	/* (?) relay connection */
#define CTYPE_PEER	5	/* (db route) peer instance */
#define CTYPE_SNAP	6	/* (db route) snapshot instance */
#define CTYPE_REPLIC	7	/* (host route) replication host */

/*
 * Replication Packet
 */

struct RouteInfo;
struct HostInfo;

typedef struct RPPkt {
    rp_magic_t	pk_Magic;	/* magic number */
    rp_cmd_t	pk_Cmd;		/* command */
    rp_hop_t	pk_Hop;		/* hops taken so far, 0 == direct */
    rp_tflags_t	pk_TFlags;	/* transaction flags */
    int32_t	pk_Bytes;
    rp_seq_t	pk_SeqNo;	/* virtual circuit sequence number */
    int32_t	pk_InstId;	/* rendezvous instance id */
    rp_addr_t	pk_SAddr;	/* source of packet */
    rp_addr_t	pk_DAddr;	/* destination of packet */
    rp_vcid_t	pk_VCId;	/* VC (mono/timeout), or 0 if broadcast */
    int16_t	pk_Unused02;
    int32_t	pk_RCode;	/* return code */
    char	pk_Data[0];
} RPPkt;

#define RPTF_READONLY	0x01
#define RPTF_NOPUSH	0x02	/* ok to buffer up */

typedef struct RPMsg {
    Node	rp_Node;	/* for queueing */
    int		rp_Refs;
    struct HostInfo *rp_SHost;
    struct HostInfo *rp_DHost;
    struct InstInfo *rp_InstInfo;
    RPPkt	rp_Pkt;
} RPMsg;
    
#define RP_MAGIC	((rp_magic_t)0xA1)
#define RP_MAXHOPS	8

/*
 * RPHelloMsg -	Hello/Route message
 *
 */
typedef struct RPHelloMsg {
    RPMsg	he_Msg;	
    dbstamp_t	he_SyncTs;	/* syncts for database */
    dbstamp_t	he_MinCTs;	/* mincts for database */
    rp_addr_t	he_Addr;	/* (32) address */
    rp_latency_t he_Latency;	/* (8) link latency */
    rp_type_t	he_Type;	/* (8) type of host */
    rp_state_t	he_State;	/* (16) state of database (if a db route) */
    rp_blkshift_t he_BlockShift;/* (8) blocksize for sys table */
    rp_totalrep_t he_RepCount;	/* (16) replication count */
    u_int16_t	he_DBNameOff;	/* database name (dbname.groupid) */
    char	he_HostName[0];	/* host name (user@host) */
} RPHelloMsg;

#define he_Pkt	he_Msg.rp_Pkt

/*
 * RPBeginMsg
 */
typedef struct RPBeginMsg {
    RPMsg	bg_Msg;
    dbstamp_t	bg_FreezeTs;
} RPBeginMsg;

/*
 * RPCommit1Msg
 */
typedef struct RPCommit1Msg {
    RPMsg	c1_Msg;
    dbstamp_t	c1_MinCTs;
} RPCommit1Msg;

/*
 * RPCommit2Msg
 */
typedef struct RPCommit2Msg {
    RPMsg	c2_Msg;
    dbstamp_t	c2_MinCTs;	/* not returned */
    rhuser_t	c2_UserId;	/* not returned */
} RPCommit2Msg;

/*
 * RPRawReadMsg
 */
typedef struct RPRawReadMsg {
    RPMsg	rr_Msg;
    dbstamp_t	rr_StartTs;	/* sent & returned */
    dbstamp_t	rr_EndTs;	/* sent & returned */
} RPRawReadMsg;

/*
 * RPRawDataFileMsg
 */
typedef struct RPRawDataFileMsg {
    RPMsg	rdf_Msg;
    int		rdf_BlockSize;
    char	rdf_FileName[0];
} RPRawDataFileMsg;

/*
 * RPOpenInstanceMsg
 */
typedef struct RPOpenInstanceMsg {
    RPMsg	oi_Msg;
    dbstamp_t	oi_SyncTs;	/* in reply only */
    char	oi_DBName[0];	/* not in reply */
} RPOpenInstanceMsg;

/*
 * RPOrderMsg	- 	result order
 */
typedef struct RPOrderMsg {
    RPMsg	om_Msg;
    int		om_NumOrder;
    int		om_Order[0];
} RPOrderMsg;

/*
 * RPLimitMsg	- 	result limit
 */
typedef struct RPLimitMsg {
    RPMsg	lm_Msg;
    int		lm_StartRow;
    int		lm_MaxRows;
} RPLimitMsg;

/*
 * RPRowMsg	- 	result row
 */
typedef struct RPRowMsg {
    RPMsg	rm_Msg;
    int		rm_ShowCount;
    int		rm_Count;
    int		rm_Offsets[0];
} RPRowMsg;

/*
 * RPSysctlMsg  -	System control message
 */
typedef struct RPSysctlMsg {
    RPMsg	sc_Msg;
    char	sc_Buffer[0];
} RPSysctlMsg;

/*
 * RPAnyMsg -	union representing any RPMsg
 */
typedef union RPAnyMsg {
    RPMsg		a_Msg;
    RPHelloMsg		a_HelloMsg;
    RPBeginMsg		a_BeginMsg;
    RPCommit1Msg	a_Commit1Msg;
    RPCommit2Msg	a_Commit2Msg;
    RPRawReadMsg	a_RawReadMsg;
    RPRawDataFileMsg	a_RawDataFileMsg;
    RPOpenInstanceMsg	a_OpenInstanceMsg;
    RPOrderMsg		a_OrderMsg;
    RPLimitMsg		a_LimitMsg;
    RPRowMsg		a_RowMsg;
    RPSysctlMsg		a_SysctlMsg;
} RPAnyMsg;

#define rpa_Pkt		a_Msg.rp_Pkt

#define RPPKTSIZE(rptype)	(sizeof(rptype) - offsetof(RPMsg, rp_Pkt))
#define RPDATASIZE(rptype)	(sizeof(rptype) - offsetof(RPMsg, rp_Pkt.pk_Data[0]))

/*
 * Hello packets
 */
#define RPCMD_HELLO		0x01	/* add (SAddr, HostName) for link */
#define RPCMD_GOODBYE		0x02	/* remove (SAddr, HostName) for link */
#define RPCMD_ADJUST		0x03	/* change (SAddr, HostName) for link */
#define RPCMD_SYNC		0x04	/* synchronization mark for client */

/*
 * Database control (local to server)
 */
#define RPCMD_START		0x20
#define RPCMD_STOP		0x21
#define RPCMD_RESTART		0x22
#define RPCMD_INFO		0x23
#define RPCMD_STOP_REPLICATOR	0x24
#define RPCMD_SETOPT		0x25
#define RPCMD_START_REPLICATOR	0x26
#define RPCMD_INITIAL_START	0x27

/*
 * Replication control (local to server)
 */
#define RPCMD_ADDLINK		0x30
#define RPCMD_REMLINK		0x31
#define RPCMD_CREATEDB		0x32
#define RPCMD_CREATESNAP	0x33
#define RPCMD_CREATEPEER	0x34
#define RPCMD_DESTROYDB		0x35
#define RPCMD_DOWNGRADEDB	0x36
#define RPCMD_UPGRADEDB		0x37

/*
 * Replicated Transactions.  These run over virtual circuits and are also
 * used in the client-server protocol.
 */

#define RPCMD_OPEN_INSTANCE	0x40	/* open database instance */
#define RPCMD_CLOSE_INSTANCE	0x41	/* close database instance */
#define RPCMD_BEGIN_TRAN	0x42	/* begin a transaction */
#define RPCMD_RESERVED_43	0x43
#define RPCMD_RUN_QUERY_TRAN	0x44	/* run and then queue a query */
#define RPCMD_REC_QUERY_TRAN	0x45	/* record a query for later commit */
#define RPCMD_ABORT_TRAN	0x46	/* abort the transaction */
#define RPCMD_COMMIT1_TRAN	0x47	/* commit phase 1 */
#define RPCMD_COMMIT2_TRAN	0x48	/* commit phase 2 */
#define RPCMD_RESULT_TRAN	0x49	/* response data */
#define RPCMD_UPDATE_INSTANCE	0x4A	/* update database instance */
#define RPCMD_UNCOMMIT1_TRAN	0x4B	/* undo commit-1 (but do not abort) */
#define RPCMD_DEADLOCK_TRAN	0x4C	/* indicate presence of deadlock */
#define RPCMD_RESULT_ORDER_TRAN	0x4D	/* response sort ordering management data */
#define RPCMD_RESULT_LIMIT_TRAN 0x4E	/* response limiting management data */
#define RPCMD_CONTINUE		0x4F	/* pipelining results */

#define RPCMD_KEEPALIVE		0x50	/* keepalive over VC instid 0 */
#define RPCMD_RAWREAD		0x51
#define RPCMD_RAWDATA		0x52
#define RPCMD_RAWDATAFILE	0x53

#define RPCMD_BREAK_QUERY	0x54	/* break running query */
#define RPCMD_ICOMMIT1_TRAN	0x55	/* inner-commit phase 1 */
#define RPCMD_ICOMMIT2_TRAN	0x56	/* inner-commit phase 2 */

#define RPCMD_ERR_UNKNOWN	-2

#define RPCMDF_REPLY		((rp_cmd_t)0x80)

