/*
 * REPLICATOR/LINK.H	- Structures tracking spanning tree state
 *
 * (c)Copyright 2000-2002 Backplane, Inc.  Please refer to the COPYRIGHT
 * file at the base of the distribution tree.
 *
 * $Backplane: rdbms/replicator/link.h,v 1.62 2002/08/20 22:06:00 dillon Exp $
 */

struct SeqInfo;
struct HostInfo;
struct Linknotify;
struct InstInfo;

/*
 * LinkInfo -	Identifies a physical link
 */

typedef struct LinkInfo {
    struct LinkInfo	*l_Next;	/* next link in chain */
    struct RouteInfo	*l_Routes;	/* routes available via this link */
    struct LinkNotify	*l_Notifies;	/* notify for addition or removal */
    struct LinkNotify	**l_NotifyApp;	/* append point */
    int			l_NotifyCount;	/* elements in list */
    struct HostInfo	*l_PeerHost;	/* host we are peering with */
    char		*l_DBName;	/* database restriction on link */
    char		*l_LinkCmdName;	/* command used to forge link */
    char 		*l_StatusMsg;	/* current link status (replic) */
    rp_type_t		l_Type;
    notify_t		l_NotifyInt;	/* notification software interrupt */
    int			l_Flags;
    int			l_Refs;		/* 0 means stale link */
    int			l_UseCount;	/* threads using structure */
    iofd_t		l_Ior;		/* file descriptor */
    iofd_t		l_Iow;		/* file descriptor */
    pid_t		l_Pid;		/* optional pid */
} LinkInfo;

#define LIF_EOF		0x0001

#define UseLinkInfo(l)		++l->l_UseCount
#define UnuseLinkInfo(l)	do { DBASSERT(l->l_UseCount > 0); if (--l->l_UseCount == 0 && l->l_Refs == 0) DestroyLinkInfo(l); } while(0)

/*
 * HostInfo -	Identifies a host instance and virtual connection
 *
 *	HostInfo identifies a host and how to get to that host.  h_SrcA
 *	identifies the MAC address we supply to our peers to allow them
 *	to identify this host as a destination for packets they send to us.
 *
 *	h_FwdSeqNo and h_FwdVCId represents our current transmit sequence
 *	number when we are the master sending to host H. 
 *
 *	h_RevSeqNo and h_RevVCId represents our current transmit sequence
 *	number when we are the slave replying to a message sent by host H.
 *
 */

typedef struct HostInfo {
    struct HostInfo	*h_Next;
    char		*h_HostName;	/* full domain name of host */
    rp_addr_t		h_Addr;		/* translated MAC source/dest */
    rp_type_t		h_Type;		/* class use best route to get type? */
    struct RouteHostInfo *h_RHInfoBase;	/* base of list */
    struct InstInfo	*h_MasterInstBase; /* instances with host as source */
    struct InstInfo	*h_SlaveInstBase; /* instances with host as dest */
    struct SeqInfo	*h_Seqs;	/* HOSTX to HOSTY to find dups */
    int			h_Refs;		/* active users, 0 means stale */
    int			h_UseCount;	/* temporary references */
} HostInfo;

#define UseHostInfo(h)		++(h)->h_UseCount
#define UnuseHostInfo(h)	do { DBASSERT((h)->h_UseCount > 0); if (--(h)->h_UseCount == 0 && (h)->h_Refs == 0) DestroyHostInfo(h); } while(0)

/*
 * RouteInfo - 	Identifies one of several possible routes to get to a host
 *
 *	Identifiers a route to a host or a route to a database@host,
 *	depending on the contents of RouteHostInfo.
 *
 *	A route is the core of the spanning tree and the RouteInfo structure
 *	is massaged in various ways by other structures in the replicator.
 */
typedef struct RouteInfo {
    struct RouteInfo	*r_LNext;	/* from Phys->l_Routes */
    struct RouteInfo	*r_RHNext;	/* from RouteHostInfo->rh_Routes */
    struct RouteHostInfo *r_RHInfo;	/* database@host */
    LinkInfo		*r_Phys;	/* Use this physical link */
    rp_latency_t	r_Latency;	/* routing weight (lower == better) */
    rp_type_t		r_Type;
    rp_hop_t		r_Hop;		/* hop (0 == direct) */
    rp_state_t		r_State;	/* state of database@host */
    rp_totalrep_t	r_RepCount;	/* reported stored # of repl. hosts */
    dbstamp_t		r_SyncTs;	/* synchronized to this point */
    dbstamp_t		r_MinCTs;	/* minimum reported ts / new commits */
    dbstamp_t		r_SaveCTs;	/* (local) what we'd like to report */
    int			r_BlockSize;	/* database's reported blocksize */
    int			r_Refs;		/* active users, 0 means stale */
    int			r_UseCount;	/* temporary references */
} RouteInfo;

#define r_Host		r_RHInfo->rh_Host

#define UseRouteInfo(r)	   ++(r)->r_UseCount
#define UnuseRouteInfo(r)  do { DBASSERT((r)->r_UseCount > 0); if (--(r)->r_UseCount == 0 && (r)->r_Refs == 0) \
				DestroyRouteInfo(r); } while(0)

/*
 * RouteHostInfo - Tracks routes associated with a specific database@host
 *		   rh_DBInfo may be NULL for a host-only route
 */

typedef struct RouteHostInfo {
    struct RouteHostInfo *rh_HNext;
    struct RouteHostInfo *rh_DNext;
    struct InstInfo	 *rh_IdleInstBase;
    RouteInfo		*rh_Routes;
    DBInfo		*rh_DBInfo;
    HostInfo		*rh_Host;
    int			rh_Refs;
    int			rh_UseCount;
    int			rh_BlockSize;	/* aggregate blocksize reported by routes */
    dbstamp_t		rh_ScanTs;
    dbstamp_t		rh_SyncTs;
    dbstamp_t		rh_MinCTs;
} RouteHostInfo;

#define UseRouteHostInfo(rh)	++(rh)->rh_UseCount
#define UnuseRouteHostInfo(rh)	do { DBASSERT((rh)->rh_UseCount > 0); if (--(rh)->rh_UseCount == 0 && (rh)->rh_Refs == 0) \
				DestroyRouteHostInfo(rh); } while(0)

/*
 * SeqInfo -	Tracks VC sequence numbers when propogating packets
 *
 *	A virtual circuit may be formed from any master host A to any slave
 *	host B.   A single SeqInfo structure represents both directions 
 *	of the link (commands sent from A to B and replies returned from B
 * 	to A) and has a sequence number counter for each direction.  
 *
 *	If B needs to talk to A as a master (B master, A slave), that 
 *	represents a DIFFERENT virtual circuit with its own SeqInfo structure.
 *
 *	SeqInfo is linked from the master.
 */

typedef struct SeqInfo {
    struct SeqInfo	*s_Next;	/* linked by master */
    HostInfo		*s_MasterHost;
    HostInfo		*s_SlaveHost;
    struct SeqInfo	*s_Other;	
    rp_seq_t		s_SeqNo;
    rp_vcid_t		s_VCId;
    rp_cmd_t		s_Cmd;		/* test RPCMDF_REPLY only */
    int			s_Refs;
    int			s_UseCount;
} SeqInfo;

#define UseSeqInfo(s)	   ++(s)->s_UseCount
#define UnuseSeqInfo(s)  do { DBASSERT((s)->s_UseCount > 0); if (--(s)->s_UseCount == 0 && (s)->s_Refs == 0) \
				DestroySeqInfo(s); } while (0)

/*
 * InstInfo -	Tracks database instances for our clients connecting to
 *		remote hosts and tracks database instances for remote hosts
 *		connecting to us.
 *
 *	There are two major classes of InstInfo structures.  The first is
 *	the 'Instance 0' InstInfo's, which are used to manage the virtual
 *	circuits between two hosts and are not associated with a particular
 *	database.  The second is a database-specific instance.
 *
 *	RHSlave represents the database@host of the terminating instance
 *	(the one owning the actual database) whether IIF_MASTER or IIF_SLAVE
 *	is set.  However, rh_DBInfo may be NULL for Instance 0's i_RHSlave.
 */

typedef struct InstInfo {
    struct InstInfo	*i_Next;	/* linked by governing instance */
    struct InstInfo	*i_VCManager;	/* VC management instance if not us */
    struct InstInfo	*i_MasterNext;	/* linked list same HostInfo source */
    struct InstInfo	*i_SlaveNext;	/* linked list same HostInfo dest */
    HostInfo		*i_MasterHost;	/* linked list same HostInfo source */
#if 0
    HostInfo		*i_SlaveHost;	/* linked list same HostInfo dest */
    DBInfo		*i_DBInfo;
#endif
    RouteHostInfo	*i_RHSlave;	/* db@host representing slave */
    List		*i_RXPktList;	/* received packets */
    notify_t		i_NotifyInt;	/* notification of reception */
    rp_vcid_t		i_VCId;		/* VCID for this instance */
    rp_type_t		i_Type;		/* type of instance */
    int			i_InstId;	/* instance ID over current VCId */
    union CLAnyMsg	*i_CLMsg;	/* current CLMsg being processed */
    int			i_Flags;
    int			i_Refs;
    int			i_UseCount;
    int			i_VCILevel;	/* VCInstance only */
    rp_cmd_t		i_VCILCmd;	/* VCInstance only */
    dbstamp_t		i_SyncTs;	/* sync ts reported by remote */
} InstInfo;

#define i_SlaveHost	i_RHSlave->rh_Host

#define IIF_MASTER	0x0001
#define IIF_SLAVE	0x0002
#define IIF_READY	0x0004		/* instance reports ready */
#define IIF_CLOSED	0x0008
#define IIF_FAILED	0x0010
#define IIF_STALLED	0x0020
#define IIF_PRIVATE	0x0100
#define IIF_RESET	0x0200
#define IIF_IDLE	0x0400
#define IIF_SYNCWAIT	0x0800		/* held in BEGIN for syncts */
#define IIF_ABORTCOMMIT 0x1000		/* aborting a commit sequence */
#define IIF_CLOSING 	0x2000		/* sent close request */
#define IIF_READONLY 	0x4000		/* current transaction in inst is ro */

#define UseInstInfo(ii)	++ii->i_UseCount
#define UnuseInstInfo(ii)	do { DBASSERT(ii->i_UseCount > 0); if (--ii->i_UseCount == 0 && ii->i_Refs == 0) \
					DestroyInstInfo(ii); } while(0)
/*
 * ClientInfo
 *
 *      ci_NumPeers -	represents the total number of replicated copies of
 *			this database, whether or not we can route to them,
 *			which are considered peers and take part in the
 *			quorum calculation.
 *
 *			This data is stored persistently.
 */
      
typedef struct ClientInfo {
    Node        ci_Node;
    int         ci_NumPeers;    /* total number of replications (1) */
    int         ci_Count;       /* current number of open instances */
    int		ci_Snaps;
    int         ci_Ready;       /* current number of ready instances */
    int         ci_Running;     /* current number of running instances */
    int         ci_Returned;
    int         ci_Closed;      /* current number of failed instances */
    int         ci_Flags;       /* CIF_ flags */
    int		ci_Level;	/* current transaction level */
    int		ci_StreamLevel;
    InstInfo    *ci_IIBase;     /* currently open instances */
    InstInfo	*ci_BestII;	/* best II for RUN_QUERY commands */
    List        ci_List;        /* RPCMD packets for instances */
    List        ci_CLMsgList;   /* client messages in progress */
    notify_t    ci_Notify;      /* notification of RPCMD packets */
    CLDataBase  *ci_Cd;
    dbstamp_t	ci_MinSyncTs;
    dbstamp_t	ci_MinCTsGood;
    dbstamp_t	ci_MinCTsBad;
} ClientInfo;

#define CIF_CHECKNEW	0x0001	/* Route table state change, recheck dbs */
#define CIF_SHUTDOWN	0x0002	/* shutting down, do not check new */
#define CIF_UNUSED04	0x0004
#define CIF_ACCEPTOK	0x0008
#define CIF_READONLY	0x0010	/* Read-only transaction (at top level) */
#define CIF_RWSYNC	0x0020	/* use higher timestamp if avail */
#define CIF_COMMIT1	0x0040	/* tracks ci_Level, based on last queued message */
#define CIF_DEADCLIENT	0x0080

/*
 * LinkNotify -	Notify links of changes in the spanning tree
 */
typedef struct LinkNotify {
    struct LinkNotify	*n_Next;	/* next notification */
    void		*n_Data;	/* route (cmd != 0) or packet (cmd 0)*/
    rp_cmd_t		n_Cmd;		/* 0, RPCMD_HELLO, GOODBYE, ADJUST */
} LinkNotify;

/*
 *  LinkMaintain - Superstructure used to maintain linkages between 
 *		   spanning trees.
 */
typedef struct LinkMaintain {
    struct LinkMaintain	*m_Next;
    char		*m_DBName;
    char		*m_LinkCmd;
    LinkInfo		*m_LinkInfo;
    int			m_Flags;
} LinkMaintain;

#define LMF_STOPME	0x0001

/*
 *  LinkEnable - Superstructure used to maintain the enable/disable
 *		 configuration for local databases.
 *
 * file format:
 *
 *	dbname count pri flags
 */
typedef struct LinkEnable {
    struct LinkEnable	*e_Next;
    char		*e_DBName;	/* database name (without timestamp) */
    int			e_Count;	/* # of database engines or 0 */
    int			e_Pri;
    int			e_Flags;
} LinkEnable;

