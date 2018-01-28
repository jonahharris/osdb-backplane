/*
 * LIBCLIENT/CLIENT.H -	Private structures for libclient
 *
 * (c)Copyright 2000-2002 Backplane, Inc.  Please refer to the COPYRIGHT
 * file at the base of the distribution tree.
 *
 * $Backplane: rdbms/libclient/client.h,v 1.32 2002/08/20 22:05:48 dillon Exp $
 */

struct RouteHostInfo;

typedef struct CLDataBase {
    Node	cd_Node;
#if 0
    List	cd_ReqList;
#endif
    struct CLDataBase *cd_Parent;
    int		cd_Refs;
    struct RouteHostInfo *cd_SyncRouteHost; /* used by repl (synchronizer) */
    iofd_t	cd_Ior;
    iofd_t	cd_Iow;
    char	*cd_DBName;
    char	*cd_HelloHost;		/* used by client/user.c */
    int		cd_Type;
    int		cd_Flags;
    int		cd_Level;
    int		cd_StreamLevel;
    int		cd_Pid;
    List	cd_List;		/* used by replicator */
    notify_t	cd_NotifyInt;		/* used by synchronizer */
    tlock_t	cd_Lock;		/* used to serialize instance reqs */
    struct DBInfo   *cd_DBInfo;
    struct RPSummary *cd_RPSummary;	/* used by replicator */
    struct RPList   *cd_RPList;		/* used by replicator */
    struct DataBase *cd_Db;		/* used by replicator/database.c */
    struct TableI   *cd_DSTerm;		/* used by replicator/database.c */
    struct RouteInfo *cd_Route;		/* used by replicator */
    dbstamp_t	    cd_ActiveMinCTs;	/* active MinCTs if in COMMIT1 state */
    dbstamp_t	    cd_StampId;		/* unique id for this database@host */
    dbstamp_t	    cd_CreateStamp;	/* creation stamp (groupid) */
    cluser_t	    cd_LLUserId;
    void	    *cd_LLAuditInfo;
    cluser_t	    (*cd_LLAuditCallBack)(void *info);
} CLDataBase;

#define CDF_COMMIT1	0x0001
#define CDF_UNUSED02	0x0002
#define CDF_UNUSED04	0x0004		/* shutdown synchronizer */
#define CDF_DEADLOCK	0x0008
#define CDF_TRIGPEND	0x0010		/* new entries in trigger log */
#if 0
#define CDF_OWNED	0x0020
#endif
#define CDF_DIDRAWDATA	0x0040
#define CDF_SYNCPEND	0x0080		/* replicator/synchronizer */
#define CDF_SYNCDONE	0x0100		/* replicator/synchronizer */
#define CDF_STREAMMODE	0x0200		/* libclient/user.c */
#define CDF_STREAMQRY	0x0400		/* libclient/user.c */
#define CDF_AUDITCOMMITGOOD 0x2000	/* audit id comitted good */
#define CDF_SYNCWAITING 0x4000		/* synchronizer waiting */

#define CD_TYPE_DATABASE	1
#define CD_TYPE_INSTANCE	2

#define CLDB_HSIZE	256
#define CLDB_HMASK	(CLDB_HSIZE-1)

struct CLRow;

typedef struct CLRes {
    CLDataBase	*cr_Instance;
    List	cr_RowList;
    struct CLRow *cr_CurRow;
    int		cr_CurRowNum;
    int		cr_NumRows;	/* used space */
    int		cr_NumCols;	/* columns in result rows */
    int		cr_NumOrder;	/* number of order by columns */
    int		*cr_Order;	/* order code */
    int		cr_StartRow;	/* first row in result set */
    int		cr_MaxRows;	/* number of rows in result set with limiting */
    int		cr_RHSize;	/* row hash size for sorting */
    struct CLRow **cr_RHash;	/* row hash */
    int		cr_TotalCols;
    int		cr_Error;	/* operating error */
} CLRes;

#define ORDER_STRING_COLMASK	0x000FFFF
#define ORDER_STRING_FWD	0x0010000
#define ORDER_STRING_REV	0x0020000

typedef struct CLRow {
    Node	co_Node;
    CLRes	*co_Res;
    int		co_Bytes;
    int		*co_Lens;	/* length(s) of each element */
    const char	*co_Data[0];	/* array of cr_NumCols string pointers */
} CLRow;

