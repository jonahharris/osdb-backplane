/*
 * LIBDBCORE/DBCORE.H - Core structures (see datacore.c)
 *
 * (c)Copyright 1999-2002 Backplane, Inc.  Please refer to the COPYRIGHT
 * file at the base of the distribution tree.
 *
 * $Backplane: rdbms/libdbcore/dbcore.h,v 1.111 2003/05/09 05:57:32 dillon Exp $
 */

struct Table;
struct TableMap;
struct DBCreateOptions;
struct Range;
struct SchemaI;
struct TableI;
struct ColI;
struct Index;
struct Query;
struct Conflict;
struct ConflictPos;
struct ResultRow;

#define ZBUF_SIZE		8192
#define MAX_ID_BUF		64	/* schema, table, column names */

#define MAX_DATAMAP_CACHE	(1024 * 1024 * 1024)	/* power of 2 */
#define MAX_BTREE_CACHE		(512 * 1024 * 1024)	/* power of 2 */

typedef const void *const_void_ptr;
typedef void *void_ptr;

typedef struct {
    struct Table *p_Tab;	/* table	   */
    dboff_t	p_Ro;		/* offset in table */
    dboff_t	p_IRo;		/* index-specific */
} dbpos_t;

/*
 * BlockHead - Each table block contains the following header
 *
 *	bh_EndOff is set to 0 for any open datablock.  Only the 
 *	last datablock in a physical file may be open.  bh_EndOff is set to
 *	a non-zero value when a block is closed out.
 *
 *	Generally bh_Type is set to BH_TYPE_DATA or BH_TYPE_FREE.
 *
 *	Must be 64-bit aligned
 */

typedef u_int32_t	bhmagic_t;

typedef struct BlockHead {
    bhmagic_t		bh_Magic;	/* magic number */
    int32_t		bh_Type;	/* table header only */
    int32_t		bh_Unused2;	/* reserved (may become size) */
    int32_t		bh_Unused3;	/* reserved */
    int64_t		bh_CRC;		/* (FUTURE) if closed-out block */
} BlockHead;

#define BH_MAGIC_TABLE	0x6D6174B1
#define BH_MAGIC	0x00AA5500

#define BH_TYPE_TABLE		1
#define BH_TYPE_FREE		2
#define BH_TYPE_DATA		3

/*
 * TableFile -	physical table file structure
 *
 *	Entries marked (heuristic) can be garbaged without effecting
 *	operation.
 *
 *	Entries marked (recovered) may be out of sync on crash and
 *	will be regenerated.
 *
 *	Note: tf_NextStamp must be right after tf_Append
 *	Note: must be 64-bit aligned.
 */

typedef int32_t	tfflags_t;

typedef struct TableFile {
    BlockHead	tf_Blk;
    int32_t	tf_Version;	/* file version */
    int32_t	tf_HeadSize;	/* size of header */
    int32_t	tf_AppendInc;	/* append increment */
    int32_t	tf_Unused01;
    tfflags_t	tf_Flags;	/* (recovered) recovery/state flags */
    int32_t	tf_BlockSize;	/* block size */
    dboff_t	tf_DataOff;	/* base of data */
    dboff_t	tf_FileSize;	/* current file size */
    dboff_t	tf_Append;	/* coherent append point */
    dbstamp_t	tf_HistStamp;	/* earliest available data */
    dbstamp_t	tf_SyncStamp;	/* we have everything before this point */
    dbstamp_t	tf_NextStamp;	/* (SYSTABLE ONLY) next alloctable stamp */
    int32_t	tf_Unused02;
    dbstamp_t	tf_Generation;	/* generation number (invalidates caches) */
    dbstamp_t	tf_CreateStamp;	/* database creation time (aka groupid) */
    char	tf_Name[64];	/* relative file name (template) */
    int32_t	tf_Error;	/* error after recovery, if any */
} TableFile;

typedef TableFile	*TableFile_p;

#define TF_VERSION		2

#define MIN_BLOCKSIZE		(4 * 1024)
#define GUARENTEED_BLOCKSIZE	(128 * 1024 - sizeof(BlockHead))
#define MIN_MEM_BLOCKSIZE	(128 * 1024)
#define FILE_BLOCKSIZE		(128 * 1024)
#define TTS_BLOCKSIZE		(128 * 1024)
#define MIN_APPEND_BLOCKSIZE	(1024 * 1024)
#define MAX_BLOCKSIZE		(8192 * 1024)

#define TFF_CREATED	0x00000001	/* always set after table created */
#define TFF_VALID	0x00000002	/* set after table validated */
#define TFF_CORRUPT	0x00000004	/* set if table is corrupt */
#define TFF_DIRTY	0x00000008
#define TFF_VALIDATING	0x00010000	/* set while table being validated */
#define TFF_REPLACED	0x00020000	/* set if table replaced */
#define TFF_LOGMODE	0x000C0000

#define LOGMODE_ALL	0x00000000
#define LOGMODE_NODATA	0x00040000
#define LOGMODE_HYBRID	0x00080000
#define LOGMODE_RESERV	0x000C0000

/*
 * ColHead - column (in physical record header)
 *
 *	Contains the offset and unaligned number of bytes of column data,
 *	excluding the terminating 0x00 0x00.  Column data is always 
 *	double-byte-terminated.  If either the offset or size is greater
 *	or equal to BSIZE_EXT_BASE, the offset and/or size information is
 *	usually located at the base of the column (in the case of the offset,
 *	you locate the base of the column by using the end of the previous 
 *	column).  The first column will never have an offset >= BSIZE_EXT_BASE.
 *
 *	This produces highly compressable columns, especially for narrow
 *	tables (which most tend to be).
 *
 *	The offset into a column is always 4-byte aligned, and there are
 *	always at least two 0x00 bytes after the end of data in each column.
 *
 *	This structure must be aligned
 */

typedef u_int16_t col_t;

#define COL_ID_MASK	0xFFFF
#define COL_ID_NUM	0x10000

typedef struct ColHead {
    col_t	ch_ColId;	/* column id */
    u_int8_t	ch_Unused;	/* (was) offset relative to RecHead */
    u_int8_t	ch_Bytes;	/* unaligned bytes or EXT code */
} ColHead;

#define BSIZE_EXT_BASE	((u_int8_t)0xF0)
#define BSIZE_EXT_32	((u_int8_t)0xF0)
#define BSIZE_EXT_NULL	((u_int8_t)0xFF)

/*
 * RecHead - physical record header structure
 *
 *	A file EOF or an rh_Magic of 0 indicates the end of the record
 *	in the current block.  If the seek point has not reached the
 *	table append point, the next record will occur at the beginning
 *	of the next block.
 *
 *	This structure must be aligned.
 */

#define VTABLE_ID_MASK	0xFFFF
#define VTABLE_ID_NUM	0x10000

typedef struct RecHead {
    rhmagic_t	rh_Magic;	/* 00 magic number (0xD1) */
    rhflags_t	rh_Flags;	/* 01 tuple flags */
    vtable_t	rh_VTableId;	/* 02 Virtual table support */
    int32_t	rh_Size;	/* 04 aligned size of record */
    dbstamp_t	rh_Stamp;	/* 08 timestamp of modification & transid */
    u_int16_t	rh_NCols;	/* 10 number of columns */
    rhhash_t	rh_Hv;		/* 12 data hash */
    rhuser_t	rh_UserId;	/* 14 authenticated source (user) */
    ColHead	rh_Cols[1];	/* 18 column headers */
    /* XXX transaction id */
} RecHead;

#define RHMAGIC		((rhmagic_t)0xD1)

#define RHF_INSERT	0x01
#define RHF_UPDATE	0x02
#define RHF_DELETE	0x04
#define RHF_REPLICATED	0x08	/* record replicated from remote */

/*
 * DataMap - cache portions of a table, by block size.  MIN_DATAMAP_BLOCK
 * represents the minimum cache block size and the default for schema
 * creation.  This value may be overriden when creating a schema.
 */

#define MIN_DATAMAP_BLOCK	(128 * 1024)
#define MIN_DATAMAP_MASK	(MIN_DATAMAP_BLOCK - 1)

typedef struct DataMap {
    Node		dm_Node;	/* ta_BCList */
    struct DataMap	*dm_HNext;	/* hash table */
    struct Table	*dm_Table;
    const char		*dm_Base;
    dboff_t		dm_Ro;		/* includes encoded fileno */
    int			dm_Refs;
} DataMap;

#define DM_HSIZE	(MAX_DATAMAP_CACHE / MIN_DATAMAP_BLOCK * 2)
#define DM_HMASK	(DM_HSIZE-1)
#define DM_REF_PERSIST	0x40000000

/*
 * IndexMap - cache portions of an index
 */

typedef struct IndexMap {
    Node		im_Node;
    struct IndexMap	*im_HNext;	/* when supported by index */
    int			im_Refs;
    struct Index	*im_Index;
    const char		*im_Base;
    dboff_t		im_Ro;
} IndexMap;

#define IM_REF_MODLINK	0x40000000	/* contains dirty data */

/*
 *  TableOps
 */

typedef struct Index *Index_p;

typedef struct TableOps {
    void	(*to_OpenTableMeta)(struct Table *tab, struct DBCreateOptions *dbc, int *error);
    void	(*to_CacheTableMeta)(struct Table *tab);
    void	(*to_CloseTableMeta)(struct Table *tab);
    Index_p	(*to_GetTableIndex)(struct Table *tab, vtable_t vt, col_t colId, int opId);
    const_void_ptr (*to_GetDataMap)(dbpos_t *pos, DataMap **pmap, int bytes);
    void	(*to_RelDataMap)(DataMap **pdm, int freeLastClose);
    int		(*to_WriteFile)(dbpos_t *pos, void *ptr, int bytes);
    int		(*to_WriteMeta)(struct Table *tab, int off, void *ptr, int bytes);
    int		(*to_FSync)(struct Table *tab);
    int		(*to_ExtendFile)(struct Table *tab, int bytes);
    void	(*to_TruncFile)(struct Table *tab, int bytes);
    void	(*to_CleanSlate)(struct Table *tab);
    dboff_t	(*to_FirstBlock)(struct Table *tab);
    dboff_t	(*to_NextBlock)(struct Table *tab, const BlockHead *bh, dboff_t ro);
} TableOps;

/*
 * Table - in-memory table reference structure
 *
 * In a transaction this structure may represent a frozen table view
 * but may, if a memory table, contain extensions holding uncomitted
 * modifications.  For a memory table these extensions can be rolled back.
 *
 * note: recursive transactions, inner transactions which are
 * rolled back will instantly rollback the extensions, so we
 * need recursive freezes XXX?
 *
 * ta_Append holds the table's append offset as of the transaction freeze
 * point and is used for most query related operations.  It may be less then
 * ta_Meta->tf_Append.  Note that ta_Meta->tf_Append cannot be safely
 * accessed without a lock.
 */
typedef struct Table {
    struct Table	*ta_Next;	/* DB hash table linkage */
    struct Table	*ta_Parent;	/* transaction stacking / extension */
    struct Table	*ta_ModNext;	/* linked list of modified tables */
    struct Conflict	*ta_TTs;	/* Phase-1 commit conflict rendezvous*/
    int			ta_TTsSlot;	/* Slot in TTS being used */
    char		*ta_Name;	/* table name */
    char		*ta_Ext;	/* table name extension */
    char		*ta_FilePath;	/* file path */
    struct DataBase	*ta_Db;		/* associated database */
    dboff_t		ta_Append;	/* copy of this table's append off */
    int			ta_Refs;	/* reference count */
    bkpl_task_t		ta_LockingTask;	/* for debug assertions */
    int			ta_LockCnt;
    List		ta_WaitList;	/* tasks waiting on lock */
    int			ta_Flags;
    const TableFile	*ta_Meta;	/* meta data mmap */
    int			ta_Fd;		/* file descriptor */
    int			ta_BCBlockSize;	/* buffer cache block size */
    int			ta_BCCount;	/* mapped entries in buffer cache */
    List		ta_BCList;	/* buffer cache DataMap's */
    int			ta_LogFileId;/* file identifier in log */
    struct Index	*ta_IndexBase;	/* indexes on table */
    TableOps		*ta_Ops;
} Table;

#define ta_OpenTableMeta	ta_Ops->to_OpenTableMeta
#define ta_CacheTableMeta	ta_Ops->to_CacheTableMeta
#define ta_CloseTableMeta	ta_Ops->to_CloseTableMeta
#define ta_GetTableIndex	ta_Ops->to_GetTableIndex
#define ta_GetDataMap		ta_Ops->to_GetDataMap
#define ta_RelDataMap		ta_Ops->to_RelDataMap
#define ta_WriteFile		ta_Ops->to_WriteFile
#define ta_WriteMeta		ta_Ops->to_WriteMeta
#define ta_FSync		ta_Ops->to_FSync
#define ta_ExtendFile		ta_Ops->to_ExtendFile
#define ta_TruncFile		ta_Ops->to_TruncFile
#define ta_CleanSlate		ta_Ops->to_CleanSlate
#define ta_FirstBlock		ta_Ops->to_FirstBlock
#define ta_NextBlock		ta_Ops->to_NextBlock

#define TAF_HASCHILDREN	0x0002
#define TAF_METALOCKED	0x0004		/* descriptor is locked */
#define TAF_MODIFIED	0x0008		/* table was marked modified */

#define TAB_HSIZE	64
#define TAB_HMASK	(TAB_HSIZE-1)

#define DS_HSIZE	64
#define DS_HMASK	(DS_HSIZE-1)

/* 
 * DataBase - in-memory database structure, also represents a transaction
 *
 *	Tables are linked into the hash table on the fly and may be
 *	left in the hash table after the reference count drops to 0.
 *
 *	The DataBase structure represents a transaction hopper, entities
 *	(in the same process or a different process) performing 
 *	parallel transactions need a separate instance of this structure
 *	for each active transaction.
 */
typedef struct DataBase {
    Node	db_Node;
    List	db_List;		/* pushed transactions */
    struct DataBase *db_Parent;		/* parent database */
    struct Query *db_RecordedQueryBase;	/* queries recorded for commit */
    struct Query **db_RecordedQueryApp;	/* append point for new queries */
    char	*db_DirPath;		/* directory path */
    Table	*db_SysTable;		/* system.dt0	*/
    Table	*db_TabHash[TAB_HSIZE];	/* lookup tables */
    Table	*db_ModTable;		/* modified tables (linked list) */
    dbstamp_t   db_FreezeTs;		/* limit selects of parent db */
    dbstamp_t   db_WriteTs;		/* timestamp to use for writing */
    dbstamp_t   db_CommitCheckTs;	/* commit phase 1 test */
    dbstamp_t	db_CommitConflictTs;	/* latest conflict ts if COMMITFAIL */
    int         db_WRCount;		/* Write-Record count */
    int		db_Flags;
    int		db_Refs;		/* open refs */
    int		db_PushType;		/* transaction level control type */
    dbstamp_t	db_StampId;		/* database id (DBSTAMP_ID_MASKed) */
    int		db_Pid;			/* pid cache to avoid syscall */
    int		db_DataLogFd;		/* data log descriptor */
    dboff_t	db_DataLogOff;		/* file offset */
    u_int	db_DataLogCount;	/* number of log files / current */
    u_int16_t	db_DataLogSeqNo;
    int		db_NextLogFileId;
    struct SchemaI *db_SchemaICache;	/* first non-root db level only */
} DataBase;

#define DBPUSH_ROOT		1
#define DBPUSH_TTS		2
#define DBPUSH_TMP		3

#define DBF_COMMIT1		0x0001	/* in commit-1 */
#define DBF_COMMITFAIL		0x0002	/* in commit-1 */
#define DBF_CALLBACK_RUNNING	0x0004	/* callback currently running */
#define DBF_EXCLUSIVE		0x0008	/* exclusive use */
#define DBF_C1CONFLICT		0x0010	/* in commit-1 */
#define DBF_CREATE		0x0020	/* create db if it does not exist */
#define DBF_READONLY		0x0040	/* read-only & parents read-only */
#define DBF_METACHANGE		0x0080	/* tmp tab, meta structure modified */

typedef struct DBCreateOptions {
	int	c_Flags;
	int	c_BlockSize;	/* use different block size */
	dbstamp_t c_TimeStamp;	/* official create timestamp */
} DBCreateOptions;

#define DBC_OPT_BLKSIZE		0x00000001
#define DBC_OPT_TIMESTAMP	0x00000002


/*
 * RawData - data structure used to read and write data files.
 */

#define DTYPE_UNKNOWN	0
#define DTYPE_STRING	1		/* string (stored as string) */

typedef struct RawData {
    Table	*rd_Table;
    DataMap	*rd_Map;		/* data mapping reference */
    const RecHead *rd_Rh;
    ColData	*rd_ColBase;		/* column data (sorted) */
    int		rd_AllocSize;
    char	rd_CookVTId[5];
    char	rd_CookTimeStamp[17];
    char	rd_CookUserId[9];
    char	rd_CookOpCode[3];
} RawData;

#define RDF_READ	0x0001
#define RDF_ALLOC	0x0002
#define RDF_ZERO	0x0004
#define RDF_FORCE	0x0008
#define RDF_USERH	0x0010		/* use existing rd_Rh */

/*
 * Index - index a [virtually tagged] physical table on a column
 *
 *	Without an index the row numbering is really in the form of
 *	record offsets and is completely unsorted.  Any query must
 *	scan the entire table.  With an index the row numbering will
 *	result in a list sorted on some critera, and may or may not
 *	represent offsets.  Indexes operate on physical colum id's and are
 *	associated with (possibly virtually tagged) physical tables.
 * 
 *	Generally any given instance of a table may have only one index,
 *	since we do not support subsorts.  Queries that do self joins
 *	can of course use a different index on each instance of the same
 *	table.
 *
 *	i_PosCache may optionally be managed by the index module to
 *	shortcut from-root searches.  This can wind up being quite
 *	useful for 'a.key = b.key' joins.
 *
 *	XXX WARNING!  NOT REENTRANT (due to block cache).  FIXME
 */
    
typedef struct Index {
    Node	i_Node;		/* global LRU list */
    struct Index *i_Next;
    Table       *i_Table;
    vtable_t 	i_VTable;
    col_t	i_ColId;
    int		i_OpClass;
    int         i_Refs;
    int		(*i_ScanRangeOp)(struct Index *index, struct Range *r);
    void	(*i_Close)(struct Index *index);
    void	(*i_SetTableRange)(struct TableI *ti, struct Table *tab, const struct ColData *colData, struct Range *r, int flags);
    void	(*i_UpdateTableRange)(struct TableI *ti, struct Range *r);
    void	(*i_NextTableRec)(struct TableI *ti);
    void	(*i_PrevTableRec)(struct TableI *ti);
    dbpos_t	i_PosCache;
    char	*i_FilePath;
    FLock	i_FLock;
    union {
	const struct BTreeHead	*BTreeHead;
    } i_Info;
    union {
	List	BTreeCacheList;
    } i_Cache;
    int		i_CacheCount;
    int		i_CacheRand;
} Index;

#define i_Fd			i_FLock.fl_Fd
#define i_BTreeHead		i_Info.BTreeHead
#define i_BTreeCacheList	i_Cache.BTreeCacheList

typedef int iflags_t;

typedef struct IndexHead {
    int         ih_Magic;       /* magic number */
    int         ih_Version;     /* version of btree */
    int         ih_HeadSize;    /* size of header */
    iflags_t	ih_Flags;	/* see index-specific flags */
} IndexHead;

/*
 * SchemaI - An instance of a schema
 */

typedef struct SchemaI {
    struct SchemaI *si_Next;
    struct SchemaI *si_CacheCopy;
    struct Query *si_Query;
    char	*si_ScmName;
    char	*si_DefaultPhysFile;
    int		si_ScmNameLen;
    struct TableI *si_FirstCacheTableI;	/* tables in schema (cache only) */
} SchemaI;

/*
 * TableI - An instance of a table in a query
 *
 *	This structure virtualizes a table and is also used to house
 *	a view, thus the optional embedded selection.
 *
 *	For example, if you specify two instances of the same table in
 *	a query, there will be two TableInstance structures pointing to
 *	the same virtualId+Table.
 *
 *	During a query, each instance of a table can only support one
 *	index for the search because we do not implement subsorts.  That is,
 *	since the index effectively renumbers the table, trying to use
 *	a second index based on the range results from the first index will
 *	simply not work.
 *
 *	We must store ti_Append at the start of any given query in order
 *	to prevent the query from stepping over itself.  For example, to
 *	prevent an UPDATE from looping on its own update.
 */

typedef struct TableI {
    struct TableI *ti_Next;		/* link within query */
    struct TableI *ti_CacheCopy;	/* high level tableI cache */
    struct Query *ti_Query;
    struct Range *ti_MarkRange;	/* first search clause */
    SchemaI	*ti_SchemaI;
    char	*ti_TabName;
    int		ti_TabNameLen;
    char	*ti_AliasName;
    int		ti_AliasNameLen;
    Table	*ti_Table;	/* this table (current transaction)   */
    struct Index *ti_Index;	/* using index (changes during scan) */
    RawData	*ti_RData;
    struct ColI	*ti_FirstColI;
    dbpos_t	ti_RanBeg;
    dbpos_t	ti_RanEnd;
    dboff_t	ti_Append;	/* table scan limit */
    dboff_t	ti_IndexAppend;	/* index scan limit */
    dboff_t	ti_Rewind;	/* rewind point for query */
    vtable_t	ti_VTable;
    int		ti_ScanOneOnly;	/* limit scan (used to optimize scan-one) */
    char	*ti_TableFile;
    int		(*ti_ScanRangeOp)(struct Index *index, struct Range *r);
    int		ti_Flags;
    int64_t	ti_DebugScanCount;
    int64_t	ti_DebugIndexScanCount;
    int64_t	ti_DebugIndexInsertCount;
    int64_t	ti_DebugConflictC1Count;
} TableI;

#define TABRAN_SLOP	0x0001	/* set range for unindexed part of table */
#define TABRAN_INDEX	0x0002	/* set range for indexed part of table */
#define TABRAN_INIT	0x0004	/* initialize ti_[Index]Append, else use */
#define TABRAN_SYNCIDX	0x0008	/* unconditionally synchronize the index */

/*
 * ColI - An Instance of a column in a table instance (in a query).
 *
 *	note: ColI ordering occurs only from Query->q_FirstColI.
 *	ColI's are not subordered by their TableI's.  In otherwords,
 *	ColI's may represent specifically parsed orderings such as
 *	data insertion ordering, select ordering, and so forth.
 *
 *	note: In a typical query, ColI->ci_CData is integrated into
 *	the table's raw columns structure.  The data in question
 *	changes dynamically as the engine scans the rows.
 */

typedef struct ColI {
    struct ColI	*ci_Next;	/* link within TableI */
    struct ColI *ci_QNext;	/* link within query (display/insert order) */
    struct ColI *ci_QSortNext;	/* link within query (sort ordering) */
    TableI	*ci_TableI;
    char	*ci_ColName;	/* column name */
    int		ci_ColNameLen;	/* column name length */
    char	*ci_Default;	/* default value */
    int		ci_DataType;	/* data type */
    int		ci_DefaultLen;
    int		ci_Flags;
    int		ci_OrderIndex;
    int		ci_Size;
    col_t	ci_ColId;
    ColData	*ci_Const;	/* init data (insert, update, create) */
    ColData	*ci_CData;	/* post resolution */
} ColI;

#define CIF_ORDER	0x0004
#define CIF_SORTORDER	0x0008
#define CIF_KEY		0x0010
#define CIF_NOTNULL	0x0020
#define CIF_DELETED	0x0040
#define CIF_SORTDESC	0x0080	/* if CIF_SORTORDER, sort descending */
#define CIF_SPECIAL	0x0100	/* special column name */
#define CIF_UNIQUE	0x0200	/* UNIQUE */
#define CIF_SET_SPECIAL	0x0400	/* set QF_SPECIAL_WHERE in query if special */
#define CIF_WILD	0x8000	/* allow wildcards */	
#define CIF_DEFAULT	0x10000 /* column has default / default request */

typedef struct DelHash {
    int			dh_Count;
    int			dh_Flags;
} DelHash;

#define DHF_INTERRUPTED	0x0001
#define DHF_SPECIAL	0x0002

/*
 * Range - Scan iteration
 *
 *	Implements the table search mechanism.  Ranges serially linked to
 *	represent the search expression, and backed linked within their
 *	table instance.
 *
 *	The r_RunRange function returns a record count (after deletions
 *	have been pruned), or a negativ number on error.
 *
 *	The r_OpFunc function returns a negative number for FALSE and
 *	a positive number for TRUE.  Numbers can be varying in order
 *	to distinguish operational groupings.  For example, the = operator
 *	will return -2 (FALSE) for the low side of the comparison, 
 *	-1 (FALSE) for the high side, and a positive number when TRUE.
 *	The <> operator will return +1 (TRUE) for the low side,
 *	+2 (TRUE) for the high side, and a negative number for TRUE.
 *
 *	XXX record count returned by r_RunRange is only 32 bits
 */

typedef union RangeArg {
    void	*ra_VoidPtr;
    struct Range *ra_RangePtr;
} RangeArg;

typedef struct Range {
    RangeArg	r_Next;
    struct Range *r_Prev;
    struct Range *r_PrevSame;	/* previous range using same table instance */
    struct Range *r_NextSame;	/* next range using same table instance */
    int		(*r_RunRange)(RangeArg next);
    int		(*r_OpFunc)(const ColData *c1, const ColData *c2);
    int		r_OpId;
    int		r_OpClass;
    TableI	*r_TableI;	/* table instance */
    DelHash	*r_DelHash;	/* delete tracking */
    const ColData *r_Col;	/* iteration (indexed) data */
    const ColData *r_Const;	/* constant data */
    int		r_Type;
    int		r_Flags;
} Range;

/*
 * r_Type
 */
#define ROPF_CONST	0x10	/* (r_Type) */

#define ROP_NOP		0	/* Pass-through (for degenerate case) */
#define ROP_JOIN	1	/* Join this and r_Next (use next's r_Oper) */
#define ROP_CONST	(2|ROPF_CONST)	/* Comparison against constant */
#define ROP_JCONST	(3|ROPF_CONST)	/* Special const compare in join */

/*
 * r_Flags
 */
#define RF_FORCESAVE	0x0001	/* force deletion scanning (__XXX fields) */

/*
 * ROPs for operator id's
 */
#define ROP_EQEQ	0x01
#define ROP_LT		0x02
#define ROP_LTEQ	0x03
#define ROP_GT		0x04
#define ROP_GTEQ	0x05
#define ROP_NOTEQ	0x06
#define ROP_LIKE	0x07
#define ROP_RLIKE	0x08
#define ROP_SAME	0x09
#define ROP_RSAME	0x0A

#define ROP_STAMP_EQEQ	0x10
#define ROP_STAMP_LT	0x11
#define ROP_STAMP_LTEQ	0x12
#define ROP_STAMP_GT	0x13
#define ROP_STAMP_GTEQ	0x14
#define ROP_VTID_EQEQ	0x16
#define ROP_USERID_EQEQ	0x17
#define ROP_OPCODE_EQEQ	0x18


/*
 * Query - Holds range sequence, table instances, and so forth.
 */

typedef struct Query {
    struct Query *q_RecNext;	/* recorded next (see db_RecordedQueryBase) */
    DataBase    *q_Db;
    RangeArg	q_RangeArg;	/* list, r_Next, or terminator */
    int		(*q_RunRange)(RangeArg next);
    SchemaI	*q_FirstSchemaI;/* list, sc_Next */
    TableI      *q_TableIQBase;	/* list, ti_Next (all TableI's) */
    ColI	*q_ColIQBase;	/* list, ci_QNext (display/insrt order only) */
    ColI	**q_ColIQAppend;
    ColI	*q_ColIQSortBase; /* list, ci_QSortNext (display/insrt order only) */
    ColI	**q_ColIQSortAppend;
    ColData     *q_ConstDataBase; /* linked ilst, cd_Next (constants) */
    SchemaI	*q_DefSchemaI;	/* default schema */
    int		(*q_TermFunc)(struct Query *q);	/* NULL to just count */
    void	(*q_SysCallBack)(void *info, RawData *rd);
    void	*q_TermInfo;	/* depends on terminator function */
    int		q_IQSortCount;
    int		q_OrderCount;	/* number of columns in q_ColIQSortBase list */
    int		q_StallCount;	/* used by database/instance.c */
    int		q_StartRow;
    int		q_MaxRows;
    int		q_CountRows;
    int64_t	q_DebugScanCount;
    int64_t	q_DebugScanIndexCount;
    int64_t	q_DebugInsertIndexCount;
    int		q_TermOp;	/* depends on terminator function */
    int		q_Error;	/* resolution/execution error */
    int		q_Flags;
    List	q_ResultBuffer; /* list, results if server sorted */
    char	*q_QryCopy;	/* for debugging only */
} Query;

#define QF_ROLLEDBACK		0x0001	/* indicates rolled-back query */
#define QF_RETURN_ALL		0x0002	/* return includes deleted records */
#define QF_SPECIAL_WHERE	0x0004	/* __special's in where clause */
#define QF_WITH_ORDER		0x0008	/* contains order by clause */
#define QF_WITH_LIMIT		0x0010	/* contains limit clause */

#define QF_CLIENT_ORDER		0x01000000
#define QF_CLIENT_LIMIT		0x02000000

#define QOP_SELECT		1
#define QOP_INSERT		2
#define QOP_DELETE		3
#define QOP_UPDATE		4
#define QOP_COUNT		5
#define QOP_CLONE		6
#define QOP_COMMIT_CHECK	7
#define QOP_SYSQUERY		8
#define QOP_TRANS		9

typedef struct ResultRow {
    Node	rr_Node;
    int		rr_NumCols;	/* number of data columns */
    char	**rr_Data;	/* array of rr_NumCols string pointers */
    int		*rr_DataLen;	/* array of rr_NumCols string lengths */
    int		rr_NumSortCols;	/* number of sort columns */
    char	**rr_SortData;	/* array of rr_NumSortCols string pointers */
    int		*rr_SortDataLen; /* array of rr_NumSortCols string lengths */
#define RR_SORTDATALEN_MASK	0x3fffffff
#define RR_SORTDESC_MASK	0x40000000
#define RR_SORTSHOW_MASK	0x80000000
} ResultRow;

typedef struct RSNode {
    Node	rn_Node;
    RecHead	*rn_Rh;
} RSNode;

typedef struct RSTable {
    RSNode	*rl_Cache;
    List	rl_List;
    DataBase	*rl_Db;
} RSTable;

/*
 * token_t - lexical token
 */

typedef struct {
    int         t_Type;
    const char  *t_Data;
    int         t_Len;
    const char  *t_FileBase;
    int         t_FileSize;
} token_t;

#define CCF_FORCE	0x0001		/* force commit (bootstrap only) */

#define SYNCMERGE_COUNT  1000
#define SYNCMERGE_BYTES  100000


