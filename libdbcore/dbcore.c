/*
 * LIBDBCORE/DBCORE.C - implements core database functionality at 
 *			physical file level
 *
 * (c)Copyright 1999-2002 Backplane, Inc.  Please refer to the COPYRIGHT
 * file at the base of the distribution tree.
 *
 * $Backplane: rdbms/libdbcore/dbcore.c,v 1.101 2002/10/02 20:18:00 dillon Exp $
 *
 *	Implements:
 *
 *	Physical file creation, deletion
 *	Extension files
 *	File Synchronization
 *	File Validation after crash
 *	Timestamp/SequenceNumbers
 *	Low level SELECT (linear physical table scanning)
 *	Low level INSERT (append)
 *	Low level UPDATE (delete mark + append)
 *	Low level DELETE (delete mark)
 *	Callbacks for query invalidation (optimistic locking) XXX
 */

#include "defs.h"

Export DataBase *OpenDatabase(const char *dirBase, const char *dbname, int flags, DBCreateOptions *dbc, int *error);
Export void DeleteDatabase(const char *dirBase, const char *dbname);
Export DataBase *PushDatabase(DataBase *par, dbstamp_t fts, int *error, int flags);
Export DataBase *PopDatabase(DataBase *db, int *error);
Export dbstamp_t SetNextStamp(DataBase *db, dbstamp_t ts);
Export void SetDatabaseId(DataBase *db, dbstamp_t dbid);
Export void CloseDatabase(DataBase *db, int freeLastClose);
Export Table *OpenTable(DataBase *db, const char *name, const char *ext, DBCreateOptions *dbc, int *error);
Export Table *OpenTableByTab(Table *tab, DBCreateOptions *dbc, int *error);
Export void CloseTable(Table *tab, int freeLastClose);
Export void InitOptionsFromTable(Table *tab, DBCreateOptions *dbc);
Export dbstamp_t GetDBCreateTs(DataBase *db);
Export int GetSysBlockSize(DataBase *db);
Export dbstamp_t GetSyncTs(DataBase *db);
Export dbstamp_t GetMinCTs(DataBase *db);
Export dbstamp_t SetSyncTs(DataBase *db, dbstamp_t synctTs);
Export void CopyGeneration(Table *rtab, Table *wtab, dbstamp_t histTs);

Export dbstamp_t AllocStamp(DataBase *db, dbstamp_t minCTs);

Export void LockTable(Table *tab);
Export void UnLockTable(Table *tab);
Export void LockDatabase(DataBase *db);
Export void UnLockDatabase(DataBase *db);
Export int GetFirstTable(TableI *ti, Range *r);
Export int GetLastTable(TableI *ti, Range *r);
Export int GetNextTable(TableI *ti, Range *r);
Export int GetPrevTable(TableI *ti, Range *r);
Export void GetSpecialTable(TableI *ti, Table *tab, Range *r);
Export void ClearSpecialTable(TableI *ti);
Export void SelectBegTableRec(TableI *ti, int flags);
Export void SelectEndTableRec(TableI *ti, int flags);
Export void SelectNextTableRec(TableI *ti, int flags);
Export void SelectPrevTableRec(TableI *ti, int flags);
Export int InsertTableRec(Table *tab, RawData *rd, vtable_t vt);
Export int UpdateTableRec(Table *tab, dbpos_t *opos, RawData *rd, vtable_t vt);
Export int DeleteTableRec(Table *tab, dbpos_t *opos, vtable_t vt);
Export const RecHead *MapDataRecord(RawData *rd, dbpos_t *pos);
Export ColData *ReadDataRecord(RawData *rd, dbpos_t *pos, int flags);
Export int RecordIsValid(TableI *ti);
Export int RecordIsValidForCommit(TableI *ti);
Export int RecordIsValidForCommitTestOnly(TableI *ti);

Prototype int DbPgSize;
Prototype int DbPgMask;
Prototype List DbList;

Prototype dboff_t WriteDataRecord(Table *tab, RawData *rd, const RecHead *rh, vtable_t vt, dbstamp_t ts, rhuser_t userid, rhflags_t flags);
Prototype void RewindDataWrites(Table *tab, dboff_t appRo);
Prototype void DestroyTableCaches(Table *tab);

static void removeRecur(const char *path);
static int SizeDataRecord(RawData *rd, const RecHead *rh, int *pcount);

int DbPgSize;
int DbPgMask;
List DbList = INITLIST(DbList);

/*
 * rhHash() - hash data fields for quicker matching
 */

__inline rhhash_t
rhHash(const RecHead *rh)
{
    rhhash_t hv = 0xA4FC;
    int c0off = offsetof(RecHead, rh_Cols[0]);

    while (c0off < rh->rh_Size) {
	hv = (hv ^ *(const rhhash_t *)((const char *)rh + c0off)) + 1;
	c0off += sizeof(rhhash_t);
    }
    return(hv);
}

/*
 * OpenDatabase() - open a database up, reference the system table.
 *
 *	Open or create a database.  The returned structure is the 
 *	'client instance' of a database.
 *
 *	XXX clear cache
 */

DataBase *
OpenDatabase(const char *dirBase, const char *dbname, int flags, DBCreateOptions *dbc, int *error)
{
    DataBase *db = NULL;
    char *dirPath;

    *error = 0;
    flags &= DBF_EXCLUSIVE | DBF_CREATE;

    /*
     * Save system page size for low level file mmap routines
     */
    if (DbPgSize == 0) { 
	DbPgSize = getpagesize();
	DbPgMask = DbPgSize - 1;
    }

    /*
     * Directory path for database
     */
    if (dirBase == NULL) {
	dirPath = strdup(dbname);
    } else {
	if (asprintf(&dirPath, "%s/%s", dirBase, dbname) < 0) {
	    *error = DBERR_NO_MEM;
	    return(NULL);
	}
    }

    /*
     * Locate an already-open copy, allocating a new one if necessary.
     */
    for (db = getHead(&DbList); db; db = getListSucc(&DbList, &db->db_Node)) {
	if (strcasecmp(db->db_DirPath, dirPath) == 0) {
	    free(dirPath);
	    break;
	}
    }
    if (db == NULL) {
	db = zalloc(sizeof(DataBase));
	db->db_DirPath = dirPath;
	db->db_FreezeTs = DBSTAMP_MAX;		/* used by synchronizer */
	db->db_WriteTs = 0;			/* unused in root-level DB */
	db->db_CommitCheckTs = DBSTAMP_MAX;	/* used by synchronizer */
	db->db_RecordedQueryApp = (void *)-1;	/* unused in root-level DB */
	db->db_PushType = DBPUSH_ROOT;
	db->db_Pid = getpid();
	db->db_DataLogFd = -1;
	db->db_NextLogFileId = 1;
	addTail(&DbList, &db->db_Node);
	initList(&db->db_List);
    }

    /*
     * If Ref count was 0 db_SysTable may be NULL, open the sys table.
     *
     * Also deal with exclusive opens here.  Exclusive opens are only
     * valid on the first reference XXX.
     */
    if (db->db_Refs++ == 0) {
	db->db_Flags = (db->db_Flags & ~DBF_EXCLUSIVE) | flags;
	if (db->db_SysTable == NULL) {
	    db->db_SysTable = OpenTable(db, "sys", "dt0", dbc, error);
	    if (db->db_SysTable == NULL && 
		*error == DBERR_CANT_OPEN &&
		(flags & DBF_CREATE)
	    ) {
		mkdir(db->db_DirPath, 0775);
		*error = 0;
		db->db_SysTable = OpenTable(db, "sys", "dt0", dbc, error);
	    }
	}
    }

    /*
     * Finish up.  If an error occured, close-up the database, set the
     * error, and return NULL
     */
    if (*error != 0) {
	CloseDatabase(db, 1);
	db = NULL;
    } else {
	DBASSERT(db->db_SysTable != NULL);
    }
    return(db);
}

void
DeleteDatabase(const char *dirBase, const char *dbname)
{
    char *path;

    if (strchr(dbname, '/'))
	return;
    safe_asprintf(&path, "%s/%s", dirBase, dbname);
    removeRecur(path);
}

static void
removeRecur(const char *path)
{
    DIR *dir;

    if ((dir = opendir(path)) != NULL) {
	struct dirent *den;
	char *npath = NULL;

	while ((den = readdir(dir)) != NULL) {
	    struct stat st;

	    if (strcmp(den->d_name, ".") == 0)
		continue;
	    if (strcmp(den->d_name, "..") == 0)
		continue;
	    safe_replacef(&npath, "%s/%s", path, den->d_name);
	    if (lstat(npath, &st) == 0) {
		if (S_ISDIR(st.st_mode)) {
		    removeRecur(npath);
		} else {
		    remove(npath);
		}
	    }
	}
	safe_free(&npath);
	closedir(dir);
	rmdir(path);
    }
}

/*
 * CloseDatabase() -	Finished using a database
 *
 *	We are finished using a database, release resource to the cache.
 *	If freeLastClose is set and the reference count goes to 0, destroy
 *	all resources.
 */

void
CloseDatabase(DataBase *db, int freeLastClose)
{
    Query *q;

    DBASSERT(db->db_Refs != 0);

    if (--db->db_Refs != 0)
	return;

    /*
     * Destroy any recorded queries.  Note that the root level DataBase
     * will not have any recorded queries.
     */
    if (db->db_PushType == DBPUSH_ROOT)
	DBASSERT(db->db_RecordedQueryBase == NULL);

    while ((q = db->db_RecordedQueryBase)) {
	db->db_RecordedQueryBase = q->q_RecNext;
	FreeQuery(q);
    }
    db->db_RecordedQueryApp = &db->db_RecordedQueryBase;

    /*
     * Make sure there are no open transactions on this database
     */
    DBASSERT(getHead(&db->db_List) == NULL);

    if (freeLastClose) {
	/*
	 * Free for real
	 */
	int i;
	Table *tab;

	if ((tab = db->db_SysTable) != NULL) {
	    db->db_SysTable = NULL;
	    CloseTable(tab, 0);
	}
	for (i = 0; i < TAB_HSIZE; ++i) {
	    while ((tab = db->db_TabHash[i]) != NULL) {
		int error = 0;

		DBASSERT(tab->ta_Refs == 0);
		tab->ta_Refs = 1;
		if (tab->ta_Meta)
		    tab->ta_OpenTableMeta(tab, NULL, &error);
		CloseTable(tab, 1);
		DBASSERT(db->db_TabHash[i] != tab);
	    }
	}
	LLFreeSchemaI(&db->db_SchemaICache);

	/*
	 * Misc
	 */
	safe_free(&db->db_DirPath);
	removeNode(&db->db_Node);
	if (db->db_DataLogFd) {
	    close(db->db_DataLogFd);
	    db->db_DataLogFd = -1;
	}

	zfree(db, sizeof(DataBase));
    } else {
	/*
	 * Free to cache.  The system table should be the only one with
	 * a reference.
	 */
	int i;
	Table *tab;

	if ((tab = db->db_SysTable) != NULL) {
	    db->db_SysTable = NULL;
	    CloseTable(tab, 0);
	}
	for (i = 0; i < TAB_HSIZE; ++i) {
	    while ((tab = db->db_TabHash[i]) != NULL) {
		int error = 0;

		DBASSERT(tab->ta_Refs == 0);
		tab->ta_Refs = 1;
		if (tab->ta_Meta)
		    tab->ta_OpenTableMeta(tab, NULL, &error);
		CloseTable(tab, 1);
		DBASSERT(db->db_TabHash[i] != tab);
	    }
	}
    }
}

/*
 * PushDatabase() -	Push a new transaction
 *
 *	Push a new transaction level on top of the current database.  The
 *	new transaction will be frozen at the supplied freeze time stamp.
 *	A negative error code is set and NULL returned on error.
 */

DataBase *
PushDatabase(DataBase *par, dbstamp_t fts, int *error, int flags)
{
    DataBase *db = zalloc(sizeof(DataBase));

    *error = 0;

    if (par->db_PushType == DBPUSH_ROOT) {
	db->db_FreezeTs = fts;
	db->db_Flags = flags & DBF_READONLY;
    } else {
	db->db_FreezeTs = fts = par->db_FreezeTs;
	db->db_Flags = (flags & par->db_Flags) & DBF_READONLY;
    }
    db->db_Pid = par->db_Pid;
    db->db_WriteTs = fts - 1;
    db->db_CommitCheckTs = DBSTAMP_MAX;		/* disable conflict test */
    db->db_DirPath = par->db_DirPath;		/* inherit */
    db->db_Parent = par;
    db->db_RecordedQueryApp = &db->db_RecordedQueryBase;
    db->db_Refs = 1;
    initList(&db->db_List);
    addTail(&par->db_List, &db->db_Node);

    switch(par->db_PushType) {
    case DBPUSH_ROOT:
	/*
	 * Note that the database's representation of the synchronization
	 * timestamp may not be up to date, so the freeze timestamp may 
	 * exceed it.
	 */
	db->db_PushType = DBPUSH_TTS;
#if 0
	if (fts > GetSyncTs(par) && GetSyncTs(par)) {
	    fprintf(stderr, "*** Transaction SyncTS out of bounds: %016qx/%016qx\n",
		fts,
		GetSyncTs(par)
	    );
	}
#endif
	break;
    case DBPUSH_TTS:
	db->db_PushType = DBPUSH_TMP;
	break;
    case DBPUSH_TMP:
	db->db_PushType = DBPUSH_TMP;
	break;
    default:
	DBASSERT(0);
	break;	/* NOT REACHED */
    }

    /*
     * Open the sys table for this transaction (creates an overlay
     * on the real sys table).
     */
    db->db_SysTable = OpenTable(db, "sys", "dt0", NULL, error);

    if (*error != 0) {
	CloseDatabase(db, 1);
	db = NULL;
    } 

    return(db);
}

/*
 * PopDatabase() -	Finished with a transaction.
 *
 *	Pop the database after a commit2 or abort.  It is possible for
 *	there to be parallel nested transactions though this feature is
 *	not currently used.
 */

DataBase * 
PopDatabase(DataBase *db, int *error)
{
    DataBase *par = db->db_Parent;

    DBASSERT(db->db_Refs == 1);
    /*DBASSERT(db->db_Refs >= 1); FUTURE */

    if (par && par->db_PushType != DBPUSH_ROOT)
	par->db_Flags |= db->db_Flags & DBF_METACHANGE;
    if (db->db_Refs == 1) {
	db->db_DirPath = NULL;		/* clear inherited strings */
	CloseDatabase(db, 1);		/* close (and destroy) */
    } else {
	--db->db_Refs;
    }
    return(par);
}

/*
 * OpenTable() -	Open a new reference at the current transactional 
 *			level
 *
 *	Locate/Create the physical table file for the named table in 
 *	the current database.  If we are in a transaction this creates 
 *	an in-memory extension table relative to the parent database's 
 *	table as of the parent database's freeze point.
 *
 *	XXX cache control
 */

__inline int
tableNameHash(const char *name)
{
    int hv = 0;
    while (*name) {
	hv += *(u_int8_t *)name + 1;
	++name;
    }
    return(hv & TAB_HMASK);
}

Table *
OpenTable(DataBase *db, const char *name, const char *ext, DBCreateOptions *dbc, int *error)
{
    Table **pt;
    Table *tab;

    *error = 0;

    /*
     * Attempt to locate the name template, creating one if necessary
     */
    pt = &db->db_TabHash[tableNameHash(name)];

    while ((tab = *pt) != NULL) {
	if (strcmp(name, tab->ta_Name) == 0 &&
	    strcmp(ext, tab->ta_Ext) == 0
	) {
	    if (tab->ta_Refs != 0)
		DBASSERT(tab->ta_Meta != NULL);
	    break;
	}
	pt = &tab->ta_Next;
    }
    if (tab == NULL) {
	if ((tab = zalloc(sizeof(Table))) == NULL) {
	    *error = DBERR_NO_MEM;
	    return(NULL);
	}
	*pt = tab;
	tab->ta_Db = db;
	tab->ta_Name = strdup(name);
	tab->ta_Ext = strdup(ext);
	tab->ta_Fd = -1;
	tab->ta_TTsSlot = -1;
	initList(&tab->ta_BCList);
	initList(&tab->ta_WaitList);

	switch(db->db_PushType) {
	case DBPUSH_ROOT:
	    tab->ta_Ops = &TabFileOps;
	    break;
	case DBPUSH_TTS:
	    tab->ta_Ops = &TabMemOps;
	    break;
	case DBPUSH_TMP:
	    tab->ta_Ops = &TabMemOps;
	    break;
	default:
	    DBASSERT(0);
	    break; /* NOT REACHED */
	}
    }
    return(OpenTableByTab(tab, dbc, error));
}

Table *
OpenTableByTab(Table *tab, DBCreateOptions *dbc, int *error)
{
    /*
     * If the first reference then instantiate the table metadata, which
     * must succeed for the open to succeed.  Otherwise we expect that
     * the table metadata already exists.
     *
     * Note that OpenTableMeta must be called even if it already exists
     * in order for it to properly lock the physical file.
     */
    if (tab->ta_Refs++ == 0) {
	tab->ta_OpenTableMeta(tab, dbc, error);
	if (tab->ta_Meta == NULL) {
	    CloseTable(tab, 1);
	    return(NULL);
	}
    } else {
	DBASSERT(tab->ta_Meta != NULL);
    }

    /*
     * Recache the physical append offset when the table is opened, in
     * case this transaction has a new freeze point.  Note that the newly
     * cached append point is shared by any other transactions using this
     * table, but since it need only be sufficient to cover the data up to
     * the freeze point this adjustment from the perspective of other threads
     * is ok.
     *
     * Note that tab->ta_Append may already be larger then the stored
     * append point due to log file / commit file synchronization sequencing.
     *
     * This lock does not need to be thread-safe
     */
    if (tab->ta_Append != tab->ta_Meta->tf_Append) {
	if (tab->ta_Fd >= 0)
	    hflock_ex(tab->ta_Fd, 4);
	if (tab->ta_Append < tab->ta_Meta->tf_Append)
	    tab->ta_Append = tab->ta_Meta->tf_Append;
	if (tab->ta_Fd >= 0)
	    hflock_un(tab->ta_Fd, 4);
    }

    /*
     * Finish up.  If an error occured, close-up the table, set the
     * error, and return NULL
     */
    if (*error != 0) {
	CloseTable(tab, 1);
	tab = NULL;
    }
    return(tab);
}

/*
 * CloseTable() -	release reference to table on database
 *
 *	On last release the table goes into the cache or, if freeLastClose
 *	is set, is freed up.
 */

void
CloseTable(Table *tab, int freeLastClose)
{
    DBASSERT(tab->ta_Refs != 0);
    if (--tab->ta_Refs != 0)
	return;

    /*
     * Ref count dropped to 0.  Sanity check db_SysTable (there
     * should not be a match).
     */
    DBASSERT((tab->ta_Flags & TAF_HASCHILDREN) == 0);
    DBASSERT(tab->ta_Db->db_SysTable != tab);

    /*
     * if freeLastClose is set, then free the table for real.
     */

    if (freeLastClose) {
	Table **pt;

	DestroyTableCaches(tab);
	if (tab->ta_TTs)
	    DestroyConflictArea(tab);
	if (tab->ta_Meta)
	    tab->ta_CloseTableMeta(tab);

	/*
	 * Unlink table from Db.  Note that shared temporary table space
	 * tables are not named.
	 */
	if (tab->ta_Name) {
	    pt = &tab->ta_Db->db_TabHash[tableNameHash(tab->ta_Name)];
	    while (*pt != tab) {
		DBASSERT(*pt != NULL);
		pt = &(*pt)->ta_Next;
	    }
	    *pt = tab->ta_Next;
	    safe_free(&tab->ta_Name);
	    safe_free(&tab->ta_Ext);
	}

	/*
	 * Free-up
	 */
	safe_free(&tab->ta_FilePath);
	if (tab->ta_Parent) {
	    tab->ta_Parent->ta_Flags &= ~TAF_HASCHILDREN;	/* XXX */
	    CloseTable(tab->ta_Parent, 0);
	    tab->ta_Parent = NULL;
	}

	zfree(tab, sizeof(Table));
    } else {
	if (tab->ta_Meta)
	    tab->ta_CacheTableMeta(tab);
    }
}

void
InitOptionsFromTable(Table *tab, DBCreateOptions *dbc)
{
    bzero(dbc, sizeof(DBCreateOptions));
    dbc->c_BlockSize = tab->ta_BCBlockSize;
    dbc->c_TimeStamp = tab->ta_Meta->tf_CreateStamp;
    dbc->c_Flags |= DBC_OPT_BLKSIZE | DBC_OPT_TIMESTAMP;
}

/*
 * DestroyTableCaches() - physically close all table-supporting 
 * caches, including index caches.
 */
void
DestroyTableCaches(Table *tab)
{
    Index *index;
    DataMap *dm;

    /*
     * Close any cached indexes
     */
    while ((index = tab->ta_IndexBase) != NULL) {
	DBASSERT(index->i_Refs == 0);
	++index->i_Refs;
	CloseIndex(&index, 1);
    }

    /*
     * Release all data cache elements
     */
    while ((dm = getHead(&tab->ta_BCList)) != NULL) {
	dm->dm_Refs &= ~DM_REF_PERSIST;
	DBASSERT(dm->dm_Refs == 0);
	DBASSERT(dm->dm_Table == tab);
	++dm->dm_Refs;
	dm->dm_Table->ta_RelDataMap(&dm, 1);
    }
    DBASSERT(tab->ta_BCCount == 0);
}

/************************************************************************
 *			TABLE MANIPULATION				*
 ************************************************************************/

dbstamp_t
GetDBCreateTs(DataBase *db)
{
    Table *tab = db->db_SysTable;
    return(tab->ta_Meta->tf_CreateStamp);
}

int
GetSysBlockSize(DataBase *db)
{
    return(db->db_SysTable->ta_Meta->tf_BlockSize);
}

/*
 * GetSyncTs() - retrieve the persistent synchronization time stamp
 *
 *	This lock does not need to be thread safe.
 */

dbstamp_t 
GetSyncTs(DataBase *db)
{
    dbstamp_t syncTs;
    Table *tab = db->db_SysTable;

    if (tab->ta_Fd >= 0)
	hflock_ex(tab->ta_Fd, 4);

    syncTs = tab->ta_Meta->tf_SyncStamp;

    if (tab->ta_Fd >= 0)
	hflock_un(tab->ta_Fd, 4);

    return(syncTs);
}

/*
 * GetMinCTs() - retrieve the persistent minimum commit time stamp
 *
 *	This lock does not need to be thread safe.
 */

dbstamp_t 
GetMinCTs(DataBase *db)
{
    dbstamp_t minCTs;
    Table *tab = db->db_SysTable;

    if (tab->ta_Fd >= 0)
	hflock_ex(tab->ta_Fd, 4);

    minCTs = tab->ta_Meta->tf_NextStamp;

    if (tab->ta_Fd >= 0)
	hflock_un(tab->ta_Fd, 4);

    return(minCTs);
}

/*
 * SetSyncTs() -	Set the synchronization timestamp
 *
 *	We only set the sync timestamp if it is larger then the one 
 *	in physical store, because there might be several binaries 
 *	operating directly on the database simultaniously.
 *
 *	This also has the side effect of updating tf_NextStamp (which
 *	generates MinCTs) when necessary
 *
 *	This lock does not need to be thread safe.
 */

dbstamp_t 
SetSyncTs(DataBase *db, dbstamp_t syncTs)
{
    Table *tab = db->db_SysTable;

    if (tab->ta_Fd >= 0)
	hflock_ex(tab->ta_Fd, 4);

    if (syncTs > tab->ta_Meta->tf_SyncStamp) {
	if (syncTs > tab->ta_Meta->tf_NextStamp) {
	    tab->ta_WriteMeta(
		tab, 
		offsetof(TableFile, tf_NextStamp), 
		&syncTs, 
		sizeof(dbstamp_t)
	    );
	}
	tab->ta_WriteMeta(
	    tab, 
	    offsetof(TableFile, tf_SyncStamp), 
	    &syncTs, 
	    sizeof(dbstamp_t)
	);
    }

    if (tab->ta_Fd >= 0)
	hflock_un(tab->ta_Fd, 4);

    return(syncTs);
}

/*
 * SetDatabaseId() - Set the timestamp merge id for this database
 *
 *	This id is used by PEERs within a replication group to negotiate
 *	unique MinCTs (min commit time stamps) in commit phase-1 & 2.
 *	All same-named databases which are designated as PEERs must have
 *	a unique ID.
 */
void 
SetDatabaseId(DataBase *db, dbstamp_t dbid)
{
    db->db_StampId = dbid;
}

/*
 * AllocStamp() - Allocate a new timestamp
 *
 *	The returned timestamp will be greater then any previously
 *	returned minimum commit time stamp and (if this host is a peer)
 *	will be unique over all copies of this particular database.
 *
 *	This lock does not need to be thread safe.
 */

dbstamp_t 
AllocStamp(DataBase *db, dbstamp_t minCTs)
{
    Table *tab = db->db_SysTable;
    dbstamp_t ts;

    if (tab->ta_Fd >= 0)
	hflock_ex(tab->ta_Fd, 4);
    if (minCTs < tab->ta_Meta->tf_NextStamp)
	minCTs = tab->ta_Meta->tf_NextStamp;
    ts = dbstamp(minCTs, db->db_StampId) + DBSTAMP_INCR;
    tab->ta_WriteMeta(tab, offsetof(TableFile, tf_NextStamp), &ts, sizeof(ts));
    ts = ts - DBSTAMP_INCR;
    if (tab->ta_Fd >= 0)
	hflock_un(tab->ta_Fd, 4);
    return(ts);
}

/*
 * SetNextStamp() - set a new high-watermark timestamp for tf_NextStamp
 *
 *	This is typically called by the synchronizer when the timestamp
 *	of the last committed record is not aligned with other copies's
 *	MinCTs's.
 *
 *	This lock does not need to be thread safe.
 */

dbstamp_t
SetNextStamp(DataBase *db, dbstamp_t ts)
{
    Table *tab = db->db_SysTable;

    if (tab->ta_Fd >= 0)
	hflock_ex(tab->ta_Fd, 4);
    if (ts > tab->ta_Meta->tf_NextStamp) {
	tab->ta_WriteMeta(
	    tab, offsetof(TableFile, tf_NextStamp), 
	    &ts, 
	    sizeof(ts)
	);
    } else {
	ts = tab->ta_Meta->tf_NextStamp;
    }
    if (tab->ta_Fd >= 0)
	hflock_un(tab->ta_Fd, 4);
    return(ts);
}

/*
 * CopyGeneration() -	Make wtab a new generation of rtab
 */

void
CopyGeneration(Table *rtab, Table *wtab, dbstamp_t histTs)
{
    TableFile tf = *wtab->ta_Meta;
    tfflags_t flags = rtab->ta_Meta->tf_Flags | TFF_REPLACED;

    fprintf(stderr, "COPY GENERATION %016qx\n", rtab->ta_Meta->tf_Generation);

    tf.tf_HistStamp = histTs;
    tf.tf_SyncStamp = rtab->ta_Meta->tf_SyncStamp;
    tf.tf_NextStamp = rtab->ta_Meta->tf_NextStamp;
    tf.tf_Generation = rtab->ta_Meta->tf_Generation + 1;
    strcpy(tf.tf_Name, rtab->ta_Meta->tf_Name);

    wtab->ta_WriteMeta(wtab, 0, &tf, sizeof(tf));
    if (wtab->ta_FilePath && rtab->ta_FilePath)
	rename(wtab->ta_FilePath, rtab->ta_FilePath);
    if (wtab->ta_Fd >= 0)
	fsync(wtab->ta_Fd);
    rtab->ta_WriteMeta(rtab, offsetof(TableFile, tf_Flags), &flags, sizeof(flags));
    if (rtab->ta_Fd >= 0)
	fsync(rtab->ta_Fd);
}

/*
 * LockTable()/UnLockTable() - master lock of table.
 *
 *	These functions implement a recursive, physical and virtual table lock
 *	that operates between processes as well as between threads.  For the
 *	moment the physical file lock will stall the entire process.  XXX
 */

void
LockTable(Table *tab)
{
    while (tab->ta_LockCnt && tab->ta_LockingTask != curTask()) {
	taskWaitOnList(&tab->ta_WaitList);
    }
    if (tab->ta_LockCnt++ == 0) {
	tab->ta_LockingTask = curTask();
	if (tab->ta_Fd >= 0)
	    hflock_ex(tab->ta_Fd, 8);
    }
    DBASSERT(tab->ta_LockingTask == curTask());
}

void
UnLockTable(Table *tab)
{
    DBASSERT(tab->ta_LockingTask == curTask());
    if (--tab->ta_LockCnt == 0) {
	tab->ta_LockingTask = NULL;
	if (tab->ta_Fd >= 0)
	    hflock_un(tab->ta_Fd, 8);
	taskWakeupList1(&tab->ta_WaitList);
    }
}

/*
 * LockDatabase()/UnLockDatabase() - master lock for database
 *
 *	The database master lock utilizes a db_SysTable-based table lock.
 */

void
LockDatabase(DataBase *db)
{
    LockTable(db->db_SysTable);
}

void
UnLockDatabase(DataBase *db)
{
    UnLockTable(db->db_SysTable);
}

/*
 * setTableRange() - range the whole table and locate the first record
 *
 *	Note that p_IRo is used by the index to track the index position and
 *	should be considered opaque.  Only ti_RanXXX.p_Ro should be used.
 *	Note that ti_RanXXX.p_Ro may skip around the table but is guarenteed
 *	to remain ordered through identical records (e.g. deletions).
 *
 *	Indexed tables may be split if the index is not up to date.  Even
 *	if we are initializing for SLOP we have to call the index's 
 *	table ranging function to calculate the split.
 */

static __inline void
setTableRange(TableI *ti, Table *tab, Range *r, int flags)
{
    if (flags & TABRAN_INIT) {
	DBASSERT(ti->ti_Index == NULL);
	if (r) {
	    col_t colId = (r->r_Col) ? (col_t)r->r_Col->cd_ColId : 0;
	    ti->ti_Index = tab->ta_GetTableIndex(tab, ti->ti_VTable, colId,
						r->r_OpClass);
	}
    }
    if (ti->ti_Index) {
	ti->ti_Flags = flags;
	ti->ti_Index->i_SetTableRange(ti, tab, r->r_Col, r, flags);
    } else {
	flags = (flags & ~TABRAN_INDEX) | TABRAN_SLOP;
	ti->ti_Flags = flags;
	DefaultSetTableRange(ti, tab, NULL, NULL, flags);
    }
}

/*
 * Get{First,Last,Next,Prev}Table() -	scan tables representing pushed 
 *					transactions.
 *
 *	Most queries scan tables from last (most recently pushed) to first,
 *	in order to properly handle table modifications (overrides) that
 *	occur in pushed transactions.  Additionally, each table is abstracted
 *	into an indexed section and a non-indexed section.  The non-indexed
 *	section represents modifications to the table for which the index
 *	does not yet incorporate.
 *
 *	When scanning forwards we scan the indexed section first, then
 *	the non-indexed section.  When scanning backwards we scan the
 *	non-indexed (trailling) section first, then the indexed section.
 */

int 
GetFirstTable(TableI *ti, Range *r)
{
    Table *tab = ti->ti_Table;

    while (tab->ta_Parent)
	tab = tab->ta_Parent;

    setTableRange(ti, tab, r, TABRAN_INDEX|TABRAN_INIT);
    return(0);
}

int 
GetLastTable(TableI *ti, Range *r)
{
    setTableRange(ti, ti->ti_Table, r, TABRAN_SLOP|TABRAN_INIT);
    return(0);
}

int 
GetNextTable(TableI *ti, Range *r)
{
    Table *tab = ti->ti_RanBeg.p_Tab;

    if ((ti->ti_Flags & TABRAN_INDEX) && ti->ti_IndexAppend != ti->ti_Append) {
	setTableRange(ti, ti->ti_Table, r, TABRAN_SLOP);
	return(0);
    }

    CloseIndex(&ti->ti_Index, 0);
    if (tab == ti->ti_Table) {
	ti->ti_RanBeg.p_Tab = NULL;
	ti->ti_RanBeg.p_Ro = -1;
	ti->ti_RanEnd.p_Tab = NULL;
	ti->ti_RanEnd.p_Ro = -1;
	return(-1);
    } else {
	tab = ti->ti_Table;
	while (tab->ta_Parent != ti->ti_RanBeg.p_Tab) {
	    tab = tab->ta_Parent;
	    DBASSERT(tab != NULL);
	}
	setTableRange(ti, tab, r, TABRAN_INDEX|TABRAN_INIT);
	return(0);
    }
}

int 
GetPrevTable(TableI *ti, Range *r)
{
    Table *tab = ti->ti_RanBeg.p_Tab;

    if ((ti->ti_Flags & TABRAN_SLOP) &&
	ti->ti_Index &&
	ti->ti_IndexAppend != tab->ta_FirstBlock(tab)
    ) {
	setTableRange(ti, tab, r, TABRAN_INDEX);
	return(0);
    }
    CloseIndex(&ti->ti_Index, 0);
#if 0
    while (tab != ti->ti_RanBeg.p_Tab)
	tab = tab->ta_Parent;
#endif

    tab = tab->ta_Parent;

    if (tab == NULL) {
	ti->ti_RanBeg.p_Tab = NULL;
	ti->ti_RanBeg.p_Ro = -1;
	ti->ti_RanEnd.p_Tab = NULL;
	ti->ti_RanEnd.p_Ro = -1;
	return(-1);
    } else {
	setTableRange(ti, tab, r, TABRAN_SLOP|TABRAN_INIT);
	return(0);
    }
}

/*
 * GetSpecialTable()
 *
 *	Used to shoehorn a special table into the transaction stack
 *	table scanning code.  Specifically, used to shoehorn in the
 *	special conflict table scan during the phase-1 commit.
 */
void
GetSpecialTable(TableI *ti, Table *tab, Range *r)
{
    setTableRange(ti, tab, r, TABRAN_SLOP|TABRAN_INIT);
}

void
ClearSpecialTable(TableI *ti)
{
    ti->ti_RanBeg.p_Tab = NULL;
    ti->ti_RanBeg.p_Ro = -1;
    ti->ti_RanEnd.p_Tab = NULL;
    ti->ti_RanEnd.p_Ro = -1;
}

/*
 * SelectBegTableRec() - Read record at RanBeg offset into raw data.
 *
 *	Select the table record represented by the positional
 *	information structure (dbpos_t).  rd->rd_Table is the
 *	governing table (e.g. a pushed Mem table if in a transaction),
 *	while p_Tab/p_Ro represent our current scan point and may
 *	cover several tables.
 *
 *	We return p_Ro == -1 on EOF.
 *
 *	Access the record at the specified offset, special casing offsets
 *	such as 0 (beginning of table).  The actual offset of the read
 *	record is returned or -1 if we have reached the end of the table.
 *
 *	The RawData structure contains a linked list of ColData's which
 *	assigned column id's.  The ColData's cd_Data and cd_Bytes fields
 *	are loaded with the contents of the record.  The data is valid
 *	until this RawData is used in another selection.
 *
 *	If flags is 0 we do not populate the rd (not even rd_Rh), but simply
 *	return the record offset.
 */

void
SelectBegTableRec(TableI *ti, int flags)
{
    RawData *rd = ti->ti_RData;

    for (;;) {
	dbpos_t pos2;
	const BlockHead *bh;
	Table *tab = ti->ti_RanBeg.p_Tab;
	int blockSize;
	int boff;

	/*
	 * Already at EOF, or found EOF.
	 */
	if (ti->ti_RanBeg.p_Ro < 0)
	    break;
	if (ti->ti_RanBeg.p_Ro == ti->ti_Append)
	    break;

	/*
	 * Sanity check on offset (note: indexes skip around any records 
	 * beyond ta_Append to satisfy this sanity check).
	 */
	DBASSERT(ti->ti_RanBeg.p_Ro > 0);
	DBASSERT(ti->ti_RanBeg.p_Ro < tab->ta_Append);

	/*
	 * Get the block offset, access the block header and do sanity checks.
	 * If we are in an index, p_Ro must specifically point to a record.
	 * If we are not in an index we allow slop (p_Ro to point to the
	 * base of a block).
	 *
	 * If we aren't an index we have to detect the end-of-block case and
	 * jump to the next block.
	 */
	blockSize = tab->ta_Meta->tf_BlockSize;
	boff = (int)ti->ti_RanBeg.p_Ro & (blockSize - 1);

	pos2 = ti->ti_RanBeg;
	pos2.p_Ro -= boff;
	bh = tab->ta_GetDataMap(&pos2, &rd->rd_Map, blockSize);
	DBASSERT(bh->bh_Magic == BH_MAGIC);

	if (ti->ti_Flags & TABRAN_INDEX) {
	    DBASSERT(ti->ti_Index && ti->ti_Index->i_NextTableRec);
	    DBASSERT(boff != 0);
	} else {
	    if (boff == 0) {
		boff = sizeof(BlockHead);
		ti->ti_RanBeg.p_Ro += boff;
	    }
	    if (((RecHead *)((char *)bh + boff))->rh_Magic == 0) {
		ti->ti_RanBeg.p_Ro = 
		    tab->ta_NextBlock(tab, bh, ti->ti_RanBeg.p_Ro - boff);
		continue;
	    }
	}

	/*
	 * Everything is hunky dory, get the information (if RDF_READ not
	 * set only rd_Rh is filled in).
	 */
	ReadDataRecord(rd, &ti->ti_RanBeg, flags | RDF_ZERO);
	return;
    }
	
    /*
     * EOF or error, derference the data map, set p_Ro to -1
     */
    ti->ti_RanBeg.p_Ro = -1;
    if (rd->rd_Map)
	rd->rd_Map->dm_Table->ta_RelDataMap(&rd->rd_Map, 0);
}

void
SelectEndTableRec(TableI *ti, int flags)
{
    RawData *rd = ti->ti_RData;
    dbpos_t pos2;
    const BlockHead *bh;
    Table *tab = ti->ti_RanEnd.p_Tab;
    int blockSize;
    int boff;

    /*
     * Already at EOF, or found EOF.
     */
    if (ti->ti_RanEnd.p_Ro < 0) {
	if (rd->rd_Map)
	    rd->rd_Map->dm_Table->ta_RelDataMap(&rd->rd_Map, 0);
	return;
    }

    /*
     * Sanity check on offset (note: indexes skip around any records 
     * beyond ta_Append to satisfy this sanity check).
     */
    DBASSERT(ti->ti_RanEnd.p_Ro > 0);
    DBASSERT(ti->ti_RanEnd.p_Ro < tab->ta_Append);

    /*
     * Get the block offset, access the block header and do sanity checks.
     * We must be an index to make this call!
     */
    blockSize = tab->ta_Meta->tf_BlockSize;
    boff = (int)ti->ti_RanEnd.p_Ro & (blockSize - 1);

    pos2 = ti->ti_RanEnd;
    pos2.p_Ro -= boff;
    bh = tab->ta_GetDataMap(&pos2, &rd->rd_Map, blockSize);
    DBASSERT(bh->bh_Magic == BH_MAGIC);
    DBASSERT(boff != 0);

    /*
     * Everything is hunky dory, get the information (if RDF_READ not
     * set only rd_Rh is filled in).
     */
    ReadDataRecord(rd, &ti->ti_RanEnd, flags | RDF_ZERO);
}


/*
 * SelectNextTableRec() -	Return next record
 *
 *	Same as SelectBegTableRec() but obtains and returns the next record.
 *	Unlike SelectBegTableRec(), the ro you pass to this routine MUST be
 *	valid.  It cannot be special cased (e.g. cannot be 0 or -1).
 *
 *	If ti_ScanOneOnly is set, the scan was restricted to the current
 *	record only and we return an immediate EOF.
 */

void
SelectNextTableRec(TableI *ti, int flags)
{
    if (ti->ti_ScanOneOnly > 0 || ti->ti_ScanOneOnly < -1) {
	ti->ti_RanBeg.p_Ro = -1;
	return;
    }
    if (ti->ti_Flags & TABRAN_INDEX) {
	DBASSERT(ti->ti_Index && ti->ti_Index->i_NextTableRec);
	ti->ti_Index->i_NextTableRec(ti);
    } else {
	RawData *rd = ti->ti_RData;
	Table *tab = ti->ti_RanBeg.p_Tab;
	const RecHead *rh;

	/*
	 * Make sure previous record was valid
	 */
	rh = tab->ta_GetDataMap(&ti->ti_RanBeg, &rd->rd_Map, 
	    sizeof(RecHead));
	DBASSERT(rh != NULL);
	DBASSERT(rh->rh_Magic == RHMAGIC);

	/*
	 * Locate the following record, skipping over record and block
	 * boundries .
	 */
	ti->ti_RanBeg.p_Ro += rh->rh_Size;

	while (ti->ti_RanBeg.p_Ro != ti->ti_RanEnd.p_Ro) {
	    int boff;
	    const BlockHead *bh;
	    int blockSize;
	    dbpos_t pos2;

	    DBASSERT(ti->ti_RanBeg.p_Ro < ti->ti_RanEnd.p_Ro);

	    /*
	     * Get the block offset, access the block header and figure out
	     * what to do.
	     */
	    blockSize = tab->ta_Meta->tf_BlockSize;
	    boff = (int)ti->ti_RanBeg.p_Ro & (blockSize - 1);

	    pos2 = ti->ti_RanBeg;
	    pos2.p_Ro -= boff;
	    bh = tab->ta_GetDataMap(&pos2, &rd->rd_Map, blockSize);
	    DBASSERT(bh->bh_Magic == BH_MAGIC);

	    if (boff == 0) {
		boff = sizeof(BlockHead);
		ti->ti_RanBeg.p_Ro += boff;
	    }
	    if (((RecHead *)((char *)bh + boff))->rh_Magic == 0) {
		ti->ti_RanBeg.p_Ro = 
		    tab->ta_NextBlock(tab, bh, ti->ti_RanBeg.p_Ro - boff);
		continue;
	    }
	    break;
	}
	if (ti->ti_RanBeg.p_Ro == ti->ti_RanEnd.p_Ro)
	    ti->ti_RanBeg.p_Ro = -1;
    }
    SelectBegTableRec(ti, flags);
}

/*
 * SelectPrevTableRec() -	Return previous record
 *
 *	(only works on a reverseable index, assert if we are not an index)
 */

void
SelectPrevTableRec(TableI *ti, int flags)
{
    if (ti->ti_ScanOneOnly > 0 || ti->ti_ScanOneOnly < -1) {
	ti->ti_RanEnd.p_Ro = -1;
	return;
    }
    DBASSERT(ti->ti_Flags & TABRAN_INDEX);
    ti->ti_Index->i_PrevTableRec(ti);
    SelectEndTableRec(ti, flags);
}

/*
 * InserTableRec - insert record into table (really just appends it)
 *
 * Appends data to the table.  The rd_Table and rd_Map are ignored in 
 * the source.  The timestamp should be mostly irrelevant because the
 * freeze point is set to DBSTAMP_MAX in a pushed transaction and
 * will be overwritten with the commit stamp when we commit.
 *
 * The record offset of the new record is returned or -1.
 */

int
InsertTableRec(Table *tab, RawData *rd, vtable_t vt)
{
    dbstamp_t ts = tab->ta_Db->db_WriteTs;

    DBASSERT(tab->ta_Parent != NULL);
    WriteDataRecord(tab, rd, NULL, vt, ts, 0, RHF_INSERT);
    return(0);
}

/*
 * UpdateTableRec() -	Delete previous record and insert new record.
 *
 *	Deletes the record specified by oro and appends the new record rd.
 *
 *	The caller is responsible for merging original and updated
 *	data into rdnew.
 */

int
UpdateTableRec(Table *tab, dbpos_t *opos, RawData *rd, vtable_t vt)
{
    dbstamp_t ts = tab->ta_Db->db_WriteTs;

    DBASSERT(ts != 0);
    DBASSERT(opos->p_Ro != 0);
    DBASSERT(tab->ta_Parent != NULL);

    if (SizeDataRecord(rd, NULL, NULL) > GUARENTEED_BLOCKSIZE)
	return(-1);

    DeleteTableRec(tab, opos, vt);
    if (WriteDataRecord(tab, rd, NULL, vt, ts, 0, RHF_UPDATE) < 0)
	DBASSERT(0);
    return(0);
}

/*
 * DeleteTableRec() -	Delete an existing record.
 *
 *   Existing records are deleted by appending a duplicate wit the
 *   RHF_DELETE flag set.  Despite the duplication of data (it's even
 *   worse for UPDATEs), we have to treat the database as append-only
 *   in order for replication and transactions to work without special
 *   cases.  The duplicate data will cause this record to be returned
 *   whenever the original record would be returned, allowing us to cull
 *   the original record.
 */

int
DeleteTableRec(Table *tab, dbpos_t *opos, vtable_t vt)
{
    DataMap *dm = NULL;
    const RecHead *rh;
    dbstamp_t ts = tab->ta_Db->db_WriteTs;

    /*
     * Make sure the record is valid, and we must be in a transaction.
     */
    rh = opos->p_Tab->ta_GetDataMap(opos, &dm, sizeof(RecHead));
    DBASSERT(tab->ta_Parent != NULL);
    DBASSERT(rh != NULL);
    DBASSERT(rh->rh_Magic == RHMAGIC);
    WriteDataRecord(tab, NULL, rh, vt, ts, 0, RHF_DELETE);
    if (dm)
	dm->dm_Table->ta_RelDataMap(&dm, 0);
    return(0);
}

/************************************************************************
 *			TABLE I/O					*
 ************************************************************************/

/*
 * WriteDataRecord - append a new record copying data in the requested column
 *		     range from the supplied RawData structure, also setting
 *		     the record backpointer and flags.
 *
 *	This may have the side effect of extending the file or creating a
 *	new file.  However, we track the append point locally and do not
 *	explicitly update the file's official append point.  Insofar as
 *	internal transactions go, we don't care.  When we commit we update
 *	the official append point prior to unlocking the database.
 *
 *	Returns the record offset of the new record, or -1 on error.
 */

static int
SizeDataRecord(RawData *rd, const RecHead *rh, int *pcount)
{
    int count;
    int bytes;

    /*
     * Calculate the size of the record, either from the RawData structure
     * or from a RecHead template.
     */
    if (rh) {
	count = rh->rh_NCols;
	bytes = rh->rh_Size;
    } else {
	ColData *cd;

	count = 0;
	if (rd) {
	    for (cd = rd->rd_ColBase; cd; cd = cd->cd_Next) {
		if (cd->cd_Data != NULL)
		    ++count;
	    }
	}
	bytes = offsetof(RecHead, rh_Cols[0]) + sizeof(ColHead) * count;

	if (rd) {
	    for (cd = rd->rd_ColBase; cd; cd = cd->cd_Next) {
		if (cd->cd_Data == NULL)
		    continue;
		if (cd->cd_Bytes >= BSIZE_EXT_BASE) /* ch_Bytes field ext */
		    bytes += 4;
		bytes += ALIGN4(cd->cd_Bytes);
	    }
	}
    }
    if (pcount)
	*pcount = count;
    return(bytes);
}

/*
 * WriteDataRecord() - write a data record
 *
 *	Note that when writing a data record, the physical table append point
 *	tf_Append is not updated.  Only the local store tab->ta_Append is
 *	updated.  tf_Append is synchronized as part of the commit 2
 *	logging operation.  The discontinuity also gives us the ability
 *	to rewind failed queries.
 */

dboff_t
WriteDataRecord(
    Table *tab, 
    RawData *rd, 
    const RecHead *rh, 
    vtable_t vt, 
    dbstamp_t ts, 
    rhuser_t userid,
    rhflags_t flags
) {
    int count;
    int bytes;
    dboff_t ro = -1;
    const TableFile *tf = tab->ta_Meta;
    DataMap *dm = NULL;
    ColData *cd;

    bytes = SizeDataRecord(rd, rh, &count);

    /*
     * Set DBF_METACHANGE if the vt represents meta-data.  This will disable
     * the meta-data caches.  We only care about non-root databases.  The
     * flag can get (semi-permanently) set on a root db, but we don't try to
     * cache schema at the root db level anyway so it's irrelevant.
     */
    if ((vt & 3) != 0 || vt < VT_MIN_USER)
	tab->ta_Db->db_Flags |= DBF_METACHANGE;

    /*
     * Maximum record size must fit in a block at the moment.  XXX
     */
    if (bytes > tf->tf_BlockSize - sizeof(BlockHead))
	return(-1);

    /*
     * Figure out where to append.  We may have to close-out the current
     * block, we may have to extend the file.  Since the table is locked
     * the append point will not change out from under us.  We may have to
     * sync the file's real append point to tab->ta_Append but we track
     * commits through tab->ta_Append and do not update tf_Append until
     * the commit completes.
     */
    ro = tab->ta_Append;
    if (ro < tf->tf_Append)
	ro = tf->tf_Append;

    for (;;) {
	int boff;

	/*
	 * Start at append point.
	 */
	DBASSERT(ro <= tf->tf_FileSize);

	/*
	 * If we are at the beginning of the file, skip to the data start.
	 */
	if (ro == 0)
	    ro = tf->tf_DataOff;

	/*
	 * Check if we hit the file EOF
	 */
	if (ro == tf->tf_FileSize) {
	    /*
	     * Extend the file by tf->tf_AppendInc.  ta_ExtendFile()
	     * will fsync() the file allowing us to safely update
	     * tf_AppendInc.
	     */
	    dboff_t nfs;

	    if (tab->ta_ExtendFile(tab, tf->tf_AppendInc) < 0) {
		DBASSERT(0);
		ro = -1;
		break;
	    }
	    nfs = tf->tf_FileSize + tf->tf_AppendInc;
	    tab->ta_WriteMeta(
		tab, 
		offsetof(TableFile, tf_FileSize), 
		&nfs, 
		sizeof(nfs)
	    );
	    continue;
	}

	/*
	 * Check if we are at the beginning of a new block, adjust
	 * the offset, and write the header.
	 */
	boff = ro & (tf->tf_BlockSize - 1);

	if (boff == 0) {
	    BlockHead bh;
	    dbpos_t pos2 = { tab, ro };
	    int r;

	    bzero(&bh, sizeof(bh));
	    bh.bh_Magic = BH_MAGIC;

	    boff = sizeof(BlockHead);
	    ro += boff;

	    r = tab->ta_WriteFile(&pos2, &bh, sizeof(bh));
	    if (r < 0) {
		DBASSERT(0);
		ro = -1;
		break;
	    }
	}

	DBASSERT(boff >= sizeof(BlockHead));

	/*
	 * Check if there's room in the current block and close it out if 
	 * there isn't.  (Note that there MUST be room in the case where we
	 * are sitting at the head of the block).
	 *
	 * A block is closed-out by writing just the first byte of the
	 * RecHead (rh_Magic) as 0
	 */
	if (bytes > tf->tf_BlockSize - boff) {
	    int r;
	    rhmagic_t rhMagic = 0;
	    dbpos_t pos2;

	    pos2.p_Tab = tab;
	    pos2.p_Ro = ro;

	    r = tab->ta_WriteFile(&pos2, &rhMagic, sizeof(rhMagic));
	    if (r < 0) {
		DBASSERT(0);
		ro = -1;
		break;
	    }
	    /*
	     * Go to the next block and retry.
	     */
	    ro += tf->tf_BlockSize - boff;
	    continue;
	}

	/*
	 * Ok, we have room.  Build and write the record, then map and
	 * return it.
	 */
	{
	    RecHead *nrh = zalloc(bytes);
	    int r;

	    nrh->rh_Magic = RHMAGIC;
	    nrh->rh_Flags = flags;
	    nrh->rh_VTableId = vt;
	    nrh->rh_Size = bytes;
	    nrh->rh_Stamp = ts;
	    nrh->rh_NCols = count;
	    nrh->rh_UserId = userid;

	    if (rh) {
		DBASSERT(bytes >= sizeof(RecHead));
		nrh->rh_Hv = rh->rh_Hv;
		bcopy(
		    &rh->rh_Cols[0],
		    &nrh->rh_Cols[0],
		    bytes - offsetof(RecHead, rh_Cols[0])
		);
	    } else {
		int bcount;
		int i;

		bcount = offsetof(RecHead, rh_Cols[0]) + 
		    sizeof(ColHead) * count;

		i = 0;
		if (rd) {
		    for (cd = rd->rd_ColBase; cd; cd = cd->cd_Next) {
			ColHead *ch;

			if (cd->cd_Data == NULL)
			    continue;

			ch = &nrh->rh_Cols[i];
			ch->ch_ColId = (col_t)cd->cd_ColId;

			if (cd->cd_Bytes >= BSIZE_EXT_BASE) {
			    ch->ch_Bytes = BSIZE_EXT_32;
			    bcount += 4;
			} else {
			    ch->ch_Bytes = cd->cd_Bytes;
			}
			bcount += ALIGN4(cd->cd_Bytes);
			++i;
		    }
		}

		bcount = offsetof(RecHead, rh_Cols[0]) + 
		    sizeof(ColHead) * count;

		i = 0;
		if (rd) {
		    for (cd = rd->rd_ColBase; cd; cd = cd->cd_Next) {
			ColHead *ch;

			if (cd->cd_Data == NULL)
			    continue;

			ch = &nrh->rh_Cols[i];
			if (ch->ch_Bytes == BSIZE_EXT_32) {
			    *(int32_t *)((char *)nrh + bcount) = cd->cd_Bytes;
			    bcount += 4;
			}
			if (cd->cd_Bytes)
			    bcopy(cd->cd_Data, ((char *)nrh + bcount), cd->cd_Bytes);
			bcount += ALIGN4(cd->cd_Bytes);
			++i;
		    }
		}
		DBASSERT(bcount == bytes);
		nrh->rh_Hv = rhHash(nrh);
	    }

	    /*
	     * Whew!  Now write it.
	     */
	    {
		dbpos_t pos2 = { tab, ro };
		r = tab->ta_WriteFile(&pos2, nrh, nrh->rh_Size);
	    }
	    zfree(nrh, bytes);

	    if (r < 0) {
		DBASSERT(0);
		ro = -1;
		break;
	    }
	    tab->ta_Append = ro + bytes;
	}
	break;
    }
    if (dm)
	dm->dm_Table->ta_RelDataMap(&dm, 0);
    return(ro);
}

/*
 * RewindDataWrites() -	rewind previously committed writes
 *			(typically only possible on dbmem)
 *
 *	This occurs when a query fails.  tab->ta_Append contains our
 *	locally tracked append point.  Rewind will restore it to a
 *	prior offset before it is synced to the actual file.
 */
void
RewindDataWrites(Table *tab, dboff_t appRo)
{
    tab->ta_Append = appRo;
    RewindTableIndexes(tab);
}

/*
 * ReadDataRecord - Access the map using ro and load rd_Cols[].
 *
 *	Load the supplied RawData structure with a breakdown of the
 *	record pointed to by rd_Table @ ro.
 *
 *	Currently only Backpointers for DELETE records are supported, and
 *	then only if there are no local modifications (which there can't
 *	be for DELETEs anyway).  We do not currently support modified 
 *	update records (i.e. the update records have to have a complete 
 *	copy of the original data).
 *
 *	Returns the next unpopulated column.  Remaining columns are
 *	typically cleared by the caller.
 *
 *	If forceCols is 0, only columns in rd that exist in the record
 *	are populated.  If forceCols is 1, we load the rd columns with
 *	the data from the record as it appears in the record, overriding
 *	any preexisting column id's.
 *
 *	NOTE!  Column id's must be sorted low-to-high both in the physical 
 *	record AND in the rd structure.
 */

ColData *
ReadDataRecord(RawData *rd, dbpos_t *pos, int flags)
{
    ColData **pcd;
    const RecHead *rh;
    int i;
    int offset;

    if (flags & RDF_USERH) {
	rh = rd->rd_Rh;
    } else {
	rh = pos->p_Tab->ta_GetDataMap(pos, &rd->rd_Map, sizeof(RecHead));
	DBASSERT(rh != NULL);
	DBASSERT(rh->rh_Magic == RHMAGIC);
	{
	    int blkSize = rd->rd_Map->dm_Table->ta_Meta->tf_BlockSize;
	    dboff_t ro = pos->p_Ro;
	    DBASSERT(((ro ^ (ro + rh->rh_Size - 1)) & ~((dboff_t)(blkSize - 1))) == 0);
	}
	rd->rd_Rh = rh;
    }

    if ((flags & RDF_READ) == 0)
	return(NULL);

    /*
     * Starting offset of column data
     */
    offset = offsetof(RecHead, rh_Cols[rh->rh_NCols]);

    pcd = &rd->rd_ColBase;

    for (i = 0; i < rh->rh_NCols; ++i) {
	const ColHead *ch = &rh->rh_Cols[i];
	ColData *cd;
	int bytes;

	if (ch->ch_Bytes < BSIZE_EXT_BASE) {
	    bytes = ch->ch_Bytes;
	} else {
	    bytes = *(int32_t *)((char *)rh + offset);
	    offset += 4;
	}
	while ((cd = *pcd) && (col_t)cd->cd_ColId < CID_RAW_LIMIT) {
	    switch((col_t)cd->cd_ColId) {
	    case CID_RAW_VTID:
		cd->cd_Data = (const void *)&rh->rh_VTableId;
		cd->cd_Bytes = sizeof(rh->rh_VTableId);
		break;
	    case CID_RAW_TIMESTAMP:
		cd->cd_Data = (const void *)&rh->rh_Stamp;
		cd->cd_Bytes = sizeof(rh->rh_Stamp);
		break;
	    case CID_RAW_USERID:
		cd->cd_Data = (const void *)&rh->rh_UserId;
		cd->cd_Bytes = sizeof(rh->rh_UserId);
		break;
	    case CID_RAW_OPCODE:
		cd->cd_Data = (const void *)&rh->rh_Flags;
		cd->cd_Bytes = sizeof(rh->rh_Flags);
		break;
	    case CID_COOK_VTID:
		cd->cd_Data = rd->rd_CookVTId;
		cd->cd_Bytes = sizeof(rh->rh_VTableId) * 2;
		snprintf(rd->rd_CookVTId, sizeof(rd->rd_CookVTId), "%04x", (u_int16_t)rh->rh_VTableId);
		break;
	    case CID_COOK_TIMESTAMP:
		cd->cd_Data = rd->rd_CookTimeStamp;
		cd->cd_Bytes = sizeof(rh->rh_Stamp) * 2;
		snprintf(rd->rd_CookTimeStamp, sizeof(rd->rd_CookTimeStamp), "%016qx", (u_int64_t)rh->rh_Stamp);
		break;
	    case CID_COOK_DATESTR:
		{
		    struct tm tp;
		    time_t t = rh->rh_Stamp / 1000000;

		    gmtime_r(&t, &tp);
		    cd->cd_Data = rd->rd_CookTimeStamp;
		    cd->cd_Bytes = 15;	/* yyyymmdd:hhmmss */
		    snprintf(
			rd->rd_CookTimeStamp, 
			sizeof(rd->rd_CookTimeStamp),
			"%04d%02d%02d:%02d%02d%02d",
			tp.tm_year + 1900,
			tp.tm_mon + 1,
			tp.tm_mday,
			tp.tm_hour,
			tp.tm_min,
			tp.tm_sec
		    );
		}
		break;
	    case CID_COOK_USERID:
		cd->cd_Data = rd->rd_CookUserId;
		cd->cd_Bytes = sizeof(rh->rh_UserId) * 2;
		snprintf(rd->rd_CookUserId, sizeof(rd->rd_CookUserId), "%08x", (u_int32_t)rh->rh_UserId);
		break;
	    case CID_COOK_OPCODE:
		cd->cd_Data = rd->rd_CookOpCode;
		cd->cd_Bytes = sizeof(rh->rh_Flags) * 2;
		snprintf(rd->rd_CookOpCode, sizeof(rd->rd_CookOpCode), "%02x", (u_int8_t)rh->rh_Flags);
		break;
	    default:
		cd->cd_Data = NULL;
		cd->cd_Bytes = 0;
		break;
	    }
	    pcd = &cd->cd_Next;
	}
	for (;;) {
	    cd = *pcd;

	    if (cd && (col_t)cd->cd_ColId < ch->ch_ColId) {
		cd->cd_Data = NULL;
		cd->cd_Bytes = 0;
		pcd = &cd->cd_Next;
		continue;
	    }
	    if (cd && ((flags & RDF_FORCE) || 
		(col_t)cd->cd_ColId == ch->ch_ColId)
	    ) {
		cd->cd_ColId = (int)ch->ch_ColId;
		if (bytes)
		    cd->cd_Data = (const char *)rh + offset;
		else
		    cd->cd_Data = "";
		cd->cd_Bytes = bytes;
		pcd = &cd->cd_Next;
	    } else if (flags & RDF_ALLOC) {
		cd = zalloc(sizeof(ColData));
		cd->cd_Next = *pcd;
		*pcd = cd;
		cd->cd_Flags = RDF_ALLOC;
		cd->cd_ColId = (int)ch->ch_ColId;
		if (bytes)
		    cd->cd_Data = (const char *)rh + offset;
		else
		    cd->cd_Data = "";
		cd->cd_Bytes = bytes;
		pcd = &cd->cd_Next;
	    }
	    break;
	}
	offset += ALIGN4(bytes);
    }
    if (flags & RDF_ZERO) {
	ColData *cd;

	while ((cd = *pcd) != NULL) {
	    cd->cd_Data = NULL;
	    cd->cd_Bytes = 0;
	    pcd = &cd->cd_Next;
	}
    }
    return(*pcd);
}

/*
 * MapDataRecord() -	map the raw data record but do not try to interpret
 *			it.  see sync.c
 */
const RecHead *
MapDataRecord(RawData *rd, dbpos_t *pos)
{
    const RecHead *rh;

    rh = pos->p_Tab->ta_GetDataMap(pos, &rd->rd_Map, sizeof(RecHead));

    DBASSERT(rh != NULL);
    DBASSERT(rh->rh_Magic == RHMAGIC);

    {
	int blkSize = rd->rd_Map->dm_Table->ta_Meta->tf_BlockSize;
	dboff_t ro = pos->p_Ro;

	DBASSERT(((ro ^ (ro + rh->rh_Size)) & ~((dboff_t)(blkSize - 1))) == 0);
    }

    rd->rd_Rh = rh;
    return(rh);
}

/*
 * RecordIsValid() - records must be in ocrrect vtable and have correct
 *		     timestamp to take part in a query.
 *
 *	note: this routine may only use ti_RData->rd_Rh.  It may not
 *	make any assumptions in regards to column population.
 */
int 
RecordIsValid(TableI *ti)
{
    const RecHead *rh = ti->ti_RData->rd_Rh;
    DataBase *db = ti->ti_RData->rd_Table->ta_Db;

    /*
     * Ignore records associated with the wrong VTable
     */
    if (ti->ti_VTable && ti->ti_VTable != rh->rh_VTableId)
	return(-1);

    /*
     * Normal validity check.  Skip over any records added after
     * the freeze point or deleted before (or at) the freeze point.
     */
    if (rh->rh_Stamp >= db->db_FreezeTs)
	return(-1);
    return(0);
}

/*
 * RecordIsValidForCommit() - Special check for commit conflict
 *
 *	This routine checks new commit-1's against any commit-2's that
 *	have already occured.  This is different from the commit-1 conflict
 *	tests vs other commit-1's.
 *
 *	note: this routine may only use ti_RData->rd_Rh.  It may not
 *	make any assumptions in regards to column population.
 */
int 
RecordIsValidForCommit(TableI *ti)
{
    const RecHead *rh = ti->ti_RData->rd_Rh;
    DataBase *db = ti->ti_RData->rd_Table->ta_Db;

    /*
     * Check for conflicts.  db_CommitCheckTs is DBSTAMP_MAX until we
     * get to the commit phase-1 check, at which point it is dropped
     * to the freeze point.  If any of our queries access data past
     * our freeze point we set DBF_COMMITFAIL.
     *
     * When setting DBF_COMMITFAIL, we must track the most recent
     * conflicting timestamp which may then be used as the freeze 
     * stamp in a later retry.
     */
    if (rh->rh_Stamp >= db->db_CommitCheckTs) {
	db->db_Flags |= DBF_COMMITFAIL;
	fprintf(stderr, ">>> drd_database (core) Commit1 Fail %016qx\n", rh->rh_Stamp);
	if (db->db_CommitConflictTs < rh->rh_Stamp)
	    db->db_CommitConflictTs = rh->rh_Stamp;
	return(-1);
    }
    return(0);
}

int 
RecordIsValidForCommitTestOnly(TableI *ti)
{
    const RecHead *rh = ti->ti_RData->rd_Rh;
    DataBase *db = ti->ti_RData->rd_Table->ta_Db;

    if (rh->rh_Stamp >= db->db_CommitCheckTs)
	return(-1);
    return(0);
}

