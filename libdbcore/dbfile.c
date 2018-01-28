/*
 * LIBDBCORE/DBFILE.C - implements table file buffer cache methods
 *
 * (c)Copyright 1999-2002 Backplane, Inc.  Please refer to the COPYRIGHT
 * file at the base of the distribution tree.
 *
 * $Backplane: rdbms/libdbcore/dbfile.c,v 1.40 2002/09/27 17:37:11 dillon Exp $
 */

#include "defs.h"

Prototype TableOps	TabFileOps;

void File_OpenTableMeta(Table *tab, DBCreateOptions *dbc, int *error);
void File_CloseTableMeta(Table *tab);
void File_CacheTableMeta(Table *tab);
Index *File_GetTableIndex(Table *tab, vtable_t vt, col_t colId, int opId);
const void *File_GetDataMap(dbpos_t *pos, DataMap **pmap, int bytes);
void File_RelDataMap(DataMap **pdm, int freeLastClose);
int File_WriteFile(dbpos_t *pos, void *ptr, int bytes);
int File_WriteMeta(Table *tab, int off, void *ptr, int bytes);
int File_FSync(Table *tab);
int File_ExtendFile(Table *tab, int bytes);
void File_TruncFile(Table *tab, int bytes);

static void file_TableValidate(Table *tab, off_t fsize, int *error);
static void file_TableCreateFile(Table *tab, DBCreateOptions *dbc, int *error);
static DataMap *file_OpenDataMap(dbpos_t *pos);

TableOps	TabFileOps = {
    File_OpenTableMeta,
    File_CacheTableMeta,
    File_CloseTableMeta,
    File_GetTableIndex,
    File_GetDataMap,
    File_RelDataMap,
    File_WriteFile,
    File_WriteMeta,
    File_FSync,
    File_ExtendFile,
    File_TruncFile,
    Fault_CleanSlate,
    Default_FirstBlock,
    Default_NextBlock
};

/*
 * File_OpenTableMap() -	Reference one of a table's files.
 *
 *	Generate a reference and buffer cache to one of the table's
 *	files.
 *
 *	The table validation hflock does not need to be thread safe.
 */

void
File_OpenTableMeta(Table *tab, DBCreateOptions *dbc, int *error)
{
    int creating;

    /*
     * If we already have metadata, try to revalidate trivially.  If we
     * can't, we have to flush and then reopen the table from scratch.
     */
    if (tab->ta_Meta) {
	/*struct stat st;*/

	DBASSERT((tab->ta_Flags & TAF_METALOCKED) == 0);
	DBASSERT(tab->ta_Fd >= 0);
	tab->ta_Flags |= TAF_METALOCKED;
	if (tab->ta_Db->db_Flags & DBF_EXCLUSIVE)
	    hflock_ex(tab->ta_Fd, 0);
	else
	    hflock_sh(tab->ta_Fd, 0);
	if ((tab->ta_Meta->tf_Flags & TFF_REPLACED) == 0)
	    return;
	DestroyTableCaches(tab);
	File_CloseTableMeta(tab);
    }

    /*
     * Calculate the file path
     */
    if (tab->ta_FilePath == NULL) {
	safe_asprintf(&tab->ta_FilePath, "%s/%s.%s", 
	    tab->ta_Db->db_DirPath, tab->ta_Name, tab->ta_Ext);
    }

    /*
     * If the file is not open then open or create it.  Due to locking
     * races we may have to retry this operation.
     *
     * XXX ta_Name , check for illegal characters (like '/', e.g. random
     * file paths).
     */
    creating = 0;
    while (*error == 0 && tab->ta_Fd < 0) {
	struct stat st1;
	struct stat st2;

	if ((tab->ta_Fd = open(tab->ta_FilePath, O_RDWR, 0660)) < 0) {
	    tab->ta_Fd = open(tab->ta_FilePath, O_CREAT|O_RDWR, 0660);
	    if (tab->ta_Fd < 0) {
		*error = DBERR_CANT_OPEN;
		break;
	    }
	}
	if (fstat(tab->ta_Fd, &st1) < 0) {
	    *error = DBERR_CANT_OPEN;
	    break;
	}

	/*
	 * Lock based on the operation we need to perform.
	 */
	if (creating > 0 || (tab->ta_Db->db_Flags & DBF_EXCLUSIVE))
	    hflock_ex(tab->ta_Fd, 0);
	else
	    hflock_sh(tab->ta_Fd, 0);

	if (stat(tab->ta_FilePath, &st2) == 0 && st1.st_ino == st2.st_ino) {
	    if (creating <= 0) {
		file_TableValidate(tab, st2.st_size, error);
		if (*error != DBERR_TABLE_TRUNC || creating < 0) {
		    tab->ta_Flags |= TAF_METALOCKED;
		    break;
		}
		creating = 1;
		*error = 0;
	    } else {
		file_TableCreateFile(tab, dbc, error);
		creating = -1;
	    }
	}
	hflock_un(tab->ta_Fd, 0);
	close(tab->ta_Fd);
	tab->ta_Fd = -1;
    }

    /*
     * Finish up.  If an error occured, close-up the table map.
     */
    if (*error != 0) {
	File_CloseTableMeta(tab);
    }
}

/*
 * File_CacheTableMeta() - called when ref-count hits 0
 *
 *	This allows the physical table file to be vacuumed or otherwise
 *	munged while we are not using it without us having to flush
 *	our caches.
 */

void
File_CacheTableMeta(Table *tab)
{
    DBASSERT((tab->ta_Flags & TAF_METALOCKED) != 0);
    DBASSERT(tab->ta_Meta != NULL);
    DBASSERT(tab->ta_Fd >= 0);
    tab->ta_Flags &= ~TAF_METALOCKED;
    hflock_un(tab->ta_Fd, 0);
}

/*
 * File_CloseTableMeta() - called to physically close the table meta
 *
 *	The table meta must be in a cached state in order to be able
 *	to close it.  We must fsync the table descriptor so the data
 *	log can assume it has been fsync'd if it finds the table closed.
 */

void
File_CloseTableMeta(Table *tab)
{
    if (tab->ta_Flags & TAF_METALOCKED) {
	DBASSERT(tab->ta_Fd >= 0);
	hflock_un(tab->ta_Fd, 0);
	tab->ta_Flags &= ~TAF_METALOCKED;
    }
    if (tab->ta_Meta) {
	munmap((void *)tab->ta_Meta,
		(sizeof(TableFile) + DbPgMask) & ~DbPgMask);
	tab->ta_Meta = NULL;
    }
    if (tab->ta_Fd >= 0) {
	fsync(tab->ta_Fd);
	close(tab->ta_Fd);
	tab->ta_Fd = -1;
    }
    tab->ta_Append = 0;
}

/*
 * file_TableValidate() - map and validate a table, creating if appropriate.
 *
 *	Nobody else is accessing the table file during the validation
 *	procedure, so we don't have to lock the metadata.
 */

static void
file_TableValidate(Table *tab, off_t fsize, int *error)
{
    TableFile tf;
    int n;

    lseek(tab->ta_Fd, 0, 0);
    bzero(&tf, sizeof(tf));			/* if truncated read */
    n = read(tab->ta_Fd, &tf, sizeof(tf));	/* try to read header */

    /*
     * Basic validation (not reverse order to avoid else's)
     */
    if (n == sizeof(tf)) {
	if ((tf.tf_Flags & TFF_CREATED) == 0)
	    *error = DBERR_TABLE_TRUNC;

	if (tf.tf_DataOff < sizeof(tf) ||
	    tf.tf_Append < tf.tf_DataOff ||
	    tf.tf_BlockSize < MIN_BLOCKSIZE ||
	    tf.tf_BlockSize > MAX_BLOCKSIZE ||
	    tf.tf_AppendInc < tf.tf_BlockSize ||
	    (tf.tf_BlockSize ^ (tf.tf_BlockSize - 1)) !=	/* pwr of 2*/
		((tf.tf_BlockSize << 1) - 1) ||
	    (tf.tf_AppendInc ^ (tf.tf_AppendInc - 1)) !=
		((tf.tf_AppendInc << 1) - 1) ||
	    tf.tf_HeadSize < sizeof(tf) ||
	    tf.tf_HeadSize > DbPgSize ||
	    ((int)tf.tf_DataOff & (tf.tf_BlockSize - 1)) != 0	/* unaligned */
	) {
		DBASSERT(0); /* XXX */
		sleep(99);
	    *error = DBERR_TABLE_CORRUPT;
	}
	if (tf.tf_DataOff < sizeof(tf)) {
		DBASSERT(0); /* XXX */
		sleep(99);
	    *error = DBERR_TABLE_CORRUPT;
	}
	if (tf.tf_Blk.bh_Type != BH_TYPE_TABLE)
	    *error = DBERR_TABLE_TYPE;
	if (tf.tf_Version != TF_VERSION)
	    *error = DBERR_TABLE_VERSION;
	if (tf.tf_Blk.bh_Magic != BH_MAGIC_TABLE)
	    *error = DBERR_TABLE_MAGIC;
    } else {
	*error = DBERR_TABLE_TRUNC;
    }

    /*
     * If no error then memory map the header
     */
    if (*error == 0) {
	tab->ta_Meta = mmap(
			    NULL, 
			    (sizeof(TableFile) + DbPgMask) & ~DbPgMask, 
			    PROT_READ, 
			    MAP_SHARED, 
			    tab->ta_Fd, 
			    0
			);
	if (tab->ta_Meta == MAP_FAILED) {
	    tab->ta_Meta = NULL;
	    *error = DBERR_TABLE_MMAP;
	} 
    }
    if (*error == 0) {
	tab->ta_BCBlockSize = MIN_DATAMAP_BLOCK;
	if (tab->ta_BCBlockSize < tab->ta_Meta->tf_BlockSize)
	    tab->ta_BCBlockSize = tab->ta_Meta->tf_BlockSize;
    }

    /*
     * XXX Perform table recovery (or maybe we should do this when we
     * open the database at a higher level ??  What about SYSTEM?)
     */
}

/*
 * file_TableCreateFile() - Create a new table file
 *
 *	Populate the passed TableFile scratch structure and then
 *	write it to the TableMap.
 */

static void
file_TableCreateFile(Table *tab, DBCreateOptions *dbc, int *error)
{
    char *zbuf = safe_malloc(ZBUF_SIZE);
    off_t off;
    TableFile tf;
    dbstamp_t createTs;
    int blkSize;

    if (dbc && (dbc->c_Flags & DBC_OPT_TIMESTAMP))
	createTs = dbc->c_TimeStamp;
    else
	createTs = 0;

    /*
     * Create with the specified block size.  If not specified use the
     * system table's blocksize.  If we still don't know then use the
     * default block size.
     */
    if (dbc && (dbc->c_Flags & DBC_OPT_BLKSIZE))
	blkSize = dbc->c_BlockSize;
    else if (tab->ta_Db->db_SysTable && tab->ta_Db->db_SysTable->ta_Meta)
	blkSize = tab->ta_Db->db_SysTable->ta_Meta->tf_BlockSize;
    else
	blkSize = FILE_BLOCKSIZE;

    bzero(zbuf, ZBUF_SIZE);
    bzero(&tf, sizeof(TableFile));
    tf.tf_Blk.bh_Magic = BH_MAGIC_TABLE;
    tf.tf_Blk.bh_Type = BH_TYPE_TABLE;
    tf.tf_Version = TF_VERSION;
    tf.tf_HeadSize = sizeof(TableFile);
    tf.tf_BlockSize = blkSize;
    if (tf.tf_BlockSize < MIN_APPEND_BLOCKSIZE)
	tf.tf_AppendInc = MIN_APPEND_BLOCKSIZE;
    else
	tf.tf_AppendInc = tf.tf_BlockSize;
    tf.tf_DataOff = tf.tf_BlockSize;
    tf.tf_Append = tf.tf_DataOff;	/* note base of block */
    tf.tf_FileSize = tf.tf_DataOff + tf.tf_BlockSize;
    tf.tf_Generation = dbstamp(0, 0);
    tf.tf_CreateStamp = createTs;

    if (tf.tf_FileSize < ZBUF_SIZE)
	tf.tf_FileSize = ZBUF_SIZE;

    /*
     * Write header
     */
    lseek(tab->ta_Fd, 0, 0);
    ftruncate(tab->ta_Fd, 0);
    if (write(tab->ta_Fd, &tf, tf.tf_HeadSize) != tf.tf_HeadSize)
	*error = DBERR_TABLE_WRITE;

    /*
     * Zero fill to FileSize, try to align the writes
     */
    {
	int n = ZBUF_SIZE - tf.tf_HeadSize;
	if (*error == 0 && write(tab->ta_Fd, zbuf, n) != n)
	    *error = DBERR_TABLE_WRITE;
    }

    off = ZBUF_SIZE;
    while (*error == 0 && off < tf.tf_FileSize) {
	int n;

	if (tf.tf_FileSize - off < ZBUF_SIZE)
	    n = (int)(tf.tf_FileSize - off);
	else
	    n = ZBUF_SIZE;
	if (write(tab->ta_Fd, zbuf, n) != n)
	    *error = DBERR_TABLE_WRITE;
	else
	    off += n;
    }
#if 0
    /*
     * Create block header for blocks.
     */
    for (off = tf.tf_DataOff; off < tf.tf_FileSize; off += tf.tf_BlockSize) {
	BlockHead bh = { 0 };

	bh.bh_Magic = BH_MAGIC;
	bh.bh_Type = BH_TYPE_FREE;
	bh.bh_StartOff = sizeof(bh);
	/* bh_EndOff must be 0, indicating that the blocks are open */

	lseek(tab->ta_Fd, off, 0);
	if (write(tab->ta_Fd, &bh, sizeof(bh)) != sizeof(bh)) {
	    *error = DBERR_TABLE_WRITE;
	    break;
	}
    }
#endif

    /*
     * Set validation bits
     */

    if (*error == 0) {
	if (fsync(tab->ta_Fd) != 0) {
	    *error = DBERR_TABLE_WRITE;
	} else {
	    tfflags_t v = TFF_CREATED|TFF_VALID;

	    lseek(tab->ta_Fd, offsetof(TableFile, tf_Flags), 0);
	    if (write(tab->ta_Fd, &v, sizeof(v)) != sizeof(v))
		*error = DBERR_TABLE_WRITE;
	    if (fsync(tab->ta_Fd) != 0) {
		v = 0;
		lseek(tab->ta_Fd, offsetof(TableFile, tf_Flags), 0);
		write(tab->ta_Fd, &v, sizeof(v));
		*error = DBERR_TABLE_WRITE;
	    }
	}
    }
    free(zbuf);
    /* XXX delete the file if an error occured? */
}

Index *
File_GetTableIndex(Table *tab, vtable_t vt, col_t colId, int opId)
{
    return(OpenIndex(tab, vt, colId, opId, OpenBTreeIndex));
}

/************************************************************************
 *				DATA MAPS 				*
 ************************************************************************
 *
 *	This implements our buffer cache.
 *
 *	GetDataMap() retrieves a datamap, reusing the existing datamap
 *	if possible and closing it otherwise.  Note that the existing
 *	datamap might belong to another table so be careful.
 *
 *	XXX make sure we do not run out of VM space
 */

const void *
File_GetDataMap(dbpos_t *pos, DataMap **pmap, int bytes)
{
    DataMap *dm;

    for (;;) {
	if ((dm = *pmap) != NULL) {
	    int bcBlockSize = dm->dm_Table->ta_BCBlockSize;
	    int bcBlockMask = bcBlockSize - 1;

	    if (dm->dm_Table == pos->p_Tab &&
		((pos->p_Ro ^ dm->dm_Ro) & ~(dboff_t)bcBlockMask) == 0
	    ) {
		if (bytes) {
		    DBASSERT (((pos->p_Ro ^ (pos->p_Ro + bytes - 1)) & 
			~(dboff_t)bcBlockMask) == 0);
		}
		return (dm->dm_Base + ((int)pos->p_Ro & bcBlockMask));
	    }
	    dm->dm_Table->ta_RelDataMap(pmap, 0);
	}
	if ((*pmap = file_OpenDataMap(pos)) == NULL) {
	    /* XXX OpenDataMap failed (not reached, cores) */
	    break;
	}
    }
    return(NULL);
}

void
File_RelDataMap(DataMap **pdm, int freeLastClose)
{
    DataMap *dm;
    Table *tab;
    int hi;

#ifdef MEMDEBUG
    freeLastClose = 2;
#endif

    if ((dm = *pdm) == NULL)
	return;
    tab = dm->dm_Table;
    *pdm = NULL;

    DBASSERT(tab->ta_Ops == &TabFileOps);
    DBASSERT(dm->dm_Refs != 0);

    hi = (dm->dm_Ro / tab->ta_BCBlockSize) & DM_HMASK;

    if (--dm->dm_Refs == 0 && freeLastClose) {
	pdm = &BCHash[hi];
	while (*pdm != dm) {
	    DBASSERT(*pdm != NULL);
	    pdm = &(*pdm)->dm_HNext;
	}
	*pdm = dm->dm_HNext;
	dm->dm_HNext = NULL;
	removeNode(&dm->dm_Node);
	--tab->ta_BCCount;
	BCMemoryUsed -= tab->ta_BCBlockSize;
	if (dm->dm_Base != MAP_FAILED) {
	    munmap((void *)dm->dm_Base, tab->ta_BCBlockSize);
	    dm->dm_Base = MAP_FAILED;
	    dm->dm_Ro = -1;
	}
	zfree(dm, sizeof(DataMap));
    }
}

DataMap *
file_OpenDataMap(dbpos_t *pos)
{
    Table *tab = pos->p_Tab;
    DataMap *dm;
    DataMap **pdm;
    DataMap **preuse = NULL;
    int hi = (pos->p_Ro / tab->ta_BCBlockSize) & DM_HMASK;
    dboff_t roBase;

    /*
     * Locate the TableMap
     */
    roBase = pos->p_Ro & ~((dboff_t)tab->ta_BCBlockSize - 1);

    /*
     * Look for dm or find suitable structure to reuse
     */
    pdm = &BCHash[hi];
    while ((dm = *pdm) != NULL) {
	if (dm->dm_Table == tab) {
	    if (dm->dm_Ro == roBase)
		break;
	    if (dm->dm_Base == MAP_FAILED && dm->dm_Refs == 0)
		preuse = pdm;
	}
	pdm = &dm->dm_HNext;
    }

    /*
     * If dm not found, reuse a suitable structure
     */
    if (dm == NULL && preuse) {
	dm = *preuse;
	pdm = preuse;
	dm->dm_Ro = roBase;
    }

    /*
     * If dm valid, move it to the head of the list, else allocate it.
     */
    if (dm) {
	if (BCHash[hi] != dm) {
	    *pdm = dm->dm_HNext;
	    dm->dm_HNext = BCHash[hi];
	    BCHash[hi] = dm;
	}
    } else {
	dm = zalloc(sizeof(DataMap));
	dm->dm_HNext = BCHash[hi];
	BCHash[hi] = dm;
	addTail(&tab->ta_BCList, &dm->dm_Node);
	++tab->ta_BCCount;
	BCMemoryUsed += tab->ta_BCBlockSize;

	dm->dm_Base = MAP_FAILED;
	dm->dm_Table = tab;
	dm->dm_Ro = roBase;
    }

    /*
     * Bump the refs, mmap the block
     */
    ++dm->dm_Refs;
    if (dm->dm_Base == MAP_FAILED) {
	DBASSERT(dm->dm_Refs == 1);
	dm->dm_Base = mmap(NULL, tab->ta_BCBlockSize, PROT_READ, MAP_SHARED,
	    tab->ta_Fd, roBase & ~((dboff_t)tab->ta_BCBlockSize - 1));
	if (dm->dm_Base == MAP_FAILED) {
	    File_RelDataMap(&dm, 1);
	    fprintf(stderr, "mmap() failed %s\n", strerror(errno));
	    DBASSERT(0);
	}
    }
    if (BCMemoryUsed > BCMemoryLimit)
	DataMapGarbageCollect();
    return(dm);
}

int
File_WriteFile(dbpos_t *pos, void *ptr, int bytes)
{
    Table *tab = pos->p_Tab;

    lseek(tab->ta_Fd, pos->p_Ro, 0);
    return(write(tab->ta_Fd, ptr, bytes));
}

int
File_WriteMeta(Table *tab, int off, void *ptr, int bytes)
{
    int r;

    lseek(tab->ta_Fd, off, 0);
    if ((r = write(tab->ta_Fd, ptr, bytes)) != bytes) {
	DBASSERT(0);
    }
    return(r);
}

int
File_FSync(Table *tab)
{
    fsync(tab->ta_Fd);
    return(0);	/* XXX */
}

/*
 * File_ExtendFile() - extend the size of a file
 *
 *	This can only be called while we hold an exclusive lock on 
 *	the database.  The caller will update the file header so we
 *	have to fsync when we do this as well.
 */

int
File_ExtendFile(Table *tab, int bytes)
{
    void *buf = zalloc(tab->ta_Meta->tf_BlockSize);
    int error = 0;

    lseek(tab->ta_Fd, tab->ta_Meta->tf_FileSize, 0);
    while (bytes > 0) {
	if (write(tab->ta_Fd, buf, tab->ta_Meta->tf_BlockSize) != tab->ta_Meta->tf_BlockSize) {
	    bytes = 0;
	    error = -1;
	    break;
	}
	bytes -= tab->ta_Meta->tf_BlockSize;
    }
    DBASSERT(bytes == 0);
    fsync(tab->ta_Fd);
    zfree(buf, tab->ta_Meta->tf_BlockSize);
    return(error);
}

void
File_TruncFile(Table *tab, int bytes)
{
    ftruncate(tab->ta_Fd, tab->ta_Meta->tf_FileSize);
}

