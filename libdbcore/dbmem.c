/*
 * LIBDBCORE/DBMEM.C 	- Implement memory-resident tables
 *
 * (c)Copyright 1999-2002 Backplane, Inc.  Please refer to the COPYRIGHT
 * file at the base of the distribution tree.
 *
 * $Backplane: rdbms/libdbcore/dbmem.c,v 1.35 2002/10/03 18:21:59 dillon Exp $
 */

#include "defs.h"

Prototype TableOps TabMemOps;

void Mem_OpenTableMeta(Table *tab, DBCreateOptions *dbc, int *error);
void Mem_CacheTableMeta(Table *tab);
void Mem_CloseTableMeta(Table *tab);
Index *Mem_GetTableIndex(Table *tab, vtable_t vt, col_t colId, int opId);
const void *Mem_GetDataMap(dbpos_t *pos, DataMap **pmap, int bytes);
void Mem_RelDataMap(DataMap **pdm, int freeLastClose);
int Mem_WriteFile(dbpos_t *pos, void *ptr, int bytes);
int Mem_WriteMeta(Table *tab, int off, void *ptr, int bytes);
int Mem_FSync(Table *tab);
int Mem_ExtendFile(Table *tab, int bytes);
void Mem_TruncFile(Table *tab, int bytes);
void Mem_CleanSlate(Table *tab);

DataMap *mem_OpenDataMap(Table *tab, dboff_t ro);

TableOps	TabMemOps = {
    Mem_OpenTableMeta,
    Mem_CacheTableMeta,
    Mem_CloseTableMeta,
    Mem_GetTableIndex,
    Mem_GetDataMap,
    Mem_RelDataMap,
    Mem_WriteFile,
    Mem_WriteMeta,
    Mem_FSync,
    Mem_ExtendFile,
    Mem_TruncFile,
    Mem_CleanSlate,
    Default_FirstBlock,
    Default_NextBlock
};

#define MF_MAX_CACHE	16

typedef struct MFNode {
    Node	mf_Node;
    const void	*mf_Base;
    int		mf_Bytes;
} MFNode;

List	MemFreeList = INITLIST(MemFreeList);
int	MemFreeCount;

/*
 * Mem_OpenTableMap() -	Reference one of a table's files.
 *
 *	Generate a reference and buffer cache to one of the table's
 *	files.
 */

void
Mem_OpenTableMeta(Table *tab, DBCreateOptions *dbc, int *error)
{
    dbstamp_t createTs;
    int blkSize;

    if (tab->ta_Meta)
	return;

    if (dbc && (dbc->c_Flags & DBC_OPT_TIMESTAMP))
	createTs = dbc->c_TimeStamp;
    else
	createTs = 0;

    tab->ta_Meta = zalloc(sizeof(TableFile));
    tab->ta_Parent = OpenTable(tab->ta_Db->db_Parent, tab->ta_Name, 
			    tab->ta_Ext, dbc, error);

    /*
     * Calculate the file path for temporary table space.  Anything beyond
     * the first BC block (typically 1MB for memory tables) will be backed
     * by a real file.  The file will be open()/reused().
     *
     * Note that the first data block for a memory table is set at
     * offset tf_BlockSize, so the buffer cache block size must be at
     * least 2x that to avoid creating a backing file for 'most' 
     * transactions.
     */
    safe_asprintf(&tab->ta_FilePath, "%s/%s.%d.tmp",
	tab->ta_Db->db_DirPath,
	tab->ta_Name,
	(int)tab->ta_Db->db_Pid
    );

    blkSize = tab->ta_Parent->ta_Meta->tf_BlockSize;
    if (blkSize < MIN_MEM_BLOCKSIZE)
	blkSize = MIN_MEM_BLOCKSIZE;

    if (*error == 0) {
	TableFile *tf = (TableFile *)tab->ta_Meta;

	tf->tf_Flags = TFF_CREATED | TFF_VALID;
	tf->tf_Blk.bh_Magic = BH_MAGIC_TABLE;
	tf->tf_Blk.bh_Type = BH_TYPE_TABLE;
	tf->tf_Version = TF_VERSION;
	tf->tf_HeadSize = sizeof(TableFile);
	tf->tf_BlockSize = blkSize;
	tf->tf_AppendInc = tf->tf_BlockSize;
	tf->tf_CreateStamp = createTs;

	tab->ta_BCBlockSize = MIN_DATAMAP_BLOCK;
	if (tab->ta_BCBlockSize < tf->tf_BlockSize * 2)
	    tab->ta_BCBlockSize = tf->tf_BlockSize * 2;

	Mem_CleanSlate(tab);
    } else {
	Mem_CloseTableMeta(tab);
    }
}

void
Mem_CacheTableMeta(Table *tab)
{
    /* empty */
}

void
Mem_CloseTableMeta(Table *tab)
{
    if (tab->ta_Parent) {
	CloseTable(tab->ta_Parent, 0);
	tab->ta_Parent = NULL;
    }
    if (tab->ta_Meta) {
	zfree((void *)tab->ta_Meta, sizeof(TableFile));
	tab->ta_Meta = NULL;
    }
    if (tab->ta_Fd >= 0) {
	close(tab->ta_Fd);
	tab->ta_Fd = -1;
    }
}

/*
 * Mem_GetTableIndex() - get the supporting index for the table
 *
 *	The supporting index is based on the column and operator.  For
 *	temporary tables used with read-only queries we do not need an
 *	index at all.   For R/W queries we generally do not need an
 *	index but if doing something like updating thousands of records
 *	we would be screwed without one, so we get one.
 */
Index *
Mem_GetTableIndex(Table *tab, vtable_t vt, col_t colId, int opId)
{
    if (tab->ta_Db->db_Flags & DBF_READONLY)
	return(NULL);
    else
	return(OpenIndex(tab, vt, colId, opId, OpenBTreeIndex));
#if 0
	return(OpenIndex(tab, vt, colId, opId, NULL)); /* instead of NULL */
#endif
}

const void *
Mem_GetDataMap(dbpos_t *pos, DataMap **pmap, int bytes)
{
    DataMap *dm;
    Table *tab = pos->p_Tab;

    for (;;) {
	if ((dm = *pmap) != NULL) {
	    int bcBlockSize = dm->dm_Table->ta_BCBlockSize;
	    int bcBlockMask = bcBlockSize - 1;

	    if (dm->dm_Table == tab &&
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
	if ((*pmap = mem_OpenDataMap(tab, pos->p_Ro)) == NULL) {
	    /* XXX OpenDataMap failed */
	    break;
	}
    }
    return(NULL);
}

/*
 * Mem_RelDataMap()
 *
 *	Note that there is no backing store for memory file buffer cache
 *	blocks, so we do not do anything to them if freeLastClose is 0
 *	or 1.  We only destroy them if freeLastClose is 2.  XXX we need
 *	overflow backing store just as we have for memory indexes.
 *
 *	Note that when the datamap is destroyed by DestroyTableCaches()
 *	DM_REF_PERSIST will be cleared, so it cannot be used to differentiate
 *	between malloc()'d and mmap()'d space.
 */
void
Mem_RelDataMap(DataMap **pdm, int freeLastClose)
{
    DataMap *dm;
    Table *tab;
    int hi;

    if ((dm = *pdm) == NULL)
	return;
    tab = dm->dm_Table;
    *pdm = NULL;

    DBASSERT(tab->ta_Ops == &TabMemOps);
    DBASSERT((dm->dm_Refs & ~DM_REF_PERSIST) != 0);

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
	BCMemoryUsed -= tab->ta_BCBlockSize;
	--tab->ta_BCCount;

	if (dm->dm_Base != NULL) {
	    if (dm->dm_Ro == 0) {
		MFNode *mf = zalloc(sizeof(MFNode));
		addHead(&MemFreeList, &mf->mf_Node);
		mf->mf_Base = dm->dm_Base;
		mf->mf_Bytes = tab->ta_BCBlockSize;
		++MemFreeCount;
		while (MemFreeCount > MF_MAX_CACHE) {
		    mf = remTail(&MemFreeList);
		    safe_free((char **)&mf->mf_Base);
		    zfree(mf, sizeof(MFNode));
		    --MemFreeCount;
		}
	    } else {
		munmap((void *)dm->dm_Base, tab->ta_BCBlockSize);
	    }
	    dm->dm_Base = NULL;
	    dm->dm_Ro = -1;
	}
	zfree(dm, sizeof(DataMap));
    }
}

DataMap *
mem_OpenDataMap(Table *tab, dboff_t ro)
{
    DataMap *dm;
    DataMap **pdm;
    DataMap **preuse = NULL;
    int hi = (ro / tab->ta_BCBlockSize) & DM_HMASK;
    dboff_t roBase;

    /*
     * Locate the TableMap
     */
    roBase = ro & ~((dboff_t)tab->ta_BCBlockSize - 1);

    /*
     * Look for dm or find suitable structure to reuse
     */
    pdm = &BCHash[hi];
    while ((dm = *pdm) != NULL) {
	if (dm->dm_Table == tab) {
	    if (dm->dm_Ro == roBase)
		break;
	    if (dm->dm_Base == NULL && dm->dm_Refs == 0)
		preuse = pdm;
	}
	pdm = &dm->dm_HNext;
    }

    /*
     * If dm not found, reuse a suitable structure.  We are likely
     * reassigning dm_Ro so set DM_REF_PERSIST properly.  Note that 
     * due to setting DM_REF_PERSIST on the first block the only type
     * of dm we can actually reuse is one without persist (i.e. Refs == 0).
     */
    if (dm == NULL && preuse) {
	dm = *preuse;
	pdm = preuse;
	dm->dm_Ro = roBase;
	if (roBase == 0)
	    dm->dm_Refs |= DM_REF_PERSIST;
    }

    /*
     * If dm valid, move it to the head of the list, else allocate it.
     * Memory data maps are persistent, so set DM_REF_PERSIST to prevent
     * the GC code from freeing the map.
     */
    if (dm) {
	if (BCHash[hi] != dm) {
	    *pdm = dm->dm_HNext;
	    dm->dm_HNext = BCHash[hi];
	    BCHash[hi] = dm;
	}
    } else {
	dm = zalloc(sizeof(DataMap));
	if (roBase == 0)
	    dm->dm_Refs |= DM_REF_PERSIST;
	dm->dm_HNext = BCHash[hi];
	BCHash[hi] = dm;
	addTail(&tab->ta_BCList, &dm->dm_Node);
	BCMemoryUsed += tab->ta_BCBlockSize;
	++tab->ta_BCCount;
	dm->dm_Base = NULL;
	dm->dm_Table = tab;
	dm->dm_Ro = roBase;
    }

    /*
     * Bump the refs and map it.  The first data block uses malloc (to
     * avoid unnecessary zeroing), successive data blocks will be file
     * backed.  A data  block is typically 1MB so this can be significant.
     */
    ++dm->dm_Refs;
    if (dm->dm_Base == NULL) {
	MFNode *mf;

	if (roBase == 0) {
	    DBASSERT((dm->dm_Refs & ~DM_REF_PERSIST) == 1);

	    if ((mf = remHead(&MemFreeList)) != NULL) {
		DBASSERT(mf->mf_Bytes == tab->ta_BCBlockSize);
		dm->dm_Base = mf->mf_Base;
		--MemFreeCount;
		mf->mf_Base = NULL;
		zfree(mf, sizeof(MFNode));
	    } else {
		dm->dm_Base = safe_malloc(tab->ta_BCBlockSize);
	    }
	} else {
	    DBASSERT(dm->dm_Refs == 1 && tab->ta_Fd >= 0);

	    dm->dm_Base = mmap(NULL, tab->ta_BCBlockSize, PROT_READ,
				MAP_SHARED, tab->ta_Fd,
				roBase & ~((dboff_t)tab->ta_BCBlockSize - 1));
	    if (dm->dm_Base == MAP_FAILED) {
		dm->dm_Base = NULL;
		Mem_RelDataMap(&dm, 1);
		fprintf(stderr, "mmap() failed %s\n", strerror(errno));
		DBASSERT(0);
	    }
	}
    }
    if (BCMemoryUsed > BCMemoryLimit)
	DataMapGarbageCollect();
    return(dm);
}

int
Mem_WriteFile(dbpos_t *pos, void *ptr, int bytes)
{
    DataMap *dm = NULL;
    const void *data;
    Table *tab = pos->p_Tab;

    if (pos->p_Ro < tab->ta_BCBlockSize) {
	DBASSERT(pos->p_Ro + bytes <= tab->ta_BCBlockSize);
	data = Mem_GetDataMap(pos, &dm, bytes);
	bcopy(ptr, (void *)data, bytes);
	Mem_RelDataMap(&dm, 0);
    } else {
	DBASSERT(tab->ta_Fd >= 0);
	lseek(tab->ta_Fd, pos->p_Ro, 0);
	bytes = write(tab->ta_Fd, ptr, bytes);
    }
    return(bytes);
}

int
Mem_WriteMeta(Table *tab, int off, void *ptr, int bytes)
{
    DBASSERT(off >= 0 && bytes >= 0 && off + bytes <= sizeof(TableFile));
    bcopy(ptr, (char *)tab->ta_Meta + off, bytes);
    return(bytes);
}

int
Mem_FSync(Table *tab)
{
    return(0);
}

/*
 * ExtendFile() - extend a temporary memory table.
 *
 *	If the table exceeds one buffer cache block we have to create
 *	real backing store for it (though the first block will always
 *	remain in memory).  Allow multiple threads to reuse the namespace
 *	by removing the file.
 */
int
Mem_ExtendFile(Table *tab, int bytes)
{
    if (tab->ta_Meta->tf_FileSize + bytes > tab->ta_BCBlockSize) {
	if (tab->ta_Fd < 0) {
	    DBASSERT(strstr(tab->ta_FilePath, ".tmp") != NULL);
	    tab->ta_Fd = open(tab->ta_FilePath, O_RDWR|O_CREAT|O_TRUNC, 0660);
	    DBASSERT(tab->ta_Fd >= 0);
	    remove(tab->ta_FilePath);
	}
	ftruncate(tab->ta_Fd, tab->ta_Meta->tf_FileSize + bytes);
    }
    return(0);
}

void
Mem_TruncFile(Table *tab, int bytes)
{
    DBASSERT(0);
}

void 
Mem_CleanSlate(Table *tab)
{
    TableFile *tf = (void *)tab->ta_Meta;
    dboff_t off;

    /*
     * Note that tf_DataOff is offset by a whole block.
     */
    DBASSERT(tab->ta_Ops == &TabMemOps);
    off = tf->tf_BlockSize;

    tf->tf_DataOff = off;
    tf->tf_Append = tf->tf_DataOff;
    tf->tf_FileSize = tf->tf_DataOff + tf->tf_BlockSize;
    tab->ta_Append = tf->tf_Append;
    RewindTableIndexes(tab);

#if 0
    pos.p_Tab = tab;

    for (
	pos.p_Ro = off; 
	pos.p_Ro < tf->tf_FileSize; 
	pos.p_Ro += tf->tf_BlockSize
    ) {
	BlockHead *bh;

	bh = (void *)Mem_GetDataMap(&pos, &dm, sizeof(BlockHead));
	bzero(bh, sizeof(BlockHead));
	bh->bh_Magic = BH_MAGIC;
	bh->bh_Type = BH_TYPE_FREE;
	bh->bh_StartOff = sizeof(BlockHead);
    }
    Mem_RelDataMap(&dm, 1);
#endif
}

