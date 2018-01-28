/*
 * LIBDBCORE/BTREE.C	- Implement B+Tree indexing
 *
 * (c)Copyright 2000-2002 Backplane, Inc.  Please refer to the COPYRIGHT
 * file at the base of the distribution tree.
 *
 * $Backplane: rdbms/libdbcore/btree.c,v 1.61 2002/12/08 19:58:34 dillon Exp $
 */

#include "defs.h"
#include "btree.h"

Prototype void OpenBTreeIndex(Index *index);

static void CloseBTreeIndex(Index *index);
static void BTreeSetTableRange(TableI *ti, Table *tab, const ColData *colData, Range *r, int flags);
static void BTreeUpdateIndex(TableI *ti, Table *tab, const ColData *colData);
static void BTreeUpdateTableRange(TableI *ti, Range *r);
static void BTreeNextTableRec(TableI *ti);
static void BTreePrevTableRec(TableI *ti);
static int BTreeCacheCheck(TableI *ti, Index *index, BTreeElm *cmp, dbpos_t *bpos, dbpos_t *bbeg, dbpos_t *bend);
static int BTreeFindBoundsFwd(TableI *ti, Index *index, BTreeElm *cmp, dbpos_t *bpos);
static int BTreeFindBoundsRev(TableI *ti, Index *index, BTreeElm *cmp, dbpos_t *bpos);

static const BTreeNode *btreeRead(Index *index, IndexMap **pim, dboff_t bnro, int *elm);
static const BTreeNode *btreeReadReScan(Index *index, IndexMap **pin, dbpos_t *bpos, int *elm);
static dboff_t btreeReadOffset(Index *index, dboff_t bnro);
static int btreeCompare(Index *index, const BTreeElm *b1, const BTreeElm *b2);
static int btreeCompareSearchFwd(Index *index, const BTreeNode *bn, int elm, BTreeElm *cmp);
static int btreeCompareSearchRev(Index *index, const BTreeNode *bn, int elm, BTreeElm *cmp);
static int btreeInsert(TableI *ti, Index *index, dboff_t bnro, BTreeElm *be, dboff_t *appro, int flags);
static void btreeSplit(Index *index, dboff_t bnro, const BTreeNode *bn, int i, BTreeElm *be, dboff_t *appro, int flags);
static void btreeInsertPhys(Index *index, dboff_t bnro, const BTreeNode *bn, int i, BTreeElm *be, dboff_t *appro, int flags);
static dboff_t btreeAppend(Index *index, BTreeNode *bn, dboff_t *appro);
static const void *btreeGetIndexMap(Index *index, IndexMap **pmap, dboff_t ro, int bytes);
static void btreeRelIndexMap(IndexMap **pmap, int freeLastClose);
static void btreeCachePurge(void);
static void btreeSynchronize(Index *index);
static void btreeUnSynchronize(Index *index);
static int btreeIndexWrite(Index *index, off_t off, void *data, int bytes);
static void btreeIndexFSync(Index *index);

static IndexMap *BTreeIndexAry[BTREE_HSIZE];
static int BTreeIndexCount;
static int BTreePurgeIndex;

/*
 * OpenBTreeIndex() -	open/create a btree-based index for a table vt & colid
 *
 *	Note that the locking functions go straight to the descriptor.  There
 *	is no need to make them thread-cooperative.
 */

void
OpenBTreeIndex(Index *index)
{
    Table *tab = index->i_Table;
    char *p1;
    BTreeHead bt;
    BTreeNode bn;
    int error = 0;
    int tempOpt = 0;

    safe_asprintf(&p1, tab->ta_Name, 0);
    safe_asprintf(&index->i_FilePath, "%s/%s.vt%04x.i%04x.o%02x", 
	tab->ta_Db->db_DirPath, 
	p1,
	index->i_VTable,
	index->i_ColId,
	index->i_OpClass
    );
    safe_free(&p1);

    /*
     * If this a temporary table, create a temporary file (reuse the
     * namespace, there could be many threads using the same file name)
     */
    if (tab->ta_Db->db_PushType != DBPUSH_ROOT) {
	safe_replacef(
	    &index->i_FilePath, 
	    "%s.%d.tmp",
	    index->i_FilePath,
	    (int)tab->ta_Db->db_Pid
	);
	tempOpt = 1;
    }

    while (error == 0) {
	int deleteMe = 0;
	struct stat st;

	/*
	 * open/create the index file.  Creation for temporary indexes
	 * is defered (we almost never have to actually create a file)
	 */
	if (tempOpt == 0) {
	    index->i_Fd = open(index->i_FilePath, O_RDWR|O_CREAT, 0660);
	    if (index->i_Fd < 0) {
		error = -1;
		break;
	    }

	    /*
	     * Fasttrack validation
	     */
	    if (read(index->i_Fd, &bt, sizeof(bt)) == sizeof(bt)) {
		if (bt.bt_Magic == BT_MAGIC &&
		    bt.bt_Version == BT_VERSION &&
		    bt.bt_Generation == tab->ta_Meta->tf_Generation
		) {
		    break;
		}
	    }

	    /*
	     * Get exclusive lock, try to validate again.  If we cannot 
	     * validate we may have to delete/recreate the index file.
	     *
	     * NOTE: The lack of BTF_SYNCED does not invalidate the file, as 
	     * there may be multiple processes accessing the file.  The reboot
	     * code (see 'drecover') is responsible for testing for BTF_SYNCED
	     * and removing the file.
	     */
	    hflock_ex(index->i_Fd, 0);
	    lseek(index->i_Fd, 0L, 0);
	    if (read(index->i_Fd, &bt, sizeof(bt)) == sizeof(bt)) {
		if (bt.bt_Magic == BT_MAGIC &&
		    bt.bt_Version == BT_VERSION &&
		    bt.bt_Generation == tab->ta_Meta->tf_Generation
		) {
		    hflock_un(index->i_Fd, 0);
		    break;
		}
		deleteMe = 1;
	    }

	    if (fstat(index->i_Fd, &st) < 0) {
		hflock_un(index->i_Fd, 0);
		error = -1;
		break;
	    } 

	    /*
	     * If the file was unlinked while we were obtaining the lock (only
	     * applies to hard index files, not temporary index files), then
	     * we have to try again.
	     */
	    if (st.st_nlink == 0) {
		hflock_un(index->i_Fd, 0);
		close(index->i_Fd);
		continue;
	    }
	} else {
	    index->i_Fd = -1;
	}

	/*
	 * File is unusable, we have to delete it and reaquire/recreate
	 */
	if (deleteMe) {			/* invalid index file, delete */
	    DBASSERT(tempOpt == 0);
	    remove(index->i_FilePath);
	    hflock_un(index->i_Fd, 0);
	    if (index->i_Fd >= 0) {
		close(index->i_Fd);
		index->i_Fd = -1;
	    }
	    continue;
	}

	/*
	 * Initial size
	 */
	if (tempOpt == 0)
	    ftruncate(index->i_Fd, BT_CACHESIZE);

	/*
	 * Creating new index file, setup the header
	 */
	bzero(&bt, sizeof(bt));
	bt.bt_Magic = BT_MAGIC;
	bt.bt_Version = 0;			/* operation in progress */
	bt.bt_HeadSize = sizeof(BTreeHead);
	bt.bt_Root = ALIGN128(sizeof(BTreeHead));
	bt.bt_Append = bt.bt_Root + sizeof(BTreeNode);
	bt.bt_ExtAppend = BT_CACHESIZE;
	bt.bt_TabAppend = tab->ta_FirstBlock(tab);
	bt.bt_FirstElm = 0;
	bt.bt_LastElm = 0;
	bt.bt_Flags = 0;
	bt.bt_Generation = tab->ta_Meta->tf_Generation;
	if (tempOpt)
	    bt.bt_Flags |= BTF_TEMP;

	/*
	 * Write the header (for temporary tables we copy the header
	 * a little later on)
	 */
	if (tempOpt == 0) {
	    lseek(index->i_Fd, 0, 0);
	    if (write(index->i_Fd, &bt, sizeof(bt)) != sizeof(bt)) {
		error = -1;
		ftruncate(index->i_Fd, 0);
		hflock_un(index->i_Fd, 0);
		break;
	    }
	}

	/*
	 * Write the root record.  The root record starts out life as
	 * a leaf with no entries in it.
	 */
	bzero(&bn, sizeof(bn));
	bn.bn_Count = 0;
	bn.bn_Flags |= BNF_LEAF;

	if (tempOpt == 0) {
	    lseek(index->i_Fd, bt.bt_Root, 0);
	    if (write(index->i_Fd, &bn, sizeof(bn)) != sizeof(bn)) {
		error = -1;
		ftruncate(index->i_Fd, 0);
		hflock_un(index->i_Fd, 0);
		break;
	    }
	    /*
	     * Lock-in changes prior to validating
	     */
	    fsync(index->i_Fd);
	}

	/*
	 * Update bt_Version, bt_Flags, validating the index file.
	 * We do not have to fsync() here, see the BTF_SYNCED handling
	 * later on for the algorithm.
	 */
	bt.bt_Version = BT_VERSION;
	bt.bt_Flags |= BTF_SYNCED;

	if (tempOpt == 0) {
	    const int vsize = sizeof(bt.bt_Version);
	    lseek(index->i_Fd, offsetof(BTreeHead, bt_Version), 0);
	    if (write(index->i_Fd, &bt.bt_Version, vsize) != vsize) {
		error = -1;
		ftruncate(index->i_Fd, 0);
		hflock_un(index->i_Fd, 0);
		break;
	    }
	    /*
	     * The index file is now valid, unlock and break out
	     */
	    hflock_un(index->i_Fd, 0);
	}
	break;
    }

    /*
     * Map or allocate the header
     */
    if (error == 0) {
	if (tempOpt) {
	    DBASSERT(bt.bt_Flags & BTF_TEMP);
	    DBASSERT(sizeof(bt) < BT_CACHESIZE);
	    index->i_BTreeHead = safe_malloc(BT_CACHESIZE);
	    bcopy(&bt, (void *)index->i_BTreeHead, sizeof(bt));
	    bcopy(&bn, (char *)index->i_BTreeHead + bt.bt_Root, sizeof(bn));
	} else {
	    DBASSERT((bt.bt_Flags & BTF_TEMP) == 0);
	    index->i_BTreeHead = mmap(
			    NULL, 
			    sizeof(BTreeHead), 
			    PROT_READ,
			    MAP_SHARED,
			    index->i_Fd,
			    0
			);
	    if (index->i_BTreeHead == MAP_FAILED) {
		index->i_BTreeHead = NULL;
		error = -1;
	    } 
	}
    }

    /*
     * Cleanup after the hard work is done.  Note that
     * BTree indexes can scan in reverse, so we can use
     * the more optimal DefaultIndexScanRangeOp2() function
     * which tracks deletions in a single pass.
     */
    if (error == 0) {
	initList(&index->i_BTreeCacheList);
	index->i_ScanRangeOp = DefaultIndexScanRangeOp2;
	index->i_SetTableRange = BTreeSetTableRange;
	index->i_UpdateTableRange = BTreeUpdateTableRange;
	index->i_NextTableRec = BTreeNextTableRec;
	index->i_PrevTableRec = BTreePrevTableRec;
	index->i_Close = CloseBTreeIndex;
	index->i_PosCache.p_IRo = (dboff_t)-1;
    } else {
	CloseBTreeIndex(index);
    }
}

static void
CloseBTreeIndex(Index *index)
{
    IndexMap *im;
    const BTreeHead *bt;

    while ((im = getHead(&index->i_BTreeCacheList)) != NULL) {
	DBASSERT(im->im_Refs == 0);
	im->im_Refs = 1;
	btreeRelIndexMap(&im, 1);
    }
    bt = index->i_BTreeHead;

    if ((bt->bt_Flags & BTF_TEMP) == 0)
	t_flock_ex(&index->i_FLock);
    btreeSynchronize(index);
    if ((bt->bt_Flags & BTF_TEMP) == 0)
	t_flock_un(&index->i_FLock);

    if (index->i_BTreeHead != NULL) {
	if (index->i_BTreeHead->bt_Flags & BTF_TEMP)
	    free((void *)index->i_BTreeHead);
	else
	    munmap((void *)index->i_BTreeHead, sizeof(BTreeHead));
	index->i_BTreeHead = NULL;
    }
    if (index->i_Fd >= 0) {
	close(index->i_Fd);
	index->i_Fd = -1;
    }
    safe_free(&index->i_FilePath);
}

/*
 * BTreeSetTableRange()	- update index if necessary and set indexed range
 *
 *	r may be NULL, indicating that the full indexed range should be
 *	returned (verses trying to restrict the range based on the
 *	expression tree).
 */

static void
BTreeSetTableRange(TableI *ti, Table *tab, const ColData *colData, Range *r, int flags)
{
    Index *index = ti->ti_Index;
    const BTreeHead *bt = index->i_BTreeHead;

    /*
     * Synchronize the index, record the maximum table offset for the purpose
     * of this query (remember, Table is shared so we need our own ti_*Append),
     * and set up the initial table range.  Note that the index may not be
     * synchronized to the point we need to scan through.
     *
     * We have to partition the table at INIT into an INDEXed area and a
     * SLOP (not yet indexed) area, then setup acording to the requested
     * mode.
     *
     * XXX unsafe access to bt_TabAppend!
     */
    DBASSERT(ti->ti_ScanOneOnly <= 0);

    if (flags & TABRAN_INIT) {
	if ((flags & TABRAN_SYNCIDX) && (bt->bt_TabAppend != tab->ta_Append))
	    BTreeUpdateIndex(ti, tab, colData);
	else if (bt->bt_TabAppend + BT_SLOP < tab->ta_Append)
	    BTreeUpdateIndex(ti, tab, colData);
	ti->ti_Append = tab->ta_Append;
	ti->ti_IndexAppend = bt->bt_TabAppend;
    }
    if (flags & TABRAN_SLOP) {
	DefaultSetTableRange(ti, tab, colData, r, flags & TABRAN_SLOP);
	return;
    }

    /*
     * Setup the range
     */
    ti->ti_RanBeg.p_Tab = tab;
    ti->ti_RanEnd.p_Tab = tab;
    ti->ti_ScanRangeOp = index->i_ScanRangeOp;

    if (bt->bt_FirstElm) {
	DBASSERT(bt->bt_LastElm);
	ti->ti_RanBeg.p_IRo = bt->bt_FirstElm;
	ti->ti_RanBeg.p_Ro = btreeReadOffset(index, bt->bt_FirstElm);
	ti->ti_RanEnd.p_IRo = bt->bt_LastElm;
	ti->ti_RanEnd.p_Ro = btreeReadOffset(index, bt->bt_LastElm);

	BTreeUpdateTableRange(ti, r);	/* r may be NULL */
	SelectBegTableRec(ti, 0); 	/* to check degenerate EOF only */
    } else {
	ti->ti_RanBeg.p_Ro = -1;
	ti->ti_RanEnd.p_Ro = -1;
    }
}

/*
 * BTreeUpdateIndex() -	Update index file by adding new records from the table
 *
 *	We obtain the update lock for the index while updating.  The lock
 *	must be thread-cooperative since the select may switch away from
 *	the thread.
 */

static void
BTreeUpdateIndex(TableI *ti, Table *tab, const ColData *colData)
{
    Index *index = ti->ti_Index;
    const BTreeHead *bt = index->i_BTreeHead;
    dboff_t appro;
    int oflags;

    if ((bt->bt_Flags & BTF_TEMP) == 0)
	t_flock_ex(&index->i_FLock);
    if (bt->bt_TabAppend >= tab->ta_Append) {
	if ((bt->bt_Flags & BTF_TEMP) == 0)
	    t_flock_un(&index->i_FLock);
	return;
    }

    /*
     * We need to update the index, temporarily unsynchronize it
     */
    btreeUnSynchronize(index);

    /*
     * Temporarily remove the index reference so we can scan the physical
     * table sequentially.  Locate the first record to scan.
     */
    oflags = ti->ti_Flags;
    ti->ti_Index = NULL;
    ti->ti_Flags = TABRAN_SLOP;
    DefaultSetTableRange(ti, tab, colData, NULL, TABRAN_SLOP|TABRAN_INIT);

    appro = bt->bt_Append;
    if (bt->bt_TabAppend != 0) {
	ti->ti_RanBeg.p_Ro = bt->bt_TabAppend;
	SelectBegTableRec(ti, 0);
    }

    /*
     * Scan physical table records
     */
    while (ti->ti_RanBeg.p_Ro > 0) {
	const RecHead *rh;

	ReadDataRecord(ti->ti_RData, &ti->ti_RanBeg, RDF_READ|RDF_ZERO);
	rh = ti->ti_RData->rd_Rh;

	/*
	 * Insert an element into the tree.  0 is returned on success,
	 * -1 if a split occured (and 'be' will contain the new node
	 * we have to add).  btreeAppend() is told to retarget both
	 * element's bn_Parent fields to the new record.
	 *
	 * we skip elements that do not belong to the requested vtable.
	 */
	if (index->i_VTable == 0 || index->i_VTable == rh->rh_VTableId) {
	    BTreeElm be;
	    /*
	     * Construct temporary BTreeElm to hold the database reference
	     * being added.
	     */
	    bzero(&be, sizeof(be));
	    be.be_Ro = ti->ti_RanBeg.p_Ro;
	    if (rh->rh_Flags & RHF_DELETE)
		be.be_Flags |= BEF_DELETED;
	    if (colData->cd_Bytes > BT_DATALEN) 
		be.be_Len = BT_DATALEN;
	    else
		be.be_Len = colData->cd_Bytes;
	    bcopy(colData->cd_Data, be.be_Data, be.be_Len);

	    if (btreeInsert(ti, index, bt->bt_Root, &be, &appro, BIF_FIRST|BIF_LAST) < 0) {
		BTreeNode bn;
		dboff_t res;

		bzero(&bn, sizeof(bn));
		bn.bn_Count = 2;
		bn.bn_Elms[0].be_Ro = bt->bt_Root;
		bn.bn_Elms[0].be_Flags = 0;
		bn.bn_Elms[1] = be;
		res = btreeAppend(index, &bn, &appro);
		btreeIndexWrite(
		    index,
		    offsetof(BTreeHead, bt_Root),
		    &res,
		    sizeof(dboff_t)
		);
	    }
	}

	/*
	 * Don't let indexing completely take over the
	 * process, but try to be reasonably efficient.
	 */
	taskQuantum();
	SelectNextTableRec(ti, 0);
    }
    ti->ti_Index = index;
    ti->ti_Flags = oflags;
    btreeIndexWrite(
	index,
	offsetof(BTreeHead, bt_TabAppend),
	&ti->ti_RanEnd.p_Ro,
	sizeof(dboff_t)
    );
    btreeIndexWrite(
	index,
	offsetof(BTreeHead, bt_Append),
	&appro,
	sizeof(dboff_t)
    );
    btreeSynchronize(index);
    if ((bt->bt_Flags & BTF_TEMP) == 0)
	t_flock_un(&index->i_FLock);
}

/*
 * BTreeUpdateTableRange() - restrict an already-indexed range
 *
 *	Given an indexed table instance and range, attempt to restrict
 *	the existing index with the range.  RanBeg is set on call, RanEnd 
 *	may or may not be set (may be -1, see above) on call.
 *
 *	We attempt to further optimize the range by looking at other AND
 *	clauses for the same column.
 *
 *	This is optional code.
 */

static void
BTreeUpdateTableRange(TableI *ti, Range *r)
{
    Range *rBase = r;
    Index *itmp;

    DBASSERT(ti->ti_ScanOneOnly <= 0);

    /*
     * Only constant expressions
     */
    for (; r && ti->ti_RanBeg.p_Ro >= 0; r = r->r_NextSame) {
	dbpos_t bbeg = ti->ti_RanBeg;
	dbpos_t bend = ti->ti_RanEnd;
	BTreeElm be;
	int foundFwd;
	int foundRev;

	/*
	 * We can accept any CONST, but we can only use
	 * a JCONST if it is the base of the range for
	 * this table instance (otherwise the constant hasn't been 
	 * resolved yet).
	 */
	if (r->r_Type == ROP_JCONST) {
	    if (r != rBase)
		continue;
	} else if (r->r_Type != ROP_CONST) {
	    continue;
	}
	if ((col_t)r->r_Col->cd_ColId != ti->ti_Index->i_ColId ||
	    r->r_OpClass != ti->ti_Index->i_OpClass
	) {
	    continue;
	}

	/*
	 * We cannot optimize the range for most special __* columns
	 * (note that __timestamp >[=] 'blah' is optimized)
	 */
	if (r->r_Flags & RF_FORCESAVE)
	    continue;

	/*
	 * Construct BTreeElm out of range constant
	 */
	bzero(&be, sizeof(be));
	be.be_Ro = 0;
	if (r->r_Const->cd_Bytes > BT_DATALEN) 
	    be.be_Len = BT_DATALEN;
	else
	    be.be_Len = r->r_Const->cd_Bytes;
	bcopy(r->r_Const->cd_Data, be.be_Data, be.be_Len);

	/*
	 * Restrict range
	 */
	switch(r->r_OpId) {
	case ROP_LTEQ:
	case ROP_LT:
	case ROP_STAMP_LTEQ:
	case ROP_STAMP_LT:
	    /*
	     * index is not precise enough to differentiate
	     */
	    foundRev = BTreeFindBoundsRev(ti, ti->ti_Index, &be, &bend);
	    if (foundRev >= 0) {
		ti->ti_RanEnd = bend;
	    } else {
		ti->ti_RanBeg.p_Ro = -1;
		ti->ti_RanEnd.p_Ro = -1;
	    }
	    break;
	case ROP_GT:
	case ROP_GTEQ:
	case ROP_STAMP_GT:
	case ROP_STAMP_GTEQ:
	    /*
	     * index is not precise enough to differentiate
	     */
	    foundFwd = BTreeFindBoundsFwd(ti, ti->ti_Index, &be, &bbeg);
	    if (foundFwd >= 0)  {
		ti->ti_RanBeg = bbeg;
	    } else {
		ti->ti_RanBeg.p_Ro = -1;
		ti->ti_RanEnd.p_Ro = -1;
	    }
	    break;
	case ROP_NOTEQ:
	    /* not supported */
	    break;
	case ROP_LIKE:
	case ROP_RLIKE:
	    foundFwd = BTreeFindBoundsFwd(ti, ti->ti_Index, &be, &bbeg);
	    if (be.be_Len && be.be_Len < BT_DATALEN) {
		unsigned char c = tolower(be.be_Data[be.be_Len-1]);
		if (c == 0xFF) {
		    while (be.be_Len < BT_DATALEN)
			be.be_Data[be.be_Len++] = 0xFF;
		} else {
		    be.be_Data[be.be_Len-1] = c + 1;
		}
	    }
	    foundRev = BTreeFindBoundsRev(ti, ti->ti_Index, &be, &bend);

	    /*
	     * We must find something for both searches or they passed
	     * each other.  They can also pass each other even if they
	     * both find something so we have to check the case specifically.
	     */
	    if (foundFwd >= 0 && foundRev >= 0) {
		IndexMap *im1 = NULL;
		IndexMap *im2 = NULL;
		const BTreeNode *bn1;
		const BTreeNode *bn2;
		int elm1;
		int elm2;

		bn1 = btreeRead(ti->ti_Index, &im1, bbeg.p_IRo, &elm1);
		bn2 = btreeRead(ti->ti_Index, &im2, bend.p_IRo, &elm2);
		if (btreeCompare(ti->ti_Index, &bn1->bn_Elms[elm1], &bn2->bn_Elms[elm2]) > 0) {
		    ti->ti_RanBeg.p_Ro = -1;
		    ti->ti_RanEnd.p_Ro = -1;
		} else {
		    ti->ti_RanBeg = bbeg;
		    ti->ti_RanEnd = bend;
		}
		btreeRelIndexMap(&im1, 0);
		btreeRelIndexMap(&im2, 0);
	    } else {
		ti->ti_RanBeg.p_Ro = -1;
		ti->ti_RanEnd.p_Ro = -1;
	    }
	    break;
	case ROP_EQEQ:
	case ROP_SAME:
	case ROP_RSAME:
	case ROP_STAMP_EQEQ:
	case ROP_VTID_EQEQ:
	case ROP_USERID_EQEQ:
	case ROP_OPCODE_EQEQ:
	    /*
	     * Locate the first and last matching element.  Optimize the
	     * JCONST case if possible, that is the b.key case for
	     * the clause 'a.key = b.key'.  If index->i_PosCache is
	     * valid from a prior search run a comparison.  Since
	     * index scans go backwards, if the cached element is
	     * greater then the constant <be> it can replace bend
	     * and we can do a reverse search to locate bbeg.
	     *
	     * BTreeCacheCheck() returns -1 on failure, 0 on partial success,
	     * 1 on complete cache-case success (the elements were found
	     * fully enclosed in the btree leaf).  bbeg and bend are not
	     * modified on failure.
	     */
	    itmp = ti->ti_Index;
	    if (r->r_Type == ROP_JCONST &&
		itmp->i_PosCache.p_IRo != (dboff_t)-1 &&
		bbeg.p_IRo == itmp->i_BTreeHead->bt_FirstElm &&
		bend.p_IRo == itmp->i_BTreeHead->bt_LastElm
	    ) {
		if (BTreeCacheCheck(ti, itmp, &be, &itmp->i_PosCache, &bbeg, &bend) > 0) {
		    ti->ti_RanBeg = bbeg;
		    ti->ti_RanEnd = bend;
		    break;
		}
	    }

	    /*
	     * We couldn't use the cache, do it from scratch
	     */
	    foundFwd = BTreeFindBoundsFwd(ti, ti->ti_Index, &be, &bbeg);
	    if (foundFwd == 0) {
		foundRev = BTreeFindBoundsRev(ti, ti->ti_Index, &be, &bend);
		DBASSERT(foundRev == 0);
		ti->ti_RanBeg = bbeg;
		ti->ti_RanEnd = bend;
	    } else {
		ti->ti_RanBeg.p_Ro = -1;
		ti->ti_RanEnd.p_Ro = -1;
	    }
	    break;
	default:
	    DBASSERT(0);
	    /* not reached */
	}
    }

    /*
     * skip any records that exceed our currently allowed table bounds.
     */
    if (ti->ti_RanBeg.p_Ro >= ti->ti_IndexAppend)
	BTreeNextTableRec(ti);
    if (ti->ti_RanEnd.p_Ro >= ti->ti_IndexAppend)
	BTreePrevTableRec(ti);
}

/*
 * BTreeNextTableRec()
 *
 *	note:	The index-specific next-table-rec does not have to check
 *		for ti_ScanOneOnly.
 *
 */

static void
BTreeNextTableRec(TableI *ti)
{
    const BTreeNode *bn;
    IndexMap *im = NULL;
    int elm;

again:
    if (ti->ti_RanBeg.p_Ro == ti->ti_RanEnd.p_Ro) {
	ti->ti_Index->i_PosCache = ti->ti_RanEnd;
	ti->ti_RanBeg.p_Ro = -1;
	btreeRelIndexMap(&im, 0);
	return;
    }
    /*
     * Access node, locate next element.  If we hit the end of the array,
     * recurse up (which will result in an equal number of down recursions
     * later).  Note that bn_Parent embeds the element index relative to
     * the parent, and btreeRead() loads it into elm.
     *
     * If we recurse all the way to the root and there is no next element,
     * we are through scanning.
     */
    bn = btreeReadReScan(ti->ti_Index, &im, &ti->ti_RanBeg, &elm);

    while (++elm == bn->bn_Count) {
	if (bn->bn_Parent == 0) {
#if 0
	    printf("****** Did not find end node %08qx\n", ti->ti_RanEnd.p_IRo);
#endif
	    ti->ti_RanBeg.p_Ro = -1;
	    btreeRelIndexMap(&im, 0);
	    return;
	}
	bn = btreeRead(ti->ti_Index, &im, bn->bn_Parent, &elm);
    }

    /*
     * Recurse down until we hit a leaf.  Note that forward
     * references will return elm = 0.  Leafs are guarenteed
     * to have at least one element.
     */
    while ((bn->bn_Flags & BNF_LEAF) == 0) {
	ti->ti_RanBeg.p_IRo = bn->bn_Elms[elm].be_Ro - 1;
	bn = btreeRead(ti->ti_Index, &im, bn->bn_Elms[elm].be_Ro, &elm);
    }

    /*
     * Update IRo.  Typically just increment it.  If we had to recurse
     * up & down we have pre-set be_Ro such that we can simply increment
     * it.
     *
     * Note that the saved table append point, ti_IndexAppend, is enforced as
     * a scan stop point so we have to skip over records that violate that
     * here before we return.
     */
    ++ti->ti_RanBeg.p_IRo;

    ti->ti_RanBeg.p_Ro = bn->bn_Elms[elm].be_Ro;
    if (ti->ti_RanBeg.p_Ro >= ti->ti_IndexAppend)
	goto again;
    btreeRelIndexMap(&im, 0);
}

/*
 * BTreePrevTableRec()
 *
 *	note:	The index-specific prev-table-rec does not have to check
 *		for ti_ScanOneOnly.
 *
 */

static void
BTreePrevTableRec(TableI *ti)
{
    const BTreeNode *bn;
    IndexMap *im = NULL;
    int elm;

again:
    if (ti->ti_RanEnd.p_Ro == ti->ti_RanBeg.p_Ro) {
	ti->ti_Index->i_PosCache = ti->ti_RanBeg;
	ti->ti_RanEnd.p_Ro = -1;
	btreeRelIndexMap(&im, 0);
	return;
    }
    /*
     * Access node, locate previous element.  If we hit the beginning
     * of the array, recurse up (which will result in an equal number
     * of down recursions later).  Note that bn_Parent embeds the
     * element index relative to the parent, and btreeRead() loads it
     * into elm.
     *
     * If we recurse all the way to the root and there is no previous
     * element, we are through scanning.
     */
    bn = btreeReadReScan(ti->ti_Index, &im, &ti->ti_RanEnd, &elm);

    while (--elm < 0) {
	if (bn->bn_Parent == 0) {
#if 0
	    printf("****** Did not find begin node %08qx\n", ti->ti_RanEnd.p_IRo);
#endif
	    ti->ti_RanEnd.p_Ro = -1;
	    btreeRelIndexMap(&im, 0);
	    return;
	}
	bn = btreeRead(ti->ti_Index, &im, bn->bn_Parent, &elm);
    }

    /*
     * Recurse down until we hit a leaf.  Note that forward
     * references will return elm = 0.  Leafs are guarenteed
     * to have at least one element.
     */
    while ((bn->bn_Flags & BNF_LEAF) == 0) {
	ti->ti_RanEnd.p_IRo = bn->bn_Elms[elm].be_Ro;
	bn = btreeRead(ti->ti_Index, &im, bn->bn_Elms[elm].be_Ro, &elm);
	elm = bn->bn_Count - 1;
	ti->ti_RanEnd.p_IRo += bn->bn_Count;
	DBASSERT(elm >= 0);
    }

    /*
     * Update IRo.  Typically just decrement it.  If we had to recurse
     * up & down we have pre-set be_Ro such that we can simply increment
     * it.
     *
     * Note that the saved table append point, ti_IndexAppend, is enforced as
     * a scan stop point so we have to skip over records that violate that
     * here before we return.
     */
    --ti->ti_RanEnd.p_IRo;

    ti->ti_RanEnd.p_Ro = bn->bn_Elms[elm].be_Ro;
    if (ti->ti_RanEnd.p_Ro >= ti->ti_IndexAppend)
	goto again;
    btreeRelIndexMap(&im, 0);
}

/*
 * BTreeCacheCheck	- JOIN optimization
 *
 *	This code optimizes the 'a.key = b.key' JOIN case.  Without this
 *	code we will scan a.key and do a full search-from-root in B for
 *	each a.key that we find.  This would not be slow since it is
 *	a btree search, but it isn't going to be fast either.
 *
 *	We can *really* optimize the second element of the JOIN case
 *	by using a cached scan index for B based on the previous a.key
 *	to shortcut our range search for the new a.key in table B.  
 *
 *	This code yields approximately 2.3x improvement in scan speed.
 *
 *	The return codes are as follows:
 *
 *	-1	We could not do a blessed thing (well, we could do something
 *		here but it isn't a common case so we don't bother).  No
 *		parameters will be modified.
 *
 *	0	Partial success.  bbeg and/or bend will be modified, getting
 *		us closer to the real range but not *EXACTLY* the range we
 *		want yet.
 *
 *	1	Complete success.  bbeg and/or ben will be modified,
 *		giving us the EXACT range we want and allowing us to
 *		shortcut the nominal search completely.
 *
 *		This occurs when the entire range can be found within
 *		a single btree node.
 *
 *		NOTE! This is really fragile.  If we make a mistake and
 *		return 1 the search will be screwed up.
 *
 *	Other optimizations are possible but this is pretty good.
 */

static int
BTreeCacheCheck(TableI *ti, Index *index, BTreeElm *cmp, dbpos_t *bpos, dbpos_t *bbeg, dbpos_t *bend)
{
    const BTreeNode *bn;
    IndexMap *im = NULL;
    int elm;
    int r;

    ++ti->ti_DebugIndexScanCount;
    bn = btreeReadReScan(index, &im, bpos, &elm);
    r = btreeCompare(index, &bn->bn_Elms[elm], cmp);
    DBASSERT(bn->bn_Flags & BNF_LEAF);
    if (r == 0) {
	r = -1;
    } else if (r < 0) {
	/*
	 * forward scan case (not typical)
	 */
	int lastElm = bn->bn_Count - 1;

	if (elm != lastElm && btreeCompare(index, &bn->bn_Elms[lastElm], cmp) > 0) {
	    bend->p_Ro = bn->bn_Elms[lastElm].be_Ro;
	    bend->p_IRo = (bpos->p_IRo & ~(dboff_t)BT_INDEXMASK) + lastElm;
	    elm = btreeCompareSearchFwd(index, bn, elm, cmp);
	    if ((r = btreeCompareSearchRev(index, bn, lastElm, cmp)) < elm) {
		bbeg->p_Ro = (dboff_t)-1;
		bbeg->p_IRo = (dboff_t)-1;
		bend->p_Ro = (dboff_t)-1;
		bend->p_IRo = (dboff_t)-1;
	    } else {
		bbeg->p_Ro = bn->bn_Elms[elm].be_Ro;
		bbeg->p_IRo = (bpos->p_IRo & ~(dboff_t)BT_INDEXMASK) + elm;
		bend->p_Ro = bn->bn_Elms[r].be_Ro;
		bend->p_IRo = (bpos->p_IRo & ~(dboff_t)BT_INDEXMASK) + r;
	    }
	    r = 1;
	} else {
	    bbeg->p_Ro = bpos->p_Ro;
	    bbeg->p_IRo = bpos->p_IRo;
	    r = 0;
	}
    } else {
	/*
	 * elm compares > 0
	 *
	 * Backwards scan case (more typical)
	 */
	if (btreeCompare(index, &bn->bn_Elms[0], cmp) < 0) {
	    elm = btreeCompareSearchRev(index, bn, elm, cmp);
	    if ((r = btreeCompareSearchFwd(index, bn, 0, cmp)) > elm) {
		bbeg->p_Ro = (dboff_t)-1;
		bbeg->p_IRo = (dboff_t)-1;
		bend->p_Ro = (dboff_t)-1;
		bend->p_IRo = (dboff_t)-1;
	    } else {
		bbeg->p_Ro = bn->bn_Elms[r].be_Ro;
		bbeg->p_IRo = (bpos->p_IRo & ~(dboff_t)BT_INDEXMASK) + r;
		bend->p_Ro = bn->bn_Elms[elm].be_Ro;
		bend->p_IRo = (bpos->p_IRo & ~(dboff_t)BT_INDEXMASK) + elm;
	    }
	    r = 1;
	} else {
	    bend->p_Ro = bpos->p_Ro;
	    bend->p_IRo = bpos->p_IRo;
	    r = 0;
	}
    }
    btreeRelIndexMap(&im, 0);
    return(r);
}

/*
 * BTreeFindBoundsFwd
 *
 * Find the forward-looking bound within the existing bbeg/bend bounds.
 *
 *	Returns -1 if we hit the end of the table, 0 if we found an exactly
 *	matching element, and +1 if we found a following element.
 */
static int
BTreeFindBoundsFwd(TableI *ti, Index *index, BTreeElm *cmp, dbpos_t *bpos)
{
    const BTreeNode *bn;
    IndexMap *im = NULL;
    int elm;
    int foundFwd;

    /*
     * Degenerate cases.
     */
    if (bpos->p_IRo < 0)
	return(-1);

    bn = btreeReadReScan(index, &im, bpos, &elm);

    /*
     * Find the transition point where the comparison becomes valid,
     * moving upwards in the tree.  If the comparison never becomes valid
     * it never the less may be in a subtree of the last element.
     */
    for (;;) {
	++ti->ti_DebugIndexScanCount;
	while (elm < bn->bn_Count) {
	    if (btreeCompare(index, cmp, &bn->bn_Elms[elm]) <= 0)
		break;
	    ++elm;
	}
	if (elm == bn->bn_Count) {
	    if (bn->bn_Parent) {
		bpos->p_IRo = bn->bn_Parent;
		bn = btreeRead(index, &im, bpos->p_IRo, &elm);
		++elm;
		continue;
	    }
	}
	break;
    }

    /*
     * Now move downwards in the tree.  Note that if we find a valid
     * compare, we actually have to use the previous element not the
     * current element.  e.g. searching for '3', ranging '2', '4', the
     * compare will succeed at '4' but we really have to recurse down 
     * at '2'.
     */
    while ((bn->bn_Flags & BNF_LEAF) == 0) {
	if (elm)
	    --elm;
	++ti->ti_DebugIndexScanCount;
	bpos->p_IRo = bn->bn_Elms[elm].be_Ro;
	bn = btreeRead(index, &im, bpos->p_IRo, &elm);
	/* elm starts at 0 */
	while (elm < bn->bn_Count) {
	    if (btreeCompare(index, cmp, &bn->bn_Elms[elm]) <= 0)
		break;
	    ++elm;
	}
    }

    /*
     * If we hit the end of the btree recursing downwards, our 'hit' is
     * the next btree element which requires going up and down again. 
     * We cannot use the last element of the miss because it might
     * 'cut' an insert/delete record pair and cause DelHash to assert.
     */
    if (elm == bn->bn_Count) {
	/*
	 * Recurse upwards until we find a parent with a next element.
	 * If there is no next element return -1.
	 */
	while (elm == bn->bn_Count) {
	    if (bn->bn_Parent == 0) {
		bpos->p_Ro = -1;
		bpos->p_IRo = -1;
		btreeRelIndexMap(&im, 0);
		return(-1);
	    }
	    bpos->p_IRo = bn->bn_Parent;
	    bn = btreeRead(ti->ti_Index, &im, bpos->p_IRo, &elm);
	    ++elm;
	}
	/*
	 * Recurse down until we hit a leaf, taking the first node
	 * we find.
	 */
	while ((bn->bn_Flags & BNF_LEAF) == 0) {
	    bpos->p_IRo = bn->bn_Elms[elm].be_Ro;
	    bn = btreeRead(ti->ti_Index, &im, bpos->p_IRo, &elm);
	}
    }
    bpos->p_IRo = (bpos->p_IRo & ~(dboff_t)BT_INDEXMASK) + elm;
    bpos->p_Ro = bn->bn_Elms[elm].be_Ro;
    foundFwd = (btreeCompare(index, cmp, &bn->bn_Elms[elm]) != 0);
    btreeRelIndexMap(&im, 0);
    return(foundFwd);
}
/* 
 * BTreeFindBoundsRev() 
 *
 *	Returns -1 if we hit the beginning of the table, 0 if we found an
 *	exactly matching element, and -1 if we found a prior element.
 */

static int
BTreeFindBoundsRev(TableI *ti, Index *index, BTreeElm *cmp, dbpos_t *bpos)
{
    const BTreeNode *bn;
    IndexMap *im = NULL;
    int elm;
    int foundRev;

    /*
     * Degenerate cases.
     */
    if (bpos->p_IRo < 0)
	return(-1);

    bn = btreeReadReScan(index, &im, bpos, &elm);

    /*
     * Starting at the current element attempt to find the transition
     * point where the comparison becomes valid, moving upwards and backwards
     * in the tree.
     */
    for (;;) {
	++ti->ti_DebugIndexScanCount;
	while (elm >= 0) {
	    if (btreeCompare(index, cmp, &bn->bn_Elms[elm]) >= 0)
		break;
	    --elm;
	}
	if (elm >= 0)	/* found */
	    break;
	/*
	 * If we hit the beginning of the tree the table does not contain
	 * any matches at all, otherwise continue the upward recursion.
	 */
	if (bn->bn_Parent == 0) {
	    bpos->p_Ro = -1;
	    bpos->p_IRo = -1;
	    btreeRelIndexMap(&im, 0);
	    return(-1);
	}
	bpos->p_IRo = bn->bn_Parent;
	bn = btreeRead(index, &im, bpos->p_IRo, &elm);
	--elm;
    }

    /*
     * Now move downwards in the tree, reverse scanning looking for the
     * match point.
     */
    while ((bn->bn_Flags & BNF_LEAF) == 0) {
	++ti->ti_DebugIndexScanCount;
	bpos->p_IRo = bn->bn_Elms[elm].be_Ro;
	bn = btreeRead(index, &im, bpos->p_IRo, &elm);
	elm = bn->bn_Count - 1;
	while (elm >= 0) {
	    if (btreeCompare(index, cmp, &bn->bn_Elms[elm]) >= 0)
		break;
	    --elm;
	}
	if (elm < 0)
	    break;
    }

    /*
     * We hit a special case if our reverse scan does not find a match.  In
     * this case the actual hit is the previous element, which requires us
     * to recurse upward and backwards through the tree to get to.
     */
    if (elm < 0) {
	/*
	 * Recurse upwards until we find a parent with a next element.
	 * If there is no next element return -1.
	 */
	while (elm < 0) {
	    if (bn->bn_Parent == 0) {
		bpos->p_Ro = -1;
		bpos->p_IRo = -1;
		btreeRelIndexMap(&im, 0);
		return(-1);
	    }
	    bpos->p_IRo = bn->bn_Parent;
	    bn = btreeRead(ti->ti_Index, &im, bpos->p_IRo, &elm);
	    --elm;
	}
	/*
	 * Recurse down until we hit a leaf, taking the last node
	 * we find.
	 */
	while ((bn->bn_Flags & BNF_LEAF) == 0) {
	    bpos->p_IRo = bn->bn_Elms[elm].be_Ro;
	    bn = btreeRead(ti->ti_Index, &im, bpos->p_IRo, &elm);
	    elm = bn->bn_Count - 1;
	    DBASSERT(elm >= 0);
	}
    }
    bpos->p_IRo = (bpos->p_IRo & ~(dboff_t)BT_INDEXMASK) + elm;
    bpos->p_Ro = bn->bn_Elms[elm].be_Ro;
    foundRev = (btreeCompare(index, cmp, &bn->bn_Elms[elm]) != 0);
    btreeRelIndexMap(&im, 0);
    return(foundRev);
}

/*
 * btreeInsert() -	insert new element into btree
 *
 *	The new element is inserted into the btree.  0 is returned on
 *	success, -1 is returned if the tree had to be split.  If the 
 *	tree had to be split, the new split node is returned in be
 *	and must be added at the current level (which may result in 
 *	another split, recursively).
 *
 *	WARNING!  At the moment the tree must be locked prior to entering
 *	this routine.
 */
static int
btreeInsert(TableI *ti, Index *index, dboff_t bnro, BTreeElm *be, dboff_t *appro, int flags)
{
    const BTreeNode *bn;
    IndexMap *im = NULL;
    int i;
    int r;
    int dummy;

    /*
     * Locate insertion or recursion point.  We go one past, then 
     * decrement i to get back to the index we need to recurse through.
     *
     * Note that we must maintain order for equal-valued elements.  This
     * behavior is assumed by (for example) the synchronizer to guarentee
     * the ordering of records within a transaction.
     */

    ++ti->ti_DebugIndexInsertCount;
    bn = btreeRead(index, &im, bnro, &dummy);
    for (i = 0; i < bn->bn_Count; ++i) {
	if (btreeCompare(index, be, &bn->bn_Elms[i]) < 0)
	    break;
    }
    --i;

    /*
     * Keep track of first and last node
     */

    if (i != bn->bn_Count - 1)
	flags &= ~BIF_LAST;

    /*
     * If not at leaf then recurse.  If the lower level node overflowed
     * it will return -1 along with the new split node info in 'be'. 
     *
     * i can be -1 on entry, indicating insertion prior to the first
     * element (at least insofar as the cached data is concerned).
     */

    if ((bn->bn_Flags & BNF_LEAF) == 0) {
	int j;

	if (i > 0) {
	    j = i;
	    flags &= ~BIF_FIRST;
	} else {
	    j = 0;
	}
	if (btreeInsert(ti, index, bn->bn_Elms[j].be_Ro, be, appro, flags) == 0)
	    be = NULL;
	else
	    bn = btreeRead(index, &im, bnro, &dummy);
    }

    /*
     * If we have something to insert (either leaf or node), go do it.  The
     * insertion point is the index after the current index.
     */

    r = 0;
    if (be) {
	++i;
	if (i != 0)
	    flags &= ~BIF_FIRST;
	if (bn->bn_Count == BT_MAXELM) {
	    /*
	     * level full, split.  The current node is split into two nodes,
	     * the contents of be is appended to the first half, and be is
	     * loaded with a reference to the (new) second half.
	     */
	    btreeSplit(index, bnro, bn, i, be, appro, flags);
	    r = -1;
	} else {
	    /*
	     * Level not full, insert at point i
	     */
	    btreeInsertPhys(index, bnro, bn, i, be, appro, flags);
	}
    }
    btreeRelIndexMap(&im, 0);
    return(r);
}

/*
 * btreeSplit() - split current node bn into two and insert be at position i
 *
 *	Stores the split node in be on return.
 */
static void
btreeSplit(Index *index, dboff_t bnro, const BTreeNode *bn, int i, BTreeElm *be, dboff_t *appro, int flags)
{
    const BTreeHead *bt;
    BTreeElm tmpbe = *be;
    BTreeNode bn1;
    BTreeNode bn2;
    const int HALF = BT_MAXELM / 2;

    bzero(&bn1, sizeof(bn1));
    bn1.bn_Parent = bn->bn_Parent;
    bn1.bn_Count = HALF;
    bn1.bn_Flags = bn->bn_Flags;
    bcopy(&bn->bn_Elms[0], &bn1.bn_Elms[0], HALF * sizeof(BTreeElm));

    bzero(&bn2, sizeof(bn2));
    bn2.bn_Parent = 0;
    bn2.bn_Count = HALF;
    bn2.bn_Flags = bn->bn_Flags;
    bcopy(&bn->bn_Elms[HALF], &bn2.bn_Elms[0], HALF * sizeof(BTreeElm));

    /*
     * Update bn1
     */
    btreeIndexWrite(index, bnro, &bn1, sizeof(bn1));

    /*
     * Update returned be (and obtain ro of bn2)
     */
    *be = bn2.bn_Elms[0];
    be->be_Flags = 0;
    be->be_Ro = btreeAppend(index, &bn2, appro);

    bt = index->i_BTreeHead;

    /*
     * If the last element was associated with the node being split,
     * we have to fix it up.  We may have to fix it up yet
     * again in btreeInsertPhys(), but that only works properly
     * if we fix it up here first.
     */
    if ((bt->bt_LastElm & ~(dboff_t)BT_INDEXMASK) == bnro) {
	dboff_t lastro = be->be_Ro + (HALF - 1);
	btreeIndexWrite(
	    index,
	    offsetof(BTreeHead, bt_LastElm),
	    &lastro,
	    sizeof(dboff_t)
	);
    }

    /*
     * Whew.  Now insert tmpbe into either bn1 or bn2.
     */
    if (i <= HALF) {
	btreeInsertPhys(index, bnro, &bn1, i, &tmpbe, appro, flags);
	/*btreeInsertPhys(index, be->be_Ro, &bn2, 0, NULL, 0);*/
    } else {
	btreeInsertPhys(index, bnro, &bn1, 0, NULL, appro, 0);
	btreeInsertPhys(index, be->be_Ro, &bn2, i - HALF, &tmpbe, appro, flags);
    }
}

static void
btreeInsertPhys(Index *index, dboff_t bnro, const BTreeNode *bn, int i, BTreeElm *be, dboff_t *appro, int flags)
{
    const BTreeHead *bt;
    BTreeNode nbn;

    bzero(&nbn, sizeof(nbn));

    if (be) {
	nbn.bn_Parent = bn->bn_Parent;
	nbn.bn_Count = bn->bn_Count + 1;
	nbn.bn_Flags = bn->bn_Flags;

	bcopy(&bn->bn_Elms[0], &nbn.bn_Elms[0], i * sizeof(BTreeElm));
	bcopy(be, &nbn.bn_Elms[i], sizeof(BTreeElm));
	bcopy(&bn->bn_Elms[i], &nbn.bn_Elms[i+1], (bn->bn_Count - i) * sizeof(BTreeElm));
	btreeIndexWrite(
	    index,
	    bnro,
	    &nbn,
	    sizeof(nbn)
	);
    } else {
	nbn = *bn;
    }

    bt = index->i_BTreeHead;

    /*
     * Update bt_FirstElm if element being inserted is the lowest-sorted
     * element in the index.
     */
    if ((bn->bn_Flags & BNF_LEAF) && (flags & BIF_FIRST)) {
	dboff_t xbnro = bnro + i;
	btreeIndexWrite(
	    index,
	    offsetof(BTreeHead, bt_FirstElm), 
	    &xbnro,
	    sizeof(dboff_t)
	);
    }

    /*
     * Update bt_LastElm if element being inserted is the highest-sorted
     * element in the index.  Also update it if bt_LastElm was previously
     * in this block -- we have to fix it up due to the merge.
     */
    if ((bn->bn_Flags & BNF_LEAF) && (flags & BIF_LAST)) {
	dboff_t xbnro = bnro + i;
	btreeIndexWrite(
	    index,
	    offsetof(BTreeHead, bt_LastElm),
	    &xbnro,
	    sizeof(dboff_t)
	);
    } else if ((bt->bt_LastElm & ~(dboff_t)BT_INDEXMASK) == bnro) {
	dboff_t lastro = bnro + (nbn.bn_Count - 1);
	DBASSERT(bt->bt_LastElm + 1 == lastro);
	btreeIndexWrite(
	    index,
	    offsetof(BTreeHead, bt_LastElm),
	    &lastro,
	    sizeof(dboff_t)
	);
    }

    /*
     * Update any adjusted parent pointers.  Yuch!
     */
    if ((bn->bn_Flags & BNF_LEAF) == 0) {
	while (i < nbn.bn_Count) {
	    BTreeElm *xe = &nbn.bn_Elms[i];
	    dboff_t xbnro = bnro + i;

	    btreeIndexWrite(
		index, 
		xe->be_Ro + offsetof(BTreeNode, bn_Parent),
		&xbnro,
		sizeof(dboff_t)
	    );
	    ++i;
	}
    }
}

dboff_t
btreeAppend(Index *index, BTreeNode *bn, dboff_t *appro)
{
    static char *ZBuf;
    const BTreeHead *bt;
    dboff_t bnro;

    bt = index->i_BTreeHead;
    bnro = (*appro + BT_INDEXMASK) & ~(dboff_t)BT_INDEXMASK;	/* align */

    /*
     * If we would otherwise write across a cache block boundry, align
     * to the next cache block
     */
    if ((bnro ^ (bnro + (sizeof(BTreeNode) - 1))) & ~(dboff_t)BT_CACHEMASK) {
	bnro = (bnro + BT_CACHEMASK) & ~(dboff_t)BT_CACHEMASK;
    }

    /*
     * If this is a temporary btree that has grown too large, create a
     * spill file.  Note that the first block is never spilled.
     */
    if (index->i_Fd < 0 &&
	bnro >= BT_CACHESIZE &&
	(index->i_BTreeHead->bt_Flags & BTF_TEMP)
    ) {
	fprintf(stderr, 
	    "Extending temporary index into file: %s\n",
	    index->i_FilePath);
	DBASSERT(strstr(index->i_FilePath, ".tmp") != NULL);
	index->i_Fd = open(index->i_FilePath, O_RDWR|O_CREAT|O_TRUNC, 0660);
	DBASSERT(index->i_Fd >= 0);
	remove(index->i_FilePath);
    }

    /*
     * Extend the index file if necessary, use real writes to try to
     * keep the file fairly contiguous.
     */
    if (bnro + sizeof(BTreeNode) > bt->bt_ExtAppend) {
	dboff_t curapp = bnro;
	dboff_t extapp = bt->bt_ExtAppend + BT_CACHESIZE;

	if (ZBuf == NULL)
	    ZBuf = zalloc(8192);

	while (curapp < extapp) {
	    int n = (extapp - curapp > 8192) ? 8192 : extapp - curapp;
	    btreeIndexWrite(index, curapp, ZBuf, n);
	    curapp += n;
	}
	btreeIndexWrite(
	    index,
	    offsetof(BTreeHead, bt_ExtAppend),
	    &extapp,
	    sizeof(dboff_t)
	);
    }

    btreeIndexWrite(index, bnro, bn, sizeof(BTreeNode));

    if ((bn->bn_Flags & BNF_LEAF) == 0) {
	int i;

	for (i = 0; i < bn->bn_Count; ++i) {
	    dboff_t par;

	    DBASSERT(bn->bn_Elms[i].be_Ro != 0);
	    par = bnro + i;
	    btreeIndexWrite(
		index,
		bn->bn_Elms[i].be_Ro + offsetof(BTreeNode, bn_Parent),
		&par,
		sizeof(dboff_t)
	    );
	}
    }
    *appro = bnro + sizeof(BTreeNode);
    return(bnro);
}

/*
 * btreeRead() -	Read the BTreeNode and set the index offset
 *
 *	The offset, bnro, consists of the base offset of the BTreeNode plus
 *	the element index.
 *
 *	Reads the btree node containing the specified index offset and
 *	sets *elm to the element index within the node.  
 */

static const BTreeNode *
btreeRead(Index *index, IndexMap **pim, dboff_t bnro, int *elm)
{
    *elm = bnro & BT_INDEXMASK;
    bnro &= ~(dboff_t)BT_INDEXMASK;

    return(btreeGetIndexMap(index, pim, bnro, sizeof(BTreeNode)));
}

/*
 * btreeReadReScan() -	Read the BTreeNode and set the index offset
 *
 *	Similar to btreeRead(), but this routine is typically entered
 *	when regaining a node lock on the btree.  This routine is
 *	responsible for checking the Ro at the existing IRo to determine
 *	if a btree insert has moved our current cursor.
 */

static const BTreeNode *
btreeReadReScan(Index *index, IndexMap **pim, dbpos_t *bpos, int *elm)
{
    const BTreeNode *bn;
    dboff_t bnro;

    *elm = (int)bpos->p_IRo & BT_INDEXMASK;
    bnro = bpos->p_IRo & ~(dboff_t)BT_INDEXMASK;

    bn = btreeGetIndexMap(index, pim, bnro, sizeof(BTreeNode));
    DBASSERT(bn->bn_Flags & BNF_LEAF);

    /*
     *	If an insert has moved our current cursor, we must iterate forwards
     *	in the BTree to locate the new IRo for our existing Ro.  As a special
     *	case the split code may reorder a node backwards up to the beginning
     *	of the leaf (and no further) before begining the forward search.
     */
    if (bn->bn_Elms[*elm].be_Ro != bpos->p_Ro) {
	*elm = 0;
	bpos->p_IRo &= ~(dboff_t)BT_INDEXMASK;

	while (bn->bn_Elms[*elm].be_Ro != bpos->p_Ro) {
	    while (++*elm == bn->bn_Count) {
		DBASSERT(bn->bn_Parent != 0);
		bn = btreeRead(index, pim, bn->bn_Parent, elm);
	    }
	    while ((bn->bn_Flags & BNF_LEAF) == 0) {
		bpos->p_IRo = bn->bn_Elms[*elm].be_Ro - 1;
		bn = btreeRead(index, pim, bn->bn_Elms[*elm].be_Ro, elm);
	    }
	    ++bpos->p_IRo;
	}
    }

    DBASSERT(bn->bn_Elms[*elm].be_Ro == bpos->p_Ro);
    return(bn);
}

static dboff_t
btreeReadOffset(Index *index, dboff_t bnro)
{
    int elm;
    const BTreeNode *bn;
    IndexMap *im = NULL;

    bn = btreeRead(index, &im, bnro, &elm);
    bnro = bn->bn_Elms[elm].be_Ro;
    btreeRelIndexMap(&im, 0);
    return(bnro);
}

static int
btreeCompare(Index *index, const BTreeElm *b1, const BTreeElm *b2)
{
    int j;

    switch(index->i_OpClass) {
    case ROP_STAMP_EQEQ:
	if (*(dbstamp_t *)b1->be_Data < *(dbstamp_t *)b2->be_Data)
	    return(-1);
	if (*(dbstamp_t *)b1->be_Data > *(dbstamp_t *)b2->be_Data)
	    return(1);
	break;
    case ROP_VTID_EQEQ:
	if (*(vtable_t *)b1->be_Data < *(vtable_t *)b2->be_Data)
	    return(-1);
	if (*(vtable_t *)b1->be_Data > *(vtable_t *)b2->be_Data)
	    return(1);
	break;
    case ROP_USERID_EQEQ:
	if (*(u_int32_t *)b1->be_Data < *(u_int32_t *)b2->be_Data)
	    return(-1);
	if (*(u_int32_t *)b1->be_Data > *(u_int32_t *)b2->be_Data)
	    return(1);
	break;
    case ROP_OPCODE_EQEQ:
	if (*(u_int8_t *)b1->be_Data < *(u_int8_t *)b2->be_Data)
	    return(-1);
	if (*(u_int8_t *)b1->be_Data > *(u_int8_t *)b2->be_Data)
	    return(1);
	break;
    case ROP_LIKE:
	for (j = 0; j != BT_DATALEN; ++j) {
	    /*
	     * break out (return match) on exact match
	     */
	    if (j == b1->be_Len && j == b2->be_Len)
		break;

	    /*
	     * If left hand string EOF or left hand string less then right
	     * hand, return -1 (left hand is less then right hand).
	     *
	     * The standard 'same' comparison is case insensitive.
	     */
	    if (j == b1->be_Len || tolower(b1->be_Data[j]) < tolower(b2->be_Data[j]))
		return(-1);

	    /*
	     * If right hand string EOF or left hand string greater then right
	     * hand, return +1 (left hand is greater then right hand).  
	     * Otherwise loop.
	     */
	    if (j == b2->be_Len || tolower(b1->be_Data[j]) > tolower(b2->be_Data[j]))
		return(1);
	}
	/*
	 * Return match if we hit the end of our key cache buffer
	 */
	break;
    case ROP_EQEQ:
	for (j = 0; j != BT_DATALEN; ++j) {
	    /*
	     * break out (return match) on exact match
	     */
	    if (j == b1->be_Len && j == b2->be_Len)
		break;

	    /*
	     * If left hand string EOF or left hand string less then right
	     * hand, return -1 (left hand is less then right hand).
	     */
	    if (j == b1->be_Len || b1->be_Data[j] < b2->be_Data[j])
		return(-1);

	    /*
	     * If right hand string EOF or left hand string greater then right
	     * hand, return +1 (left hand is greater then right hand).  
	     * Otherwise loop.
	     */
	    if (j == b2->be_Len || b1->be_Data[j] > b2->be_Data[j])
		return(1);
	}
	break;
    default:
	DBASSERT(0);
	break;
    }
    return(0);
}

/*
 * The compare at <elm> is < 0.  Locate the first node going forwards whos
 * copare is >= 0.  Use a binary search.
 */
static int
btreeCompareSearchFwd(Index *index, const BTreeNode *bn, int elm, BTreeElm *cmp)
{
    int try;
    int last = bn->bn_Count - 1;

    while (elm != last) {
	try = (elm + last + 1) / 2;	/* elm < 0 so slide towards last */
	if (btreeCompare(index, &bn->bn_Elms[try], cmp) < 0) {
	    elm = try;
	} else {
	    if (last == try)
		++elm;
	    else
		last = try;
	}
    }
    return(elm);
}

/*
 * The compare at <elm> is > 0.  Locate the first node going backwards whos
 * copare is <= 0.  Use a binary search.
 */
static int
btreeCompareSearchRev(Index *index, const BTreeNode *bn, int elm, BTreeElm *cmp)
{
    int try;
    int first = 0;

    while (elm != first) {
	try = (first + elm) / 2;	/* elm > 0 so slide towards first */
	if (btreeCompare(index, &bn->bn_Elms[try], cmp) > 0) {
	    elm = try;
	} else {
	    if (first == try)
		--elm;
	    else
		first = try;
	}
    }
    return(elm);
}

static const void *
btreeGetIndexMap(Index *index, IndexMap **pmap, dboff_t ro, int bytes)
{
    IndexMap *im;
    int hv;

    /*
     * Make sure the request does not cross a cache block boundry
     */
    DBASSERT(((ro ^ (ro + bytes - 1)) & ~(dboff_t)BT_CACHEMASK) == 0);

    /*
     * Fast cache case
     */
    if ((im = *pmap) != NULL) {
	if (ro >= im->im_Ro && ro < im->im_Ro + BT_CACHESIZE)
	    return(im->im_Base + (int)(ro - im->im_Ro));
	btreeRelIndexMap(pmap, 0);	/* allow im to stay in register */
    }

    /*
     * Look the block up in our cache
     */
    hv = (ro / BT_CACHESIZE + index->i_CacheRand) & BTREE_HMASK;
    for (
	im = BTreeIndexAry[hv];
	im;
	im = im->im_HNext
    ) {
	if (im->im_Index != index)
	    continue;
	if (ro >= im->im_Ro && ro < im->im_Ro + BT_CACHESIZE) {
	    ++im->im_Refs;
	    *pmap = im;
	    return(im->im_Base + (int)(ro - im->im_Ro));
	}
    }

    /*
     * Failed, allocate a new cache entry.  The block at offset 0 may
     * be temporarily located in memory (BTF_TEMP set).  This feature
     * is used by temporary tables to avoid having to instantiate disk
     * files during small transactions.
     */
    im = zalloc(sizeof(IndexMap));
    im->im_Refs = 1;
    im->im_Index = index;
    im->im_Ro = ro & ~(dboff_t)BT_CACHEMASK;
    if (im->im_Ro == 0 && (index->i_BTreeHead->bt_Flags & BTF_TEMP)) {
	im->im_Base = (const void *)index->i_BTreeHead;
    } else {
	im->im_Base = mmap(NULL, BT_CACHESIZE, PROT_READ, MAP_SHARED,
			index->i_Fd, im->im_Ro);
	DBASSERT(im->im_Base != MAP_FAILED);
    }
    *pmap = im;
    addHead(&index->i_BTreeCacheList, &im->im_Node);
    im->im_HNext = BTreeIndexAry[hv];
    BTreeIndexAry[hv] = im;
    ++index->i_CacheCount;
    ++BTreeIndexCount;
    if (BTreeIndexCount > BT_MAXCACHE)
	btreeCachePurge();
    return(im->im_Base + (int)(ro - im->im_Ro));
}

static void
btreeRelIndexMap(IndexMap **pmap, int freeLastClose)
{
    IndexMap *im;

    if ((im = *pmap) != NULL) {
	*pmap = NULL;
	DBASSERT(im->im_Refs > 0);
	if (--im->im_Refs == 0) {
	    if (freeLastClose) {
		int hv;
		IndexMap **pim;

		removeNode(&im->im_Node);
		hv = (im->im_Ro / BT_CACHESIZE + im->im_Index->i_CacheRand) &
			BTREE_HMASK;
		for (
		    pim = &BTreeIndexAry[hv];
		    *pim != im;
		    pim = &(*pim)->im_HNext
		) {
		    ;
		}
		*pim = im->im_HNext;
		if (im->im_Ro != 0 ||
		    (im->im_Index->i_BTreeHead->bt_Flags & BTF_TEMP) == 0
		) {
		    munmap((void *)im->im_Base, BT_CACHESIZE);
		}
		im->im_Base = NULL;
		im->im_HNext = (void *)-1;
		--im->im_Index->i_CacheCount;
		--BTreeIndexCount;
		zfree(im, sizeof(IndexMap));
	    }
	}
    }
}

static void
btreeCachePurge(void)
{
    int i;

    fprintf(stderr, "BTree Cache Purge\n");

    for (i = BT_CACHESIZE / 16; i; --i) {
	int hv = --BTreePurgeIndex & BTREE_HMASK;
	IndexMap *im;

restart:
	for (im = BTreeIndexAry[hv]; im; im = im->im_HNext) {
	    if (im->im_Refs == 0) {
		im->im_Refs = 1;
		btreeRelIndexMap(&im, 1);
		goto restart;
	    }
	}
    }
}

/*
 * btreeSynchronize() - indicate to crash recovery code that the btree is
 *			good.
 *
 *	After completing an index update and fsyncing we can safely set
 *	the BTF_SYNCED bit.  We do not have to fsync after setting the bit.
 *
 *	Note: Temporary indexes do not need to be synchronized or
 *	unsynchronized.  The file descriptor must already be locked
 *	on entry to this call and will be locked on return.
 */
void
btreeSynchronize(Index *index)
{
    const BTreeHead *bt;

    if ((bt = index->i_BTreeHead) == NULL || index->i_Fd < 0)
	return;
    if (bt->bt_Flags & (BTF_SYNCED|BTF_TEMP))
	return;

    /*
     * Make sure the file is synchronized to disk, then
     * set the BTF_SYNCED flag.
     */
    if ((bt->bt_Flags & BTF_SYNCED) == 0) {
	iflags_t flags = bt->bt_Flags | BTF_SYNCED;

	fprintf(stderr, "Synchronizing index %s\n", index->i_FilePath);
	btreeIndexFSync(index);
	btreeIndexWrite(index, offsetof(BTreeHead, bt_Flags),
	    &flags, sizeof(iflags_t));
    }
}

/*
 * btreeUnSynchronize() - indicate to crash recovery code that the btree may
 *			  be corrupt.
 *
 *	In order to do asynchronous writes to the btree we must clear the
 *	BTF_SYNCED bit.  If the bit is found to be clear when the database
 *	subsystem is started up, the index file will be regenerated from
 *	scratch.
 *
 *	We must fsync after clearing BTF_SYNCED so we can guarentee that
 *	it is clear if a crash occurs after making additional asynchronous
 *	modifications.
 *
 *	Note: Temporary indexes do not need to be synchronized or
 *	unsynchronized.  The file descriptor must already be locked
 *	on entry to this call and will be locked on return.
 */
void
btreeUnSynchronize(Index *index)
{
    const BTreeHead *bt;
    iflags_t flags;

    if ((bt = index->i_BTreeHead) == NULL || index->i_Fd < 0)
	return;
    if ((bt->bt_Flags & BTF_SYNCED) == 0)
	return;
    if (bt->bt_Flags & BTF_TEMP)
	return;
    flags = bt->bt_Flags & ~BTF_SYNCED;
    btreeIndexWrite(index, offsetof(BTreeHead, bt_Flags),
			&flags, sizeof(iflags_t));
    btreeIndexFSync(index);
}

static int
btreeIndexWrite(Index *index, off_t off, void *data, int bytes)
{
    /*
     * Handle temporary btrees.  We try to cache a temporary btree in
     * memory but if it gets too large we have to spill it into a file.
     * The first block is always left in memory.
     */
    if (index->i_BTreeHead->bt_Flags & BTF_TEMP) {
	if (off < BT_CACHESIZE) {
	    DBASSERT(off >= 0 && off + bytes <= BT_CACHESIZE);
	    bcopy(data, (char *)index->i_BTreeHead + off, bytes);
	    return(0);
	}
	DBASSERT(index->i_Fd >= 0);
    }

    /*
     * We have a real file descriptor, write to it
     */
    lseek(index->i_Fd, off, 0);
    if (write(index->i_Fd, data, bytes) == bytes)
	return(0);
    return(-1);
}

static void
btreeIndexFSync(Index *index)
{
    if ((index->i_BTreeHead->bt_Flags & BTF_TEMP) == 0) {
	DBASSERT(index->i_Fd >= 0);
	fsync(index->i_Fd);
    }
}

