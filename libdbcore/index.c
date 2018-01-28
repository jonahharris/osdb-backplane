/*
 * LIBDBCORE/INDEX.C	- Misc Indexing support
 *
 * (c)Copyright 1999-2002 Backplane, Inc.  Please refer to the COPYRIGHT
 * file at the base of the distribution tree.
 *
 * $Backplane: rdbms/libdbcore/index.c,v 1.50 2002/10/03 21:12:18 dillon Exp $
 *
 *	NOTE: with normal indexes we need to special case reindexing
 *	when doing QOP_COMMIT_CHECKs, as reindexing should not be
 *	necessary (due to the FreezeTs being maxed)
 */

#include "defs.h"
#include "conflict.h"

Prototype Index *OpenIndex(Table *tab, vtable_t vt, col_t colId, int opClass, void (*func)(Index *));
Prototype void CloseIndex(Index **pindex, int freeLastClose);
Prototype void RewindTableIndexes(Table *tab);
Prototype void DefaultSetTableRange(TableI *ti, Table *tab, const ColData *colData, Range *r, int flags);
Prototype int DefaultIndexScanRangeOp1(Index *index, Range *r);
Prototype int DefaultIndexScanRangeOp2(Index *index, Range *r);
Prototype int ConflictScanRangeOp(Range *r, struct Conflict *co, int mySlot);
Prototype int GetIndexOpClass(int colId, int opId);

static int ScanInstanceRemainderValid(TableI *ti, Range *r);

List IndexLRUList = INITLIST(IndexLRUList);
int IndexCount;

Index *
OpenIndex(Table *tab, vtable_t vt, col_t colId, int opClass, void (*func)(Index *index))
{
    Index *index;
    Index **pi;

    for (pi = &tab->ta_IndexBase; (index = *pi) != NULL; pi = &index->i_Next) {
	if (index->i_ColId == colId &&
	    index->i_VTable == vt &&
	    index->i_OpClass == opClass
	) {
	    removeNode(&index->i_Node);
	    addTail(&IndexLRUList, &index->i_Node);
	    break;
	}
    }
    if (index == NULL) {
	index = zalloc(sizeof(Index));
	index->i_ColId = colId;
	index->i_Next = tab->ta_IndexBase;
	index->i_Table = tab;
	index->i_VTable = vt;
	index->i_OpClass = opClass;
	index->i_CacheRand = random();
	index->i_Fd = -1;
	t_flock_init(&index->i_FLock);
	initNode(&index->i_Node);
	addTail(&IndexLRUList, &index->i_Node);
	++IndexCount;

	tab->ta_IndexBase = index;
	++index->i_Refs;
	if (colId != 0)
	    func(index);
	--index->i_Refs;
	DBASSERT(index->i_ScanRangeOp != NULL);
    }
    ++index->i_Refs;
    return(index);
}

void
CloseIndex(Index **pindex, int freeLastClose)
{
    Index *index;

    if ((index = *pindex) != NULL) {
	*pindex = NULL;

	if (--index->i_Refs == 0) {
	    if (freeLastClose) {
		Index **pi;
		Table *tab;

		/*
		 * Unlink from table first to avoid
		 * close races
		 */
		tab = index->i_Table;
		for (
		    pi = &tab->ta_IndexBase; 
		    *pi != index; 
		    pi = &(*pi)->i_Next
		) {
		    DBASSERT(*pi != NULL);
		}
		*pi = index->i_Next;
		removeNode(&index->i_Node);
		--IndexCount;
		DBASSERT(IndexCount >= 0);

		/*
		 * Close the index, assert that all is
		 * well.
		 */
		if (index->i_Close)
		    index->i_Close(index);

		DBASSERT(index->i_CacheCount == 0);

		zfree(index, sizeof(Index));
	    }
	}
    }
}

/*
 * RewindTableIndexes()
 */
void
RewindTableIndexes(Table *tab)
{
    Index *index;

    DBASSERT(tab->ta_Db->db_PushType != DBPUSH_ROOT);

    while ((index = tab->ta_IndexBase) != NULL) {
	DBASSERT(index->i_Refs == 0);
	++index->i_Refs;
	CloseIndex(&index, 1);
    }
}

/*
 * DefaultSetTableRange() -	Set range for non-indexed tables
 *
 *	Set the table range for a non-indexed table.  Only p_Ro will be
 *	used but we set p_IRo anyway for continuity.  ti_IndexAppend is
 *	used by indexes to handle sequential record scanning of 'slop'
 * 	records.  That is, records that have not yet been indexed.
 *	A certain amount of slop is allowed to reduce the frequency of
 *	writes to index files and to make physical media synchronization
 *	more efficient by delaying index updates.  In the non-index case
 *	ti_IndexAppend is set to the table's first block.
 *
 *	Warning: This routine is also called by index code when updating
 *	an index.
 */

void
DefaultSetTableRange(TableI *ti, Table *tab, const ColData *colData, Range *r, int flags)
{
    if (flags & TABRAN_INIT) {
	ti->ti_Append = tab->ta_Append;
	ti->ti_IndexAppend = tab->ta_FirstBlock(tab);
    }
    ti->ti_RanBeg.p_Tab = tab;
    ti->ti_RanBeg.p_Ro = ti->ti_IndexAppend;
    ti->ti_RanBeg.p_IRo = -1;
    ti->ti_RanEnd.p_Tab = tab;
    ti->ti_RanEnd.p_Ro = ti->ti_Append;
    ti->ti_RanEnd.p_IRo = -1;
    ti->ti_ScanRangeOp = DefaultIndexScanRangeOp1;
    SelectBegTableRec(ti, 0);		/* to check degenerate EOF */
}

/*
 * DefaultIndexScanRangeOp1()
 * DefaultIndexScanRangeOp2()	- used with reverseable indexes
 *
 * Scan the range using the given index.  The index may or may not
 * match the column being tested in this range.
 *
 * Higher level functions run through the transaction levels 
 * backwards, necessary for tracking deletions.  If Op1
 * is called, however, we cannot run through any given table
 * backwards and must make two passes to deal with deletions
 * at that level.  We tend to use Op1 for non-indexed tables or the
 * non-indexed portion of a table, and Op2 for indexed tables (see btree.c).
 *
 * ti_ScanOneOnly handles two cases:  First, if > 0, it handles the
 * degenerate 'scan just one record' case without having to mess
 * around with RanEnd.  Second, if < 0, it handles a special scanning
 * case where we want to see all available records, whether deleted or
 * not.
 *
 * Note that r->r_DelHash is NULL when scanning the commit conflict
 * table, since this table represents transactionally inconsistent
 * data it makes no sense to try to track deletions and certainly makes
 * no sense to try to match them up.  Any hits in the conflict table
 * will always fail the commit validity test.
 */
int
DefaultIndexScanRangeOp1(Index *index, Range *r)
{
    TableI *ti = r->r_TableI;
    dbpos_t ranBeg;
    int count;
    int rv;

    /*
     * Remember ranBeg for later restore 
     */
    ranBeg = ti->ti_RanBeg;

    /*
     * If not a degenerate case then look for delete records.  Without
     * an index we can't do a backwards scan so we have to pick-out
     * the deletion records all in one go using an extra scan.  This
     * is wasteful, but we do not have much of a choice.
     */
    if (ti->ti_ScanOneOnly == 0) {
	for (
	    SelectBegTableRec(ti, 0);
	    ti->ti_RanBeg.p_Ro > 0;
	    SelectNextTableRec(ti, 0)
	) {
	    const RecHead *rh;

	    /*
	     * cooperative multitasking
	     */
	    taskQuantum();

	    /*
	     * Ignore records that are outside the scope of the scan.
	     */
	    if (RecordIsValid(ti) < 0)
		continue;

	    ReadDataRecord(ti->ti_RData, &ti->ti_RanBeg, RDF_READ | RDF_ZERO);
	    rh = ti->ti_RData->rd_Rh;
	    /*
	     * Optimize the allocation of the delete-hash element if
	     * possible. 
	     */
	    if (rh->rh_Flags & RHF_DELETE) {
		if ((r->r_Type & ROPF_CONST) == 0 ||
		    r->r_OpFunc(r->r_Col, r->r_Const) >= 0
		) {
		    /*
		     * Check for commit conflict.  To implement infinitely
		     * fine-grained conflict resolution make sure all 
		     * remaining clauses also compare good.
		     */
		    if (RecordIsValidForCommitTestOnly(ti) < 0) {
			if (ScanInstanceRemainderValid(ti, r->r_NextSame) == 0)
			    RecordIsValidForCommit(ti);
			continue;
		    }
		    DBASSERT(r->r_DelHash != NULL);
		    SaveDelHash(r->r_DelHash, &ti->ti_RanBeg,
				rh->rh_Hv, rh->rh_Size);
		} else if (r->r_Flags & RF_FORCESAVE) {
		    SaveDelHash(r->r_DelHash, &ti->ti_RanBeg,
				rh->rh_Hv, rh->rh_Size);
		}
	    }
	}
    }

    ti->ti_RanBeg = ranBeg;

    /*
     * Scan the table records for matches, filtering out any deletions
     */
    count = 0;
    rv = 0;

    {
	for (
	    SelectBegTableRec(ti, 0);
	    ti->ti_RanBeg.p_Ro > 0;
	    SelectNextTableRec(ti, 0)
	) {
	    const RecHead *rh;
	    int flags;

	    ++ti->ti_DebugScanCount;

	    /*
	     * cooperative multitasking
	     */
	    taskQuantum();

	    if (RecordIsValid(ti) < 0)
		continue;
	    /*
	     * We must allocate unspecified columns if doing an UPDATE, so
	     * we know what the contents of the unmentioned fields was.
	     */
	    if (ti->ti_Query && (ti->ti_Query->q_TermOp == QOP_UPDATE ||
		ti->ti_Query->q_TermOp == QOP_CLONE))
		flags = RDF_READ | RDF_ALLOC | RDF_ZERO;
	    else
		flags = RDF_READ | RDF_ZERO;
	    ReadDataRecord(ti->ti_RData, &ti->ti_RanBeg, flags);
	    rh = ti->ti_RData->rd_Rh;

	    /*
	     * Handle WHERE clause.  We may have to match against the
	     * deletion hash anyway - a case that occurs when the WHERE
	     * clause is operating on most special (__*) fields, since
	     * those fields may differentiate matching records.
	     */
	    if ((r->r_Type & ROPF_CONST) && 
		r->r_OpFunc(r->r_Col, r->r_Const) < 0
	    ) {
		if ((r->r_Flags & RF_FORCESAVE) &&
		    !(rh->rh_Flags & RHF_DELETE) &&
		    ti->ti_ScanOneOnly >= 0
		) {
		    (void)MatchDelHash(r->r_DelHash, rh);
		}
		continue;
	    }
	    if (ti->ti_ScanOneOnly >= 0) {
		/*
		 * Normal scan removes deleted records.  We should only
		 * encounter a deleted record when doing a ranged scan.
		 */
		if (RecordIsValidForCommitTestOnly(ti) < 0) {
		    if (ScanInstanceRemainderValid(ti, r->r_NextSame) == 0)
			RecordIsValidForCommit(ti);
		    continue;
		}
		if (rh->rh_Flags & RHF_DELETE) {
		    DBASSERT(ti->ti_ScanOneOnly == 0);
		    continue;
		}
		DBASSERT(r->r_DelHash != NULL);
		if (MatchDelHash(r->r_DelHash, rh) == 0)
		    continue;
		++ti->ti_ScanOneOnly;
		rv = r->r_RunRange(r->r_Next);
		--ti->ti_ScanOneOnly;
	    } else {
		/*
		 *  Special scan keeps deleted records (XXX do commit check?)
		 */
		--ti->ti_ScanOneOnly;
		rv = r->r_RunRange(r->r_Next);
		++ti->ti_ScanOneOnly;
	    }

	    /*
	     * If the select was interrupted, break out now.  Ultimately the
	     * top level scan will see the negative return code and mark 
	     * the delete hash as having been interrupted.
	     */
	    if (rv < 0)
		break;
	    count += rv;
	}
    }
    ti->ti_RanBeg = ranBeg;
    if (rv < 0)
	count = rv;
    return(count);
}

/*
 * DefaultIndexScanRangeOp2() - same as Op1, but scans in reverse
 *
 *	Scanning in reverse allows us to optimally track deletions.
 *	Not only can we do the scan in one pass, we also tend to
 *	avoid building up large deletion hash tables when scanning
 *	really huge tables.
 *
 *	This is kinda a hack.  Termination functions expect the
 *	position to be at ti_RanBeg rather then ti_RanEnd, but we
 *	are scanning backwards using ti_RanEnd so when we do a
 *	subroutine call we copy it (hack!) temporarily.
 */
int
DefaultIndexScanRangeOp2(Index *index, Range *r)
{
    TableI *ti = r->r_TableI;
    dbpos_t ranBeg;
    dbpos_t ranEnd;
    int count;
    int rv;

    /*
     * Special case - forward scan of 'raw' records.  This
     * is used by the synchronizer's Raw*() routines and for
     * history scans.
     *
     * case #2: unoptimizable operator, such as __opcode = 'blah', which
     * prevents us from being able to pair insertions and deletions.
     */
    if (ti->ti_ScanOneOnly < 0 || (r->r_Flags & RF_FORCESAVE))
	return(DefaultIndexScanRangeOp1(index, r));

    /*
     * Remember ranBeg, ranEnd for later restore 
     */
    ranBeg = ti->ti_RanBeg;
    ranEnd = ti->ti_RanEnd;

    /*
     * Scan the table records for matches, filtering out any deletions.
     * We scan in reverse, iterating ti_RanEnd backwards.
     */
    count = 0;
    rv = 0;

    {
	for (
	    SelectEndTableRec(ti, 0);
	    ti->ti_RanEnd.p_Ro > 0;
	    SelectPrevTableRec(ti, 0)
	) {
	    const RecHead *rh;
	    int flags;

	    ++ti->ti_DebugScanCount;

	    /*
	     * cooperative multitasking
	     */
	    taskQuantum();

	    if (RecordIsValid(ti) < 0)
		continue;
	    /*
	     * We must allocate unspecified columns if doing an UPDATE, so
	     * we know what the contents of the unmentioned fields was.
	     */
	    if (ti->ti_Query && (ti->ti_Query->q_TermOp == QOP_UPDATE ||
		ti->ti_Query->q_TermOp == QOP_CLONE))
		flags = RDF_READ | RDF_ALLOC | RDF_ZERO;
	    else
		flags = RDF_READ | RDF_ZERO;
	    ReadDataRecord(ti->ti_RData, &ti->ti_RanEnd, flags);
	    rh = ti->ti_RData->rd_Rh;

	    /*
	     * If the WHERE clause fails we may still be required to
	     * track deletions.  This occurs when the clause is on a 
	     * special __* field which would otherwise differentiate
	     * matching insert/delete records.
	     */
	    if ((r->r_Type & ROPF_CONST) &&
		r->r_OpFunc(r->r_Col, r->r_Const) < 0
	    ) {
#if 0
		if (r->r_Flags & RF_FORCESAVE) {
		    if (rh->rh_Flags & RHF_DELETE) {
			DBASSERT(ti->ti_ScanOneOnly == 0);
			SaveDelHash(r->r_DelHash, &ti->ti_RanEnd,
				    rh->rh_Hv, rh->rh_Size);
		    } else {
			(void)MatchDelHash(r->r_DelHash, rh);
		    }
		}
#endif
		continue;
	    }
	    /*
	     * Normal scan removes deleted records
	     */
	    if (RecordIsValidForCommitTestOnly(ti) < 0) {
		if (ScanInstanceRemainderValid(ti, r->r_NextSame) == 0)
		    RecordIsValidForCommit(ti);
		continue;
	    }
	    if (rh->rh_Flags & RHF_DELETE) {
		DBASSERT(ti->ti_ScanOneOnly == 0);
		SaveDelHash(r->r_DelHash, &ti->ti_RanEnd,
			    rh->rh_Hv, rh->rh_Size);
		continue;
	    }
	    DBASSERT(r->r_DelHash != NULL);
	    if (MatchDelHash(r->r_DelHash, rh) == 0)
		continue;
	    ++ti->ti_ScanOneOnly;
	    ti->ti_RanBeg = ti->ti_RanEnd;
	    rv = r->r_RunRange(r->r_Next);
	    ti->ti_RanBeg = ranBeg;
	    --ti->ti_ScanOneOnly;

	    /*
	     * Rather then mess around with Beg/End, just set the ScanOneOnly
	     * flag to enforce a single-record recursion.  This saves a lot
	     * of memory copying since dbpos_t is big.
	     */
	    if (rv < 0)
		break;
	    count += rv;
	}
    }
    ti->ti_RanEnd = ranEnd;
    if (rv < 0)
	count = rv;
    return(count);
}

/*
 * ScanInstanceRemainderValid()
 *	
 *	Try to match additional constant clauses within this table instance.
 *	Return 0 if all such clauses match, and -1 if any fail.  This is
 *	not a perfect test since join elements are not included, we use it
 *	only to implement fine-grained conflict resolution.  If we accidently
 *	return a match (0), it simply results in a somewhat larger conflict
 *	area.
 */
static int
ScanInstanceRemainderValid(TableI *ti, Range *r)
{
    while (r) {
	if ((r->r_Type & ROPF_CONST) && r->r_OpFunc(r->r_Col, r->r_Const) < 0)
	    return(-1);
	r = r->r_NextSame;
    }
    return(0);
}

/*
 * ConflictScanRangeOp()	- Look for potential commit-1/commit-1 
 *				  conflicts
 *
 *	0 is returned on success, -1 if a conflict was found.
 */

int
ConflictScanRangeOp(Range *r, Conflict *tts, int mySlot)
{
    TableI *ti = r->r_TableI;
    int slot;
    int rv = 0;

    /*
     * Locate active slots (including other threads from our pid) with 
     * sequence numbers less then our specific slot sequence number, meaning
     * that the scan will be non-inclusive of this specific slot.
     */
    slot = -1;
    while (rv == 0 && (slot = FindConflictSlot(tts, slot + 1)) >= 0) {
	const RecHead *rh;
	ConflictPos *pos;
	dboff_t ro;

#if 0
	printf(" Conflict %d/%d (%qx/%qx)\n", slot, mySlot,
	    tts->co_Head->ch_Slots[slot].cs_SeqNo,
	    tts->co_Head->ch_Slots[mySlot].cs_SeqNo
	);
#endif

	if (tts->co_Head->ch_Slots[slot].cs_SeqNo >=
	    tts->co_Head->ch_Slots[mySlot].cs_SeqNo
	) {
	    continue;
	}
	for (rh = FirstConflictRecord(tts, slot, &pos, &ro);
	     rh != NULL;
	     rh = NextConflictRecord(tts, slot, &pos, &ro)
	) {
	    ++ti->ti_DebugConflictC1Count;
	    ti->ti_RData->rd_Rh = rh;

#if 0
	    printf(">>> CONFLICT RH %p (%x) @ %qx\n", rh, rh->rh_Magic, ro);
	    printf(">>> SLOTINFO %qx/%qx\n", 
		tts->co_Head->ch_Slots[slot].cs_Off,
		tts->co_Head->ch_Slots[slot].cs_Size
	    );
#endif
	    DBASSERT(rh->rh_Magic == RHMAGIC);

	    if (RecordIsValid(ti) < 0)
		continue;
	    ReadDataRecord(ti->ti_RData, &ti->ti_RanBeg, RDF_READ|RDF_ZERO|RDF_USERH);
	    rh = ti->ti_RData->rd_Rh;
	    if (ScanInstanceRemainderValid(ti, r) < 0)
		continue;
	    rv = -1;
	    break;
	}
	ReleaseConflictRecord(tts, pos);
    }
    return(rv);
}

int
GetIndexOpClass(int colId, int opId)
{
    int opClass;

    switch(colId) {
    case CID_RAW_TIMESTAMP:
        opClass = ROP_STAMP_EQEQ;
        break;
    case CID_RAW_VTID:
        opClass = ROP_VTID_EQEQ;
        break;
    case CID_RAW_USERID:
        opClass = ROP_USERID_EQEQ;
        break;
    case CID_RAW_OPCODE:
        opClass = ROP_OPCODE_EQEQ;
        break;
    default:
        if (opId == ROP_LIKE || opId == ROP_RLIKE)
            opClass = ROP_LIKE;
        else
            opClass = ROP_EQEQ;
        break;
    }
    return(opClass);
}

