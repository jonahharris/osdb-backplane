/*
 * LIBDBCORE/SCAN.C	-
 *		Implements core table scanning ops, and default
 *		terminators for selection (which just counts), 
 *		deletions, updates, and insertions.
 *
 * (c)Copyright 1999-2002 Backplane, Inc.  Please refer to the COPYRIGHT
 * file at the base of the distribution tree.
 *
 * $Backplane: rdbms/libdbcore/scan.c,v 1.46 2002/09/21 21:08:53 dillon Exp $
 */

#include "defs.h"

Export int RunQuery(Query *q);

Prototype int RunRange(RangeArg a);

Prototype int OpGtStampMatch(const ColData *d1, const ColData *d2);
Prototype int OpGtEqStampMatch(const ColData *d1, const ColData *d2);
Prototype int OpLtStampMatch(const ColData *d1, const ColData *d2);
Prototype int OpLtEqStampMatch(const ColData *d1, const ColData *d2);

Prototype int TermRange(RangeArg ra);

static int TermRangeCounter(Query *q);
static int TermRangeInsert(Query *q);
static int TermRangeDelete(Query *q);
static int TermRangeUpdate(Query *q);
static int TermRangeClone(Query *q);

/*
 * RunQuery() -	Execute a query
 *
 *	Execute a query or sequence of queries that have been built by 
 *	the low or high level query routines.
 */

int
RunQuery(Query *q)
{
    int r;
    TableI *ti;

    for (ti = q->q_TableIQBase; ti; ti = ti->ti_Next) {
	ti->ti_Rewind = ti->ti_Table->ta_Append;
    }
    r = q->q_RunRange(q->q_RangeArg);

    /*
     * A negative return value can occur for a number of reasons but it is
     * usually due to an insert or update failing (e.g. duplicate record).
     * Rewinds only occur when operating on temporary tables - it is not
     * possible to rewind a root table.
     *
     * DBERR_LIMIT_ABORT is not really an error.  It means that the SELECT
     * was aborted due to hitting the requested record limit and is converted
     * into the record limit.
     */
    if (r < 0) {
	switch(r) {
	case DBERR_LIMIT_ABORT:
	    r = q->q_MaxRows;
	    q->q_Flags &= ~QF_CLIENT_LIMIT;
	    break;
	default:
	    for (ti = q->q_TableIQBase; ti; ti = ti->ti_Next) {
		RewindDataWrites(ti->ti_Table, ti->ti_Rewind);
	    }
	    break;
	}
    }
    return(r);
}

int
TermRange(RangeArg ra)
{
    Query *q = ra.ra_VoidPtr;

    switch(q->q_TermOp) {
    case QOP_SELECT:
	if ((q->q_Flags & (QF_WITH_LIMIT|QF_WITH_ORDER)) == QF_WITH_LIMIT) {
	    if (q->q_CountRows < q->q_StartRow) {
		++q->q_CountRows;
		return(0);
	    }
	    if (q->q_CountRows >= q->q_MaxRows + q->q_StartRow)
		return(DBERR_LIMIT_ABORT);
	}
	++q->q_CountRows;
	if (q->q_TermFunc)
	    return(q->q_TermFunc(q));
	return(TermRangeCounter(q));
    case QOP_COUNT:
	if ((q->q_Flags & (QF_WITH_LIMIT|QF_WITH_ORDER)) == QF_WITH_LIMIT) {
	    if (q->q_CountRows < q->q_StartRow) {
		++q->q_CountRows;
		return(0);
	    }
	    if (q->q_CountRows >= q->q_MaxRows + q->q_StartRow)
		return(DBERR_LIMIT_ABORT);
	}
	++q->q_CountRows;
	return(TermRangeCounter(q));
    case QOP_INSERT:
	return(TermRangeInsert(q));
    case QOP_DELETE:
	return(TermRangeDelete(q));
    case QOP_UPDATE:
	return(TermRangeUpdate(q));
    case QOP_CLONE:
	return(TermRangeClone(q));
    case QOP_COMMIT_CHECK:
	DBASSERT(0);
    case QOP_SYSQUERY:
	return(LLTermSysQuery(q));
    case QOP_TRANS:
	return(0);
    }
    return(-1);
}

/*
 * RunRange() - execute table scan
 *
 *	This is a recursive function which runs the optimized query denoted
 *	by a linked list of Range structures.  At the end of the recursion
 *	chain is a termination function.  The termination function is usually
 *	I/O but can be other things, like a counting function for example.
 */

int
RunRange(RangeArg a)
{
    Range *r = a.ra_RangePtr;
    TableI *ti = r->r_TableI;
    int count;

    /*
     * If this is the first scan reference to this table instance we
     * must iterate through all physical tables making up the instance,
     * otherwise we just call ti_ScanRangeOp() on the already-restricted
     * and possibly indexed range.
     *
     * We iterate backwards through physical tables in order to track
     * deletions.  The indexing code is responsible for populating the
     * deletion hash table but it should be noted that a record creation and
     * deletion occuring within the same physical table may be handled by
     * the indexing code 'silently', without adding it to the deletion hash.
     *
     * Indexing is handled by the Get*Table() routines, which assign the
     * range and index it if possible, using the first Range structure to
     * prime the range.
     *
     * Additional indexed restrictions may be emplaced on the table instance
     * only for expressions operating on the same column.  This is handled
     * through the i_UpdateTableRange() call.
     */
    if (r->r_PrevSame == NULL) {
	DelHash delHash;
	int rv;

	InitDelHash(&delHash);

	/*
	 * Relax deletion hash table requirements if a special column
	 * (like __timestamp) is used inside a WHERE clause.  Most special
	 * columns cannot participate in query optimization and are
	 * delete-tracked.  However, there is one case, __timestamp > 'blah',
	 * where we DO optimize the query and delete records could be left
	 * sitting around.
	 */
	if (ti->ti_Query && (ti->ti_Query->q_Flags & QF_SPECIAL_WHERE))
	    delHash.dh_Flags |= DHF_SPECIAL;
	r->r_DelHash = &delHash;

	/*
	 * Scan the normal transaction stack
	 */
	count = 0;
	for (
	    rv = GetLastTable(ti, r);
	    rv == 0;
	    rv = GetPrevTable(ti, r)
	) {
	    rv = ti->ti_ScanRangeOp(ti->ti_Index, r);
#if 0
	    CloseIndex(&ti->ti_Index, 0);
#endif
	    if (rv < 0) {
		count = rv;
		delHash.dh_Flags |= DHF_INTERRUPTED;
		break;
	    }
	    count += rv;
#if 0
	    ti->ti_RData->rd_Table = NULL;
	    ti->ti_RData->rd_Rh = NULL;
#endif
	}
	CloseIndex(&ti->ti_Index, 0);
	DoneDelHash(&delHash);
	r->r_DelHash = NULL;

	/*
	 * Scan the special commit conflict table.  If the current
	 * transaction level TTsSlot is >= 0, then the parent table
	 * will have a valid (shared) TTs conflict structure from
	 * which we can pull conflict data.
	 */
	if ((ti->ti_Table->ta_Db->db_Flags & DBF_COMMIT1) && 
	    ti->ti_Table->ta_TTsSlot >= 0
	) {
	    rv = ConflictScanRangeOp(
		r, 
		ti->ti_Table->ta_Parent->ta_TTs, 
		ti->ti_Table->ta_TTsSlot
	    );
#if 0
	    ti->ti_RData->rd_Table = NULL;
	    ti->ti_RData->rd_Rh = NULL;
#endif
	    if (rv < 0) {
		ti->ti_Table->ta_Db->db_Flags |= DBF_COMMITFAIL | DBF_C1CONFLICT;
		count = 1;
	    }
	}
    } else {
	Index *index = ti->ti_Index;

	r->r_DelHash = r->r_PrevSame->r_DelHash;
	count = ti->ti_ScanRangeOp(index, r);
	r->r_DelHash = NULL;
    }
    return(count);
}

int
OpGtStampMatch(const ColData *d1, const ColData *d2)
{
    if (*(dbstamp_t *)d1->cd_Data > *(dbstamp_t *)d2->cd_Data)
        return(1);
    return(-1);
}

int
OpGtEqStampMatch(const ColData *d1, const ColData *d2)
{
    if (*(dbstamp_t *)d1->cd_Data >= *(dbstamp_t *)d2->cd_Data)
        return(1);
    return(-1);
}

int
OpLtStampMatch(const ColData *d1, const ColData *d2)
{
    if (*(dbstamp_t *)d1->cd_Data < *(dbstamp_t *)d2->cd_Data)
        return(1);
    return(-1);
}

int
OpLtEqStampMatch(const ColData *d1, const ColData *d2)
{
    if (*(dbstamp_t *)d1->cd_Data <= *(dbstamp_t *)d2->cd_Data)
        return(1);
    return(-1);
}

/*
 * Note that each termination range falls within a single physical table
 */

static int
TermRangeCounter(Query *q)
{
    TableI *ti;

    if ((ti = q->q_TableIQBase) != NULL) {
	return(1);
    } else {
	return(0);
    }
}

static int
TermRangeInsert(Query *q)
{
    TableI *ti = q->q_TableIQBase;
    int total;

    while (ti) {
	ColI *ci;

	/*
	 * Set the data fields for the insert.  CIF_ORDER or CIF_DEFAULT
	 * is implied if ci_Const is non-NULL.
	 */
	for (ci = ti->ti_FirstColI; ci; ci = ci->ci_Next) {
	    if (ci->ci_Const) {
		DBASSERT(ci->ci_Flags & (CIF_ORDER|CIF_DEFAULT));
		ci->ci_CData->cd_Data = ci->ci_Const->cd_Data;
		ci->ci_CData->cd_Bytes = ci->ci_Const->cd_Bytes;
	    }
	}
	/*
	 * Insert the physical record.
	 */
	InsertTableRec(ti->ti_Table, ti->ti_RData, ti->ti_VTable);
	ti = ti->ti_Next;
    }

    /*
     * insert returns 0 on success, a negative number on failure
     */
    total = HLCheckDuplicate(q);
    return(total);
}

/*
 * Note that each termination range falls within a single physical table,
 * but the physical deletion itself is appended to the current transaction's
 * table.
 */

static int
TermRangeDelete(Query *q)
{
    TableI *ti = q->q_TableIQBase;
    int total = 1;

    if (ti == NULL)
	total = 0;

    while (ti) {
	DeleteTableRec(ti->ti_Table, &ti->ti_RanBeg, ti->ti_VTable);
	ti = ti->ti_Next;
    }
    return(total);
}

/*
 * Note that each termination range falls within a single physical table,
 * but the physical deletion (update=delete+insert) itself is appended
 * to the current transaction's table.
 */

static int
TermRangeUpdate(Query *q)
{
    TableI *ti = q->q_TableIQBase;
    int total;

    if (ti == NULL)
	return(0);

    while (ti) {
	    ColI *ci;

	    /*
	     * Update the data fields.  (CIF_ORDER implied by ci_Const being
	     * non-NULL)
	     */
	    for (ci = ti->ti_FirstColI; ci; ci = ci->ci_Next) {
		if (ci->ci_Const) {
		    DBASSERT(ci->ci_Flags & CIF_ORDER);
		    ci->ci_CData->cd_Data = ci->ci_Const->cd_Data;
		    ci->ci_CData->cd_Bytes = ci->ci_Const->cd_Bytes;
		}
	    }
	    /*
	     * Update the table record
	     */
	    UpdateTableRec(ti->ti_Table, &ti->ti_RanBeg, ti->ti_RData, ti->ti_VTable);
	    ti = ti->ti_Next;
    }

    /*
     * Check for duplicate keys and/or duplicate unique fields.  This also
     * acts as a guard to the update.  Returns a negative error on failure,
     * 0 on success.
     */
    if ((total = HLCheckDuplicate(q)) == 0)
	total = 1;
    return(total);
}


static int
TermRangeClone(Query *q)
{
    TableI *ti = q->q_TableIQBase;
    int total;

    if (ti == NULL)
	return(0);

    while (ti) {
	    ColI *ci;

	    /*
	     * Update the data fields.  (CIF_ORDER implied by ci_Const being
	     * non-NULL)
	     */
	    for (ci = ti->ti_FirstColI; ci; ci = ci->ci_Next) {
		if (ci->ci_Const) {
		    DBASSERT(ci->ci_Flags & CIF_ORDER);
		    ci->ci_CData->cd_Data = ci->ci_Const->cd_Data;
		    ci->ci_CData->cd_Bytes = ci->ci_Const->cd_Bytes;
		}
	    }
	    /*
	     * Insert the physical record.  CLONE does not delete the
	     * original record.
	     */
	    InsertTableRec(ti->ti_Table, ti->ti_RData, ti->ti_VTable);
	    ti = ti->ti_Next;
    }

    /*
     * Check for duplicate keys and/or duplicate unique fields.  This also
     * acts as a guard to the update.  Returns a negative error on failure,
     * 0 on success.
     */
    if ((total = HLCheckDuplicate(q)) == 0)
	total = 1;
    return(total);
}


