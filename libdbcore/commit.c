/*
 * LIBDBCORE/COMMIT.C	- Commit Phase1/2 Low Level support
 *
 * (c)Copyright 2000-2002 Backplane, Inc.  Please refer to the COPYRIGHT
 * file at the base of the distribution tree.
 *
 * $Backplane: rdbms/libdbcore/commit.c,v 1.55 2002/10/03 21:54:13 dillon Exp $
 *
 *	Commiting a transaction is a two-phase process. 
 *
 *	Commit-Phase1:
 *
 *	    Copy all new records made during the transaction to the temporary
 *	    commit conflict lock area and lock the database.
 *
 *	    Clear out the data from the temporary table(s).
 *
 *	    Rerun the transaction's queries (rebuilding the temporary tables
 *	    on the fly, since intermediate state is required for the queries
 *	    to run properly), and look for commit conflicts at the same time.
 *	    (These queries would also scan other commit-1 data in the
 *	    conflict lock area).
 *
 *	    Unlock the database, return success, blocked, or failure.  A
 *	    'blocked' status is returned if any of the queries would conflict
 *	    with other client's commit-1 data in the conflict lock area.
 *
 *	Commit-DeadLock-Resolution:
 *
 *	    The replicator will collect the results from its phase-1 commit
 *	    ops.  If it cannot obtain a quorum due to too many 'blocked'
 *	    status returns it will initiate deadlock resolution.
 *
 *	    The conflict is resolved by calculating the phase-2 commit
 *	    timestamp from the quorum (inclusive of the blocked nodes)
 *	    and then passing that information to ALL nodes, initiating another
 *	    commit-1 cycle.  The nodes will block until they have sufficient
 *	    information from the conflicting nodes to make a final 
 *	    determination of the winner and will then return a 'success'
 *	    for that node and a 'failure' for the other nodes.
 *
 *	Commit-Phase2:
 *
 *	    Copy the record modifications to the real database
 */

#include "defs.h"

Export int Commit1(DataBase *db, dbstamp_t *minCTs, void (*callBack)(void *data, DataBase *db, int status), void *data, int flags);
Export void UnCommit1(DataBase *db);
Export void Abort1(DataBase *db);
Export int Commit2(DataBase *db, dbstamp_t cts, rhuser_t userid, int flags);
Prototype void CopyQueryUp(DataBase *db, int flags);

/*
 * Commit1() -	Phase1 commit
 *
 *	Returns 0 on success, -1 on failure.
 */

int 
Commit1(
    DataBase *db,
    dbstamp_t *minCTs,
    void (*callBack)(void *data, DataBase *db, int status),
    void *data,
    int flags
) {
    DataBase *par = db->db_Parent;
    Query *q;
    int r;
    int i;

    DBASSERT(par != NULL);

    /*
     * Subtransactions must always succeed since there is nobody competing
     * with a subtransaction against it's parent transaction.
     *
     * The returned MinCTs, in this case, is set to db_WriteTs (which is
     * typically FreezeTs - 1).
     */
    if (db->db_PushType == DBPUSH_TMP) {
	*minCTs = par->db_WriteTs;
	db->db_Flags |= DBF_COMMIT1;
	return(0);
    }
    DBASSERT(db->db_PushType == DBPUSH_TTS);

    /*
     * Create a conflict area for the data and copy.  We have to lock the
     * database before doing this so the conflict area remains synchronized
     * with the database through the commit-1 operation.
     *
     * Note that no (thread) task switches may occur while the database
     * is locked.
     */
    LockDatabase(par);
    CreateConflictArea(db);

    /*
     * Clear our temporary table(s) in preparation for rerunning the query
     * set.
     */
    for (i = 0; i < TAB_HSIZE; ++i) {
	Table *tab;

	for (tab = db->db_TabHash[i]; tab; tab = tab->ta_Next) {
	    if ((flags & CCF_FORCE) == 0)
		tab->ta_CleanSlate(tab);
	}
    }

    /*
     * Now we must rerun our queries looking for:
     *
     *	(A) conflicts against any data committed after the freeze timestamp
     *	    (we return COMMIT1 FAILURE)
     *
     *	(B) conflicts against any shared temporary tables entered
     *	    BEFORE ours.  (we return COMMIT1 BLOCKED).
     *
     *  We do not have to record our queries for other clients to run against
     *  their own mods because in commit phase 2 we simply copy the data
     *  records from the temporary space into the database (we don't run
     *  the queries a third time), and other clients have already checked
     *  their own queries against the data records so the copy will not
     *  effect them.
     */

    r = 0;
    db->db_CommitCheckTs = db->db_FreezeTs;
    db->db_CommitConflictTs = 0;
    db->db_FreezeTs = DBSTAMP_MAX;
    db->db_Flags |= DBF_COMMIT1;
    db->db_Flags &= ~(DBF_COMMITFAIL | DBF_C1CONFLICT);

#if 0
    printf("*** Commit1 %p %016qx\n", db, db->db_CommitCheckTs);
#endif

    for (q = db->db_RecordedQueryBase; q; q = q->q_RecNext) {
	int saveOp = q->q_TermOp;

	switch(q->q_TermOp) {
	case QOP_SELECT:
	case QOP_SYSQUERY:
	case QOP_COUNT:
	    /*
	     * These queries are turned into simple selection tests
	     */
	    q->q_TermOp = QOP_COUNT;
	    r = RunQuery(q);
	    r = 0;
	    break;
	case QOP_DELETE:
	case QOP_UPDATE:
	case QOP_CLONE:
	case QOP_INSERT:
	    /*
	     * These queries must be run normally, unless part of a 
	     * subtransaction that was rolledback.  If part of a rolled
	     * back transaction only the GUARD queries associated with 
	     * these queries are run.  XXX Unfortunately, this is very
	     * difficult at the moment because subtransactions are
	     * collapsed together into this parent, so I am punting.  This
	     * means that GUARD queries associated with rolled-back
	     * subtransactions will *not* be run, thus a conflict with
	     * the rolled-back subtransaction will not be recognized.
	     */
	    if (q->q_Flags & QF_ROLLEDBACK)
		continue;
	    r = RunQuery(q);
	    r = 0;
	    break;
	case QOP_TRANS:
	    /*
	     * Ignore TRANSactional queries, their encapsulated
	     * transaction will be moved up to the parent on a successful
	     * commit leaving an empty shell for this q.
	     */
	    continue;
	default:
	    DBASSERT(0);
	    break;
	}

	q->q_TermOp = saveOp;

	if (db->db_Flags & DBF_COMMITFAIL) {
	    r = -1;
	    break;
	}
    }
    db->db_FreezeTs = db->db_CommitCheckTs;
    db->db_CommitCheckTs = DBSTAMP_MAX;
#if 0
    printf("Commit1 %p DONE\n", db);
#endif
    if ((db->db_Flags & (DBF_COMMITFAIL|DBF_C1CONFLICT)) == 0)
	AssertConflictAreaSize(db);

    /*
     * We unlock the database, allowing other client's conflict blocks to
     * be deleted
     */
    UnLockDatabase(par);

    /*
     * On success set the COMMIT1 flag and allocate a proposed commit time
     * stamp.
     */
    if (r >= 0) {
	*minCTs = AllocStamp(par, *minCTs);
    } else {
	*minCTs = db->db_CommitConflictTs;
	UnCommit1(db);
	printf(">>> drd_database, Commit Failed %016qx\n", db->db_CommitConflictTs);
    }
    return(r);
}

/*
 * UnCommit1() - undo the effects a phase-1 commit
 */

void 
UnCommit1(DataBase *db)
{
    DBASSERT(db->db_Flags & DBF_COMMIT1);
    DBASSERT(db->db_Parent != NULL);

    /*
     * We shortcutted subtransaction commit-1's in Commit1(), so
     * we have to shortcut them here to.
     */
    if (db->db_PushType == DBPUSH_TMP) {
	db->db_Flags &= ~DBF_COMMIT1;
	return;
    }
    DBASSERT (db->db_PushType == DBPUSH_TTS);

    /*
     * Release previously obtained conflict areas.  XXX at the moment we
     * must do this while the database is held locked to avoid data being
     * ripped out from under other C1 checks.
     */
    LockDatabase(db->db_Parent);
    FreeConflictArea(db);
    UnLockDatabase(db->db_Parent);

    /*
     * Even when aborting we have to move queries from any sub transactions
     * up, since they must still be used to form a guard within the higher
     * level transaction.
     */
    CopyQueryUp(db, QF_ROLLEDBACK);
    db->db_Flags &= ~DBF_COMMIT1;
}

/*
 * CopyQueryUp() -	Copy Query's up when popping a level
 *
 *	Queries are popped up in order to be available for conflict 
 *	resolution at a higher level.  However, queries are not popped
 *	up into the root transaction since that represents the terminus
 *	of the transaction, and there is nothing more to be done at
 *	that point.
 *
 *	If we are in a read-only transaction we do not need to keep queries
 *	around for commit ops and free them rather then pop them up.
 */

void
CopyQueryUp(DataBase *db, int flags)
{
    DataBase *par = db->db_Parent;

    if (par->db_PushType != DBPUSH_ROOT) {
	Query *q;

	for (q = db->db_RecordedQueryBase; q; q = q->q_RecNext) {
	    TableI *ti;

	    q->q_Flags |= flags;

	    for (ti = q->q_TableIQBase; ti; ti = ti->ti_Next) {
		Table *otab;
		DataMap *dm;

		if ((dm = ti->ti_RData->rd_Map) != NULL)
		    dm->dm_Table->ta_RelDataMap(&ti->ti_RData->rd_Map, 1);
		DBASSERT(ti->ti_Index == NULL);
		otab = ti->ti_Table;
		ti->ti_Table = ti->ti_Table->ta_Parent;
		DBASSERT(ti->ti_Table != NULL);
		DBASSERT(ti->ti_Table->ta_Db == par);
		ti->ti_RData->rd_Table = ti->ti_Table;
		DBASSERT(ti->ti_Table->ta_Refs > 0); /* must already have ref*/
		++ti->ti_Table->ta_Refs;
		CloseTable(otab, 1);
	    }
	    q->q_Db = par;
	}
	if (db->db_RecordedQueryBase != NULL) {
	    *par->db_RecordedQueryApp = db->db_RecordedQueryBase;
	    par->db_RecordedQueryApp = db->db_RecordedQueryApp;
	    db->db_RecordedQueryBase = NULL;
	    db->db_RecordedQueryApp = &db->db_RecordedQueryBase;
	}
    }
}

/*
 * Abort1() -	Phase1 abort
 *
 *	Generally called to abort after Commit1() has succeeded.  This routine
 *	is also called by Commit1() itself if commit1 fails (the caller of
 *	commit1 should not call abort1 if commit1 fails).
 *
 *	We just leave the queries on the current database's record chain.
 *	When the current db is closed, the queries will be thrown away
 *	along with the temporary tables.
 */

void
Abort1(DataBase *db)
{
    DataBase *par = db->db_Parent;

    DBASSERT(par != NULL);
    UnCommit1(db);
}

/*
 * Commit2() -	Phase2 commit
 *
 *	This routine is called only after Commit1() has succeeded, and
 *	there may be further restrictions imposed by the replicator
 *	in regards to quourm operations.
 */

int
Commit2(DataBase *db, dbstamp_t cts, rhuser_t userid, int flags)
{
    DataBase *par = db->db_Parent;
    int r = 0;
    int i;

#if 0
    printf("*** Commit2 %p %016qx\n", db, cts);
#endif

    DBASSERT(par != NULL);
    DBASSERT(db->db_Flags & DBF_COMMIT1);
    DBASSERT(cts > db->db_SysTable->ta_Meta->tf_SyncStamp);
    /*
     * XXX eventually remove this, a C1 conflict error is ok if a 
     *	   quorum says it is.
     */
    DBASSERT((db->db_Flags & (DBF_C1CONFLICT|DBF_COMMITFAIL)) == 0);

    /*
     * Copy physical data from the current transaction level to
     * its parent.  As with any operation which must lock the database,
     * this must be non-blocking (nor can it issue a thread switch).
     *
     * We destroy any temporary table spaces as we process them.
     *
     * XXX collapse in-transaction deletions 
     */
    LockDatabase(par);
    for (i = 0; i < TAB_HSIZE; ++i) {
	Table *tab;

	for (tab = db->db_TabHash[i]; tab; tab = tab->ta_Next) {
	    Table *parTab;
	    RawData *rd;
	    TableI *ti;
	    int didAny = 0;
	    rhflags_t lastOp = 0;

	    parTab = tab->ta_Parent;

	    /*
	     * Refs should generally never be 0, since we pop-up all
	     * queries from lower levels (whether they are rolled back or
	     * not).
	     */
	    if (tab->ta_Refs == 0) {
		fprintf(stderr, "Warning: Refs == 0 (%s)\n", tab->ta_Name);
		continue;
	    }
	    rd = AllocRawData(tab, NULL, 0);
	    ti = AllocPrivateTableI(rd);

	    /*
	     * tab->ta_Parent after this point is the real parent, not
	     * the shared temporary table.
	     */
	    GetLastTable(ti, NULL);

	    for (
		SelectBegTableRec(ti, 0);
		ti->ti_RanBeg.p_Ro >= 0;
		SelectNextTableRec(ti, 0)
	    ) {
		const RecHead *rh;

		ReadDataRecord(rd, &ti->ti_RanBeg, 0);	/* get rd_Rh */
		rh = rd->rd_Rh;

		/*
		 * Sanity.  If this record is an update, the last one had
		 * better have been a deletion.
		 */
		if (rh->rh_Flags & RHF_UPDATE)
			DBASSERT(lastOp & RHF_DELETE);

		WriteDataRecord(parTab, NULL, rh, rh->rh_VTableId,
		    cts, userid, rh->rh_Flags);
		didAny = 1;
		lastOp = rh->rh_Flags;
	    }
	    LLFreeTableI(&ti);

	    /*
	     * Database table integrity.  This code typically writes
	     * the appropriate information to the log.  The writes above,
	     * at worst, appended to the table file and nobody will see them
	     * until we update the table header's append point, which is
	     * NOT done until SynchronizeDatabase().
	     */
	    if (didAny)
		SynchronizeTable(parTab);
	}
    }

    /*
     * Tail end of the database integrity code.  Finish writing out the log,
     * fsync it, and then do any necessary delayed updates of the tables 
     * (typically only updating their append point in the header) as well
     * as synchronizing any index changes that have occured.
     */
    SynchronizeDatabase(par, cts);

    /*
     * Release previously obtained conflict areas.  Note that the database
     * lock is held while the conflict area is being freed, to avoid the
     * space being ripped up while a C1 check is in progress.  XXX
     */
    FreeConflictArea(db);
    UnLockDatabase(par);

    /*
     * If this is a sub transaction we have to move the queries to
     * the next higher transaction level so they are included in
     * the guard for that transaction's commit.
     *
     * If this is a top-level transaction we can allow the queries
     * to be thrown away when the current database level is popped.
     */
    CopyQueryUp(db, 0);

    db->db_Flags &= ~DBF_COMMIT1;

    return(r);
}


