/*
 * LIBDBCORE/HLQUERY.C	- High level query building
 *
 * (c)Copyright 1999-2002 Backplane, Inc.  Please refer to the COPYRIGHT
 * file at the base of the distribution tree.
 *
 * $Backplane: rdbms/libdbcore/hlquery.c,v 1.46 2002/08/20 22:05:51 dillon Exp $
 */

#include "defs.h"

Export Range *HLAddClause(Query *q, Range *lr, TableI *ti, const ColData *col1, const ColData *const2, int opId, int type);
Export SchemaI *HLGetSchemaI(Query *q, const char *scmName, int scmLen);
Export TableI *HLGetTableI(Query *q, SchemaI *si, const char *useName, int useLen, const char *aliasName, int aliasLen);
Export TableI *HLFindTableI(Query *q, const char *useName, int useLen);
Export ColI *HLGetColI(Query *q, const char *useName, int useLen, int flags);
Export ColI *HLGetRawColI(Query *q, TableI *ti, col_t col);
Export int HLResolveNullScans(Query *q);
Export int HLCheckFieldRestrictions(Query *q, int flags);
Export int HLCheckDuplicate(Query *q);

Prototype void FreeRangeList(Range *r);

static int OpEqEqVTIdMatch(const ColData *d1, const ColData *d2);
static int OpEqEqUserIdMatch(const ColData *d1, const ColData *d2);
static int OpEqEqOpCodeMatch(const ColData *d1, const ColData *d2);

/*
 * Generate a WHERE clause.  Clauses are ANDed.
 *
 *	This adds a WHERE clause to the query.  WHERE clauses are tied into
 *	their associated TableI's as well as linked serially relative to
 *	the query.  Each TableI must generally wind up with at least one WHERE 
 *	clause to enforce a scan on that table but the resolver will handle
 *	certain cases for us.
 *
 *	When appending a new clause we have to shift the terminator from 
 *	the previous structure to the new one.
 */

Range *
HLAddClause(Query *q, Range *lr, TableI *ti, const ColData *col1, 
    const ColData *const2, int opId, int type)
{
    Range *r = zalloc(sizeof(Range));
    Range *s;

    if (type & ROPF_CONST) {
	int stampOpt = 0;
	dataop_func_t *opary;

	DBASSERT(col1->cd_DataType != 0);
	opary = DataTypeFuncAry[col1->cd_DataType];

	switch(opId) {
	case ROP_LIKE:
	    r->r_OpFunc = opary[DATAOP_LIKE];
	    break;
	case ROP_RLIKE:
	    r->r_OpFunc = opary[DATAOP_RLIKE];
	    break;
	case ROP_SAME:
	    r->r_OpFunc = opary[DATAOP_SAME];
	    break;
	case ROP_RSAME:
	    r->r_OpFunc = opary[DATAOP_RSAME];
	    break;
	case ROP_EQEQ:
	    r->r_OpFunc = opary[DATAOP_EQEQ];
	    break;
	case ROP_NOTEQ:
	    r->r_OpFunc = opary[DATAOP_NOTEQ];
	    break;
	case ROP_LT:
	    r->r_OpFunc = opary[DATAOP_LT];
	    break;
	case ROP_LTEQ:
	    r->r_OpFunc = opary[DATAOP_LTEQ];
	    break;
	case ROP_GT:
	    r->r_OpFunc = opary[DATAOP_GT];
	    break;
	case ROP_GTEQ:
	    r->r_OpFunc = opary[DATAOP_GTEQ];
	    break;
	case ROP_STAMP_LT:
	    r->r_OpFunc = OpLtStampMatch;
	    break;
	case ROP_STAMP_LTEQ:
	    r->r_OpFunc = OpLtEqStampMatch;
	    break;
	case ROP_STAMP_GT:
	    r->r_OpFunc = OpGtStampMatch;
	    stampOpt = 1;
	    break;
	case ROP_STAMP_GTEQ:
	    r->r_OpFunc = OpGtEqStampMatch;
	    stampOpt = 1;
	    break;
	case ROP_VTID_EQEQ:
	    r->r_OpFunc = OpEqEqVTIdMatch;
	    stampOpt = -1;
	    break;
	case ROP_USERID_EQEQ:
	    r->r_OpFunc = OpEqEqUserIdMatch;
	    /* stampOpt already 0; */
	    break;
	case ROP_OPCODE_EQEQ:
	    r->r_OpFunc = OpEqEqOpCodeMatch;
	    /* stampOpt already 0; */
	    break;
	default:
	    DBASSERT(0);
	    break;
	}

	/*
	 * RF_FORCESAVE will prevent indexers from optimizing their
	 * ranges based on this clause, and will force the deletion
	 * hash to track records even if they do not match the clause.
	 * This is necessasry for __* special fields that would otherwise
	 * differentiate insertion/deletion record matchups.
	 *
	 * QF_SPECIAL_WHERE will relax deletion hash requirements (at the
	 * cost of cpu) for certain special cases.
	 */
	if ((col_t)col1->cd_ColId < CID_RAW_LIMIT) {
	    switch(stampOpt) {
	    case -1:
		/*
		 * This op may be optimized and does not require
		 * special delete-hash handling.
		 */
		break;
	    case 0:
		/*
		 * This op may not be optimized because it may differentiate
		 * insertion/deletion pairs (e.g. __userid = 'blah' or
		 * __timestamp = 'blah').  Note that __timestamp based
		 * inequalities can almost always be optimized.
		 */
		r->r_Flags |= RF_FORCESAVE;
		break;
	    case 1:
		/*
		 * This op may be optimized but requires special delete-hash
		 * handling because the insertion/deletion count may not match
		 * up.  The special handling is only required for normal 
		 * scans, not historical scans.  Note that the rawscan code
		 * uses a historical scan and does not have a 'q'uery, so
		 * this optimization is mandatory.
		 */
		if (ti->ti_ScanOneOnly >= 0)
		    q->q_Flags |= QF_SPECIAL_WHERE;
		break;
	    }
	}
    }

    if (lr) {
	r->r_RunRange = lr->r_RunRange;
	r->r_Next = lr->r_Next;
	lr->r_RunRange = RunRange;
	lr->r_Next.ra_RangePtr = r;
    } else if (q) {
	r->r_RunRange = q->q_RunRange;
	r->r_Next = q->q_RangeArg;
	q->q_RunRange = RunRange;
	q->q_RangeArg.ra_RangePtr = r;
    }
    r->r_Prev = lr;
    r->r_TableI = ti;
    r->r_Col = col1;
    r->r_Const = const2;
    r->r_OpId = opId;
    r->r_OpClass = GetIndexOpClass((col1 ? (col_t)col1->cd_ColId : 0), opId);
    r->r_Type = type;

    if (ti->ti_MarkRange == NULL) {
	ti->ti_MarkRange = r;
    } else {
	for (s = r->r_Prev; s; s = s->r_Prev) {
	    if (s->r_TableI == ti) {
		r->r_PrevSame = s;
		s->r_NextSame = r;
		break;
	    }
	}
	DBASSERT(s != NULL);
    }
    return(r);
}

/*
 * FreeRangeList() - called mainly by sync.c, query.c has its own
 *		     range freeing routine.
 */
void
FreeRangeList(Range *nr)
{
    Range *r;

    while ((r = nr) != NULL) {
	if (r->r_RunRange == RunRange)
	    nr = r->r_Next.ra_RangePtr;
	else
	    nr = NULL;
	zfree(r, sizeof(Range));
    }
}

/*
 * Obtain an (unresolved) schema instance
 */

SchemaI *
HLGetSchemaI(Query *q, const char *scmName, int scmLen)
{
    SchemaI *si;

    if (scmLen >= MAX_ID_BUF)
	return(NULL);

    for (si = q->q_FirstSchemaI; si; si = si->si_Next) {
	if (si->si_ScmNameLen == scmLen &&
	    strncasecmp(si->si_ScmName, scmName, scmLen) == 0
	) {
	    return(si);
	}
    }

    if ((si = LLGetSchemaI(q->q_Db, scmName, scmLen)) == NULL) {
	return(NULL);
    }

    DBASSERT(si->si_Next == NULL);

    si->si_Query = q;
    si->si_Next = q->q_FirstSchemaI;
    q->q_FirstSchemaI = si;

    return(si);
}

/*
 * HLGetTableI() -	Create a table instance based on an alias
 *
 *	This routine must instantiate the table instance and initialize
 *	it.  Note that the range fields ti_RanBeg and ti_RanEnd are not
 *	initialized here, but in the range scan instead.
 */
TableI *
HLGetTableI(Query *q, SchemaI *si, const char *useName, int useLen, const char *aliasName, int aliasLen)
{
    TableI *ti;
    Table *tab;
    const char *scmName;
    const char *tabName;
    int scmLen;
    int tabLen;
    int error = 0;

    for (scmLen = 0; scmLen < useLen; ++scmLen) {
	if (useName[scmLen] == '.')
	    break;
    }
    if (scmLen == useLen) {
	/*
	 * 'table' form
	 */
	scmName = "";
	scmLen = 0;
	tabName = useName;
	tabLen = useLen;
    } else {
	/*
	 * 'schema.table' form
	 */
	scmName = useName;
	tabName = useName + scmLen + 1;
	tabLen = useLen - scmLen - 1;
    }

    if (tabLen >= MAX_ID_BUF || scmLen >= MAX_ID_BUF)
	return(NULL);

    if (scmLen)
	si = HLGetSchemaI(q, scmName, scmLen);

    if (si == NULL)
	return(NULL);

    if ((ti = LLGetTableI(q->q_Db, si, tabName, tabLen)) == NULL) {
	return(NULL);
    }
    DBASSERT(ti->ti_Next == NULL);

    tab = OpenTable(q->q_Db, ti->ti_TableFile, "dt0", NULL, &error);
    if (tab == NULL) {
	dberror("Unable to open physical table %s\n", ti->ti_TableFile);
	return(NULL);
    }
    ti->ti_Query = q;
    ti->ti_Table = tab;
    if (aliasLen) {
	ti->ti_AliasName = safe_malloc(aliasLen + 1);
	ti->ti_AliasNameLen = aliasLen;
	bcopy(aliasName, ti->ti_AliasName, aliasLen);
	ti->ti_AliasName[aliasLen] = 0;
    }
    ti->ti_RData = AllocRawData(tab, NULL, 0);

    ti->ti_Next = q->q_TableIQBase;
    q->q_TableIQBase = ti;

    return(ti);
}

TableI *
HLFindTableI(Query *q, const char *useName, int useLen)
{
    TableI *ti;

    if ((ti = q->q_TableIQBase) == NULL)
	return(NULL);
    if (ti->ti_Next == NULL && useLen == 0)
	return(ti);
    while (ti) {
	if (ti->ti_AliasNameLen && 
	    useLen == ti->ti_AliasNameLen &&
	    strncasecmp(ti->ti_AliasName, useName, useLen) == 0
	) {
	    break;
	}
	ti = ti->ti_Next;
    }
    return(ti);
}

/*
 * Obtain a named column structure and associate it with the specified 
 * Query.  Create TableI's and SchemaI's as required.  The named
 * column can be in the following forms:
 *
 *	table.column
 *	column
 *
 * NOTE!  If CIF_DEFAULT is passed, ci_Const is loaded with the default
 *	  value for the column (if any).
 *
 * NOTE!  When selecting all columns useName will be NULL and useLen 0.
 *	  If CIF_WILD is set, deleted columns will not be included (typical
 *	  when parsing an SQL command).  If CIF_WILD is not set, however,
 *	  this routine is being used to get a complete column list whether
 *	  deleted or not so deleted columns are included.
 */
ColI *
HLGetColI(Query *q, const char *useName, int useLen, int flags)
{
    ColI *ci;
    TableI *ti;
    const char *tabName;
    const char *colName;
    int tabLen;
    int colLen;

    /*
     * parse column specification and locate the table instance
     */
    for (tabLen = useLen - 1; tabLen >= 0; --tabLen) {
	if (useName[tabLen] == '.')
	    break;
    }
    if (tabLen < 0) {
	/*
	 * column
	 */
	tabName = "";
	tabLen = 0;
	colName = useName;
	colLen = useLen;
    } else {
	/*
	 * table.column
	 */
	tabName = useName;
	colName = useName + tabLen + 1;
	colLen = useLen - tabLen - 1;
    }

    /*
     * Locate table instance
     */
    if ((ti = HLFindTableI(q, tabName, tabLen)) == NULL)
	return(NULL);

    /*
     * If no column instances have been loaded, get the whole list
     */
    if (ti->ti_FirstColI == NULL) {
	if ((ci = LLGetColI(q->q_Db, ti, NULL, 0)) == NULL)
	    return(NULL);
	ti->ti_FirstColI = ci;
	while (ci) {
	    ci->ci_TableI = ti;
	    ci = ci->ci_Next;
	}
    }

    /*
     * Locate the requested column instance.  XXX if same column specified
     * twice with CIF_ORDER set, an error is returned.  e.g. SELECT a, a FROM
     *
     * If colName is NULL we are selecting all columns.  XXX messy.
     */
    for (ci = ti->ti_FirstColI; ci; ci = ci->ci_Next) {
	if (colName == NULL ||
	    (ci->ci_ColNameLen == colLen &&
	    strncasecmp(ci->ci_ColName, colName, colLen) == 0)
	) {
	    /*
	     * Do not include deleted columns if wildcarding.
	     */
	    if (colName == NULL && (ci->ci_Flags & CIF_DELETED))
		continue;

	    /*
	     * Do not include special columns unless explicitly requested
	     * and allowed.
	     */
	    if (ci->ci_Flags & CIF_SPECIAL) {
		if ((flags & CIF_SPECIAL) == 0)
		    continue;
		if (colName == NULL)
		    continue;
	    }

	    /*
	     * INSERTs inherit column defaults
	     */
	    if ((flags & CIF_DEFAULT) && ci->ci_Const == NULL && ci->ci_Default)
		ci->ci_Const = GetConst(q, ci->ci_Default, ci->ci_DefaultLen);

	    /*
	     * Assign the record scan raw column to the column
	     */
	    DBASSERT(ci->ci_DataType != 0);
	    if (ci->ci_CData == NULL)
		ci->ci_CData = GetRawDataCol(ti->ti_RData, ci->ci_ColId, ci->ci_DataType);

	    if (flags & CIF_ORDER) {
		if (ci->ci_Flags & CIF_ORDER)
		    return(NULL);
		*q->q_ColIQAppend = ci;
		q->q_ColIQAppend = &ci->ci_QNext;
		ci->ci_OrderIndex = q->q_OrderCount;
		++q->q_OrderCount;
	    }
	    if (flags & CIF_SORTORDER) {
		if (ci->ci_Flags & CIF_SORTORDER)
		    return(NULL);
		*q->q_ColIQSortAppend = ci;
		q->q_ColIQSortAppend = &ci->ci_QSortNext;
		if (ci->ci_OrderIndex < 0)
		    ci->ci_OrderIndex = q->q_OrderCount + q->q_IQSortCount;
		++q->q_IQSortCount;
	    }
#if 0
	    if ((flags & CIF_SET_SPECIAL) && (ci->ci_Flags & CIF_SPECIAL))
		q->q_Flags |= QF_SPECIAL_WHERE;
#endif
	    ci->ci_Flags |= flags & ~(CIF_SPECIAL | CIF_SET_SPECIAL | CIF_DEFAULT);
	    if (colName != NULL)
		return(ci);
	}
    }
    if (colName == NULL)
	ci = ti->ti_FirstColI;
    return(ci);
}

ColI *
HLGetRawColI(Query *q, TableI *ti, col_t col)
{
    ColI *ci;
    ColI **pci;

    /*
     * If no column instances have been loaded, get the whole list
     */
    if (ti->ti_FirstColI == NULL) {
	if ((ci = LLGetColI(q->q_Db, ti, NULL, 0)) == NULL)
	    return(NULL);
	ti->ti_FirstColI = ci;
	while (ci) {
	    ci->ci_TableI = ti;
	    ci = ci->ci_Next;
	}
    }
    for (pci = &ti->ti_FirstColI; (ci = *pci) != NULL; pci = &ci->ci_Next) {
	if (ci->ci_ColId == col)
	    break;
    }
    if (ci == NULL) {
	ci = *pci = zalloc(sizeof(ColI));
	ci->ci_Size = sizeof(ColI);
	ci->ci_TableI = ti;
	ci->ci_ColId = col;
	ci->ci_OrderIndex = -1;
	/*
	 * XXX see calls to this function.  The ROP had better override
	 * this.
	 */
	ci->ci_DataType = DATATYPE_STRING;
    }
    if (ci->ci_CData == NULL)
	ci->ci_CData = GetRawDataCol(ti->ti_RData, ci->ci_ColId, ci->ci_DataType);
    return(ci);
}

/*
 * Locate any TableI's in the query which have not yet been
 * ranged for the scan and add a range.  Any pre-existing expression
 * will typically use an index which is already restricted to
 * the appropriate VTable.  If there is no expression we add one
 * that tests for the VTable explicitly, thus eliminating other VTable's
 * from the scan.  This becomes most obvious when you have a huge table
 * and do something like 'select * FROM hugetable$cols;'.
 */

int
HLResolveNullScans(Query *q)
{
    TableI *ti;
    Range *r = NULL;

    for (ti = q->q_TableIQBase; ti; ti = ti->ti_Next) {
	if (ti->ti_MarkRange == NULL) {
	    if (r == NULL && q->q_RunRange == RunRange) {
		r = q->q_RangeArg.ra_RangePtr;
		while (r->r_RunRange == RunRange)
		    r = r->r_Next.ra_RangePtr;
	    }
	    /*
	     * XXX CID_RAW_VTID overrides DATATYPE_STRING. We need to
	     * pass something less confusing then DATATYPE_STRING here.
	     */
	    r = HLAddClause(
		    q, 
		    r,
		    ti,
		    GetRawDataCol(ti->ti_RData, CID_RAW_VTID, DATATYPE_STRING),
		    GetConst(q, &ti->ti_VTable, sizeof(ti->ti_VTable)),
		    ROP_VTID_EQEQ,
		    ROP_CONST
	    );
	    ti->ti_MarkRange = r;
	}
    }
    return(0);
}

/*
 * HLCheckFieldRestrictions() -		Check KEY and NOTNULL requirements
 *
 *	ti_FirstColI - linked list of all columns in table
 *	q_ColIQBase  - all columns inserted into or SET
 *
 * Note: KEY and UNIQUE fields are implicitly not-NULL.  This function is
 * called by UPDATE and INSERT.
 */
int
HLCheckFieldRestrictions(Query *q, int flags)
{
    TableI *ti;

    for (ti = q->q_TableIQBase; ti; ti = ti->ti_Next) {
	ColI *ci;

	for (ci = ti->ti_FirstColI; ci; ci = ci->ci_Next) {
	    if (ci->ci_Flags & CIF_DELETED)
		continue;

	    /*
	     * Check for unspecified defaults and add them in.
	     */
	    if ((ci->ci_Flags & (CIF_ORDER|CIF_DEFAULT)) == CIF_DEFAULT &&
		(flags & CIF_DEFAULT)
	    ) {
		HLGetColI(q, ci->ci_ColName, ci->ci_ColNameLen, flags);
	    }

	    /*
	     * Ignore fields that do not have restrictions.  Note: KEY and
	     * UNIQUE implies NOT NULL, so at the moment our fall-through
	     * always checks not-null.  XXX for more complex requirements
	     * we should just have llquery set NOTNULL if it sees KEY or
	     * UNIQUE and then check NOTNULL below.
	     */
	    if ((ci->ci_Flags & (CIF_KEY|CIF_UNIQUE|CIF_NOTNULL)) == 0)
		continue;

	    /*
	     * All fields for an insert must be checked whether they were
	     * specified or not.  Only fields being changed by an update
	     * (the CIF_ORDER test below) need to be checked.
	     */
	    if (q->q_TermOp == QOP_INSERT || (ci->ci_Flags & CIF_ORDER)) {
		if (ci->ci_Const == NULL || ci->ci_Const->cd_Data == NULL)
		    return(-1);
	    } else {
		/*
		 * make sure the data is fetched if its a key field for
		 * HLCheckDuplicate()
		 */
		(void)GetRawDataCol(ti->ti_RData, ci->ci_ColId, ci->ci_DataType);
	    }
	}
    }
    return(0);
}

/*
 * HLCheckDuplicate() -		Check data in raw record for key and unique
 *				field requirements.  Called from INSERT,
 *				UPDATE, and CLONE.
 *
 *	#1: Construct a COUNT query out of the key fields and run it 
 *	    to determine whether the record would create a duplicate key.
 *	    We expect a return count of 1.
 *
 *	#2: Construct a COUNT query for each listed UNIQUE field and run it
 *	    to determine whether the record would create a duplicate unique
 *	    field.
 *
 *	We return 0 on success, and a DBERR_* if a duplicate is detected.
 *	We do not bother recording our query, since this test will also be
 *	run at commit-1.
 */
int
HLCheckDuplicate(Query *q)
{
    Query *nq;
    TableI *ti;
    ColI *ci;
    Range *r = NULL;  

    nq = GetQuery(q->q_Db);
    ti = q->q_TableIQBase;
    ti = GetTableIQuick(nq, ti->ti_Table, ti->ti_VTable, NULL, 0);
    DBASSERT(ti != NULL);
    DBASSERT(ti->ti_Next == NULL);

    /*
     * Check the set of keys from the insertion or update
     * column list.  ColIQBase is pre-arranged to have
     * at least the key fields.
     *
     * note: we can use ci->ci_Const or the GetRawDataCol results
     * from the parent query directly because we free nq rather
     * then release.  Otherwise we would have to dup the constant
     * structure and data.
     */
    for (ci = q->q_TableIQBase->ti_FirstColI; ci; ci = ci->ci_Next) {
        const ColData *cc;
        const ColData *rdc;
   
        if (ci->ci_Flags & CIF_DELETED) 
            continue;
        if ((ci->ci_Flags & CIF_KEY) == 0)
            continue;
	if (ci->ci_Const) {
	    rdc = GetRawDataCol(ti->ti_RData, ci->ci_ColId, ci->ci_DataType);
	    r = HLAddClause(nq, r, ti, rdc, ci->ci_Const, ROP_EQEQ, ROP_CONST);
	} else {
	    rdc = GetRawDataCol(ti->ti_RData, ci->ci_ColId, ci->ci_DataType);
	    cc = GetRawDataCol(q->q_TableIQBase->ti_RData, ci->ci_ColId, ci->ci_DataType);
	    r = HLAddClause(nq, r, ti, rdc, cc, ROP_EQEQ, ROP_CONST);
	}
    }
    
    if (r == NULL) {
        FreeQuery(nq);
    } else {
        int count = RunQuery(nq);
        FreeQuery(nq);
	if (count > 1)
	    count = DBERR_RECORD_ALREADY;
	if (count < 0)
	    return(count);
    }

    /*
     * Check each single-column UNIQUE constraint (being modified)
     *
     * XXX need to support multi-column grouped unique constraints
     */
    for (ci = q->q_ColIQBase; ci; ci = ci->ci_QNext) {
	if (ci->ci_Flags & CIF_DELETED)
	    continue;
	if (ci->ci_Flags & CIF_UNIQUE) {
	    const ColData *rdc;
	    const ColData *cc;
	    int count;

	    nq = GetQuery(q->q_Db);
	    ti = q->q_TableIQBase;
	    ti = GetTableIQuick(nq, ti->ti_Table, ti->ti_VTable, NULL, 0);
	    rdc = GetRawDataCol(ti->ti_RData, ci->ci_ColId, ci->ci_DataType);
	    cc = GetConst(nq, ci->ci_Const->cd_Data, ci->ci_Const->cd_Bytes);
	    (void)HLAddClause(nq, NULL, ti, rdc, cc, ROP_EQEQ, ROP_CONST);

	    count = RunQuery(nq);
	    FreeQuery(nq);
	    if (count > 1)
		count = DBERR_RECORD_ALREADY;
	    if (count < 0)
		return(count);
	}
    }
    return(0);
}

static int
OpEqEqVTIdMatch(const ColData *d1, const ColData *d2)
{  
    if (*(vtable_t *)d1->cd_Data >= *(vtable_t *)d2->cd_Data)
        return(1);
    return(-1);
}

static int
OpEqEqUserIdMatch(const ColData *d1, const ColData *d2)
{  
    if (*(rhuser_t *)d1->cd_Data >= *(rhuser_t *)d2->cd_Data)
        return(1);
    return(-1);
}

static int
OpEqEqOpCodeMatch(const ColData *d1, const ColData *d2)
{  
    if (*(u_int8_t *)d1->cd_Data >= *(u_int8_t *)d2->cd_Data)
        return(1);
    return(-1);
}

