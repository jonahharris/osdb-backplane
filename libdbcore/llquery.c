/*
 * LIBDBCORE/LLQUERY.C	- Low level query building and system table caches
 *
 * (c)Copyright 1999-2002 Backplane, Inc.  Please refer to the COPYRIGHT
 * file at the base of the distribution tree.
 *
 * $Backplane: rdbms/libdbcore/llquery.c,v 1.48 2002/10/02 21:31:33 dillon Exp $
 *
 *	This module is able to build and execute low level queries which
 *	operate directly on system tables.
 *
 *	These queries must still utilize the Query structure because they
 *	still fall under a freeze/commit/replication environment.
 *
 *	The LLBootstrap routines are used ONLY for bootstrapping a new 
 *	database.  They do not fall under the commit/replication environment.
 */

#include "defs.h"

Export struct TableI *LLGetTableI(struct DataBase *db, struct SchemaI *si, const char *tabName, int tabLen);
Export struct SchemaI *LLGetSchemaI(struct DataBase *db, const char *scmName, int scmLen);
Prototype ColI *LLGetColI(DataBase *db, TableI *ti, const char *colName, int colLen);
Prototype int LLSystemVId(DataBase *db, const char *fileName, int fileLen, int vt);
Export void LLFreeSchemaI(struct SchemaI **psi);
Export void LLFreeTableI(struct TableI **pti);
Prototype void LLFreeColI(ColI **pci);

Prototype int LLBootstrapSchema(DataBase *db, SchemaI *tsi);
Prototype int LLBootstrapTable(DataBase *db, SchemaI *si, TableI *tti);
Prototype int LLBootstrapColumn(DataBase *db, TableI *ti, col_t colId, ColI *tci);
Prototype int LLTermSysQuery(Query *q);
Prototype const char *ColFlagsToString(int flags);

typedef struct SchemaIManage {
    SchemaI    *sim_Base;
    SchemaI    **sim_App;
} SchemaIManage;

typedef struct TableIManage {
    TableI     *tim_Base;
    TableI     **tim_App;
    SchemaI	*tim_SchemaI;
} TableIManage;

typedef struct ColIManage {
    ColI       *cim_Base;
    ColI       **cim_App;
} ColIManage;

static int LLSystemQuery(DataBase *db, const char *file, vtable_t vt, col_t *cols, ColData *tests, int count, void (*callback)(void *data, RawData *rd), void *data);
static int LLSystemInsert(DataBase *db, const char *file, vtable_t vt, RawData *rd);
static void LLGetSchemaICallBack(void *data, RawData *rd);
static void LLGetTableICallBack(void *data, RawData *rd);
static void LLGetColICallBack(void *data, RawData *rd);
static void sortColIManage(ColIManage *cim);

/*
 * LLSystemQuery() -	Execute low level system query
 */

static int
LLSystemQuery(
    DataBase *db,
    const char *file,
    vtable_t vt,
    col_t *cols,
    ColData *tests,
    int count,
    void (*callback)(void *data, RawData *rd),
    void *data
) {
    Query *q;
    Table *tab;
    TableI *ti;
    ColData *cd;
    int i;
    int error = 0;
    Range *r = NULL;

    /*
     * Get a handle on the table
     */
    if (file == NULL || file[0] == 0) {
	tab = db->db_SysTable;
	tab = OpenTableByTab(tab, NULL, &error);
    } else {
	tab = OpenTable(db, file, "dt0", NULL, &error);
    }
    if (tab == NULL)
	return(-1);

    /*
     * Create a query
     */
    q = GetQuery(db);
    q->q_TermOp = QOP_SYSQUERY;
    ti = GetTableIQuick(q, tab, vt, cols, count);
    CloseTable(tab, 0);
    q->q_SysCallBack = callback;
    q->q_TermInfo = data;

    /*
     * Create tests for scan (the default op is an exact match, which is
     * what we want).  If there are no tests we do not add a dummy range
     * but instead deal with the situation in LLTermSysQuery.
     */

    i = 0;
    cd = ti->ti_RData->rd_ColBase;
    while (i < count) {
	if (tests[i].cd_Data != NULL) {
	    int rop = ROP_EQEQ;

	    if (cols[i] < CID_RAW_LIMIT) {
		switch(cols[i]) {
		case CID_RAW_VTID:
		    rop = ROP_VTID_EQEQ;
		    break;
		case CID_RAW_USERID:
		    rop = ROP_USERID_EQEQ;
		    break;
		case CID_RAW_TIMESTAMP:
		    rop = ROP_STAMP_EQEQ;
		    break;
		case CID_RAW_OPCODE:
		    rop = ROP_OPCODE_EQEQ;
		    break;
		case CID_COOK_VTID:
		case CID_COOK_USERID:
		case CID_COOK_TIMESTAMP:
		case CID_COOK_DATESTR:
		case CID_COOK_OPCODE:
		    /* use ROP_EQEQ */
		    break;
		default:
		    DBASSERT(0);
		}
	    }

	    r = HLAddClause(q, r, ti, cd, DupConst(q, &tests[i]), rop, ROP_CONST);
	}
	++i;
	cd = cd->cd_Next;
    }

    HLResolveNullScans(q);

    /*
     * Run the query, then release it (implements a guard)
     */
    i = RunQuery(q);
    RelQuery(q);
    return(i);
}

/*
 * Note that each termination range falls within a single physical table
 */

int
LLTermSysQuery(Query *q)
{
    TableI *ti = q->q_TableIQBase;
    RawData *rd = ti->ti_RData;

    DBASSERT(ti->ti_MarkRange != NULL);
    DBASSERT(ti->ti_Next == NULL);	/* no joins on sysquery yet */

    if (q->q_SysCallBack)
	q->q_SysCallBack(q->q_TermInfo, rd);

    return(1);
}

/*
 * LLSystemInsert() -	support for record insertion during bootstrapping
 */
static int
LLSystemInsert(DataBase *db, const char *file, vtable_t vt, RawData *rd)
{
    Table *tab;
    int error = 0;

    if (file == NULL || file[0] == 0) {
	tab = db->db_SysTable;
	tab = OpenTableByTab(tab, NULL, &error);
    } else {
	tab = OpenTable(db, file, "dt0", NULL, &error);
    }
    if (tab == NULL)
	return(error);

    if (InsertTableRec(tab, rd, vt) < 0) {
	error = -1;
    } else {
	SynchronizeTable(tab);
	SynchronizeDatabase(db, db->db_WriteTs);
    }

    CloseTable(tab, 0);

    return(error);
}

/*
 * LLSystemVId() -	check if the virtual table id is being used in the
 *			physical file.
 *
 *			Returns 0 if the vt is being used, -1 if it is not.
 *
 *	Note, this and all other LL routines MUST include CID_RAW_VTID to
 *	prevent the null-scan resolver from inserting into the list,
 *	since we make assumptions on ColData ordering.
 */

int 
LLSystemVId(DataBase *db, const char *fileName, int fileLen, int vt)
{
    int rv;
    char buf1[16];
    /* NOTE: columns must be ordered */
    col_t cols[] = { CID_RAW_VTID, CID_TABLE_VID, CID_TABLE_FILE };
    ColData tests[] = { 
	{ NULL, 0, 0, NULL },
	{ NULL, 0, 0, NULL },
	{ NULL, 0, fileLen, fileName }
    };

    /*
     * Look in sys.tables for virtual id
     */
    if (vt >= 0) {
	snprintf(buf1, sizeof(buf1), VT_FMT_STRING, (vtable_t)vt);
	tests[1].cd_Data = buf1;
	tests[1].cd_Bytes = strlen(buf1);
    }
    rv = LLSystemQuery(db, NULL, 
	    VT_SYS_TABLE, cols, tests, arysize(cols), NULL, NULL);
    if (rv != 0)
	return(0);
    return(-1);
}

/*
 * LLGetSchemaI() 	- Obtain schema or list of schemas from system table
 *
 *	Note, this and all other LL routines MUST include CID_RAW_VTID to
 *	prevent the null-scan resolver from inserting into the list,
 *	since we make assumptions on ColData ordering.
 */

SchemaI *
LLGetSchemaI(DataBase *db, const char *scmName, int scmLen)
{
    SchemaI *si = NULL;
    DataBase *dbScan = NULL;

    /*
     * Try to avoid issuing a sys query by locating the schema in our
     * cache.  The cache exists only at the first pushdown level after
     * the root.  It cannot exist at the root because that would require
     * us to manage freeze ts changes, and it cannot exist at other deeper
     * levels because commit sequences pull the queries up and this could
     * cause the cache to be freed out from under the query.
     *
     * DBF_METACHANGE indicates that a meta-data modification was made
     * in the transaction and disables the cache.
     */
    if (scmName && db->db_PushType != DBPUSH_ROOT) {
	dbScan = db;
	while (
	    dbScan->db_Parent->db_PushType != DBPUSH_ROOT &&
	    (dbScan->db_Flags & DBF_METACHANGE) == 0
	) {
	    dbScan = dbScan->db_Parent;
	}
	if ((dbScan->db_Flags & DBF_METACHANGE) == 0) {
	    for (si = dbScan->db_SchemaICache; si; si = si->si_Next) {
		if (scmLen == si->si_ScmNameLen &&
		    bcmp(scmName, si->si_ScmName, scmLen) == 0
		) {
		    SchemaI *osi = si;
		    si = zalloc(sizeof(SchemaI));
		    si->si_ScmName = safe_strdup(osi->si_ScmName);
		    si->si_DefaultPhysFile = safe_strdup(osi->si_DefaultPhysFile);
		    si->si_ScmNameLen = osi->si_ScmNameLen;
		    si->si_CacheCopy = osi;
		    break;
		}
	    }
	}
    }

    /*
     * If we could not find a copy in our cache, query the database.  If
     * we find the schema in the database, create a dup for the cache.
     */
    if (si == NULL) {
	/* NOTE: columns must be ordered */
	col_t cols[] = { CID_RAW_VTID, CID_SCHEMA_NAME, CID_TABLE_FILE };
	ColData tests[] = {
	    { NULL },
	    { NULL, 0, scmLen, scmName },
	    { NULL }
	};
	SchemaIManage sim = { NULL, &sim.sim_Base };

	LLSystemQuery(db, NULL, VT_SYS_SCHEMA,
		cols, tests, arysize(cols), LLGetSchemaICallBack, &sim);
	si = sim.sim_Base;

	/*
	 * Store a copy in our database schemai cache.  We can only do this
	 * at the Db just below the root Db, and only if we didn't hit a
	 * Db level with a METACHANGE.
	 */
	if (
	    si != NULL &&
	    scmName &&
	    dbScan &&
	    (dbScan->db_Flags & DBF_METACHANGE) == 0
	) {
	    SchemaI *sicopy;

	    sicopy = zalloc(sizeof(SchemaI));
	    sicopy->si_ScmName = safe_strdup(si->si_ScmName);
	    sicopy->si_DefaultPhysFile = safe_strdup(si->si_DefaultPhysFile);
	    sicopy->si_ScmNameLen = si->si_ScmNameLen;
	    sicopy->si_Next = dbScan->db_SchemaICache;
	    dbScan->db_SchemaICache = sicopy;
	    si->si_CacheCopy = sicopy;
	}
    }
    return(si);
}

void 
LLFreeSchemaI(SchemaI **psi)
{
    SchemaI *si;

    while ((si = *psi) != NULL) {
	*psi = si->si_Next;
	free(si->si_ScmName);
	free(si->si_DefaultPhysFile);
	LLFreeTableI(&si->si_FirstCacheTableI);
	zfree(si, sizeof(SchemaI));
    }
}

static void
LLGetSchemaICallBack(void *data, RawData *rd)
{
    ColData *cd = rd->rd_ColBase;
    SchemaIManage *sim = data;
    SchemaI *si;

    cd = cd->cd_Next;	/* skip raw vtid */

    /*
     * Garbage
     */
    if (cd->cd_Bytes <= 0) {
	DBASSERT(0);
	return;
    }
    si = zalloc(sizeof(SchemaI));
    si->si_ScmName = safe_malloc(cd->cd_Bytes + 1);
    si->si_ScmNameLen = cd->cd_Bytes;
    bcopy(cd->cd_Data, si->si_ScmName, cd->cd_Bytes);
    si->si_ScmName[cd->cd_Bytes] = 0;

    cd = cd->cd_Next;	/* phys file */
    si->si_DefaultPhysFile = safe_malloc(cd->cd_Bytes + 1);
    bcopy(cd->cd_Data, si->si_DefaultPhysFile, cd->cd_Bytes);
    si->si_DefaultPhysFile[cd->cd_Bytes] = 0;

    *sim->sim_App = si;
    sim->sim_App = &si->si_Next;
}

/*
 * LLGetTableI() - Obtain table or tables that exist in the requested schema
 *		   from the system vtable.
 *
 *	TableI entries are synthesized for special tables such as 
 *	'table$cols'.
 *
 *	NULL can be passed for si and/or tabName (tabLen == 0) to get
 *	a complete list.  This also has the side effect of creating a
 *	guard on the entire sys.tables table.  Such a guard is usually
 *	necessary when creating new tables to guarentee that all replication
 *	hosts choose the same virtual table id.
 *
 *	Note, this and all other LL routines MUST include CID_RAW_VTID to
 *	prevent the null-scan resolver from inserting into the list,
 *	since we make assumptions on ColData ordering.
 */

TableI *
LLGetTableI(DataBase *db, SchemaI *si, const char *tabName, int tabLen)
{
    TableI *ti = NULL;

    /*
     * Attempt to avoid making a sys-query by locating the tableI in the
     * cache attached to the schema, if any.
     */
    if (tabName && si->si_CacheCopy) {
	for (ti = si->si_CacheCopy->si_FirstCacheTableI; ti; ti = ti->ti_Next) {
	    if (tabLen == ti->ti_TabNameLen &&
		bcmp(tabName, ti->ti_TabName, tabLen) == 0
	    ) {
		TableI *oti = ti;

		ti = zalloc(sizeof(TableI));
		ti->ti_TabName = safe_strdup(oti->ti_TabName);
		ti->ti_TabNameLen = oti->ti_TabNameLen;
		ti->ti_VTable = oti->ti_VTable;
		ti->ti_TableFile = safe_strdup(oti->ti_TableFile);
		ti->ti_SchemaI = si;
		ti->ti_CacheCopy = oti;
		break;
	    }
	}
    }
    if (ti == NULL) {
	static const char *SpecialNames[] = { "$cols" };

	/* NOTE: columns must be ordered */
	col_t cols[] = { CID_RAW_VTID, CID_SCHEMA_NAME, CID_TABLE_NAME, 
			    CID_TABLE_VID, CID_TABLE_FILE };
	ColData tests[] = {
	    { NULL },
	    { NULL, 0, (si ? si->si_ScmNameLen : 0), (si ? si->si_ScmName : NULL)},
	    { NULL, 0, tabLen, tabName },
	    { NULL },
	    { NULL }
	};
	TableIManage tim = { NULL, &tim.tim_Base, si };
	int i;
	int xi = -1;

	for (i = tabLen - 1; i >= 0; --i) {
	    if (tabName[i] == '$') {
		/*
		 * columns table for table
		 */
		for (xi = arysize(SpecialNames) - 1; xi >= 0; --xi) {
		    if (tabLen - i == strlen(SpecialNames[xi]) &&
			strncasecmp(tabName + i, SpecialNames[xi], tabLen - i) == 0
		    ) {
			break;
		    }
		}
		/*
		 * Strip off the base name if we find a match.  Consider
		 * the entire name the base name if we do not find a match
		 * (so we match $DELxxx renames)
		 */
		if (xi >= 0)
		    tests[2].cd_Bytes = i;
		break;
	    }
	}
	LLSystemQuery(db, NULL, VT_SYS_TABLE, 
		cols, tests, arysize(cols), LLGetTableICallBack, &tim);
	if (xi >= 0) {
	    for (ti = tim.tim_Base; ti; ti = ti->ti_Next) {
		char *oldName;

		oldName = ti->ti_TabName;
		asprintf(&ti->ti_TabName, "%s%s", oldName, SpecialNames[xi]);
		ti->ti_TabNameLen = strlen(ti->ti_TabName);
		free(oldName);

		ti->ti_VTable += xi + 1;
	    }
	}
	ti = tim.tim_Base;
	if (ti != NULL && tabName && si->si_CacheCopy) {
	    TableI *ticopy;

	    ticopy = zalloc(sizeof(TableI));
	    ticopy->ti_TabName = safe_strdup(ti->ti_TabName);
	    ticopy->ti_TabNameLen = ti->ti_TabNameLen;
	    ticopy->ti_VTable = ti->ti_VTable;
	    ticopy->ti_TableFile = safe_strdup(ti->ti_TableFile);
	    ticopy->ti_SchemaI = si->si_CacheCopy;
	    ticopy->ti_Next = ticopy->ti_SchemaI->si_FirstCacheTableI;
	    ticopy->ti_SchemaI->si_FirstCacheTableI = ticopy;
	    ti->ti_CacheCopy = ticopy;
	}
    }
    return(ti);
}

void
LLFreeTableI(TableI **pti)
{
    TableI *ti;

    while ((ti = *pti) != NULL) {
	*pti = ti->ti_Next;

	if (ti->ti_Table) {
	    CloseTable(ti->ti_Table, 0);
	    ti->ti_Table = NULL;
	}
	if (ti->ti_RData) {
	    FreeRawData(ti->ti_RData);
	    ti->ti_RData = NULL;
	}
	LLFreeColI(&ti->ti_FirstColI);

	DBASSERT(ti->ti_Index == NULL);

	safe_free(&ti->ti_AliasName);
	if (ti->ti_DebugScanCount > 60 ||
	    ti->ti_DebugIndexScanCount > 25 ||
	    ti->ti_DebugIndexInsertCount > 100
	) {
	    dbinfo("DEBUGCNTS scan=%qd iscan=%qd iins=%qd\n",
		ti->ti_DebugScanCount,
		ti->ti_DebugIndexScanCount,
		ti->ti_DebugIndexInsertCount
	    );
	}
	safe_free(&ti->ti_TabName);
	safe_free(&ti->ti_TableFile);
	zfree(ti, sizeof(TableI));
    }
}

static void
LLGetTableICallBack(void *data, RawData *rd)
{
    ColData *cd = rd->rd_ColBase;
    TableIManage *tim = data;
    char idBuf[16];
    TableI *ti;

    cd = cd->cd_Next;	/* skip vtid */
    cd = cd->cd_Next;	/* skip schema */

    /*
     * Start at table name
     */
    if (cd->cd_Bytes <= 0) {
	DBASSERT(0);
	return;
    }
    ti = zalloc(sizeof(TableI));
    ti->ti_TabName = safe_malloc(cd->cd_Bytes + 1);
    ti->ti_TabNameLen = cd->cd_Bytes;
    bcopy(cd->cd_Data, ti->ti_TabName, cd->cd_Bytes);
    ti->ti_TabName[ti->ti_TabNameLen] = 0;

    cd = cd->cd_Next;	/* vid */
    DBASSERT(cd->cd_Bytes < sizeof(idBuf));
    bcopy(cd->cd_Data, idBuf, cd->cd_Bytes);
    idBuf[cd->cd_Bytes] = 0;
    ti->ti_VTable = strtol(idBuf, NULL, 16);

    cd = cd->cd_Next;	/* table file */
    ti->ti_TableFile = safe_malloc(cd->cd_Bytes + 1);
    bcopy(cd->cd_Data, ti->ti_TableFile, cd->cd_Bytes);
    ti->ti_TableFile[cd->cd_Bytes] = 0;
    ti->ti_SchemaI = tim->tim_SchemaI;

    *tim->tim_App = ti;
    tim->tim_App = &ti->ti_Next;
}


/*
 * LLGetColI() 	- Obtain column or list of columns that exist in an SQL
 *		  vtable by looking them up in the SQL vtable's column
 *		  vtable.
 *
 *	The columns for special tables, virtual table id (1... VT_INCREMENT-1),
 *	are synthesized and do not reside in the database.
 */

ColI *
LLGetColI(DataBase *db, TableI *ti, const char *colName, int colLen)
{
    ColI *ci = NULL;

    /*
     * Attempt to avoid making a sys-query by locating the ColI in the
     * cache attached to the TableI, if any.  We only optimize the
     * all-columns case, which is what the HL code usually requests.
     */
    if (colName == NULL && ti->ti_CacheCopy && ti->ti_CacheCopy->ti_FirstColI) {
	ColI *ciscan;

	for (
	    ciscan = ti->ti_CacheCopy->ti_FirstColI;
	    ciscan; 
	    ciscan = ciscan->ci_Next
	) {
	    ColI *nci = zalloc(ciscan->ci_Size);
	    int off = sizeof(ColI);

	    nci->ci_Size = ciscan->ci_Size;
	    nci->ci_ColNameLen = ciscan->ci_ColNameLen;
	    if (ciscan->ci_Flags & CIF_SPECIAL) {
		nci->ci_ColName = ciscan->ci_ColName;
	    } else {
		nci->ci_ColName = (char *)nci + off;
		bcopy(ciscan->ci_ColName, nci->ci_ColName, nci->ci_ColNameLen + 1);
	    }
	    off += nci->ci_ColNameLen + 1;
	    nci->ci_DefaultLen = ciscan->ci_DefaultLen;
	    if ((ciscan->ci_Flags & CIF_DEFAULT) && ciscan->ci_Default) {
		nci->ci_Default = (char *)nci + off;
		bcopy(ciscan->ci_Default, nci->ci_Default, nci->ci_DefaultLen + 1);
	    }
	    off += nci->ci_DefaultLen + 1;
	    nci->ci_OrderIndex = -1;
	    nci->ci_ColId = ciscan->ci_ColId;
	    nci->ci_DataType = ciscan->ci_DataType;
	    nci->ci_Flags = ciscan->ci_Flags;
	    nci->ci_Next = ci;
	    ci = nci;
	}
    }

    /*
     * If we could not find a cached copy, execute a system query
     */
    if (ci == NULL) {
	vtable_t cvtable;
	/* NOTE: columns must be ordered */
	col_t cols[] = { CID_RAW_VTID, CID_COL_NAME,
			 CID_COL_TYPE, CID_COL_FLAGS, CID_COL_STATUS,
			 CID_COL_ID, CID_COL_DEFAULT };
	ColData tests[] = {
	    { NULL, 0, sizeof(vtable_t), (void *)&cvtable },
	    { NULL, 0, colLen, colName },
	    { NULL },
	    { NULL },
	    { NULL },
	    { NULL },
	    { NULL }
	};
	ColIManage cim = { NULL, &cim.cim_Base };

	if (ti->ti_VTable) {
	    if ((ti->ti_VTable & (VT_INCREMENT - 1)) == 0) {
		cvtable = ti->ti_VTable + 1;
		LLSystemQuery(db, ti->ti_TableFile, ti->ti_VTable + 1, 
		    cols, tests, arysize(cols), LLGetColICallBack, &cim);
	    } else {
		cvtable = ti->ti_VTable & (VT_INCREMENT - 1);
		LLSystemQuery(db, NULL, ti->ti_VTable & (VT_INCREMENT - 1),
		    cols, tests, arysize(cols), LLGetColICallBack, &cim);
	    }
	}

	/*
	 * Column ordering determines how efficient duplicate-record checks are,
	 * enforce low-to-high ordering.
	 */
	sortColIManage(&cim);

	/*
	 * Special columns
	 */
	if (colName == NULL || (colLen && colName[0] == '_')) {
	    static char *SpecialColNames[] = { "__timestamp", "__vtid", "__userid", "__opcode", "__date" };
	    static col_t SpecialColIds[] = { CID_COOK_TIMESTAMP, CID_COOK_VTID, CID_COOK_USERID, CID_COOK_OPCODE, CID_COOK_DATESTR };
	    int i;

	    for (i = 0; i < arysize(SpecialColNames); ++i) {
		ColI *ci;

		if (colName == NULL ||
		    (colLen == strlen(SpecialColNames[i]) &&
		    bcmp(colName, SpecialColNames[i], colLen) == 0)
		) {
		    ci = zalloc(sizeof(ColI));
		    ci->ci_Size = sizeof(ColI);
		    ci->ci_ColName = SpecialColNames[i];
		    ci->ci_ColNameLen = strlen(ci->ci_ColName);
		    ci->ci_OrderIndex = -1;
		    ci->ci_ColId = SpecialColIds[i];
		    ci->ci_DataType = DATATYPE_STRING;
		    ci->ci_Flags |= CIF_SPECIAL;
		    *cim.cim_App = ci;
		    cim.cim_App = &ci->ci_Next;
		}
	    }
	}

	/*
	 * Copy the columns to the cache for later reuse.
	 */
	if (colName == NULL && ti->ti_CacheCopy) {
	    for (ci = cim.cim_Base; ci != NULL; ci = ci->ci_Next) {
		ColI *cicopy;
		int off = sizeof(ColI);

		cicopy = zalloc(ci->ci_Size);
		cicopy->ci_Size = ci->ci_Size;
		cicopy->ci_ColNameLen = ci->ci_ColNameLen;
		if (ci->ci_Flags & CIF_SPECIAL) {
		    cicopy->ci_ColName = ci->ci_ColName;
		} else {
		    cicopy->ci_ColName = (char *)cicopy + off;
		    bcopy(ci->ci_ColName, cicopy->ci_ColName, cicopy->ci_ColNameLen + 1);
		}
		off += cicopy->ci_ColNameLen + 1;
		cicopy->ci_DefaultLen = ci->ci_DefaultLen;
		if ((ci->ci_Flags & CIF_DEFAULT) && ci->ci_Default) {
		    cicopy->ci_Default = (char *)cicopy + off;
		    bcopy(ci->ci_Default, cicopy->ci_Default, ci->ci_DefaultLen + 1);
		}
		off += cicopy->ci_DefaultLen + 1;
		cicopy->ci_OrderIndex = -1;
		cicopy->ci_ColId = ci->ci_ColId;
		cicopy->ci_DataType = ci->ci_DataType;
		cicopy->ci_Flags = ci->ci_Flags;
		cicopy->ci_Next = ti->ti_CacheCopy->ti_FirstColI;
		ti->ti_CacheCopy->ti_FirstColI = cicopy;
	    }
	}
	ci = cim.cim_Base;
    }
    return(ci);
}

void
LLFreeColI(ColI **pci)
{
    ColI *ci;

    while ((ci = *pci) != NULL) {
	*pci = ci->ci_Next;
	zfree(ci, ci->ci_Size);
    }
}

static void
LLGetColICallBack(void *data, RawData *rd)
{
    ColData *cd = rd->rd_ColBase;
    ColIManage *cim = data;
    char idBuf[16];
    ColI *ci;
    int nameLen;
    int defLen;
    int off = sizeof(ColI);

    /*
     * vtid 
     */
    if (cd->cd_Bytes <= 0) {
	DBASSERT(0);
	return;
    }
    cd = cd->cd_Next;	/* name */
    nameLen = cd->cd_Bytes;
    defLen = cd->cd_Next->cd_Next->cd_Next->cd_Next->cd_Next->cd_Bytes;

    ci = zalloc(sizeof(ColI) + nameLen + 1 + defLen + 1);
    ci->ci_Size = sizeof(ColI) + nameLen + 1 + defLen + 1;
    ci->ci_ColName = (char *)ci + off;
    ci->ci_ColNameLen = nameLen;
    ci->ci_OrderIndex = -1;
    bcopy(cd->cd_Data, ci->ci_ColName, cd->cd_Bytes);
    off += nameLen + 1;

    cd = cd->cd_Next;	/* type */
    if (cd->cd_Bytes > 0) {
	ci->ci_DataType = DataTypeLookup(cd->cd_Data, cd->cd_Bytes);
    } else {
	ci->ci_DataType = DATATYPE_STRING;	/* XXX */
    }
    cd = cd->cd_Next;	/* flags */
    if (cd->cd_Bytes > 0) {
	int i;

	for (i = 0; i < cd->cd_Bytes; ++i) {
	    switch(cd->cd_Data[i]) {
	    case 'U':
		ci->ci_Flags |= CIF_UNIQUE;
		break;
	    case 'K':
		ci->ci_Flags |= CIF_KEY;
		break;
	    case 'N':
		ci->ci_Flags |= CIF_NOTNULL;
		break;
	    case 'D':
		ci->ci_Flags |= CIF_DELETED;
		break;
	    case 'V':
		/* ci->ci_Flags |= CIF_DEFAULT; -- not necessary */
		break;
	    default:
		break;
	    }
	}
    }
    cd = cd->cd_Next;	/* status */
    cd = cd->cd_Next;	/* id */
    DBASSERT(cd->cd_Bytes < sizeof(idBuf));
    bcopy(cd->cd_Data, idBuf, cd->cd_Bytes);
    idBuf[cd->cd_Bytes] = 0;
    ci->ci_ColId = strtol(idBuf, NULL, 16);

    cd = cd->cd_Next;	/* default */
    if (cd->cd_Data) {
	ci->ci_Default = (char *)ci + off;
	ci->ci_DefaultLen = defLen;
	ci->ci_Flags |= CIF_DEFAULT;
	bcopy(cd->cd_Data, ci->ci_Default, defLen);
    }
    off += defLen + 1;


    *cim->cim_App = ci;
    cim->cim_App = &ci->ci_Next;
}

const char *
ColFlagsToString(int flags)
{
    static char CFBuf[32];
    int i = 0;

    if (flags & CIF_KEY)
	CFBuf[i++] = 'K';
    if (flags & CIF_UNIQUE)
	CFBuf[i++] = 'U';
    if (flags & CIF_NOTNULL)
	CFBuf[i++] = 'N';
    if (flags & CIF_DELETED)
	CFBuf[i++] = 'D';
    CFBuf[i++] = 0;
    return(CFBuf);
}

/************************************************************************
 *			     BOOTSTRAP ROUTINES				*
 ************************************************************************
 *
 *	These routines provide high level support for creating and dropping
 *	virtual schema, tables, and columns, and (soon) replication mastery.
 *	These routines have been placed here rather then in the high level
 *	query module because they are absolutely critical to the proper
 *	operation of the database.
 */

int
LLBootstrapSchema(DataBase *db, SchemaI *tsi)
{
    ColData data[] = {
	{ &data[1], CID_SCHEMA_NAME, 0, tsi->si_ScmName },
	{ NULL, CID_TABLE_FILE, 0, tsi->si_DefaultPhysFile }
    };
    RawData rd;
    SchemaI *si;

    if ((si = LLGetSchemaI(db, tsi->si_ScmName, strlen(tsi->si_ScmName))) != NULL) {
	LLFreeSchemaI(&si);
	return(-1);
    }
    data[0].cd_Bytes = strlen(data[0].cd_Data);
    data[1].cd_Bytes = strlen(data[1].cd_Data);
    bzero(&rd, sizeof(rd));
    rd.rd_ColBase = &data[0];

    return(LLSystemInsert(db, NULL, VT_SYS_SCHEMA, &rd));
}

int 
LLBootstrapTable(DataBase *db, SchemaI *si, TableI *tti)
{
    TableI *ti;
    Table *tab;
    char *tableFile;
    int error = 0;

    if (tti->ti_SchemaI)
	si = tti->ti_SchemaI;

    if ((ti = LLGetTableI(db, si, tti->ti_TabName, tti->ti_TabNameLen)) != NULL) {
	LLFreeTableI(&ti);
	return(-1);
    }

    if (tti->ti_TableFile)
	tableFile = tti->ti_TableFile;
    else
	tableFile = si->si_DefaultPhysFile;

    DBASSERT(tti->ti_VTable != 0);
    DBASSERT(tableFile != NULL);

    tab = OpenTable(db, tableFile, "dt0", NULL, &error);

    if (tab != NULL) {
	char buf1[16];
	ColData data[] = {
	    { &data[1], CID_SCHEMA_NAME, si->si_ScmNameLen, si->si_ScmName },
	    { &data[2], CID_TABLE_NAME, tti->ti_TabNameLen, tti->ti_TabName },
	    { &data[3], CID_TABLE_VID, 0, buf1 },
	    { NULL, CID_TABLE_FILE, 0, tableFile }
	};
	RawData rd;

	snprintf(buf1, sizeof(buf1), VT_FMT_STRING, tti->ti_VTable);
	data[2].cd_Bytes = strlen(buf1);
	data[3].cd_Bytes = strlen(tableFile);

	CloseTable(tab, 0);

	bzero(&rd, sizeof(rd));
	rd.rd_ColBase = &data[0];
	return(LLSystemInsert(db, NULL, VT_SYS_TABLE, &rd));
    }
    return(error);
}

int
LLBootstrapColumn(DataBase *db, TableI *ti, col_t colId, ColI *tci)
{
    ColI *ci;
    int error = 0;

    /*
     * Does column already exist?
     */
    if ((ci = LLGetColI(db, ti, tci->ci_ColName, tci->ci_ColNameLen)) != NULL) {
	LLFreeColI(&ci);
	return(-1);
    }

    /*
     * Find an unused virtual column ID pair within the specified physical 
     * file.  Tables always use even virtual table id's and their column 
     * definitions use the following vtable_t.
     */
    if (colId == 0) {
	for (colId = CID_MIN_USER; colId < COL_ID_NUM; ++colId) {
	    char buf[16];
	    col_t vcols[] = { CID_COL_ID };
	    ColData vtests[] = { { NULL, 0, 0, buf } };

	    snprintf(buf, sizeof(buf), COL_FMT_STRING, colId);
	    vtests[0].cd_Bytes = strlen(buf);

	    if (LLSystemQuery(db, ti->ti_TableFile, ti->ti_VTable + 1, vcols, vtests, arysize(vcols), NULL, NULL) == 0) {
		break;
	    }
	}
    }
    if (colId == COL_ID_NUM)
	return(-1);

    /*
     * Add the column
     */
    {
	char buf[16];
	ColData data[] = {
	    { &data[1], CID_COL_NAME, tci->ci_ColNameLen, tci->ci_ColName },
	    { &data[2], CID_COL_TYPE },
	    { &data[3], CID_COL_FLAGS },
	    { &data[4], CID_COL_STATUS },
	    { NULL, CID_COL_ID, 0, buf }
	};
	RawData rd;

	bzero(&rd, sizeof(rd));
	rd.rd_ColBase = &data[0];

	snprintf(buf, sizeof(buf), COL_FMT_STRING, colId);
	data[4].cd_Bytes = strlen(buf);

	error = LLSystemInsert(db, ti->ti_TableFile, ti->ti_VTable + 1, &rd);
    }
    return(error);
}

static void
sortColIManage(ColIManage *cim)
{
    ColI *ci;
    ColI **ary;
    int count;
    static int sortColIFunc(const void *s1, const void *s2);

    count = 0;
    for (ci = cim->cim_Base; ci; ci = ci->ci_Next)
	++count;

    if (count > 1) {
	int i;

	ary = safe_malloc(sizeof(ColI *) * count);

	count = 0;
	for (ci = cim->cim_Base; ci; ci = ci->ci_Next)
	    ary[count++] = ci;
	qsort(ary, count, sizeof(ColI *), sortColIFunc);
	cim->cim_App = &cim->cim_Base;
	cim->cim_Base = NULL;
	for (i = 0; i < count; ++i) {
	    *cim->cim_App = ary[i];
	    cim->cim_App = &(ary[i])->ci_Next;
	}
	*cim->cim_App = NULL;
	free(ary);
    }
}

static int
sortColIFunc(const void *s1, const void *s2)
{
    const ColI *c1 = *(ColI **)s1;
    const ColI *c2 = *(ColI **)s2;
    return(c1->ci_ColId - c2->ci_ColId);
}

