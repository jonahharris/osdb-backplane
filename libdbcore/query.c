/*
 * LIBDBCORE/QUERY.C	- Query structure support
 *
 * (c)Copyright 1999-2002 Backplane, Inc.  Please refer to the COPYRIGHT
 * file at the base of the distribution tree.
 *
 * $Backplane: rdbms/libdbcore/query.c,v 1.45 2002/08/20 22:05:52 dillon Exp $
 */

#include "defs.h"

#define ESCAPE_SIZE	4   /* Escapes are four chars; format: \xHH */

Export Query *GetQuery(DataBase *db);
Export void RelQuery(Query *q);
Export void FreeQuery(Query *q);
Export dbstamp_t ConstHexToStamp(ColData *cd);

Export void FreeResultRow(ResultRow *rr);
Export void FreeResultBuffer(Query *q);

Prototype int PushQuery(Query *q);
Prototype int PopQuery(Query *q, int commitMe);
Prototype ColData *GetConst(Query *q, const void *data, int bytes);
Prototype ColData *GetConstUnEscape(Query *q, const void *data, int bytes);
Prototype ColData *DupConst(Query *q, const ColData *cd);
Prototype TableI *GetTableIQuick(Query *q, Table *tab, vtable_t vid, col_t *cols, int count);
Prototype RawData *AllocRawData(Table *tab, col_t *cols, int count);
Prototype ColData *GetRawDataCol(RawData *rd, col_t colId, int dataType);
Prototype void FreeRawData(RawData *rd);
Prototype TableI *AllocPrivateTableI(RawData *rd);
Prototype void FreePrivateTableI(TableI *ti);

void ResetQuery(Query *q);

static char hexToBin(char hexDigit);

/*
 * Obtain an (unresolved) query structure that we can use to build
 * a query.
 */

Query *
GetQuery(DataBase *db)
{
    Query *q = zalloc(sizeof(Query));

    q->q_Db = db;
    q->q_ColIQAppend = &q->q_ColIQBase;
    q->q_ColIQSortAppend = &q->q_ColIQSortBase;
    q->q_RunRange = TermRange;
    q->q_RangeArg.ra_VoidPtr = q;
    q->q_TermOp = QOP_COUNT;		/* default op */
    q->q_MaxRows = -1;
    initList(&q->q_ResultBuffer);

    return(q);
}

/*
 * Push an empty query into a new transaction level.  The query becomes
 * a dummy place holder for the transaction.  This may only be run if you
 * are already pushed at least one level down.
 */
int
PushQuery(Query *q)
{
    DataBase *subDb;
    int error;

    if ((subDb = PushDatabase(q->q_Db, q->q_Db->db_FreezeTs, &error, 0)) == NULL)
	return(error);
    q->q_Db = subDb;
    q->q_TermOp = QOP_TRANS;
    return(0);
}

/*
 * Pop the transaction level associated with the dummy place holder query.
 *
 * 	Commits of sub-transactions cannot fail, so no callback sequencing
 *	is required.
 */
int
PopQuery(Query *q, int commitMe)
{
    int error;
    DataBase *db = q->q_Db;

    if (q->q_Error)
	commitMe = 0;
    ResetQuery(q);
    if (db->db_Flags & DBF_COMMIT1) {
	Abort1(db);
    } else if (commitMe) {
	dbstamp_t minCTs = 0;

	if ((error = Commit1(db, &minCTs, NULL, NULL, 0)) == 0)
	    error = Commit2(db, minCTs, 0, 0);
	else
	    Abort1(db);
    } else {
	/*
	 * When popping a database level copy any queries up to the higher
	 * database level (if one exists), marking them as ROLLEDBACK
	 */
	CopyQueryUp(db, QF_ROLLEDBACK);
    }
    q->q_Db = PopDatabase(db, &error);
    if (q->q_Error)
	error = q->q_Error;
    return(error);
}

void
ResetQuery(Query *q)
{
    /*
     * Free Range structures, be careful not to 
     * free the terminator as a range structure.
     */
    while (q->q_RunRange == RunRange) {
	Range *r = q->q_RangeArg.ra_RangePtr;

	q->q_RunRange = r->r_RunRange;
	q->q_RangeArg = r->r_Next;

	zfree(r, sizeof(Range));
    }

    /*
     * Free SchemaI (indirectly frees underlying tables and 
     * columns underlying tables).
     */

    LLFreeTableI(&q->q_TableIQBase);
    LLFreeSchemaI(&q->q_FirstSchemaI);
    q->q_DefSchemaI = NULL;
    q->q_ColIQBase = NULL;
    q->q_ColIQAppend = &q->q_ColIQBase;
    q->q_ColIQSortBase = NULL;
    q->q_ColIQSortAppend = &q->q_ColIQSortBase;
    q->q_IQSortCount = 0;
    q->q_OrderCount = 0;
    q->q_StartRow = 0;
    q->q_MaxRows = -1;
    q->q_CountRows = 0;

    /*
     * Free constants
     */
    {
	ColData *cd;

	while ((cd = q->q_ConstDataBase) != NULL) {
	    q->q_ConstDataBase = cd->cd_Next;
	    if (cd->cd_Bytes)
		zfree(cd, sizeof(ColData) + cd->cd_Bytes + 1);
	    else if (cd->cd_Data)
		zfree(cd, sizeof(ColData) + 1);
	    else
		zfree(cd, sizeof(ColData));
	}
    }

    FreeResultBuffer(q);
    q->q_Error = 0;
    q->q_Flags = 0;
    safe_free(&q->q_QryCopy);
}

/*
 * RelQuery()	- release a chain of queries after successful execution
 *
 *	If part of a transaction then we must set the query aside rather
 *	then free it.  See the CommitPhase1 and CommitPhase2 routines.
 *
 *	The query is recorded but only for special runs, clear q_TermInfo
 */
void
RelQuery(Query *q)
{
    DataBase *db = q->q_Db;

    if (db->db_Flags & DBF_READONLY) {
	FreeQuery(q);
    } else {
	DBASSERT(listIsEmpty(&q->q_ResultBuffer));
	*db->db_RecordedQueryApp = q;
	db->db_RecordedQueryApp = &q->q_RecNext;
	q->q_TermInfo = NULL;
    }
}

/*
 * FreeQuery()	- destroy query chain
 */

void
FreeQuery(Query *q)
{
    ResetQuery(q);
    zfree(q, sizeof(Query));
}

/*
 * GetConst() -	Create a ColData structure containing constant data
 *
 *	The data must be allocated in order to survive potentially multiple
 *	queries within a transaction.
 *
 *	XXX ref count / caching / reuse
 *	XXX LLSys routines aren't using this call but creating temporary
 *	    stack structures.  WRONG!
 */

ColData *
GetConst(Query *q, const void *data, int bytes)
{
    ColData *cd;

    if (bytes) {
	char *ptr;

	cd = zalloc(sizeof(ColData) + bytes + 1);	/* XXX remove + 1 */
	ptr = (char *)(cd + 1);
	bcopy(data, ptr, bytes);
	ptr[bytes] = 0;
	cd->cd_Data = ptr;
	cd->cd_Bytes = bytes;
    } else if (data) {
	/*
	 * 0 bytes but not NULL
	 */
	cd = zalloc(sizeof(ColData) + 1);
	cd->cd_Data = (char *)(cd + 1);
    } else {
	/*
	 * 0 bytes and NULL
	 */
	cd = zalloc(sizeof(ColData));
    }
    cd->cd_ColId = 0;
    cd->cd_Next = q->q_ConstDataBase;
    q->q_ConstDataBase = cd;

    return(cd);
}

dbstamp_t
ConstHexToStamp(ColData *cd)
{
    if (cd->cd_Data && cd->cd_Bytes && cd->cd_Bytes <= 16) {
	char buf[18];
	bcopy(cd->cd_Data, buf, cd->cd_Bytes);
	buf[cd->cd_Bytes] = 0;
	return(strtouq(buf, NULL, 16));
    }
    return(0);
}

/*
 * GetConstUnEscape() -	Create a ColData structure containing constant data
 *			retrieved from the lexer.  The data must be 
 *			unescaped.
 *
 *	The data must be allocated in order to survive potentially multiple
 *	queries within a transaction.
 *
 *	XXX ref count / caching / reuse
 *	XXX LLSys routines aren't using this call but creating temporary
 *	    stack structures.  WRONG!
 */

ColData *
GetConstUnEscape(Query *q, const void *escData, int bytes)
{
    ColData *cd;
    char *ptr;
    const char *data;
    int escCount = 0;
    int i;

    data = (const char *)escData;

    if (bytes) {
	for (i=0, escCount=0; i < bytes; ++i) {
	    if (data[i] == '\\' && i + 1 < bytes) {
		++i;
		if (data[i] == 'x' || data[i] == 'X')
		    escCount+=3;
		else
		    escCount+=1;
	    }
	}
    }
    if (escCount == 0 || escCount == bytes)
	return(GetConst(q, data, bytes - escCount));

    cd = zalloc(sizeof(ColData) + bytes + 1 - escCount);
    ptr = (char *)(cd + 1);

    for (i=0; i < bytes; ) {
	if (data[i] == '\\') {
	    if (i+1 < bytes) {
		if (data[i+1] == 'x' || data[i+1] == 'X') {
		    if ((bytes - i) >= ESCAPE_SIZE) {
			*ptr = hexToBin(data[i+2]) << 4;
			*ptr |= hexToBin(data[i+3]);
			++ptr;
			i += ESCAPE_SIZE;
		    } else {
			break;
		    }
		} else {
		    ++i;
		    *ptr++ = data[i++];
		}
	    } else {
		break;
	    }
	} else {
	    *ptr++ = data[i++];
	}
    }
    *ptr = 0;
    cd->cd_Data = (char *)(cd + 1);
    cd->cd_Bytes = bytes - escCount;

    cd->cd_ColId = 0;
    cd->cd_Next = q->q_ConstDataBase;
    q->q_ConstDataBase = cd;

    return(cd);
}

/*
 * DupConst() - Duplicate a ColData structure
 *
 *	Used to convert a temporary ColData into a more permanent one
 */

ColData *
DupConst(Query *q, const ColData *ocd)
{
    ColData *cd;

    if (ocd->cd_Bytes) {
	char *ptr;

	cd = zalloc(sizeof(ColData) + ocd->cd_Bytes + 1);
	ptr = (char *)(cd + 1);
	bcopy(ocd->cd_Data, ptr, ocd->cd_Bytes);
	cd->cd_Data = ptr;
	cd->cd_Bytes = ocd->cd_Bytes;
    } else {
	cd = zalloc(sizeof(ColData));
    }
    cd->cd_ColId = 0;
    cd->cd_Next = q->q_ConstDataBase;
    q->q_ConstDataBase = cd;
    return(cd);
}

/*
 * GetTableIQuick() - create a quick tableI structure for a table
 *
 *	Note that we do not set the table range here, it is set in the
 *	range scan.
 *
 *	The passed may cols array may be thrown away by the caller on return.
 */
TableI *
GetTableIQuick(Query *q, Table *tab, vtable_t vid, col_t *cols, int count)
{
    TableI *ti = zalloc(sizeof(TableI));

    DBASSERT(tab->ta_Refs > 0);		/* table must already be ref'd */
    ++tab->ta_Refs;
    ti->ti_Query = q;
    ti->ti_Table = tab;
    ti->ti_VTable = vid;
    ti->ti_RData = AllocRawData(tab, cols, count);
    ti->ti_Next = q->q_TableIQBase;
    q->q_TableIQBase = ti;

    return(ti);
}

/*
 * AllocRawData() - allocate raw data storage and optionally integrated 
 *		    columns
 *
 *	Column ID's and/or more columns can be added later.  This routine
 *	is used by the very low level code for low level selections,
 *	inserts, and so forth, and by high level routines to collect
 *	the data elements associated with a row in a table together.
 *
 *	Set the DataType to DATATYPE_STRING, which is what the vast
 *	majority of our LLSystemQueries want.  XXX Special raw columns such
 *	as timestamp, vtid, and so forth, override the datatype in hlquery.c
 */

RawData *
AllocRawData(Table *tab, col_t *cols, int count)
{
    RawData *rd;
    ColData *cd;
    ColData **pcd;
    int bytes = sizeof(RawData) + sizeof(ColData) * count;
    int i;

    rd = zalloc(bytes);
    rd->rd_AllocSize = bytes;
    rd->rd_Table = tab;

    cd = (ColData *)(rd + 1);
    pcd = &rd->rd_ColBase;

    for (i = 0; i < count; ++i, ++cd) {
	*pcd = cd;
	if (cols) {
	    cd->cd_ColId = (int)cols[i];
	    cd->cd_DataType = DATATYPE_STRING;
	}
	pcd = &cd->cd_Next;
    }
    return(rd);
}

TableI *
AllocPrivateTableI(RawData *rd)
{
    TableI *ti = zalloc(sizeof(TableI));

    ti->ti_RData = rd;
    ti->ti_Table = rd->rd_Table;
    DBASSERT(ti->ti_Table->ta_Refs > 0);	/* must already be ref'd */
    ++ti->ti_Table->ta_Refs;
    return(ti);
}

/*
 * Obtain a specific column from a raw data structure, creating a
 * new element if necessary.  The ColData's within a RawData are
 * required to be sorted by column id.
 */

ColData *
GetRawDataCol(RawData *rd, col_t colId, int dataType)
{
    ColData **pcd = &rd->rd_ColBase;
    ColData *cd;

    while ((cd = *pcd) != NULL) {
	if ((col_t)cd->cd_ColId == colId)
	    return(cd);
	if ((col_t)cd->cd_ColId > colId)
	    break;
	pcd = &cd->cd_Next;
    }
    cd = zalloc(sizeof(ColData));
    cd->cd_Next = *pcd;
    cd->cd_ColId = (int)colId;
    cd->cd_DataType = dataType;
    cd->cd_Flags = RDF_ALLOC;
    *pcd = cd;
    return(cd);
}

void
FreeRawData(RawData *rd)
{
    ColData *cd;
    ColData **pcd;

    if (rd->rd_Map)
	rd->rd_Map->dm_Table->ta_RelDataMap(&rd->rd_Map, 0);

    pcd = &rd->rd_ColBase;
    while ((cd = *pcd) != NULL) {
	if (cd->cd_Flags & RDF_ALLOC) {
	    *pcd = cd->cd_Next;
	    zfree(cd, sizeof(ColData));
	} else {
	    pcd = &cd->cd_Next;
	}
    }
    zfree(rd, rd->rd_AllocSize);
}

void
FreeResultRow(ResultRow *rr)
{
    int i;

    DBASSERT(rr->rr_Node.no_Next == NULL);	/* assert not on list */

    for (i = 0; i < rr->rr_NumCols; i++) {
	if (rr->rr_DataLen[i])
	    zfree(rr->rr_Data[i], rr->rr_DataLen[i]);
    }
    for (i = 0; i < rr->rr_NumSortCols; i++) {
	if (rr->rr_SortDataLen[i] & RR_SORTDATALEN_MASK) {
	    zfree(rr->rr_SortData[i],
		rr->rr_SortDataLen[i] & RR_SORTDATALEN_MASK);
	}
    }

    if (rr->rr_NumCols) {
	zfree(rr->rr_Data, sizeof(char *) * rr->rr_NumCols); 
	zfree(rr->rr_DataLen, sizeof(int) * rr->rr_NumCols);
    }
    if (rr->rr_NumSortCols) {
	zfree(rr->rr_SortData, sizeof(char *) * rr->rr_NumSortCols);
	zfree(rr->rr_SortDataLen, sizeof(int) * rr->rr_NumSortCols);
    }
    zfree(rr, sizeof(ResultRow));
}


void
FreeResultBuffer(Query *q)
{
    ResultRow *row;

    while ((row = remHead(&q->q_ResultBuffer)) != NULL)
	FreeResultRow(row);
}


/* hexToBin - Convert a hexadecimal digit to its binary value
 *
 * Arguments:	hexDigit	A single ASCII hex digit
 * Globals:	None
 *
 * Returns:	The binary value of the input digit,
 *		or 0 if it is out of range.
 */
static char
hexToBin(char hexDigit)
{
    char binary = 0;
 
    if (hexDigit >= '0' && hexDigit <= '9')
	binary = hexDigit - '0';
    else if (hexDigit >= 'A' && hexDigit <= 'F')
	binary = hexDigit - 'A' + 10;
    else if (hexDigit >= 'a' && hexDigit <= 'f')
	binary = hexDigit - 'a' + 10;
    return(binary); 
}
