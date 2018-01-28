/*
 * SIMPLEQUERY.C
 *
 * (c)Copyright 2000-2002 Backplane, Inc.  Please refer to the COPYRIGHT
 * file at the base of the distribution tree.
 *
 * $Backplane: rdbms/libdbcore/simplequery.c,v 1.4 2002/08/20 22:05:52 dillon Exp $
 *
 *	Used by low level utilities only
 */

#include "defs.h"
#include "simplequery.h"

Export struct SimpleQuery *StartSimpleQuery(struct DataBase *db, const char *qry);
Export char **GetSimpleQueryResult(struct SimpleQuery *sq);
Export void EndSimpleQuery(struct SimpleQuery *sq);

static int SSTermRange(Query *q);

SimpleQuery *
StartSimpleQuery(DataBase *db, const char *qry)
{
    SimpleQuery *sq = zalloc(sizeof(SimpleQuery));
    int type;
    token_t t;

    sq->sq_Db = PushDatabase(db, db->db_SysTable->ta_Meta->tf_SyncStamp, &sq->sq_Error, DBF_READONLY);
    sq->sq_QryStr = safe_strdup(qry);
    DBASSERT(sq->sq_Db != NULL);

    sq->sq_Query = GetQuery(sq->sq_Db);
    type = SqlInit(&t, sq->sq_QryStr, strlen(sq->sq_QryStr));
    type = ParseSql(&t, sq->sq_Query, type);
    if (type & TOKF_ERROR) {
	fprintf(stderr, "StartSimpleQuery: Parse error on '%s'\n", qry);
	EndSimpleQuery(sq);
	return(NULL);
    }
    sq->sq_Query->q_TermFunc = SSTermRange;
    sq->sq_Query->q_TermInfo = sq;
    sq->sq_Error = RunQuery(sq->sq_Query);
    FreeQuery(sq->sq_Query);
    sq->sq_Query = NULL;
    return(sq);
}

char **
GetSimpleQueryResult(SimpleQuery *sq)
{
    while (sq->sq_RIndex < sq->sq_WIndex) {
	return(sq->sq_Rows[sq->sq_RIndex++]);
    }
    return(NULL);
}

void
EndSimpleQuery(SimpleQuery *sq)
{
    int i;

    for (i = 0; i < sq->sq_WIndex; ++i) {
	char **rows = sq->sq_Rows[i];
	int j;

	for (j = 0; j < sq->sq_Cols; ++j) {
	    if (rows[j]) {
		free(rows[j]);
		rows[j] = NULL;
	    }
	}
	free(rows);
	sq->sq_Rows[i] = NULL;
    }
    free(sq->sq_Rows);
    sq->sq_Rows = NULL;
    PopDatabase(sq->sq_Db, &sq->sq_Error);
    sq->sq_Db = NULL;
    zfree(sq, sizeof(SimpleQuery));
}

static int
SSTermRange(Query *q)
{
    SimpleQuery *sq = q->q_TermInfo;
    ColI *ci;
    int cols = 0;
    char **rows;

    for (ci = q->q_ColIQBase; ci; ci = ci->ci_QNext)
	++cols;
    if (sq->sq_Cols == 0) {
	sq->sq_Cols = cols;
    } else if (sq->sq_Cols && cols != sq->sq_Cols) {
	DBASSERT(0);
    }
    if (sq->sq_WIndex == sq->sq_MaxIndex) {
	if (sq->sq_MaxIndex)
	    sq->sq_MaxIndex = sq->sq_MaxIndex * 2;
	else
	    sq->sq_MaxIndex = 64;
	sq->sq_Rows = realloc(sq->sq_Rows, sq->sq_MaxIndex * sizeof(char **));
	DBASSERT(sq->sq_Rows != NULL);
    }
    rows = safe_malloc(sizeof(char *) * cols);
    sq->sq_Rows[sq->sq_WIndex++] = rows;

    cols = 0;
    for (ci = q->q_ColIQBase; ci; ci = ci->ci_QNext) {
	ColData *ccd;

	if ((ccd = ci->ci_CData) && ccd->cd_Data)
	    rows[cols] = safe_strndup(ccd->cd_Data, ccd->cd_Bytes);
	++cols;
    }
    return(1);
}

