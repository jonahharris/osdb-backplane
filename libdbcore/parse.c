/*
 * LIBDBCORE/PARSE.C
 *
 * (c)Copyright 1999-2002 Backplane, Inc.  Please refer to the COPYRIGHT
 * file at the base of the distribution tree.
 *
 * $Backplane: rdbms/libdbcore/parse.c,v 1.58 2002/08/20 22:05:52 dillon Exp $
 *
 *	Parse SQL text into queries that can then be run.
 */

#include "defs.h"

Export int ExecuteSql(Query *qhold, const char *ctl, ...);
Export int ParseSql(token_t *t, Query *q, int type);

int ParseSqlInsert(token_t *t, Query *q, int type);
int ParseSqlDelete(token_t *t, Query *q, int type);
int ParseSqlSelect(token_t *t, Query *q, int type);
int ParseSqlUpdate(token_t *t, Query *q, int type);
int ParseSqlCreate(token_t *t, Query *q, int type);
int ParseSqlDrop(token_t *t, Query *q, int type);
int ParseSqlAlter(token_t *t, Query *q, int type);
int ParseSqlAlterTable(token_t *t, Query *q, int type);
int ParseSqlAlterTableAddColumn(token_t *t, Query *q, TableI *ti, int type);
int ParseSqlAlterTableAlterColumn(token_t *t, Query *q, TableI *ti, int type);
int ParseSqlAlterTableDropColumn(token_t *t, Query *q, TableI *ti, int type);

int ParseSqlCreateTable(token_t *t, Query *q, int type);
int ParseSqlCreateSchema(token_t *t, Query *q, int type);
int ParseSqlDropTable(token_t *t, Query *q, int type);
int ParseSqlDropSchema(token_t *t, Query *q, int type);

int ParseSqlTable(token_t *t, Query *q, int type);
int ParseSqlCol(token_t *t, Query *q, int flags, int type);
int ParseSqlColAssignment(token_t *t, Query *q, int type);
int ParseSqlColOrder(token_t *t, Query *q, int type);
int ParseSqlData(token_t *t, Query *q, ColData **pcd, int type);
int ParseSqlExp(token_t *t, Query *q, int type);
int ParseSqlExpVal(token_t *t, Query *q, ColI **pci, ColData **pcd, int type);
int ParseSqlColType(token_t *t, Query *q, int type, const char *scmName, const char *tabName, col_t cid);

/*
 * ExecuteSql() - parse and execute SQL using the place holder query.
 */

int 
ExecuteSql(Query *qhold, const char *ctl, ...)
{
    Query *q = GetQuery(qhold->q_Db);
    char *qry = NULL;
    int error;
    va_list va;

    va_start(va, ctl);
    error = vasprintf(&qry, ctl, va);
    va_end(va);
    if (error >= 0) {
	token_t t;
	int type;

	error = 0;
	dbinfo2("MACROSQL: %s\n", qry);

	/*
	 * The parser will accept the \0 in liu of a semicolon so
	 * include it in the lexer initialization.
	 */
	type = ParseSql(&t, q, SqlInit(&t, qry, strlen(qry) + 1));
	dbinfo2("RESULT %08x\n", type);

	if (type & TOKF_ERROR) {
	    FreeQuery(q);
	    error = -(int)(type & TOKF_ERRMASK);
	} else {
	    error = RunQuery(q);
	    RelQuery(q);
	}
	free(qry);
    } else {
	FreeQuery(q);
    }
    if (error < 0)
	qhold->q_Error = error;
    return(error);
}

/*
 * ParseSql() - fill a query with the parsed SQL command.
 *
 *	SQLCOMMAND ';'
 */

int
ParseSql(token_t *t, Query *q, int type)
{
    /*
     * We don't do nut'in outside a transaction.
     */
    if (q->q_Db->db_PushType == DBPUSH_ROOT)
	return(SqlError(t, DBTOKTOERR(DBERR_NOT_IN_TRANSACTION)));

    switch(type) {
    case TOK_INSERT:
	type = ParseSqlInsert(t, q, SqlToken(t));
	break;
    case TOK_DELETE:
	type = ParseSqlDelete(t, q, SqlToken(t));
	break;
    case TOK_SELECT:
    case TOK_COUNT:
    case TOK_HISTORY:
	type = ParseSqlSelect(t, q, type);
	break;
    case TOK_UPDATE:
    case TOK_CLONE:
	type = ParseSqlUpdate(t, q, type);
	break;
    case TOK_CREATE:
	type = ParseSqlCreate(t, q, SqlToken(t));
	break;
    case TOK_DROP:
	type = ParseSqlDrop(t, q, SqlToken(t));
	break;
    case TOK_ALTER:
	type = ParseSqlAlter(t, q, SqlToken(t));
	break;
    default:
	type = SqlError(t, DBTOKTOERR(DBERR_UNRECOGNIZED_KEYWORD));
	break;
    }
    if (type == TOK_SEMI)
	type = SqlToken(t);
    else if (type == TOK_EOF1)
	type = SqlToken(t);
    else if ((type & TOKF_ERROR) == 0)
	type = SqlError(t, DBTOKTOERR(DBERR_SYNTAX_ERROR));
    return(type);
}

/*
 * INSERT INTO table [ '(' columns ')' ] VALUES '(' data ')'
 *
 * note: key constraints are handled in the scan code
 */

int
ParseSqlInsert(token_t *t, Query *q, int type)
{
    ColI *ci;

    q->q_TermOp = QOP_INSERT;

    type = SqlSkip(t, TOK_INTO);
    type = ParseSqlTable(t, q, type);

    /*
     * Columns
     */
    if (type == TOK_OPAREN) {
	type = ParseSqlCol(t, q, CIF_ORDER, SqlToken(t));
	while (type == TOK_COMMA)
	    type = ParseSqlCol(t, q, CIF_ORDER|CIF_DEFAULT, SqlToken(t));
	type = SqlSkip(t, TOK_CPAREN);
    } else {
	type = SqlError(t, DBTOKTOERR(DBERR_MUST_DECLARE_COLS));
    }

    ci = q->q_ColIQBase;

    /*
     * Values
     */
    type = SqlSkip(t, TOK_VALUES);
    type = SqlSkip(t, TOK_OPAREN);
    while (ci) {
	type = ParseSqlData(t, q, &ci->ci_Const ,type);
	ci = ci->ci_QNext;
	if (type != TOK_COMMA)
	    break;
	type = SqlToken(t);
    }
    if (ci)
	type = SqlError(t, DBTOKTOERR(DBERR_TOO_LITTLE_DATA));
    else if (type != TOK_CPAREN)
	type = SqlError(t, DBTOKTOERR(DBERR_TOO_MUCH_DATA));
    type = SqlSkip(t, TOK_CPAREN);

    /*
     * Must insert into at least one table
     */
    if (q->q_TableIQBase == NULL)
	type = SqlError(t, DBTOKTOERR(DBERR_TABLE_REQUIRED));

    /*
     * Insert must contain valid key and other fields
     */

    if (HLCheckFieldRestrictions(q, CIF_DEFAULT) < 0)
	type = SqlError(t, DBTOKTOERR(DBERR_KEYNULL));

    return(type);
}

/*
 * DELETE FROM table WHERE exp
 */

int
ParseSqlDelete(token_t *t, Query *q, int type)
{
    q->q_TermOp = QOP_DELETE;
    type = SqlSkip(t, TOK_FROM);
    type = ParseSqlTable(t, q, type);

    if (type == TOK_WHERE) {
	type = SqlSkip(t, TOK_WHERE);
	type = ParseSqlExp(t, q, type);
    } else {
	type = SqlError(t, DBTOKTOERR(DBERR_MISSING_WHERE));
    }
    if (q->q_TableIQBase == NULL)
	type = SqlError(t, DBTOKTOERR(DBERR_TABLE_REQUIRED));
    return(type);
}

/*
 * SELECT columns FROM tables WHERE exp
 */

int
ParseSqlSelect(token_t *t, Query *q, int type)
{
    token_t redo;
    int rtype;

    switch(type) {
    case TOK_HISTORY:
	/*
	 * NOTE!  The HISTORY command returns records in
	 * 	  the correct order as a side effect of
	 *	  the RETURN_ALL flag setting the
	 *	  ScanOneOnly flag negative.
	 */
	q->q_Flags |= QF_RETURN_ALL;
	/* fall through */
    case TOK_SELECT:
	q->q_TermOp = QOP_SELECT;
	break;
    default:
	q->q_TermOp = QOP_COUNT;
	break;
    }

    type = SqlToken(t);
    redo = *t;
    rtype = type;

    if (q->q_TermOp == QOP_SELECT) {
	/*
	 * Skip columns to get to tables so we can parse the tables first,
	 * then redo the columns.
	 */
	type = ParseSqlCol(t, NULL, CIF_WILD|CIF_ORDER|CIF_SPECIAL, type);
	while (type == TOK_COMMA) {
	    type = ParseSqlCol(t, NULL, CIF_WILD|CIF_ORDER|CIF_SPECIAL, SqlToken(t));
	}
    }

    /*
     * Parse tables
     */
    type = SqlSkip(t, TOK_FROM);
    type = ParseSqlTable(t, q, type);
    while (type == TOK_COMMA) {
	type = ParseSqlTable(t, q, SqlToken(t));
    }

    /*
     * Go back and parse the selection columns
     */
    if (q->q_TermOp == QOP_SELECT) {
	rtype = ParseSqlCol(&redo, q, CIF_ORDER|CIF_WILD|CIF_SPECIAL, rtype);
	while (rtype == TOK_COMMA) {
	    rtype = ParseSqlCol(&redo, q, CIF_ORDER|CIF_WILD|CIF_SPECIAL, SqlToken(&redo));
	}
	if (rtype & TOKF_ERROR) {
	    type = rtype;
	    t->t_Type = type;
	}
    }

    /*
     * Parse optional where clause
     */
    if (type == TOK_WHERE) {
	type = SqlSkip(t, TOK_WHERE);
	type = ParseSqlExp(t, q, type);
    }

    /*
     * Parse optional ORDER BY clause
     */
    if (q->q_TermOp == QOP_SELECT && type == TOK_ORDER) {
	type = SqlSkip(t, TOK_ORDER);
	type = SqlSkip(t, TOK_BY);
	for(;;) {
	    type = ParseSqlColOrder(t, q, type);
	    if (type != TOK_COMMA)
		break;
	    type = SqlSkip(t, TOK_COMMA);
	}
	q->q_Flags |= QF_WITH_ORDER|QF_CLIENT_ORDER;
    }

    /*
     * Parse optional LIMIT clause
     */
    if (type == TOK_LIMIT) {
	type = SqlSkip(t, TOK_LIMIT);
	if (type == TOK_INT) {
	    q->q_MaxRows = atoin(t->t_Data, t->t_Len);
	    type = SqlSkip(t, TOK_INT);

	    if (type == TOK_COMMA) {
		/* Optional "offset, rows" syntax. */
		type = SqlSkip(t, TOK_COMMA);

		if (type == TOK_INT) {
		    q->q_StartRow = q->q_MaxRows;
		    q->q_MaxRows = atoin(t->t_Data, t->t_Len);		
		    type = SqlSkip(t, TOK_INT);
		} else {
		    type = SqlError(t, DBTOKTOERR(DBERR_SYNTAX_ERROR));
		}
	    }
	} else {
	    type = SqlError(t, DBTOKTOERR(DBERR_SYNTAX_ERROR));
	}
	q->q_Flags |= QF_WITH_LIMIT|QF_CLIENT_LIMIT;
    }

    /*
     * Cleanup NULL scans, finish up
     */
    HLResolveNullScans(q);
    if (q->q_TableIQBase == NULL)
	type = SqlError(t, DBTOKTOERR(DBERR_TABLE_REQUIRED));
    return(type);
}

/*
 * UPDATE tables SET col = data [, var = data ]* WHERE exp
 * CLONE ....
 *
 *	UPDATE - normal SQL update
 *	CLONE - basically an update without the delete.
 *
 * note: key constraints are handled in the scan code
 */

int
ParseSqlUpdate(token_t *t, Query *q, int type)
{
    switch(type) {
    case TOK_CLONE:
	q->q_TermOp = QOP_CLONE;
	break;
    case TOK_UPDATE:
	q->q_TermOp = QOP_UPDATE;
	break;
    default:
	DBASSERT(0);
	break;
    }

    type = SqlToken(t);

    /*
     * Tables
     */
    type = ParseSqlTable(t, q, type);
    while (type == TOK_COMMA) {
	type = ParseSqlTable(t, q, SqlToken(t));
    }

    /*
     * Assignments
     */
    type = SqlSkip(t, TOK_SET);
    type = ParseSqlColAssignment(t, q, type);
    while (type == TOK_COMMA)
	type = ParseSqlColAssignment(t, q, SqlToken(t));

    /*
     * Required WHERE clause
     */
    if (type == TOK_WHERE) {
	type = SqlSkip(t, TOK_WHERE);
	type = ParseSqlExp(t, q, type);
    } else {
	type = SqlError(t, DBTOKTOERR(DBERR_MISSING_WHERE));
    }
    if (HLCheckFieldRestrictions(q, 0) < 0)
	type = SqlError(t, DBTOKTOERR(DBERR_KEYNULL));

    if (q->q_TableIQBase == NULL)
	type = SqlError(t, DBTOKTOERR(DBERR_TABLE_REQUIRED));

    return(type);
}

/*
 * CREATE SCHEMA ...
 * CREATE TABLE ...
 */

int
ParseSqlCreate(token_t *t, Query *q, int type)
{
    switch(type) {
    case TOK_TABLE:
	type = ParseSqlCreateTable(t, q, SqlToken(t));
	break;
    case TOK_SCHEMA:
	type = ParseSqlCreateSchema(t, q, SqlToken(t));
	break;
    default:
	type = SqlError(t, DBTOKTOERR(DBERR_SYNTAX_ERROR));
	break;
    }
    return(type);
}

/*
 * DROP SCHEMA ...
 * DROP TABLE ...
 */

int
ParseSqlDrop(token_t *t, Query *q, int type)
{
    switch(type) {
    case TOK_TABLE:
	type = ParseSqlDropTable(t, q, SqlToken(t));
	break;
    case TOK_SCHEMA:
	type = ParseSqlDropSchema(t, q, SqlToken(t));
	break;
    default:
	type = SqlError(t, DBTOKTOERR(DBERR_SYNTAX_ERROR));
	break;
    }
    return(type);
}

/*
 * ALTER
 */

int
ParseSqlAlter(token_t *t, Query *q, int type)
{
    switch(type) {
    case TOK_TABLE:
	type = ParseSqlAlterTable(t, q, SqlToken(t));
	break;
    default:
	type = SqlError(t, DBTOKTOERR(DBERR_SYNTAX_ERROR));
	break;
    }
    return(type);
}

/*
 * ALTER TABLE tableName
 *
 *	ADD [COLUMN] create_def
 *	DROP [COLUMN] create_def
 *	MODIFY [COLUMN] create_def
 *
 *	RENAME [AS] new_table_name
 */

int
ParseSqlAlterTable(token_t *t, Query *q, int type)
{
    TableI *ti;

    /*
     * tableName
     */

    if (PushQuery(q) < 0)
	return(SqlError(t, DBTOKTOERR(DBERR_CANT_PUSH)));

    type = ParseSqlTable(t, q, type);
    if (type & TOKF_ERROR) {
	PopQuery(q, 0);
	return(type);
    }

    if ((ti = q->q_TableIQBase) == NULL)
	type = SqlError(t, DBTOKTOERR(DBERR_TABLE_NOT_FOUND));

    /*
     * Sequence of alterations to be done on the table
     */
    while ((type & TOKF_ERROR) == 0) {
	switch(type) {
	case TOK_ADD:
	    type = SqlToken(t);
	    if (type == TOK_COLUMN)
		type = SqlToken(t);
	    type = ParseSqlAlterTableAddColumn(t, q, ti, type);
	    break;
	case TOK_ALTER:
	    type = SqlToken(t);
	    if (type == TOK_COLUMN)
		type = SqlToken(t);
	    type = ParseSqlAlterTableAlterColumn(t, q, ti, type);
	    break;
	case TOK_DROP:
	    type = SqlToken(t);
	    if (type == TOK_COLUMN)
		type = SqlToken(t);
	    type = ParseSqlAlterTableDropColumn(t, q, ti, type);
	    break;
	default:
	    type = SqlError(t, DBTOKTOERR(DBERR_SYNTAX_ERROR));
	    break;
	}
	if (type != TOK_COMMA)
	    break;
	type = SqlToken(t);
    }
    if (type & TOKF_ERROR) {
	PopQuery(q, 0);
    } else {
	if (PopQuery(q, 1) < 0) {
	    type = SqlError(t, DBTOKTOERR(DBERR_MACRO_SQL));
	} else {
	    dbinfo2("Committed\n");
	}
    }
    return(type);
}

/*
 * ALTER TABLE tabName ADD [COLUMN] {ParseColType}+
 */

int
ParseSqlAlterTableAddColumn(token_t *t, Query *q, TableI *ti, int type)
{
    col_t cid = CID_MIN_USER;
    ColI *ci;

    /*
     * Figure out the column id to start creating new columns at.
     */
    ci = HLGetColI(q, NULL, 0, 0);
    while (ci) {
	if (cid <= ci->ci_ColId)
	    cid = ci->ci_ColId + 1;
	ci = ci->ci_Next;
    }

    /*
     * Create the new columns
     */

    type = ParseSqlColType(t, q, type, ti->ti_SchemaI->si_ScmName, ti->ti_TabName, cid);
    ++cid;
    while (type == TOK_COMMA && cid < COL_ID_NUM) {
	type = SqlToken(t);
	type = ParseSqlColType(t, q, type, ti->ti_SchemaI->si_ScmName, ti->ti_TabName, cid);
	++cid;
    }
    return(type);
}

/*
 * ALTER TABLE tabName ALTER [COLUMN] name type features
 */
int
ParseSqlAlterTableAlterColumn(token_t *t, Query *q, TableI *ti, int type)
{
    type = SqlError(t, DBTOKTOERR(DBERR_FEATURE_NOT_SUPPORTED));
    return(type);
}

/*
 * ALTER TABLE tabName DROP [COLUMN] name
 *
 *	Deletes the specified column.  In actual fact the column
 *	is renamed to '$DELcolid', since we must keep the original 
 *	column id intact so no new column uses it.
 */
int
ParseSqlAlterTableDropColumn(token_t *t, Query *q, TableI *ti, int type)
{
    /*
     * Get the column name
     */
    for (;;) {
	ColI *ci;

	if ((type & TOKF_ID) == 0) {
	    type = SqlError(t, DBTOKTOERR(DBERR_SYNTAX_ERROR));
	    break;
	}
	ci = HLGetColI(q, t->t_Data, t->t_Len, 0);
	if (ci == NULL) {
	    type = SqlError(t, DBTOKTOERR(DBERR_COLUMN_NOT_FOUND));
	    break;
	}
	type = SqlToken(t);

	ExecuteSql(
	    q,
	    "UPDATE %s.%s$cols SET ColName = '$DEL" COL_FMT_STRING "',"
	    " ColFlags = '%s'"
	    " WHERE ColName = '%s'",
	    ti->ti_SchemaI->si_ScmName,
	    ti->ti_TabName,
	    ci->ci_ColId,
	    ColFlagsToString(ci->ci_Flags | CIF_DELETED),
	    ci->ci_ColName
	);
	if (type != TOK_COMMA)
	    break;
	type = SqlToken(t);
    }
    return(type);
}

/************************************************************************
 *			MACRO SQL COMMANDS				*
 ************************************************************************/

/*
 * CREATE SCHEMA schema [ USING file ]
 */

int
ParseSqlCreateSchema(token_t *t, Query *q, int type)
{
    SchemaI *si;
    char *scmName;
    char *fileName;

    if ((type & TOKF_ID) == 0)
	return(SqlError(t, DBTOKTOERR(DBERR_SYNTAX_ERROR)));

    /*
     * XXX when key fields set for sys.schemas, we can simply run the
     * SQL macro, we don't have to check here.
     */
    if ((si = HLGetSchemaI(q, t->t_Data, t->t_Len)) != NULL)
	return(SqlError(t, DBTOKTOERR(DBERR_SCHEMA_EXISTS)));

    scmName = safe_strndup(t->t_Data, t->t_Len);

    type = SqlToken(t);
    if (type == TOK_USING) {
	type = SqlToken(t);
	if (type != TOK_DQSTRING || t->t_Len <= 2) {
	    free(scmName);
	    return(SqlError(t, DBTOKTOERR(DBERR_SYNTAX_ERROR)));
	}
	fileName = safe_strndup(t->t_Data, t->t_Len);
	type = SqlToken(t);
    } else {
	fileName = safe_strndup(scmName, strlen(scmName));
    }

    if (PushQuery(q) < 0)
	return(SqlError(t, DBTOKTOERR(DBERR_CANT_PUSH)));

    /*
     * Insert new table into sys.tables
     */
    ExecuteSql(
	q,
	"INSERT INTO sys.schemas"
	    " ( SchemaName, TableFile )"
	    " VALUES ( '%s', '%s' )",
	scmName,
	fileName
    );
    free(scmName);
    free(fileName);

    /*
     * Pop the transaction
     */
    if (type & TOKF_ERROR) {
	PopQuery(q, 0);
    } else {
	if (PopQuery(q, 1) < 0) {
	    type = SqlError(t, DBTOKTOERR(DBERR_MACRO_SQL));
	} else {
	    dbinfo2("Committed\n");
	}
    }
    return(type);
}

/*
 * CREATE TABLE [schema '.']table [ USING file ] 
 *	'(' [ typedecl [ , typedecl]* ')'
 */

int
ParseSqlCreateTable(token_t *t, Query *q, int type)
{
    SchemaI *si = q->q_DefSchemaI;
    TableI *ti;
    int hi;
    vtable_t vt = 0;
    int i;
    int tabLen;
    int fileLen;
    const char *fileName;
    const char *tabName;

    if ((type & TOKF_ID) == 0)
	return(SqlError(t, DBTOKTOERR(DBERR_SYNTAX_ERROR)));

    tabName = t->t_Data;
    tabLen = t->t_Len;

    /*
     * Obtain schema or use default
     */
    for (i = t->t_Len - 1; i >= 0; --i) {
	if (t->t_Data[i] == '.') {
	    si = HLGetSchemaI(q, t->t_Data, i);
	    tabName = t->t_Data + i + 1;
	    tabLen = t->t_Len - i - 1;
	    break;
	}
    }

    ++i;
    if (si == NULL)
	return(SqlError(t, DBTOKTOERR(DBERR_NO_DEFAULT_SCHEMA)));

    /*
     * Fail if table already exists
     *
     * XXX when key fields set for sys.tables, we can simply run the
     * SQL macro, we don't have to check here.
     */
    ti = HLGetTableI(q, si, t->t_Data + i, t->t_Len - i, NULL, 0);
    if (ti != NULL)
	return(SqlError(t, DBTOKTOERR(DBERR_TABLE_EXISTS)));

    type = SqlToken(t);

    /*
     * Parse USING.  If no USING then the physical file used for the table
     * is the name of the schema.
     */

    if (type == TOK_USING) {
	type = SqlToken(t);
	if (type != TOK_DQSTRING || t->t_Len <= 2)
	    return(SqlError(t, DBTOKTOERR(DBERR_SYNTAX_ERROR)));
	fileName = t->t_Data + 1;
	fileLen = t->t_Len - 2;
	/* XXX check validity of file name (no ..'s, slashes, etc.) */
	type = SqlToken(t);
    } else {
	fileName = si->si_DefaultPhysFile;
	fileLen = strlen(fileName);
    }

    /*
     * Guard to lockout any other table create from occuring within this
     * physical file at the same time, because we need to guarentee that
     * all replication hosts create the same virtual table id.
     */
    LLSystemVId(q->q_Db, fileName, fileLen, -1);

    /*
     * Locate an unused TableVId >= VT_MIN_USER modulo VT_INCREMENT.  This
     * algorithm is required to be deterministic.
     */
    hi = strhash(tabName, tabLen) ^ strhash(si->si_ScmName, si->si_ScmNameLen);
    for (;;) {
	vt = hi & (VTABLE_ID_MASK - (VT_INCREMENT - 1));
	if (vt >= VT_MIN_USER) {
	    if (LLSystemVId(q->q_Db, fileName, fileLen, vt) < 0)
		break;
	}
	hi += VT_INCREMENT;
	/* XXX loop limit? */
    }

    fileName = safe_strndup(fileName, fileLen);
    tabName = safe_strndup(tabName, tabLen);

    /*
     * Create table within a transaction so we can run several queries and
     * still be able to roll it back in case of error.
     */
    if (PushQuery(q) < 0)
	return(SqlError(t, DBTOKTOERR(DBERR_CANT_PUSH)));

    /*
     * Create physical table (in a transaction environment this will
     * recursively create up to a real physical file, but any populating of
     * the file within the transaction will go to memory.  XXX relay on
     * the vacuum cleaner to remove non-existant physical files. XXX race,
     * really need to defer physical file creation).  XXX should create
     * physical file when schema is committed.
     */

    {
	Table *tab;
	int error;

	tab = OpenTable(q->q_Db, fileName, "dt0", NULL, &error);
	if (tab)
	    CloseTable(tab, 1);
    }

    /*
     * Insert new table into sys.tables
     */
    ExecuteSql(
	q,
	"INSERT INTO sys.tables"
	    " ( SchemaName, TableName, TableVId, TableFile )"
	    " VALUES ( '%s', '%s', '" VT_FMT_STRING "', '%s' )",
	si->si_ScmName, 
	tabName,
	vt, 
	fileName
    );

    /*
     * Generate columns.  These also generate inserts.
     */
    if (type == TOK_OPAREN) {
	col_t cid = CID_MIN_USER;

	type = ParseSqlColType(t, q, SqlToken(t), si->si_ScmName, tabName, cid++);
	while (type == TOK_COMMA && cid < COL_ID_NUM) {
	    type = ParseSqlColType(t, q, SqlToken(t), si->si_ScmName, tabName, cid++);
	}
	if (type != TOK_CPAREN)
	    type = SqlError(t, DBTOKTOERR(DBERR_SYNTAX_ERROR));
	type = SqlToken(t);
    }

    /*
     * Pop the transaction
     */
    if (type & TOKF_ERROR) {
	PopQuery(q, 0);
    } else {
	if (PopQuery(q, 1) < 0) {
	    type = SqlError(t, DBTOKTOERR(DBERR_MACRO_SQL));
	} else {
	    dbinfo2("Committed\n");
	}
    }

    return(type);
}

/*
 * DROP SCHEMA schemaname
 *
 */

int
ParseSqlDropSchema(token_t *t, Query *q, int type)
{
    SchemaI *si;
    char *scmName;
    int r;

    if ((type & TOKF_ID) == 0)
	return(SqlError(t, DBTOKTOERR(DBERR_SYNTAX_ERROR)));

    if ((si = HLGetSchemaI(q, t->t_Data, t->t_Len)) == NULL)
	return(SqlError(t, DBTOKTOERR(DBERR_SCHEMA_NOT_FOUND)));

    scmName = safe_strndup(t->t_Data, t->t_Len);
    type = SqlToken(t);

    if (strcasecmp(scmName, "sys") == 0) {
	free(scmName);
	return(SqlError(t, DBTOKTOERR(DBERR_CANT_REMOVE_SYS_SCHEMA)));
    }

    if (PushQuery(q) < 0) {
	free(scmName);
	return(SqlError(t, DBTOKTOERR(DBERR_CANT_PUSH)));
    }

    r = ExecuteSql(
	q,
	"SELECT TableName FROM sys.tables WHERE SchemaName = '%s'",
	scmName
    );
    if (r) {
	free(scmName);
	PopQuery(q, 0);
	return(SqlError(t, DBTOKTOERR(DBERR_CANT_RM_SCH_WITH_TABLES)));
    }

    ExecuteSql(
	q,
	"DELETE FROM sys.schemas WHERE SchemaName = '%s'",
	scmName
    );
    free(scmName);

    /*
     * Pop the transaction
     */
    if (type & TOKF_ERROR) {
	PopQuery(q, 0);
    } else {
	if (PopQuery(q, 1) < 0) {
	    type = SqlError(t, DBTOKTOERR(DBERR_MACRO_SQL));
	} else {
	    dbinfo2("Committed\n");
	}
    }
    return(type);
}

/*
 * DROP TABLE [schema.]tablename
 *
 */

int
ParseSqlDropTable(token_t *t, Query *q, int type)
{
    TableI *ti;
    int saveError;

    type = ParseSqlTable(t, q, type);
    if (type & TOKF_ERROR)
	return(type);
    if ((ti = q->q_TableIQBase) == NULL)
	return(SqlError(t, DBTOKTOERR(DBERR_TABLE_NOT_FOUND)));

    if (PushQuery(q) < 0)
	return(SqlError(t, DBTOKTOERR(DBERR_CANT_PUSH)));

    /*
     * Create a schema to hold the deleted table.  It is ok for this
     * query to fail due the schema already existing.
     */
    saveError = q->q_Error;
    ExecuteSql(
	q,
	"CLONE sys.schemas SET SchemaName = '$DEL_%s' WHERE SchemaName = '%s'",
	ti->ti_SchemaI->si_ScmName,
	ti->ti_SchemaI->si_ScmName
    );
    q->q_Error = saveError;

    ExecuteSql(
	q,
	"UPDATE sys.tables SET TableName = '$DEL" VT_FMT_STRING "',"
	" SchemaName = '$DEL_%s' WHERE"
	" TableName = '%s' AND SchemaName = '%s'",
	ti->ti_VTable, 
	ti->ti_SchemaI->si_ScmName,
	ti->ti_TabName,
	ti->ti_SchemaI->si_ScmName
    );
    if (PopQuery(q, 1) < 0) {
	type = SqlError(t, DBTOKTOERR(DBERR_MACRO_SQL));
    } else {
	dbinfo2("Committed\n");
    }
    return(type);
}

/************************************************************************
 *			PARSER HELPER ROUTINES				*
 ************************************************************************
 *
 *	These routine parse the nity grity stuff for the parser.  Note
 *	that expressions are parsed in parseexp.c (XXX)
 */

/*
 * ParseSqlTable() - Parse table selector
 *
 *	[schema.]table[=alias]
 */
int
ParseSqlTable(token_t *t, Query *q, int type)
{
    token_t save;
    TableI *ti = NULL;

    if ((type & TOKF_ID) == 0)
	return(SqlError(t, DBTOKTOERR(DBERR_EXPECTED_TABLE)));

    save = *t;
    type = SqlToken(t);
    if (type == TOK_EQ) {
	type = SqlToken(t);
	if ((type & TOKF_ID) == 0)
	    return(SqlError(t, DBTOKTOERR(DBERR_EXPECTED_ID)));
	if ((ti = HLGetTableI(q, q->q_DefSchemaI, save.t_Data, save.t_Len, t->t_Data, t->t_Len)) == NULL)
	    type = SqlError(t, DBTOKTOERR(DBERR_TABLE_NOT_FOUND));

	type = SqlToken(t);
    } else {
	if ((ti = HLGetTableI(q, q->q_DefSchemaI, save.t_Data, save.t_Len, NULL, 0)) == NULL)
	    type = SqlError(t, DBTOKTOERR(DBERR_TABLE_NOT_FOUND));
    }
    if (ti && (q->q_Flags & QF_RETURN_ALL))
	ti->ti_ScanOneOnly = -1;
    return(type);
}

/*
 * ParseSqlCol() - parse a column selector
 *
 *	If q is NULL, we do a dummy parsing of the selector, else we
 *	do a real one.
 */

int
ParseSqlCol(token_t *t, Query *q, int flags, int type)
{
    if (type == TOK_STAR) {
	if (flags & CIF_WILD) {
	    if (q && HLGetColI(q, NULL, 0, flags) == NULL)
		type = SqlError(t, DBTOKTOERR(DBERR_COLUMN_NOT_FOUND));
	    else
		type = SqlToken(t);
	} else {
	    type = SqlError(t, DBTOKTOERR(DBERR_WILDCARD_ILLEGAL));
	}
    } else if (type & TOKF_ID) {
	if (q && HLGetColI(q, t->t_Data, t->t_Len, flags) == NULL) {
	    type = SqlError(t, DBTOKTOERR(DBERR_COLUMN_NOT_FOUND));
	} else {
	    type = SqlToken(t);
	}
    } else {
	type = SqlError(t, DBTOKTOERR(DBERR_EXPECTED_COLUMN));
    }
    return(type);
}

/*
 * Parse a data object, set type.  For the moment we parse to generic
 * data (i.e. string, integer, etc... all converted to strings).
 */

int
ParseSqlData(token_t *t, Query *q, ColData **pcd, int type)
{
    switch(type) {
    case TOK_STRING:
	DBASSERT(t->t_Len >= 2);
	*pcd = GetConstUnEscape(q, t->t_Data + 1, t->t_Len - 2);
	type = SqlToken(t);
	break;
    case TOK_NULL:
	*pcd = GetConst(q, NULL, 0);
	type = SqlToken(t);
	break;
    default:
	type = SqlError(t, DBTOKTOERR(DBERR_EXPECTED_DATA));
	break;
    }
    return(type);
}

/*
 * ParseSqlColAssign() -	parse col = 'data'
 *
 *	CIF_ORDER is implied.  We use this so HLCheckFieldRestrictions can
 *	identify the fields being set or modified.
 */

int
ParseSqlColAssignment(token_t *t, Query *q, int type)
{
    if (type & TOKF_ID) {
	ColI *ci = HLGetColI(q, t->t_Data, t->t_Len, CIF_ORDER);

	if (ci == NULL) {
	    type = SqlError(t, DBTOKTOERR(DBERR_COLUMN_NOT_FOUND));
	} else {
	    type = SqlToken(t);
	    type = SqlSkip(t, TOK_EQ);
	    if (ci->ci_Const != NULL)
		type = SqlError(t, DBTOKTOERR(DBERR_DUPLICATE_COLUMN));
	    else
		type = ParseSqlData(t, q, &ci->ci_Const, type);
	}
    } else {
	type = SqlError(t, DBTOKTOERR(DBERR_EXPECTED_COLUMN));
    }
    return(type);
}

/*
 * ParseSqlColOrder(token_t *t, Query *q, int type)
 *
 *	Parse column in ORDER BY clause. CIF_SORTORDER is assumed.
 */
int
ParseSqlColOrder(token_t *t, Query *q, int type)
{
    token_t colid = *t;
    int desc = 0;

    type = SqlSkip(t, TOK_ID);	/* Skip column for now. */
    if (type == TOK_DESC) {
	/* Handle optional DESC clause. */
	type = SqlSkip(t, TOK_DESC);
	desc = CIF_SORTDESC;
    }
    ParseSqlCol(&colid, q, CIF_SPECIAL | CIF_SORTORDER | desc, TOK_ID);
    return(type);
}


/*
 * ParseExp() - parses the WHERE clause for several SQL statements
 *
 *	Stage 1 resolution must have already occured prior to being able
 *	to parse range elements!
 *
 *	This routine generates Range elements
 */
int
ParseSqlExp(token_t *t, Query *q, int type)
{
    Range *r = NULL;

    for (;;) {
	int opid;
	int ropid;
	int stamp_opid = -1;
	int stamp_ropid = -1;
	ColI *lhs = NULL;
	ColI *rhs = NULL;
	ColData *lcd = NULL;
	ColData *rcd = NULL;

	type = ParseSqlExpVal(t, q, &lhs, &lcd, type);

	opid = ROP_EQEQ;
	ropid = opid;

	switch(type) {
	case TOK_EQ:
	    stamp_opid = ROP_STAMP_EQEQ;
	    stamp_ropid = ROP_STAMP_EQEQ;
	    break;
	case TOK_LT:
	    opid = ROP_LT;
	    ropid = ROP_GTEQ;
	    stamp_opid = ROP_STAMP_LT;
	    stamp_ropid = ROP_STAMP_GTEQ;
	    break;
	case TOK_LTEQ:
	    opid = ROP_LTEQ;
	    ropid = ROP_GT;
	    stamp_opid = ROP_STAMP_LTEQ;
	    stamp_ropid = ROP_STAMP_GT;
	    break;
	case TOK_LIKE:
	    opid = ROP_LIKE;
	    ropid = ROP_RLIKE;
	    break;
	case TOK_SAME:
	    opid = ROP_SAME;
	    ropid = ROP_RSAME;
	    break;
	case TOK_GT:
	    opid = ROP_GT;
	    ropid = ROP_LTEQ;
	    stamp_opid = ROP_STAMP_GT;
	    stamp_ropid = ROP_STAMP_LTEQ;
	    break;
	case TOK_GTEQ:
	    opid = ROP_GTEQ;
	    ropid = ROP_LT;
	    stamp_opid = ROP_STAMP_GTEQ;
	    stamp_ropid = ROP_STAMP_LT;
	    break;
	case TOK_NOTEQ:
	    opid = ROP_NOTEQ;
	    ropid = opid;
	    break;
	default:
	    type = SqlError(t, DBTOKTOERR(DBERR_EXPECTED_OPERATOR));
	    break;
	}
	if ((type & TOKF_ERROR) == 0)
	    type = ParseSqlExpVal(t, q, &rhs, &rcd, SqlToken(t));

	if (type & TOKF_ERROR)
	    break;

	if (lcd && rcd) {
	    type = SqlError(t, DBTOKTOERR(DBERR_CANNOT_HAVE_TWO_CONSTS));
	    break;
	}

	/*
	 * Optimize cooked column compares against timestamps for the
	 * CID_COOK_TIMESTAMP column, because the btree only caches the
	 * first 8 characters and the timestamp is 16 characters in 
	 * ascii-hex.
	 */
	if (stamp_opid >= 0 && lhs && rcd && 
	    (lhs->ci_ColId == CID_COOK_TIMESTAMP || 
	     lhs->ci_ColId == CID_COOK_DATESTR)
	) {
	    dbstamp_t ts = ConstHexToStamp(rcd);
	    lhs = HLGetRawColI(q, lhs->ci_TableI, CID_RAW_TIMESTAMP);
	    rcd = GetConst(q, &ts, sizeof(ts));
	    opid = stamp_opid;
	    ropid = stamp_ropid;
	} else if (stamp_opid >= 0 && rhs && lcd && 
	    (rhs->ci_ColId == CID_COOK_TIMESTAMP ||
	     rhs->ci_ColId == CID_COOK_DATESTR)
	) {
	    dbstamp_t ts = ConstHexToStamp(lcd);
	    rhs = HLGetRawColI(q, rhs->ci_TableI, CID_RAW_TIMESTAMP);
	    lcd = GetConst(q, &ts, sizeof(ts));
	    opid = stamp_opid;
	    ropid = stamp_ropid;
	}

	if (lhs && rhs) {
	    /*
	     * For a join either scan the right hand table looking for matches
	     * against the left hand table, or vise versa.  Hopefully one of
	     * the two sides is already restricted so we can avoid actually
	     * scanning the whole table.  Prefer the left hand side.
	     */
	    int rhsBest = 0;
	    Range *r2;
	    for (r2 = r; r2; r2 = r2->r_Prev) {
		if (r2->r_TableI == rhs->ci_TableI && 
		    (r2->r_Type == ROP_CONST || r2->r_Type == ROP_JCONST)) {
		    rhsBest = 1;
		}
		if (r2->r_TableI == lhs->ci_TableI && 
		    (r2->r_Type == ROP_CONST || r2->r_Type == ROP_JCONST)) {
		    rhsBest = -1;
		    break;
		}
	    }
	    if (rhsBest > 0) {
		r = HLAddClause(q, r, rhs->ci_TableI, rhs->ci_CData, NULL, -1, ROP_JOIN);
		r = HLAddClause(q, r, lhs->ci_TableI, lhs->ci_CData, r->r_Col, opid, ROP_JCONST);
	    } else {
		r = HLAddClause(q, r, lhs->ci_TableI, lhs->ci_CData, NULL, -1, ROP_JOIN);
		r = HLAddClause(q, r, rhs->ci_TableI, rhs->ci_CData, r->r_Col, opid, ROP_JCONST);
	    }
	} else if (rcd) {
	    r = HLAddClause(q, r, lhs->ci_TableI, lhs->ci_CData, rcd, opid, ROP_CONST);
	} else {
	    r = HLAddClause(q, r, rhs->ci_TableI, rhs->ci_CData, lcd, ropid, ROP_CONST);
	}

	if (type == TOK_AND_CLAUSE) {
	    type = SqlToken(t);
	    continue;
	}
	break;
    }
    return(type);
}

int
ParseSqlExpVal(token_t *t, Query *q, ColI **pci, ColData **pcd, int type)
{
    if (type != TOK_NULL && (type & TOKF_ID)) {
	*pci = HLGetColI(q, t->t_Data, t->t_Len, CIF_SPECIAL|CIF_SET_SPECIAL);
	if (*pci == NULL)
	    type = SqlError(t, DBTOKTOERR(DBERR_COLUMN_NOT_FOUND));
	else
	    type = SqlToken(t);
    } else {
	type = ParseSqlData(t, q, pcd, type);
    }
    return(type);
}

/*
 * ParseSqlColType
 *
 *	NAME TYPE
 *
 * Currently only type VARCHAR is supported
 */
int
ParseSqlColType(token_t *t, Query *q, int type, const char *scmName, const char *tabName, col_t cid)
{
    char *colName;
    char *colType;
    char *colFlags;
    char *alloc = NULL;
    ColData *def = NULL;
    int error;

    if ((type & TOKF_ID) == 0)
	return(SqlError(t, DBTOKTOERR(DBERR_SYNTAX_ERROR)));
    colName = safe_strndup(t->t_Data, t->t_Len);
    colFlags = safe_strdup("");
    type = SqlToken(t);

    colType = safe_strndup_tolower(t->t_Data, t->t_Len);
    if (DataTypeLookup(colType, t->t_Len) < 0)
	type = SqlError(t, DBTOKTOERR(DBERR_UNRECOGNIZED_TYPE));

    type = SqlToken(t);

    while ((type & TOKF_ID) != 0) {
	char flag = 0;

	switch(type) {
	case TOK_UNIQUE:
	    flag = 'U';
	    if (strchr(colFlags, 'K')) {
		type = SqlError(t, DBTOKTOERR(DBERR_NOT_BOTH_UNIQUE_PRIMARY));
		continue;
	    }
	    type = SqlToken(t);
	    break;
	case TOK_PRIMARY:
	    type = SqlToken(t);
	    switch(type) {
	    case TOK_KEY:
		flag = 'K';
		if (strchr(colFlags, 'U')) {
		    type = SqlError(t, DBTOKTOERR(DBERR_NOT_BOTH_UNIQUE_PRIMARY));
		    continue;
		}
		break;
	    }
	    type = SqlToken(t);
	    break;
	case TOK_DEFAULT:
	    if (def) {
		type = SqlError(t, DBTOKTOERR(DBERR_DUPLICATE_DEFAULT));
		continue;
	    }
	    type = SqlToken(t);
	    type = ParseSqlData(t, q, &def, type);
	    flag = 'V';
	    break;
	case TOK_NOT:
	    type = SqlToken(t);
	    switch(type) {
	    case TOK_NULL:
		flag = 'N';
		break;
	    default:
		/*
		 * will fall through to unrecognized attribute
		 */
		break;
	    }
	    type = SqlToken(t);
	    break;
	default:
	    break;
	}
	if (flag == 0) {
	    type = SqlError(t, DBTOKTOERR(DBERR_UNRECOGNIZED_ATTR));
	    break;
	}
	if (strchr(colFlags, flag) == NULL) {
	    int l = strlen(colFlags);
	    colFlags = realloc(colFlags, l + 2);
	    colFlags[l++] = flag;
	    colFlags[l] = 0;
	}
    }

    error = ExecuteSql(
	q,
	"INSERT INTO %s.%s$cols"
	    " ( ColName, ColType, ColFlags, ColId, ColDefault )"
	    " VALUES ( '%s', '%s', '%s', '" COL_FMT_STRING "', %s%s%s )",
	scmName,
	tabName,
	colName,
	colType,
	colFlags,
	cid,
	((def == NULL || def->cd_Data == NULL) ? "" : "'"),
	((def == NULL || def->cd_Data == NULL) ? "NULL" : DBMSEscape(def->cd_Data, &alloc, def->cd_Bytes)),
	((def == NULL || def->cd_Data == NULL) ? "" : "'")
    );
    safe_free(&alloc);
    if (error < 0)
	type = SqlError(t, DBTOKTOERR(error));
    free(colName);
    free(colType);
    free(colFlags);
    return(type);
}

