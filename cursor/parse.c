/*
 * CURSOR/PARSE.C
 *
 * (c)Copyright 2000-2002 Backplane, Inc.  Please refer to the COPYRIGHT 
 * file at the base of the distribution tree.
 *
 * $Backplane: rdbms/cursor/parse.c,v 1.40 2002/08/20 22:05:45 dillon Exp $
 *
 *	Parse SQL text into queries that can then be run.
 */

#include "defs.h"

Export int ParseSql(token_t *t, Query *q, int type);

static int ParseSqlInsert(token_t *t, Query *q, int type);
static int ParseSqlDelete(token_t *t, Query *q, int type);
static int ParseSqlSelect(token_t *t, Query *q, int type);
static int ParseSqlUpdate(token_t *t, Query *q, int type);
static int ParseSqlCreate(token_t *t, Query *q, int type);
static int ParseSqlAlter(token_t *t, Query *q, int type);
static int ParseSqlDrop(token_t *t, Query *q, int type);
static int ParseSqlElement(token_t *t, Query *q, int type, int flags);

int ParseSqlTable(token_t *t, Query *q, int type);
int ParseSqlCol(token_t *t, Query *q, int flags, int type);
int ParseSqlColDefs(token_t *t, Query *q, int flags, int type);
int ParseSqlAddCol(token_t *t, Query *q, int flags, int type);
int ParseSqlDropCol(token_t *t, Query *q, int flags, int type);
int ParseSqlColAssignment(token_t *t, Query *q, int type);
int ParseSqlData(token_t *t, Query *q, int flags, int type);
static int ParseSqlExp(token_t *t, Query *q, int flags, int type);
static void DumpASData(Query *q);

static void columnListDestruct(List *columnList);
static void columnListAdd(List *columnList, char *columnName, int columnIndex, int flags);

Export Query *QueryConstruct(Query *parentQuery, u_int varFlags, FILE *outFile);
Export void QueryReset(Query *query);
Export void QueryDestruct(Query *query);
static void querySetAlias(Query *q, token_t *t);
static const char *queryFindAlias(Query *q, const char *alias, int len);

Export ColumnEntry *columnListSearch(char *key, List *columnList);
Export ColumnEntry *columnQuerySearch(char *key, Query *query);
Export int convertColumnNameToRowExpression(char *key, Query *query, char **rowExpression);

static char *ParseExp(token_t *t);

#define CLA_QUOTED	0x0001

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
    case TOK_ALTER:
	type = ParseSqlAlter(t, q, SqlToken(t));
	break;
    case TOK_DROP:
	type = ParseSqlDrop(t, q, SqlToken(t));
	break;
    default:
	type = SqlError(t, TOK_ERR_UNRECOGNIZED_KEYWORD);
	break;
    }
    if (type == TOK_SEMI)
	type = SqlToken(t);
    else if (type == TOK_EOF1)
	type = SqlToken(t);
    else
	type = SqlError(t, TOK_ERR_SYNTAX_ERROR);
    return(type);
}


/* 
 * CREATE TABLE table '(' column type [ NOT NULL ] [ PRIMARY KEY / UNIQUE ] ')'
 */
static int 
ParseSqlCreate(token_t *t, Query *q, int type)
{
    const char *beginCreate;
    ColumnEntry *symbol;

    type = SqlSkip(t, TOK_TABLE);

    fprintf(q->outFile, " CREATE TABLE ");

    /*
     * Parse Table
     */
    type = ParseSqlTable(t, q, type);

    /* 
     * Parse Columns
     */
    if (type == TOK_OPAREN) {
	fprintf(q->outFile, " ( ");
	type = ParseSqlColDefs(t, q, CIF_PRINT, SqlToken(t));
	while (type == TOK_COMMA) {
	    fprintf(q->outFile, ", ");
	    type = ParseSqlColDefs(t, q, CIF_PRINT, SqlToken(t));
	}
	type = SqlSkip(t, TOK_CPAREN);
    }
    else {
	type = SqlError(t, TOK_ERR_MUST_DECLARE_COLS);
    }

    fprintf(q->outFile, " ) "); 
    beginCreate = t->t_Data; 
    fwrite_nonl(beginCreate, 1, t->t_Data - beginCreate, q->outFile); 
    fprintf(q->outFile, "\" ");

    symbol = (ColumnEntry *)getHead(&q->symbolList);
    while (symbol != NULL) {
	fprintf(q->outFile, ", ");
	fprintf(q->outFile, symbol->columnName);
	symbol = (ColumnEntry *)getListSucc(&q->symbolList, (Node *)symbol);
    }

    return(type);
}

/* 
 * DROP TABLE table
 */
static int 
ParseSqlDrop(token_t *t, Query *q, int type)
{
    type = SqlSkip(t, TOK_TABLE);

    fprintf(q->outFile, "DROP TABLE ");

    type = ParseSqlTable(t, q, type);

    fprintf(q->outFile, "\"");

    return(type);
}

/*
 * ALTER TABLE table    
 *
 *	ADD [ COLUMN ] name type [ NOT NULL ] [ PRIMARY KEY / UNIQUE ]
 *
 *	DROP [ COLUMN ] name
 */
static int
ParseSqlAlter(token_t *t, Query *q, int type)
{
    char *key;
    const char *beginAlter;
    ColumnEntry *symbol;

    type = SqlSkip(t, TOK_TABLE);

    fprintf(q->outFile, "ALTER TABLE ");

    key = safe_strndup(t->t_Data, t->t_Len);

    /*
     * Substitute table name if $var
     */
    type = ParseSqlTable(t, q, type);

    if (type == TOK_ADD) {
	type = ParseSqlAddCol(t, q, CIF_PRINT, type);
    }
    else if (type == TOK_DROP) {
	type = ParseSqlDropCol(t, q, CIF_PRINT, type);
    }

    beginAlter = t->t_Data;
    fwrite_nonl(beginAlter, 1, t->t_Data - beginAlter, q->outFile);
    fprintf(q->outFile, "\"");

    symbol = (ColumnEntry *)getHead(&q->symbolList);
    while (symbol != NULL) {

	fprintf(q->outFile, ", ");
	fprintf(q->outFile, symbol->columnName);
	symbol = (ColumnEntry *)getListSucc(&q->symbolList, (Node *)symbol);
    }

    return(type);
}

/*
 * INSERT INTO table [ '(' columns ')' ] VALUES '(' data ')'
 */

static int
ParseSqlInsert(token_t *t, Query *q, int type)
{
    const char	*beginInsert;
    int		ret;
    ColumnEntry	*symbol;

    fprintf(q->outFile, "INSERT INTO "); 

    type = SqlSkip(t, TOK_INTO);
    type = ParseSqlTable(t, q, type);

    beginInsert = t->t_Data;

    /*
     * Columns
     */
    if (type == TOK_OPAREN) {
	type = ParseSqlCol(t, q, CIF_ORDER, SqlToken(t));
	while (type == TOK_COMMA)
	    type = ParseSqlCol(t, q, CIF_ORDER, SqlToken(t));
	type = SqlSkip(t, TOK_CPAREN);
    } else {
	type = SqlError(t, TOK_ERR_MUST_DECLARE_COLS);
    }

    /* Write the columns and tables */
    fwrite_nonl(beginInsert, 1, t->t_Data-beginInsert, q->outFile); 

    /*
     * Values
     */
    type = SqlSkip(t, TOK_VALUES);
    type = SqlSkip(t, TOK_OPAREN);
    fprintf(q->outFile, "VALUES (");
    for (;;) {
	type = ParseSqlData(t, q, CIF_CONST, type);
	if (type != TOK_COMMA)
	    break;
	fprintf(q->outFile, ", ");
	type = SqlToken(t);
    }
    if (type != TOK_CPAREN)
	type = SqlError(t, TOK_ERR_TOO_MUCH_DATA);
    type = SqlSkip(t, TOK_CPAREN);

    /* Close the paren in the VALUES clause */
    fprintf(q->outFile, ")");

    /* Close the specifier string in the asprintf */
    fprintf(q->outFile, "\"");

    /* Write any variables that were in the VALUES clause, and
     * close the asprintf
    */
    symbol = (ColumnEntry *)getHead(&q->symbolList);
    while (symbol != NULL) {
	char *rowString;

	ret = convertColumnNameToRowExpression(symbol->columnName,
		    q->parentQuery, &rowString);
	if (ret == q->queryLevel) {
	    fprintf(q->outFile, ", %s", rowString);
	    free(rowString);
	} else if (ret >= 0) {
	    fprintf(q->outFile, ", EscapeStr(&LIST, %s)", rowString);
	    free(rowString);
	} else if (symbol->flags & CLA_QUOTED) {
	    fprintf(q->outFile, ", EscapeStr(&LIST, %s)", symbol->columnName);
	} else {
	    fprintf(q->outFile, ", %s", symbol->columnName ? "NULL" : symbol->columnName);
	}
	symbol = (ColumnEntry *)getListSucc(&q->symbolList, (Node *)symbol);
    }

    /* Clear the symbol list */
    columnListDestruct(&q->symbolList); 

    return(type);
}

/*
 * DELETE FROM table WHERE exp
 *
 * example exp:
 *
 *	t.a = 'x' AND t.u = joe.blah AND t.a = str
 */
static int
ParseSqlDelete(token_t *t, Query *q, int type)
{
    fprintf(q->outFile, "DELETE FROM "); 

    type = SqlSkip(t, TOK_FROM);
    type = ParseSqlTable(t, q, type);

    if (type == TOK_WHERE) {
	fprintf(q->outFile, "WHERE ");
	type = SqlSkip(t, TOK_WHERE);
	type = ParseSqlExp(t, q, CIF_ALIAS, type);
	DumpASData(q);
    } else {
	type = SqlError(t, TOK_ERR_MISSING_WHERE);
    }

    /* Clear the symbol list */
    columnListDestruct(&q->symbolList); 

    return(type);
}

/*
 * SELECT columns FROM tables WHERE exp
 */

static int
ParseSqlSelect(token_t *t, Query *q, int type)
{
    const char *beginSelect;
    int qtype = type;
    token_t redo;
    int rtype;

    type = SqlToken(t);
  
    redo = *t;
    rtype = type;

    switch(qtype) {
    case TOK_SELECT:
	fprintf(q->outFile, "SELECT ");
	break;
    case TOK_COUNT:
	fprintf(q->outFile, "COUNT ");
	break;
    case TOK_HISTORY:
	fprintf(q->outFile, "HISTORY ");
	break;
    }

    /*
     * Skip columns to get to tables so we can parse the tables first,
     * then redo the columns.
     */
    beginSelect = t->t_Data;
    if (qtype == TOK_SELECT || qtype == TOK_HISTORY) {
	type = ParseSqlCol(t, NULL, CIF_WILD|CIF_ORDER, type);
	while (type == TOK_COMMA) {
	    type = ParseSqlCol(t, NULL, CIF_WILD|CIF_ORDER, SqlToken(t));
	}
    }
    fwrite_nonl(beginSelect, 1, t->t_Data-beginSelect, q->outFile); 

    /*
     * Parse tables
     */
    type = SqlSkip(t, TOK_FROM);
    fprintf(q->outFile, " FROM ");
    type = ParseSqlTable(t, q, type);
    while (type == TOK_COMMA) {
	fprintf(q->outFile, ",");
	type = ParseSqlTable(t, q, SqlToken(t));
    }


    /*
     * Go back and parse the selection columns
     */
    if (qtype == TOK_SELECT || qtype == TOK_HISTORY) {
	rtype = ParseSqlCol(&redo, q, CIF_ORDER|CIF_WILD|CIF_ADDCOL, rtype);
	while (rtype == TOK_COMMA) {
	    rtype = ParseSqlCol(&redo, q, CIF_ORDER|CIF_WILD|CIF_ADDCOL,
		    SqlToken(&redo));
	}
    }

    /*
     * Parse optional where clause, then cleanup null scans
     */
    if (type == TOK_WHERE) {
	int dq = '"';

	fprintf(q->outFile, "%c\n", dq);
	fprintfIndent(q->outFile, "%cWHERE", dq);
	type = SqlSkip(t, TOK_WHERE);
	type = ParseSqlExp(t, q, CIF_ALIAS, type);
    }
    if (type == TOK_ORDER) {
	fprintf(q->outFile, "\"\n");
	fprintfIndent(q->outFile, "\"ORDER BY ");
	type = SqlSkip(t, TOK_ORDER);
	type = SqlSkip(t, TOK_BY);

	for(;;) {
	    type = ParseSqlCol(t, q, CIF_PRINT, TOK_ID);
	    if (type == TOK_DESC) {
		/* Handle optional DESC clause. */
		type = SqlSkip(t, TOK_DESC);
		fprintf(q->outFile, " DESC");
	    }
	    if (type != TOK_COMMA)
		break;
	    fprintf(q->outFile, ", ");
	    type = SqlSkip(t, TOK_COMMA);
	}
    }
    if (type == TOK_LIMIT) {
	fprintf(q->outFile, "\"\n");
	fprintf(q->outFile, "\" LIMIT ");
	type = SqlSkip(t, TOK_LIMIT);

	type = ParseSqlElement(t, q, type, CIF_PRINT);
	if (type == TOK_COMMA) {
	    /* Optional "offset, rows" syntax. */
	    type = SqlSkip(t, TOK_COMMA);
	    fprintf(q->outFile, ", ");
	    type = ParseSqlElement(t, q, type, CIF_PRINT);
	} 
    }
    DumpASData(q);

    /* Clear the symbol list */
    columnListDestruct(&q->symbolList); 

    return(type);
}

/*
 * UPDATE tables SET col = data [, var = data ]* WHERE exp
 *
 * UPDATE user.test=t SET t.pay = '$10', t.discount = '$50' 
 *	WHERE t.name = 'Fu' AND t.pay = '$0';
 */

static int
ParseSqlUpdate(token_t *t, Query *q, int type)
{
    switch(type) {
    case TOK_CLONE:
	fprintf(q->outFile, "CLONE ");
	break;
    default:
	fprintf(q->outFile, "UPDATE "); 
	break;
    }

    /*
     * Tables
     */
    type = ParseSqlTable(t, q, SqlToken(t));
    while (type == TOK_COMMA) {
	fprintf(q->outFile, ",");
	type = ParseSqlTable(t, q, SqlToken(t));
    }

    /*
     * Assignments
     */
    type = SqlSkip(t, TOK_SET);
    fprintf(q->outFile, "SET "); 
    type = ParseSqlColAssignment(t, q, type);
    while (type == TOK_COMMA) {
	fprintf(q->outFile, ", "); 
	type = ParseSqlColAssignment(t, q, SqlToken(t));
    }

    /*
     * Required WHERE clause
     */
    if (type == TOK_WHERE) {
	fprintf(q->outFile, " WHERE ");
	type = SqlSkip(t, TOK_WHERE);
	type = ParseSqlExp(t, q, CIF_ALIAS, type);
	DumpASData(q);
    } else {
	type = SqlError(t, TOK_ERR_MISSING_WHERE);
    }

    /* Clear the symbol list */
    columnListDestruct(&q->symbolList); 

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
    if ((type & TOKF_ID) == 0)
	return(SqlError(t, TOK_ERR_EXPECTED_TABLE));

    type = ParseSqlElement(t, q, type, CIF_PRINT);
    if (type == TOK_EQ) {
	type = SqlToken(t);
	if ((type & TOKF_ID) == 0)
	    return(SqlError(t, TOK_ERR_EXPECTED_ID));
	querySetAlias(q, t);
	fprintf(q->outFile, "=");
	fwrite(t->t_Data, 1, t->t_Len, q->outFile);
	type = SqlToken(t);
    }
    fprintf(q->outFile, " ");
    return(type);
}

int 
ParseSqlDropCol(token_t *t, Query *q, int flags, int type)
{
    if (type == TOK_DROP) {

	if (flags & CIF_PRINT)
	    fprintf(q->outFile, " DROP ");

	type = SqlSkip(t, TOK_DROP);
	if (type == TOK_COLUMN) {
	    
	    if (flags & CIF_PRINT) 
		fprintf(q->outFile, "COLUMN ");

	    type = SqlSkip(t, TOK_COLUMN);
	}

	if (type == TOK_ID) {
	    type = ParseSqlElement(t, q, type, flags|CIF_WITHSPACE);
	} else {
	    type = SqlError(t, TOK_ERR_EXPECTED_ID);
	}
    }
    else {
	type = SqlError(t, TOK_ERR_EXPECTED_ID);
    }
    return(type);
}

int 
ParseSqlAddCol(token_t *t, Query *q, int flags, int type)
{
    if (type == TOK_ADD) {

	if (flags & CIF_PRINT)
	    fprintf(q->outFile, " ADD ");

	type = SqlSkip(t, TOK_ADD);
	if (type == TOK_COLUMN) {
	    
	    if (flags & CIF_PRINT) 
		fprintf(q->outFile, "COLUMN ");

	    type = SqlSkip(t, TOK_COLUMN);
	}

        if (type == TOK_ID) {
	    type = ParseSqlElement(t, q, type, flags|CIF_WITHSPACE);

	    if (type == TOK_ID) {
		type = ParseSqlElement(t, q, type, flags|CIF_WITHSPACE);

		if (type == TOK_NOT) {
		    if (flags & CIF_PRINT)
			fprintf(q->outFile, " NOT ");

		    type = SqlSkip(t, TOK_NOT);
		    if (type == TOK_NULL) {
			if (flags & CIF_PRINT)
			    fprintf(q->outFile, " NULL ");
			    type = SqlSkip(t, TOK_NULL);
		    } else {
			type = SqlError(t, TOK_ERR_EXPECTED_ID);
		    }
	  	}

		/*
		 * optional primary key or unique clause
		 */

		if (type == TOK_PRIMARY) {
		    if (flags & CIF_PRINT)
			fprintf(q->outFile, " PRIMARY ");

		    type = SqlSkip(t, TOK_PRIMARY);
		    if (type == TOK_KEY) {
			if (flags & CIF_PRINT) 
			    fprintf(q->outFile, " KEY ");
			type = SqlSkip(t, TOK_KEY);
		    } else {
			type = SqlError(t, TOK_ERR_EXPECTED_ID);
		    }
		} else if (type == TOK_UNIQUE) {
		    if (flags & CIF_PRINT)
			fprintf(q->outFile, " UNIQUE ");
		    type = SqlToken(t);
		}
	    } else {
		type = SqlError(t, TOK_ERR_EXPECTED_ID);
	    }
	} else  {
	    type = SqlError(t, TOK_ERR_EXPECTED_ID);
	}
    } else {
	type = SqlError(t, TOK_ERR_EXPECTED_ID);
    }
    return(type);
}

/*
 * ParseSqlColDefs() 
 * 
 *	Parse column definition statements 
 * 	of the form:
 *
 *	colname coltype [ NOT NULL ] [ PRIMARY KEY / UNIQUE ]
 */
int
ParseSqlColDefs(token_t *t, Query *q, int flags, int type)
{
    if (type & TOKF_ID) {
	type = ParseSqlElement(t, q, type, (flags & CIF_PRINT)|CIF_WITHSPACE);
        if (type == TOK_ID) {
	    type = ParseSqlElement(t, q, type, (flags & CIF_PRINT)|CIF_WITHSPACE);

	    /* optional not null clause */

	    if (type == TOK_NOT) {
		if (flags & CIF_PRINT)
		    fprintf(q->outFile, " NOT ");
		
		type = SqlToken(t);
		if (type == TOK_NULL) {
		    if (flags & CIF_PRINT)
			fprintf(q->outFile, " NULL ");

		    type = SqlToken(t);
	        } else  {
		    type = SqlError(t, TOK_ERR_EXPECTED_ID);
		}
	    }

	    /* optional primary key or unique clause */

	    if (type == TOK_PRIMARY) {
		if (flags & CIF_PRINT)
		    fprintf(q->outFile, " PRIMARY ");

		type = SqlToken(t);
		if (type == TOK_KEY) { 
		    if (flags & CIF_PRINT)
			fprintf(q->outFile, " KEY ");
		    type = SqlToken(t);
		} else {
		    type = SqlError(t, TOK_ERR_EXPECTED_ID);
		}
	    } else if (type == TOK_UNIQUE) {
		if (flags & CIF_PRINT)
		    fprintf(q->outFile, " UNIQUE ");
		type = SqlToken(t);
	    }
	} else {
	    type = SqlError(t, TOK_ERR_EXPECTED_COLUMN);
	}
    } else {
	type = SqlError(t, TOK_ERR_EXPECTED_COLUMN);
    }
    fprintf(q->outFile, "\n\t\t\t");
    return(type);
}

/*
 * ParseSqlCol() - parse a column selector
 */
int
ParseSqlCol(token_t *t, Query *q, int flags, int type)
{
    if (type == TOK_STAR) {
	if (flags & CIF_WILD) {
	    if (flags & CIF_PRINT)
		fprintf(q->outFile, " * ");
	    type = SqlToken(t);
	} else {
	    type = SqlError(t, TOK_ERR_WILDCARD_ILLEGAL);
	}
    } else if (type & TOKF_ID) {
	type = ParseSqlElement(t, q, type, flags);
    } else {
	type = SqlError(t, TOK_ERR_EXPECTED_COLUMN);
    }
    return(type);
}

/*
 * Parse a data object.  A data object is a constant of some sort,
 * either a single-quoted string (TOK_STRING), something from the
 * string table (TOK_ID) in the current SQL statement which is just
 * passed through, something in the string table (TOK_ID) from a 
 * previous level SQL statement which must be converted into a 
 * '%s' constant, or C code, which must be converted into a '%s' constant.
 *
 * Complexity:  You have to count open/close parens.
 */
int
ParseSqlData(token_t *t, Query *q, int flags, int type)
{
    ColumnEntry	*column;
    char	*key;

    switch(type) {
    case TOK_NULL:
    case TOK_STRING:
	DBASSERT(t->t_Len >= 2);
	fwrite_nonl(t->t_Data, 1, t->t_Len, q->outFile);
	type = SqlToken(t);
	break;
    case TOK_OPAREN:
	/* XXX Count parens, spit out to outfile */
	key = ParseExp(t);
	if (key) {
	    columnListAdd(&q->symbolList, key, 0, CLA_QUOTED); 
	    fprintf(q->outFile, "%%s");
	}
	type = SqlToken(t);
	break;
    default:
	if (type & TOKF_ID) {
	    /* If a symbol is has a prefix of the form 'p.' where p matches
	     * the current alias, then write it to the output file.
	     * Otherwise add it to the symbol list, and write %s to the
	     * output file in its place.
	     */
	    key = safe_strndup(t->t_Data, t->t_Len);
	    if (flags & CIF_ALIAS) {
		int i;

		for (i = 0; i < t->t_Len; ++i) {
		    if (t->t_Data[i] == '.')
			break;
		}
		if (i != t->t_Len && queryFindAlias(q, t->t_Data, i) != NULL) {
		    fwrite_nonl(t->t_Data, 1, t->t_Len, q->outFile);
		} else if (key[0] == '$') {
		    /*
		     * Symbol is prefixed with a '$', dynamic replacement
		     */
		    columnListAdd(&q->symbolList, key + 1, 0, 0); 
		    fprintf(q->outFile, "%%s");
		} else {
		    /*
		     * Symbol is not prefixed with a '$', assume a C expression
		     * representing constant data.
		     */
		    columnListAdd(&q->symbolList, key, 0, CLA_QUOTED); 
		    fprintf(q->outFile, "%%s");
		}
	    } else {
		if ((column = columnListSearch(key, &q->columnList)) != NULL) {
		    /*
		     * Symbol in string list for this level, just output it
		     */
		    fprintf(q->outFile, " ");
		    fwrite_nonl(t->t_Data, 1, t->t_Len, q->outFile);
		} else if (key[0] == '$') {
		    /*
		     * Symbol is prefixed with a '$', dynamic replacement
		     */
		    columnListAdd(&q->symbolList, key + 1, 0, 0); 
		    fprintf(q->outFile, "%%s");
		} else {
		    /*
		     * Symbol is not prefixed with a '$', assume a C expression
		     * representing constant data.
		     */
		    columnListAdd(&q->symbolList, key, 0, CLA_QUOTED); 
		    fprintf(q->outFile, "%%s");
		}
	    }
	    free(key);

	    type = SqlToken(t);
	} else {
	    type = SqlError(t, TOK_ERR_SYNTAX_ERROR);
	}
	break;
    }
    return(type);
}

/*
 * ParseSqlColAssign() -	parse col = 'data'
 *
 *	CIF_ORDER is implied.  We use this so HLCheckFieldRestrictions can
 *	identify the fields being set or modified.
 *
 *	UPDATE tables SET col = data [, var = data ]* WHERE exp
 *
 *			      ^^^^^ this data can only be a constant of
 *				some sort.
 */

int
ParseSqlColAssignment(token_t *t, Query *q, int type)
{
    if (type & TOKF_ID) {
	type = ParseSqlElement(t, q, type, CIF_PRINT);
	fprintf(q->outFile, "=");
	type = SqlSkip(t, TOK_EQ);
	type = ParseSqlData(t, q, CIF_ALIAS|CIF_CONST, type);
    } else {
	type = SqlError(t, TOK_ERR_EXPECTED_COLUMN);
    }
    return(type);
}

/*
 * ParseSqlExp() - parses the WHERE clause for several SQL statements
 *
 *	val op val [AND val op val]*
 *
 *	op = operator, see below.
 *	val will be an ID column designator from the current SQL
 *	statement, which you would pass through directly, or from
 *	a previous level SQL statement which you would turn into
 *	a constant '%s' in the asprintf, or it would be something else,
 *	presumably C, which you would also turn into a constant
 *	'%s' in the asprintf.
 */

static int
ParseSqlExp(token_t *t, Query *q, int flags, int type)
{
    for (;;) {
	int op = 0;

	fprintf(q->outFile, " ");
	type = ParseSqlData(t, q, flags, type);
	fprintf(q->outFile, " ");

	op = type;

	switch(type) {
	case TOK_LIKE:
	    fprintf(q->outFile, "LIKE ");
	    break;
	case TOK_SAME:
	    fprintf(q->outFile, "SAME ");
	    break;
	case TOK_EQ:
	    fprintf(q->outFile, "=");
	    break;
	case TOK_LT:
	    fprintf(q->outFile, "<");
	    break;
	case TOK_LTEQ:
	    fprintf(q->outFile, "<=");
	    break;
	case TOK_GT:
	    fprintf(q->outFile, ">");
	    break;
	case TOK_GTEQ:
	    fprintf(q->outFile, ">=");
	    break;
	case TOK_NOTEQ:
	    fprintf(q->outFile, "<>");
	    break;
	default:
	    type = SqlError(t, TOK_ERR_EXPECTED_OPERATOR);
	    break;
	}

	if ((type & TOKF_ERROR) == 0)
	    type = ParseSqlData(t, q, flags, SqlToken(t));
	fprintf(q->outFile, " ");

	if (type & TOKF_ERROR)
	    break;

	if (type == TOK_AND_CLAUSE) {
	    fprintf(q->outFile, " AND ");
	    type = SqlToken(t);
	    continue;
	}
	break;
    }
    return(type);
}

static void
DumpASData(Query *q)
{
    ColumnEntry	*symbol = NULL;
    int ret;

    /* Close the specifier string in the asprintf */
    fprintf(q->outFile, "\"");

    /* Write any variables that were in the WHERE clause, and
     * close the asprintf
    */
    symbol = (ColumnEntry *)getHead(&q->symbolList);
    while (symbol != NULL) {
	char *rowString;

	ret = convertColumnNameToRowExpression(symbol->columnName,
		    q->parentQuery, &rowString);
	if (ret == q->queryLevel) {
	    fprintf(q->outFile, ", %s", rowString);
	    free(rowString);
	} else if (ret >= 0) {
	    fprintf(q->outFile, ", EscapeStr(&LIST, %s)", rowString);
	    free(rowString);
	} else if (symbol->flags & CLA_QUOTED) {
	    fprintf(q->outFile, ", EscapeStr(&LIST, %s)", symbol->columnName);
	} else {
	    fprintf(q->outFile, ", %s", symbol->columnName ? symbol->columnName : "NULL");
	}
	symbol = (ColumnEntry *)getListSucc(&q->symbolList, (Node *)symbol);
    }
}

Query *
QueryConstruct(Query *parentQuery, u_int varFlags, FILE *outFile)
{
    Query 	*query;
    static int GotoId = 64;

    if ((query = (Query *)zalloc(sizeof(*query))) == NULL) {
	fprintf(stderr, "FATAL: Cannot allocate memory for query\n");
	return(NULL);
    }

    if (parentQuery == NULL) {
	query->queryLevel = 0;
	query->parentQuery = NULL;
	query->outFile = outFile;
    }
    else {
	query->queryLevel = parentQuery->queryLevel + 1;
	query->parentQuery = parentQuery;
	query->outFile = parentQuery->outFile;
    }

    initList(&query->columnList);
    initList(&query->symbolList);
    initList(&query->aliasList);

    query->varFlags = varFlags;
    query->gotoId = GotoId++;

    return(query);
}

void
QueryReset(Query *query)
{
    columnListDestruct(&query->columnList);
    columnListDestruct(&query->symbolList);
    columnListDestruct(&query->aliasList);
    query->columnCount = 0;
}

void
QueryDestruct(Query *query)
{
    if (query == NULL)
	return;
    QueryReset(query);
    zfree(query, sizeof(Query));

    return;
}

static void
querySetAlias(Query *q, token_t *t)
{
    if (q == NULL)
	return;

    if (t == NULL)
	return;

    columnListAdd(&q->aliasList, safe_strndup(t->t_Data, t->t_Len), 0, 0);
}

static const char *
queryFindAlias(Query *q, const char *alias, int len)
{
    ColumnEntry *node;

    for (
	node = getHead(&q->aliasList);
	node;
	node = getListSucc(&q->aliasList, &node->columnNode)
    ) {
	if (strlen(node->columnName) == len &&
	    strncmp(node->columnName, alias, len) == 0
	) {
	    return(node->columnName);
	}
    }
    return(NULL);
}

static void
columnListDestruct(List *columnList)
{
    ColumnEntry *deleteNode;

    if (columnList == NULL)
	return;

    while ((deleteNode = (ColumnEntry *)remTail(columnList)) != NULL) {
	DBASSERT(deleteNode->columnName != NULL);
	free(deleteNode->columnName);
	zfree(deleteNode, sizeof(ColumnEntry));
    }
    return;
}

static void
columnListAdd(List *columnList, char *columnName, int columnIndex, int flags)
{
    ColumnEntry	*newColumn;

    newColumn = zalloc(sizeof(*newColumn));

    newColumn->columnName = strdup(columnName);
    newColumn->columnIndex = columnIndex; 
    newColumn->flags = flags;

    addTail(columnList, (Node *)newColumn);
    return;
}

ColumnEntry *
columnListSearch(char *key, List *columnList)
{
    ColumnEntry	*column;

    column = (ColumnEntry *)getHead(columnList);
    while (column != NULL) {
	if (!strcmp(key, column->columnName)) {
	    return(column);
	}
	column = (ColumnEntry *)getListSucc(columnList, (Node *)column);
    }
    return(NULL);
}

ColumnEntry *
columnQuerySearch(char *key, Query *query)
{
    ColumnEntry	*column;

    while (query != NULL) {
	if ((column = columnListSearch(key, &query->columnList)))
	    return(column);

	query = query->parentQuery;
    }
    return(NULL);
}

int
convertColumnNameToRowExpression(char *key, Query *query, char **rowExpression)
{
    ColumnEntry	*column;

    while (query != NULL) {
	if ((column = columnListSearch(key, &query->columnList)) != NULL) {
	    safe_asprintf(rowExpression, "ROW%d[%d]",
		    query->queryLevel, column->columnIndex);
	    return(query->queryLevel);
	}
	query = query->parentQuery;
    }
    return(-1);
}

static char *
ParseExp(token_t *token)
{
    const char *startExpr = token->t_Data;
    int type;
    int parenCount;

    parenCount = 1;
    while (parenCount) {
	type = SqlToken(token);

	if (type == TOK_OPAREN)
	    ++parenCount;

	if (type == TOK_CPAREN)
	    --parenCount;

	if (type & TOKF_ERROR) {
	    LexPrintError(token);
	    return(NULL);
	}

	if (type == TOK_EOF2) {
	    LexPrintError(token);
	    fprintf(stderr, "FATAL: Unexpected end of file\n");
	    return(NULL);
	}
    }
    return(safe_strndup(startExpr, token->t_Data + token->t_Len - startExpr));
}

static int
ParseSqlElement(token_t *t, Query *q, int type, int flags)
{
    char *key;

    key = safe_strndup(t->t_Data, t->t_Len);
    if (type == TOK_INT) {
	if (flags & CIF_PRINT)
	    fprintf(q->outFile, "%d", atoin(t->t_Data, t->t_Len));
	type = SqlSkip(t, TOK_INT);
    } else if ((type & TOKF_ID) && t->t_Len) {
	if (flags & CIF_PRINT) {
	    if (key[0] == '$') {
		columnListAdd(&q->symbolList, key + 1, 0, 0);
		fprintf(q->outFile, "%%s");
	    } else {
		fprintf(q->outFile, "%s", key);
	    }
	}
	type = SqlToken(t);
    } else {
	type = SqlError(t, TOK_ERR_SYNTAX_ERROR);
    }
    if ((flags & (CIF_PRINT|CIF_WITHSPACE)) == (CIF_PRINT|CIF_WITHSPACE))
	fputc(' ', q->outFile);
    if (flags & CIF_ADDCOL) {
	columnListAdd(&q->columnList, key, q->columnCount, CLA_QUOTED);
	++q->columnCount;
    }
    free(key);
    return(type);
}

