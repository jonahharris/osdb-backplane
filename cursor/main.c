/*
 * CURSOR/MAIN.C
 *
 * (c)Copyright 2000-2002 Backplane, Inc.  Please refer to the COPYRIGHT 
 * file at the base of the distribution tree.
 *
 * $Backplane: rdbms/cursor/main.c,v 1.55 2003/05/09 05:57:28 dillon Exp $
 */

#include "defs.h"

static int processCursors(const char *fileMap, unsigned long fileSize,
			FILE *outFile);
static int processSQLSection(token_t *token, const char *realBase, int realSize,
			int sqlLine, const char *sqlSection, unsigned long sqlSectionSize,
			const char *dbExpr, Query *query, FILE *outFile, int readOnly);
static int getSubsection(token_t *token,
			const char **endSection, u_int *varFlags);
static void writeCommitBlock(FILE *outFile, Query *query, const char *dbExpr, int readOnly);
/*
static int processSubsection(token_t *token,
			const char **endSection, u_int *varFlags);
*/

static void IndentSet(const char *start);
static void IndentGen(FILE *stream);
static void usage(char *progName);

Prototype int fprintfIndent(FILE *stream, const char *fmt, ...);
Prototype void IndentIncrement(void);
Prototype void IndentDecrement(void);
Prototype void fwrite_nonl(const char *ptr, int size, int nelm, FILE *fo);
Prototype void fprintfQuery(FILE *stream);

static int getIndentLevel(const char *begin);
static int SkipCExp(token_t *t, int type, char **pstr);
static void cleanup(int sigNo);
static void synchronizeLine(token_t *t, FILE *outFile);

char *OutputFileName = NULL;
char *InputFileName = "<stdin>";

int
main(int argc, char **argv)
{
    int rfd;
    char *fileMap;
    char *realOutputFileName;
    struct stat status;
    FILE *outFile;

    if (argc < 3) {
	usage(argv[0]);
	exit(-1);
    }

    signal(SIGTERM, cleanup);
	
    /* Open the input file and get its attributes */
    InputFileName = argv[1];
    if ((rfd = open(argv[1], O_RDONLY)) < 0) {
	fprintf(stderr, "Cannot open file `%s', %s\n",
 	    argv[1], strerror(errno));
	exit(-1);
    }

    if (fstat(rfd, &status) != 0) {
	fprintf(stderr, "Cannot get status for file `%s', %s\n",
 	    argv[1], strerror(errno));
	close(rfd);
	exit(-1);
    }

    /* Open the output file */
    realOutputFileName = argv[2];
    safe_asprintf(&OutputFileName, "%s.tmp", realOutputFileName);
    remove(OutputFileName);
    if ((outFile = fopen(OutputFileName, "w")) == NULL) {
	fprintf(stderr, "Cannot open output file `%s', %s\n",
 	    argv[2], strerror(errno));
	close(rfd);
	exit(-1);
    }

    /* Map the entire file as a read-only shared memory segment */
    fileMap = (char *)mmap(NULL, status.st_size, PROT_READ, MAP_SHARED, rfd, 0);
    if (fileMap == MAP_FAILED) {
	fprintf(stderr, "Cannot map file `%s', %s\n",
 	    argv[1], strerror(errno));
	close(rfd);
	remove(argv[2]);
	exit(-1);
    }

    /* Process all embedded SQL sections in the file */
    if (processCursors(fileMap, (unsigned long)status.st_size, outFile) != 0) {
	fprintf(stderr, "Error processing input file `%s', %s\n",
 	    argv[1], strerror(errno));
	remove(argv[2]);
	exit(1);
    }

    /* Unmap the shared memory segment and close the input file */
    munmap(fileMap, status.st_size);
    close(rfd); 

    /* Check for output file write errors */
    if (ferror(outFile)) {
	remove(OutputFileName);
	fprintf(stderr, "FATAL: Error writing to output file `%s'\n", argv[2]);
	exit(1);
    }
    fclose(outFile);
    rename(OutputFileName, realOutputFileName);

    return(0);
}

/*  processCursors -	Process all embedded SQL sections in the image provided
 *
*/
static int
processCursors(const char *fileMap, unsigned long fileSize, FILE *outFile)
{
    const char	*blockStart;
    u_int	varFlags;
    int		braceCount;
    char 	*dbExpr;
    char	*tsVar;
    int		type;
    token_t	token;
    token_t	sqlToken;
    char	*statusVarName = NULL;
    Query	*query;
    int		didSynchronize = 1;

    type = SqlInit(&token, fileMap, fileSize);
    blockStart = token.t_Data;

    while (!(type & (TOKF_EOF|TOKF_ERROR))) {
	/* BEGIN or BEGINRO is the start of a block of embedded SQL */
	if (type == TOK_BEGIN || type == TOK_BEGINRO) {
	    int readOnly = 0;
	    int syncMe = 0;
	    int streaming = 0;
	    char *cflags = NULL;
	    const char	*openBrace;
	    int	openBraceLine;

	    didSynchronize = 0;

	    /* Write the preceeding block of non-SQL code to the output file */
	    fwrite(blockStart, 1, token.t_Data-blockStart, outFile);

	    /* Get initial indent level */
	    IndentSet(token.t_Data);

	    if (type == TOK_BEGINRO)
		readOnly = 1;

	    /* Next token after BEGIN must be open paren */
	    if (SqlToken(&token) != TOK_OPAREN) {
		fprintf(stderr, "FATAL: Open paren must follow BEGIN\n");
		return(-1);
	    }
	    type = SqlToken(&token);
	    type = SkipCExp(&token, type, &dbExpr);
	    if (type != TOK_COMMA) {
		fprintf(stderr, "FATAL: Missing comma in BEGIN(db,ts,status)\n");
		return(-1);
	    }
	    type = SqlToken(&token);
	    type = SkipCExp(&token, type, &tsVar);
	    if (type != TOK_COMMA) {
		fprintf(stderr, "FATAL: Missing comma in BEGIN(db,ts,status)\n");
		return(-1);
	    }
	    type = SqlToken(&token);
	    type = SkipCExp(&token, type, &statusVarName);

	    /*
	     * Optional C-specified flags variable
	     */
	    if (type == TOK_COMMA) {
		type = SqlToken(&token);
		type = SkipCExp(&token, type, &cflags);
	    }

	    /*
	     * Close paren
	     */
	    if (type != TOK_CPAREN) {
		fprintf(stderr, "FATAL: Missing close paren in BEGIN\n");
		return(-1);
	    }

	    /* Generate the open transaction expression */
	    fprintf(outFile, "\n");
	    synchronizeLine(&token, outFile);

	    /*
	     * Extensions
	     */
	    while ((type = SqlToken(&token)) & TOKF_ID) {
		switch(type) {
		case TOK_SYNC:
		    syncMe = 1;
		    continue;
		case TOK_READONLY:
		    readOnly = 1;
		    continue;
		case TOK_STREAMING:
		    streaming = 1;
		    continue;
		default:
		    break;
		}
		break;
	    }

	    fprintfIndent(outFile, "PushCLTrans(%s, %s, %s|%s|%s|%s);\n", 
		dbExpr,
		tsVar, 
		(readOnly ? "CPF_READONLY" : "0"),
		(syncMe ? "CPF_RWSYNC" : "0"),
		(streaming ? "CPF_STREAM" : "0"),
		(cflags ? cflags : "0")
	    );
	    synchronizeLine(&token, outFile);

	    /* Open transaction process loop */
	    fprintfIndent(outFile, "/* BEGIN */\n");
	    synchronizeLine(&token, outFile);

	    /* Next token must be open brace */
	    if (type != TOK_OBRACE) {
		fprintf(stderr, "FATAL: Open brace must follow BEGIN()\n");
		return(-1);
	    }
	    openBrace = token.t_Data;
	    openBraceLine = token.t_Line;

	    /* Scan until close brace */
	    braceCount = 1;
	    varFlags = 0;
	    while (braceCount) {
	    	type = SqlToken(&token);

		if (type == TOK_OBRACE)
		    ++braceCount;

		if (type == TOK_INSERT || 
		    type ==  TOK_SELECT || 
		    type ==  TOK_HISTORY || 
		    type ==  TOK_COUNT || 
		    type ==  TOK_UPDATE ||
		    type ==  TOK_CLONE ||
		    type ==  TOK_DELETE 
		) {
		    varFlags |= VF_SUBLEVELS;
		}
		if (type == TOK_SELECT || type == TOK_HISTORY) {
		    varFlags |= VF_SELECT;
		}
		if (type == TOK_return) {
		    fprintf(stderr, "FATAL: Unexpected 'return' inside BEGIN\n");
		    return(-1);
		}

		if (type == TOK_CBRACE)
		    --braceCount;

		if (type & TOKF_ERROR) {
		    LexPrintError(&token);
		    return(-1);
		}

		if (type == TOK_EOF2) {
		    fprintf(stderr, "FATAL: Unexpected end of file\n");
		    return(-1);
		}
	    }

#ifdef CURDEBUG
printf("processCursors: Should be end brace: %s\n",
safe_strndup(token.t_Data, token.t_Len));
#endif

	    /* Set up the top-level query */
	    query = QueryConstruct(NULL, varFlags, outFile);
	    query->statusVarName = statusVarName;
	    query->tsVarName = tsVar;

	    /* Process the embedded SQL section */
	    if (processSQLSection(&sqlToken, token.t_FileBase, token.t_FileSize,
		    openBraceLine, openBrace, token.t_Data + token.t_Len - openBrace,
		    dbExpr, query, outFile, readOnly)
		    & TOKF_ERROR) {
		return(-1);
	    }

	    QueryDestruct(query);
	    free((void *)dbExpr);
	    synchronizeLine(&token, outFile);

#ifdef CURDEBUG
printf("processCursors: Should be end brace: %s\n",
safe_strndup(token.t_Data, token.t_Len));
#endif

	    blockStart = token.t_Data + token.t_Len;
	} else {
	    if (didSynchronize == 0) {
		synchronizeLine(&token, outFile);
		didSynchronize = 1;
	    }
	    type = SqlToken(&token);
	}
    }

    /* Write the final code block */
    fwrite(blockStart, 1, token.t_Data-blockStart, outFile);

    return(0);
}

/* processSQLSection -	Process an embedded SQL section
 *
 * Returns:	Last token processed
*/
static int
processSQLSection(token_t *token, const char *realBase, int realSize, int realLine, 
		const char *sqlSection, unsigned long sqlSectionSize, const char *dbExpr,
		Query *query, FILE *outFile, int readOnly)
{
    int		ret;
    int		type;
    int		sqlType;
    u_int	varFlags;
    int		braceCount = 0;
    token_t	subToken;
    const char	*blockStart;
    char 	*rowString;
    int 	firstBrace = 1;
    char	*columnName;
    Query	*newQuery;

#ifdef CURDEBUG
printf("processSQLSection BEGIN: %d\n", query->queryLevel);
printf("processSQLSection processing block (BLK)");
fwrite(sqlSection, 1, sqlSectionSize, stdout);
printf("(BLK)\n\n");
#endif

    type = SqlInit(token, sqlSection, sqlSectionSize);
    SqlSetSubsection(token, realBase, realSize, realLine);

    blockStart = token->t_Data;

    do {
	int didQuery = 0;

	switch (type) {
	case TOK_CREATE:
	case TOK_ALTER:
	case TOK_DROP:
	case TOK_INSERT:
	case TOK_SELECT:
	case TOK_HISTORY:
	case TOK_COUNT:
	case TOK_UPDATE:
	case TOK_CLONE:
	case TOK_DELETE:
	case TOK_ROLLBACK:
	case TOK_COMMIT:
	    sqlType = type;
	    break;

	case TOK_OBRACE:
	    /* At the first brace, insert the default definitions.
	     * Ignore everything before the first brace
	    */
	    if (firstBrace) {
		firstBrace = 0;
		fprintfIndent(outFile, "%c\n", TOK_OBRACE);
		IndentIncrement();

		if (query->varFlags & VF_SUBLEVELS) {
		    fprintfIndent(outFile, "List LIST = INITLIST(LIST);\n"); 
		    fprintfIndent(outFile, "char *QUERY;\n");
		    fprintfIndent(outFile, "int RESULT;\n");
		    if (query->varFlags & VF_SELECT)
			fprintfIndent(outFile, "res_t ROWS;\n"); 
		    if (query->queryLevel == 0 && readOnly == 0)
			fprintfIndent(outFile, "dbstamp_t minCTs;\n");
		} else {
		    if (query->queryLevel == 0 && readOnly == 0)
			fprintfIndent(outFile, "dbstamp_t minCTs;\n");
		}
		blockStart = token->t_Data + token->t_Len;
	    }
#ifdef CURDEBUG
printf("Open brace; count: %d\n", braceCount);
#endif
	    ++braceCount;
	    type = SqlToken(token);
	    continue;

	case TOK_CBRACE:
	    --braceCount;
#ifdef CURDEBUG
printf("Close brace; count: %d\n", --braceCount);
#endif
	    /* At the top level of a nested set of SQL statements,
	     * write out any remaining non-SQL code and the
	     * commit/abort code before writing the final close brace
	     */
	    if (braceCount == 0 && query->queryLevel == 0) {
		if (token->t_Data - 1  > blockStart)
		    fwrite(blockStart, 1,
			    token->t_Data - blockStart - 1, outFile);

		writeCommitBlock(outFile, query, dbExpr, readOnly);

		IndentDecrement();
		fprintfIndent(outFile, "%c\n", TOK_CBRACE);
	    } else {
		if(token->t_Data - 1  > blockStart) {
		    fwrite(blockStart, 1,
			    token->t_Data - blockStart - 1, outFile);
		}

		IndentDecrement();
		fprintfIndent(outFile, "%c\n", TOK_CBRACE);

		blockStart = token->t_Data + token->t_Len;
	    }
	    type = SqlToken(token);
	    synchronizeLine(token, outFile);
	    continue;

	case TOK_ID:
	    /* If the ID is in the column table, replace it with the
	     * equivalent row array expression
	     *
	     * NOTE - convertColumnNameToRowExpression returns a pointer to a
	     * dynamically allocated string; this must be freed
	    */
	    columnName = safe_strndup(token->t_Data, token->t_Len);
	    ret=convertColumnNameToRowExpression(columnName, query, &rowString);
	    if (ret >= 0) {
		/* Write all the text that came before this token */
		fwrite(blockStart, 1, token->t_Data - blockStart, outFile);

		/* Write the new string equivalent */
		fwrite(rowString, 1, strlen(rowString), outFile);
		free(rowString);

		/* Restart the string count */
		blockStart = token->t_Data + token->t_Len;
	    }
	    free(columnName);

	    type = SqlToken(token);
	    continue;
 
	default:
	    type = SqlToken(token);
	    continue;
	}

        if (sqlType == TOK_CREATE) {
#ifdef CURDEBUG
printf("CREATE\n");
#endif

	    /* 
	     * Dump everything before the CREATE statement to the output file 
	     */
	    fwrite(blockStart, 1, token->t_Data - blockStart, outFile);
	    blockStart = token->t_Data;
	    fprintf(outFile, "\n");
	    synchronizeLine(token, outFile);

	    /* 
	     * Start writing the query definition expression 
	     */
	    fprintfIndent(outFile, "safe_asprintf(&QUERY, \"");

	    /* 
	     * Call the parser to process and write the CREATE statement 
	     */
	    type = ParseSql(token, query, type);
	    if (type & TOKF_ERROR) {
		LexPrintError(token);
		break;
	    }

	    /* 
	     * Finish writing the query definition expression 
	     */
	    fprintf(outFile, "%c;\n\n", TOK_CPAREN);
	    synchronizeLine(token, outFile);
	    fprintfQuery(outFile);
	    fprintfIndent(outFile,
		"if (QueryCLTrans(%s, QUERY, &RESULT) != NULL) %c\n",
		dbExpr, TOK_OBRACE);
	    IndentIncrement();
	    fprintfIndent(outFile, "DBASSERT(0);\n");
	    IndentDecrement();
	    fprintfIndent(outFile, "%c\n", TOK_CBRACE);
	    didQuery = 1;
	    synchronizeLine(token, outFile);

	    blockStart = token->t_Data;
	}

	else if (sqlType == TOK_DROP) {
#ifdef CURDEBUG
printf("DROP\n"):
#endif

	    /* 
	     * Dump everything before the DROP statement to the output file 
	     */
	    fwrite(blockStart, 1, token->t_Data - blockStart, outFile);
	    blockStart = token->t_Data;
	    fprintf(outFile, "\n");
	    synchronizeLine(token, outFile);

	    /* 
	     * Start writing the query definition expression 
	     */
	    fprintfIndent(outFile, "safe_asprintf(&QUERY, \"");

	    /* 
	     * Call the parser to process and write the DROP statement 
	     */
	    type = ParseSql(token, query, type);
	    if (type & TOKF_ERROR) {
		LexPrintError(token);
		break;
	    }

	    /* 
	     * Finish writing the query definition expression 
	     */
	    fprintf(outFile, "%c;\n\n", TOK_CPAREN);
	    synchronizeLine(token, outFile);
	    fprintfQuery(outFile);
	    fprintfIndent(outFile,
		"if (QueryCLTrans(%s, QUERY, &RESULT) != NULL) %c\n",
		dbExpr, TOK_OBRACE);
	    IndentIncrement();
	    fprintfIndent(outFile, "DBASSERT(0);\n");
	    IndentDecrement();
	    fprintfIndent(outFile, "%c\n", TOK_CBRACE);
	    didQuery = 1;
	    synchronizeLine(token, outFile);

	    blockStart = token->t_Data;
	}

	else if (sqlType == TOK_ALTER) {
#ifdef CURDEBUG
printf("ALTER\n"):
#endif

	    /* 
	     * Dump everything before the ALTER statement to the output file 
	     */
	    fwrite(blockStart, 1, token->t_Data - blockStart, outFile);
	    blockStart = token->t_Data;
	    fprintf(outFile, "\n");
	    synchronizeLine(token, outFile);

	    /* 
	     * Start writing the query definition expression 
	     */
	    fprintfIndent(outFile, "safe_asprintf(&QUERY, \"");

	    /* 
	     * Call the parser to process and write the ALTER statement 
	     */
	    type = ParseSql(token, query, type);
	    if (type & TOKF_ERROR) {
		LexPrintError(token);
		break;
	    }

	    /* 
	     * Finish writing the query definition expression 
	     */
	    fprintf(outFile, "%c;\n\n", TOK_CPAREN);
	    synchronizeLine(token, outFile);
	    fprintfQuery(outFile);
	    fprintfIndent(outFile,
		"if (QueryCLTrans(%s, QUERY, &RESULT) != NULL) %c\n",
		dbExpr, TOK_OBRACE);
	    IndentIncrement();
	    fprintfIndent(outFile, "DBASSERT(0);\n");
	    IndentDecrement();
	    fprintfIndent(outFile, "%c\n", TOK_CBRACE);
	    synchronizeLine(token, outFile);
	    didQuery = 1;

	    blockStart = token->t_Data;
	}

	else if (sqlType == TOK_INSERT) {
#ifdef CURDEBUG
printf("INSERT\n");
#endif

	    /* Dump everything before the INSERT statement to the output file */
	    fwrite(blockStart, 1, token->t_Data - blockStart, outFile);
	    blockStart = token->t_Data;
	    fprintf(outFile, "\n");
	    synchronizeLine(token, outFile);

	    /* Start writing the query definition expression */
	    fprintfIndent(outFile, "safe_asprintf(&QUERY, \"");

	    /* call the parser to process and write the INSERT statement */
	    type = ParseSql(token, query, type);
	    if (type & TOKF_ERROR) {
		LexPrintError(token);
		break;
	    }

	    /* Finish writing the query definition expression */
	    fprintf(outFile, "%c;\n", TOK_CPAREN);
	    fprintfQuery(outFile);
	    fprintfIndent(outFile,
		"if (QueryCLTrans(%s, QUERY, &RESULT) != NULL) %c\n",
		dbExpr, TOK_OBRACE);
	    IndentIncrement();
	    fprintfIndent(outFile, "DBASSERT(0);\n");
	    IndentDecrement();
	    fprintfIndent(outFile, "%c\n", TOK_CBRACE);
	    synchronizeLine(token, outFile);
	    didQuery = 1;

	    blockStart = token->t_Data;
	}
	else if (sqlType == TOK_SELECT || sqlType == TOK_HISTORY) {
	    const char	*startSection;
	    const char	*endSection;
	    int	startSectionLine;
#ifdef CURDEBUG
printf("SELECT\n");
#endif

	    /* Dump everything before the SELECT statement to the output file */
	    fwrite(blockStart, 1, token->t_Data - blockStart, outFile);
	    fprintf(outFile, "\n");
	    synchronizeLine(token, outFile);

	    /* Start writing the query definition expression */
	    fprintfIndent(outFile, "safe_asprintf(&QUERY, \"");

	    /* Call the parser to process and write the SELECT statement */
	    type = ParseSql(token, query, type);
	    if (type & TOKF_ERROR) {
		LexPrintError(token);
		break;
	    }

	    /* Finish writing the query definition expression */
	    fprintf(outFile, "%c;\n", TOK_CPAREN);

	    /* Select statements are followed by a block of code delimited by
	     * braces; Count braces to get limits of subsection, and determine
	     * if any of the optional variables (COLUMNS, etc.) are used
	     */
	    startSection = token->t_Data;
	    startSectionLine = token->t_Line;
	    varFlags = 0;
	    if ((type = getSubsection(token, &endSection, &varFlags))
		    == TOKF_ERROR)
		return(TOKF_ERROR);

	    /* Generate query code and mandatory variable definitions */
	    fprintfQuery(outFile);
	    fprintfIndent(outFile,
		"ROWS = QueryCLTrans(%s, QUERY, &RESULT);\n" , dbExpr);
	    didQuery = 1;
	    fprintfIndent(outFile, "if (ROWS != NULL) %c\n", TOK_OBRACE);
	    IndentIncrement();
	    fprintfIndent(outFile, "const char **ROW%d;\n", query->queryLevel);

	    /* Define optional variables */
	    if (varFlags & VF_COLUMNS)
		fprintfIndent(outFile, "int COLUMNS = ResColumns(ROWS);\n");

	    fprintf(outFile, "\n");
	    fprintfIndent(outFile,
	    	"for (ROW%d = ResFirstRow(ROWS); ROW%d != NULL; ROW%d = ResNextRow(ROWS))\n",
	   	 query->queryLevel, query->queryLevel, query->queryLevel);
	    synchronizeLine(token, outFile);

	    /* Add new query to list to store data for the subsection */
	    newQuery = QueryConstruct(query, varFlags, NULL);

	    /* Call self to process subsection */
	    if (processSQLSection(&subToken, token->t_FileBase, token->t_FileSize, 
		    startSectionLine, startSection, endSection - startSection, dbExpr,
		    newQuery, outFile, readOnly) == TOKF_ERROR)
		return(TOKF_ERROR);

	    QueryDestruct(newQuery);


	    /* Free the row storage */
	    fprintfIndent(outFile, "FreeCLRes(ROWS);\n");

	    /* Close the brace opened in the if (ROWS) statement above */
	    IndentDecrement();
	    fprintfIndent(outFile, "%c %c%c IF ROWS %c%c\n",
		TOK_CBRACE, '/', '*', '*', '/');
	    synchronizeLine(token, outFile);

	    IndentDecrement();
	    blockStart = token->t_Data;
	}
	else if (sqlType == TOK_COUNT) {
	    /* Dump everything before the COUNT statement to the output file */
	    fwrite(blockStart, 1, token->t_Data - blockStart, outFile);
	    blockStart = token->t_Data;
	    fprintf(outFile, "\n");

	    /* Start writing the query definition expression */
	    fprintfIndent(outFile, "safe_asprintf(&QUERY, \"");

	    /* call the parser to process and write the UPDATE statement */
	    type = ParseSql(token, query, type);
	    if (type & TOKF_ERROR) {
		LexPrintError(token);
		break;
	    }

	    /* Finish writing the query definition expression */
	    fprintf(outFile, "%c;\n", TOK_CPAREN);
	    synchronizeLine(token, outFile);
	    fprintfQuery(outFile);
	    fprintfIndent(outFile,
		"if (QueryCLTrans(%s, QUERY, &RESULT) != NULL) %c\n",
		dbExpr, TOK_OBRACE);
	    IndentIncrement();
	    fprintfIndent(outFile, "DBASSERT(0);\n");
	    IndentDecrement();
	    fprintfIndent(outFile, "%c\n", TOK_CBRACE);
	    synchronizeLine(token, outFile);
	    didQuery = 1;

	    blockStart = token->t_Data;
	}
	else if (sqlType == TOK_UPDATE || sqlType == TOK_CLONE) {
#ifdef CURDEBUG
printf("UPDATE\n");
#endif

	    /* Dump everything before the UPDATE statement to the output file */
	    fwrite(blockStart, 1, token->t_Data - blockStart, outFile);
	    blockStart = token->t_Data;
	    fprintf(outFile, "\n");
	    synchronizeLine(token, outFile);

	    /* Start writing the query definition expression */
	    fprintfIndent(outFile, "safe_asprintf(&QUERY, \"");

	    /* call the parser to process and write the UPDATE statement */
	    type = ParseSql(token, query, type);
	    if (type & TOKF_ERROR) {
		LexPrintError(token);
		break;
	    }

	    /* Finish writing the query definition expression */
	    fprintf(outFile, "%c;\n", TOK_CPAREN);
	    fprintfQuery(outFile);
	    fprintfIndent(outFile,
		"if (QueryCLTrans(%s, QUERY, &RESULT) != NULL) %c\n",
		dbExpr, TOK_OBRACE);
	    IndentIncrement();
	    fprintfIndent(outFile, "DBASSERT(0);\n");
	    IndentDecrement();
	    fprintfIndent(outFile, "%c\n", TOK_CBRACE);
	    synchronizeLine(token, outFile);
	    didQuery = 1;

	    blockStart = token->t_Data;
	}
	else if (sqlType == TOK_DELETE) {
#ifdef CURDEBUG
printf("DELETE\n");
#endif

	    /* Dump everything before the DELETE statement to the output file */
	    fwrite(blockStart, 1, token->t_Data - blockStart, outFile);
	    blockStart = token->t_Data;
	    fprintf(outFile, "\n");
	    synchronizeLine(token, outFile);

	    /* Start writing the query definition expression */
	    fprintfIndent(outFile, "safe_asprintf(&QUERY, \"");

	    /* call the parser to process and write the DELETE statement */
	    type = ParseSql(token, query, type);
	    if (type & TOKF_ERROR) {
		LexPrintError(token);
		break;
	    }

	    /* Finish writing the query definition expression */
	    fprintf(outFile, "%c;\n", TOK_CPAREN);
	    fprintfQuery(outFile);
	    fprintfIndent(outFile,
		"if (QueryCLTrans(%s, QUERY, &RESULT) != NULL) %c\n",
		dbExpr, TOK_OBRACE);
	    IndentIncrement();
	    fprintfIndent(outFile, "DBASSERT(0);\n");
	    IndentDecrement();
	    fprintfIndent(outFile, "%c\n", TOK_CBRACE);
	    synchronizeLine(token, outFile);
	    didQuery = 1;

	    blockStart = token->t_Data;
	}
	else if (sqlType == TOK_ROLLBACK) {
#ifdef CURDEBUG
printf("ROLLBACK\n");
#endif
	    /* Dump everything before ROLLBACK to the output file */
	    fwrite(blockStart, 1, token->t_Data - blockStart, outFile);
	    blockStart = token->t_Data;
	    fprintf(outFile, "\n");
	    synchronizeLine(token, outFile);

	    /* Remove the semicolon after the ROLLBACK keyword */
	    if((type = SqlToken(token)) != TOK_SEMI) {
		fprintf(stderr, "ROLLBACK must be followed by a semicolon\n");
		LexPrintError(token);
		return(TOKF_ERROR);
	    }

	    /* Generate the rollback expression */
	    fprintfIndent(outFile, "goto rollback%d_%d;\n", 
		query->queryLevel,
		query->gotoId
	    );
	    query->varFlags |= VF_ROLLBACK;
	    type = SqlToken(token);
	    blockStart = token->t_Data;
	    synchronizeLine(token, outFile);
	}
	else if (sqlType == TOK_COMMIT) {
#ifdef CURDEBUG
printf("COMMIT\n");
#endif
	    /* Dump everything before COMMIT to the output file */
	    fwrite(blockStart, 1, token->t_Data - blockStart, outFile);
	    blockStart = token->t_Data;
	    fprintf(outFile, "\n");
	    synchronizeLine(token, outFile);

	    /* Remove the semicolon after the COMMIT keyword */
	    if((type = SqlToken(token)) != TOK_SEMI) {
		fprintf(stderr, "COMMIT must be followed by a semicolon\n");
		LexPrintError(token);
		return(TOKF_ERROR);
	    }

	    /* Generate the commit expression */
	    fprintfIndent(outFile, "goto commit%d_%d;\n", 
		query->queryLevel,
		query->gotoId
	    );
	    type = SqlToken(token);
	    query->varFlags |= VF_COMMIT;
	    blockStart = token->t_Data;
	    synchronizeLine(token, outFile);
	}

	if (sqlType != TOK_ROLLBACK && sqlType != TOK_COMMIT) {
	    fprintfIndent(outFile, "if (RESULT < 0 && RESULT != DBERR_RECORD_ALREADY)\n");
	    IndentIncrement();
	    fprintfIndent(outFile, "LogWrite(HIPRI, \"%s query returned %%d "
		"(\\\"%%s\\\") in %%s (%%s:%%d) (%%s)\", RESULT, "
		"GetCLErrorDesc(RESULT), __PRETTY_FUNCTION__, "
		"__FILE__, __LINE__, %s);\n",
		SqlTokenTypeDesc(sqlType),
		((didQuery) ? "QUERY" : "\"\"")
	    );
	    IndentDecrement();

	}
	if (didQuery) {
	    fprintfIndent(outFile, "free(QUERY);\n");
	    fprintfIndent(outFile, "QUERY=NULL;\n");
	    fprintfIndent(outFile, "FreeEscapes(&LIST);\n");
	    synchronizeLine(token, outFile);
	}

	QueryReset(query);
    }
    while ((type & (TOKF_ERROR|TOKF_EOF)) == 0);

#ifdef CURDEBUG
printf("processSQLSection END: %d\n", query->queryLevel);
#endif
    return(type);
}

static void
writeCommitBlock(FILE *outFile, Query *query, const char *dbExpr, int readOnly)
{
    /* Generate the commit and rollback calls */
    if (query->varFlags & VF_COMMIT)
	fprintfIndent(outFile, "commit%d_%d:\n", query->queryLevel, query->gotoId);

    if (readOnly == 0) {
	fprintfIndent(outFile,"minCTs = 0;\n");
	fprintfIndent(outFile,"if (Commit1CLTrans(%s, &minCTs) == 0) %c\n",
	    dbExpr, TOK_OBRACE);
	IndentIncrement();
	fprintfIndent(outFile,"if (Commit2CLTrans(%s, minCTs) == 0) %c\n",
	    dbExpr, TOK_OBRACE);
	/*
	 * Note that the freeze timestamp, tsVar, is set to minCTs + 1
	 * indicating the minimum freezets to use after a valid commit to
	 * maintain transactional consistency, or the next freezets to use
	 * after a commit failure for which you might be able to reissue
	 * the transaction and have a success.
	 */
	IndentIncrement();
	fprintfIndent(outFile, "%s = 0;\n", query->statusVarName);
	IndentDecrement();
	fprintfIndent(outFile, "%c else %c\n", TOK_CBRACE, TOK_OBRACE);
	IndentIncrement();
	fprintfIndent(outFile, "%s = -1;\n", query->statusVarName);
	IndentDecrement();
	fprintfIndent(outFile, "%c\n", TOK_CBRACE);
	fprintfIndent(outFile, "%s = minCTs + 1;\n", query->tsVarName);
	IndentDecrement();
	fprintfIndent(outFile, "%c else %c\n", TOK_CBRACE, TOK_OBRACE);
	IndentIncrement();
	fprintfIndent(outFile, "%s = minCTs + 1;\n", query->tsVarName);
    } else {
	IndentIncrement();
	fprintfIndent(outFile,"%c\n", TOK_OBRACE);
    }
    if (query->varFlags & VF_ROLLBACK)
	fprintfIndent(outFile, "rollback%d_%d:\n", query->queryLevel, query->gotoId);
    fprintfIndent(outFile, "AbortCLTrans(%s);\n", dbExpr);
    fprintfIndent(outFile, "%s = -1;\n", query->statusVarName);
    IndentDecrement();
    fprintfIndent(outFile, "%c\n", TOK_CBRACE);
}

static int
getSubsection(token_t *token, const char **endSection, u_int *varFlags)
{
    int braceCount;
    int type;
    int selectFound;
    int selBraceLevel = 0;
    const char *openBrace;

    /* Next token must be open brace */
    if (token->t_Type != TOK_OBRACE) {
	fprintf(stderr, "FATAL: Open brace must follow BEGIN()\n");
	return(TOKF_ERROR);
    }
    openBrace = token->t_Data;

    /* Scan until close brace */
    selectFound = 0;
    braceCount = 1;
    while (braceCount) {
	type = SqlToken(token);

	if (type == TOK_OBRACE)
	    ++braceCount;

	if (type == TOK_CBRACE)
	    --braceCount;

	if (type == TOK_SELECT || type == TOK_HISTORY) {
	    if (selectFound == 0) {
		selectFound = 1;
		*varFlags |= VF_SELECT;
		selBraceLevel = braceCount;
	    }
	}

	if (type == TOK_INSERT || 
	    type ==  TOK_SELECT || 
	    type ==  TOK_HISTORY || 
	    type ==  TOK_COUNT || 
	    type ==  TOK_UPDATE || 
	    type ==  TOK_DELETE
	) {
	    *varFlags |= VF_SUBLEVELS;
	}

	if (type == TOK_ID) {
	    /* Flag the use of optional variables defined in SELECT.
	     * The checking ignores instances of variables inside nested
	     * SELECT statments
	     */
	    if (!selectFound || (selectFound && braceCount == selBraceLevel)) {
		if (token->t_Len == strlen("COLUMNS")) {
		    if (!strncmp(token->t_Data, "COLUMNS", strlen("COLUMNS")))
			*varFlags |= VF_COLUMNS;
		}
	    }
	}

	if (type & TOKF_ERROR) {
	    fprintf(stderr, "FATAL: Lex error\n");
	    LexPrintError(token);
	    return(TOKF_ERROR);
	}

	if (type == TOK_EOF2) {
	    fprintf(stderr, "FATAL: Unexpected end of file\n");
	    return(TOKF_ERROR);
	}
    }

    type = SqlToken(token);
    *endSection = token->t_Data;
    return(type);
}

#ifdef NOTDEF
static int
processSubsection(token_t *token, Query *query,
	const char *section, int sectionSize, int readOnly)
{
    int baseIndent;
    int type;
    token_t token;

    type = SqlInit(token, sqlSection, sqlSectionSize);

    /* Find indent level of first token; useful for preserving
     * the indents in the original code
     */
    if ((baseIndent = getIndentLevel(token->t_Data)) < 0) {
	fprintf("Base indent failed\n");
	return(TOKF_ERROR);
    }

    do {

	/* Get the indent level of this token; adjust the global indent
	 * level if it has changed
	 */
	indent = getIndentLevel(token->t_Data);
	if (indent > currentIndent) {
	    IndentIncrement();
	    ++currentIndent;
	}
	if (indent < currentIndent) {
	    --currentIndent;
	    IndentDecrement();
	}

	switch (type) {
	case TOK_INSERT:
	case TOK_SELECT:
	case TOK_HISTORY:
	case TOK_COUNT:
	case TOK_UPDATE:
	case TOK_DELETE:
	    fprintf(stderr, "ERROR - processSubsection cannot handle SQL statements!\n");
	    break;
#ifdef NOTDEF
	case TOK_OBRACE:
	    fprintfIndent(outFile, "%c\n", TOK_OBRACE);
	    IndentIncrement();
	    type = SqlToken(token);
	    continue;

	case TOK_CBRACE:
	    fprintfIndent(outFile, "%c\n", TOK_CBRACE);
	    IndentDecrement();
	    type = SqlToken(token);
	    continue;
#endif

	case TOK_ID:

	    /* If the ID is in the column table, replace it with the
	     * equivalent row array expression
	     *
	     * NOTE - convertColumnNameToRowExpression returns a pointer to a
	     * dynamically allocated string; this must be freed
	    */
	    columnName = safe_strndup(token->t_Data, token->t_Len);
	    ret=convertColumnNameToRowExpression(columnName, query, &rowString);
	    if (ret >= 0) {
		/* Write all the text that came before this token */
		fwrite(blockStart, 1, token->t_Data - blockStart, outFile);

		/* Write the new string equivalent */
		fwrite(rowString, 1, strlen(rowString), outFile);
		free(rowString);

		/* Restart the string count */
		blockStart = token->t_Data + token->t_Len;
	    }
	    free(columnName);

	    type = SqlToken(token);
	    continue;
 
	default:
	    type = SqlToken(token);
	    continue;
	}

	type = SqlToken(token);
	*endSection = token->t_Data;
	return(type);
    }
}
#endif


static int IndentLevel;

/* Assume indent of 4 spaces */
static void
IndentSet(const char *start)
{
    IndentLevel = getIndentLevel(start);
    return;
}

void
IndentIncrement(void)
{
    ++IndentLevel;

    return;
}

void
IndentDecrement(void)
{
    if (IndentLevel > 0)
	--IndentLevel;
    return;
}

/* Assume indent of 4 spaces */
static void 
IndentGen(FILE *outStream)
{
    int i;

    for (i=0; i<IndentLevel/2; i++)
	fwrite("\t", 1, 1, outStream);

    if (IndentLevel%2)
	fwrite("    ", 1, 4, outStream);

    return;
}

static void
usage(char *progName)
{
    fprintf(stderr, "Usage: %s inputFile outputFile\n", progName);
}

int
fprintfIndent(FILE *stream, const char *fmt, ...)
{
    va_list ap;
    va_start(ap, fmt);
    IndentGen(stream);
    return(vfprintf(stream, fmt, ap));
}

#ifdef NOTDEF
size_t
fwriteIndent(const char *ptr, int size, int nelm, FILE *fo)
{
    const char *tab = '\t';
    const char *space = ' ';

    if (IndentLevel/2)
	fwrite(tab, 1, IndentLevel/2, fo);

    if (IndentLevel%2)
	fwrite(space, 1, 4, fo);

    return(fwrite(ptr, size, nelm, fo));
}
#endif

void 
fwrite_nonl(const char *ptr, int size, int nelm, FILE *fo)
{
    int i;
    int b = 0;
    int n = nelm * size;

    for (i = 0; i < n; ++i) {
	if (ptr[i] == '\n') {
	    if (i != b)
		fwrite(ptr + b, i - b, 1, fo);
	    b = i + 1;
	}
    }
    if (i != b)
	fwrite(ptr + b, i - b, 1, fo);
}

void
fprintfQuery(FILE *stream)
{
    fprintf(stream, "#ifdef CURSOR_LOGQUERY\n");
    fprintf(stream, "LogWrite(DEBUGPRI, \"Executing query: %%s\", QUERY);\n");
    fprintf(stream, "#endif\n");
}

static int
getIndentLevel(const char *begin)
{
    const char 	*startWhite;
    const char 	*endWhite;
    int		indent;
    int		spaces;

    if (begin == NULL)
	return(-1);

    /* Search backwards for start of line */ 
    startWhite=begin;
    endWhite=begin;
    while(*startWhite != '\n') {
	/* Ignore non-whitespace */
	if (*startWhite != '\t' && *startWhite != ' ')
	    endWhite = startWhite;

	startWhite--;
    }
    ++startWhite;

    /* Count indent levels */
    indent = 0;
    spaces = 0;
    while(startWhite < endWhite) {
	if (*startWhite == '\t') {
	    spaces = 0;
	    indent += 2;
	}
	else if (*startWhite == ' ')
	    ++spaces;
	else {
	    printf("Bad char %c\n", *startWhite);
	    break;
	}

	++startWhite;
    }
    indent += spaces/4;

    return(indent);
}

/*
 * Skip C expression, stopping at a comma or close parenthesis and
 * counting parens.  This parses the inside of a parenthesized 
 * expression.
 *
 *	( CEXP .......    )
 *	( CEXP .......    , CEXP .......... )
 *       ^ start of scan  ^ end of scan (comma or close paren)
 */
static int
SkipCExp(token_t *t, int type, char **pstr)
{
    int paren = 0;
    int len;
    const char *base = t->t_Data;

    while ((type & TOKF_ERROR) == 0) {
	if (type == TOK_OPAREN) {
	    ++paren;
	}
	if (type == TOK_CPAREN) {
	    if (--paren < 0)
		break;
	}
	if ((type == TOK_COMMA || type == TOK_OBRACE) && paren == 0)
	    break;
	type = SqlToken(t);
    }
    if (type & TOKF_ERROR) {
	len = 0;
    } else {
	len = t->t_Data - base;
    }
    *pstr = safe_strndup(base, len);
    return(type);
}

void
cleanup(int sigNo)
{
    if (OutputFileName) {
	remove(OutputFileName);
    }
    exit(1);
}

static void
synchronizeLine(token_t *t, FILE *outFile)
{
//    fprintf(outFile, "#line %d \"%s\"\n", t->t_Line, InputFileName);
}

