/*
 * UTILS/MLQUERY.C		- Mid level data utility (non-replicative)
 *
 * (c)Copyright 2000-2002 Backplane, Inc.  Please refer to the COPYRIGHT
 * file at the base of the distribution tree.
 *
 * $Backplane: rdbms/utils/mlquery.c,v 1.24 2002/08/20 22:06:06 dillon Exp $
 *
 *	MLQUERY database[:defaultSchema]
 */

#include "defs.h"
#include <ctype.h>

void DSPrintSelectHeader(Query *q);
int DSTermRange(Query *q);
void DSOutputData(const char *ptr, int bytes);

DataBase *Db;
int QuietOpt;
int EchoOpt;
int NotDone = 1;

int
task_main(int ac, char **av)
{
    int i;
    int error = 0;
    char *dataBase = NULL;
    char *useSchema;
    const char *dbDir = DefaultDBDir();

    for (i = 1; i < ac; ++i) {
	char *ptr = av[i];

	if (*ptr != '-') {
	    if (dataBase != NULL) {
		fprintf(stderr, "Unexpected argument: %s\n", ptr);
		exit(1);
	    }
	    dataBase = ptr;
	    continue;
	}
	ptr += 2;
	switch(ptr[-1]) {
	case 'q':
	    QuietOpt = 1;
	    break;
	case 'e':
	    EchoOpt = 1;
	    break;
	case 'D':
	    dbDir = (*ptr) ? ptr : av[++i];
	    break;
	default:
	    fprintf(stderr, "Unknown option: %s\n", ptr - 2);
	    exit(1);
	}
    }
    if (dataBase == NULL) {
	fprintf(stderr, "Version 1.00\n");
	fprintf(stderr, "%s [-D dbdir] [-e] [-q] dataBase[:schema]\n", av[0]);
	exit(1);
    }
    if ((useSchema = strrchr(dataBase, ':')) != NULL)
	*useSchema++ = 0;

    if (dataBase[0] == '/')
	dbDir = NULL;

    Db = OpenDatabase(dbDir, dataBase, DBF_EXCLUSIVE, NULL, &error);
    if (Db == NULL) {
	fprintf(stderr, "OpenDatabase: error %d\n", error);
	exit(1);
    }
    if (QuietOpt == 0)
	printf("Database open %p\n", Db);

    printf("*** WARNING, MLQUERY OPS DO NOT REPLICATION SYNCHRONOUSLY ***\n");
    printf("*** NEVER RUN OPS THAT MIGHT CONFLICT WITH OPS RUN ON     ***\n");
    printf("*** REPLICATION PEERS ***\n");

    while (NotDone) {
	char buf[4096];
	int l;
	int type;
	Query *q;
	token_t t;

	if (QuietOpt == 0) {
	    printf("(%s:%s) ", dataBase, ((useSchema) ? useSchema : "?"));
	    fflush(stdout);
	}
	if (fgets(buf, sizeof(buf), stdin) == NULL)
	    break;
	if ((l = strlen(buf)) == 0 || buf[l-1] != '\n') {
	    fprintf(stderr, "Unexpected input\n");
	    break;
	}
	buf[--l] = 0;
	if (buf[0] == '#' || buf[0] == 0)
	    continue;

	if (EchoOpt)
	    printf("%s\n", buf);

	q = GetQuery(Db);

	if (useSchema != NULL)
	    q->q_DefSchemaI = HLGetSchemaI(q, useSchema, strlen(useSchema));

	type = SqlInit(&t, buf, l + 1);		/* include \0 terminator */

	switch(type) {
	case TOK_BEGIN:
	    FreeQuery(q);
	    Db = PushDatabase(Db, 0, &error, 0);
	    printf("Begin Transaction %p\n", Db);
	    break;
	case TOK_COMMIT:
	    FreeQuery(q);
	    if (Db->db_Parent) {
		int error = 0;
		dbstamp_t minCTs = 0;

		if ((error = Commit1(Db, &minCTs, NULL, NULL, 0)) == 0) {
		    error = Commit2(Db, minCTs, 0, 0);
		} else {
		    Abort1(Db);
		}
		Db = PopDatabase(Db, &error);
		if (error)
		    printf("Commit failed with error %d\n", error);
		else
		    printf("Commit succeeded!\n");
	    } else {
		printf("Not in transaction\n");
	    }
	    break;
	case TOK_ROLLBACK:
	    FreeQuery(q);
	    if (Db->db_Parent) {
		int error = 0;

		Abort1(Db);
		Db = PopDatabase(Db, &error);
		if (error)
		    printf("Rollback Error %d\n", error);
		else
		    printf("Rollback Succeeded\n");
	    } else {
		printf("Not in transaction\n");
	    }
	    break;
	default:
	    type = ParseSql(&t, q, type);
	    if (type & TOKF_ERROR) {
		LexPrintError(&t);
		FreeQuery(q);
	    } else {
		int r;

		q->q_TermFunc = DSTermRange;
		q->q_TermInfo = q->q_TableIQBase;
		if (q->q_TermOp == QOP_SELECT)
		    DSPrintSelectHeader(q);
		r = RunQuery(q);
		printf("Results %d\n", r);
		RelQuery(q);
	    }
	    break;
	}
    }
    return(0);
}

void
DSPrintSelectHeader(Query *q)
{
    ColI *ci;
    TableI *ti;
    int multiTables = 0;

    if ((ti = q->q_TableIQBase) != NULL && ti->ti_Next)
	multiTables = 1;

    printf("DESC ");
    for (ci = q->q_ColIQBase; ci; ci = ci->ci_QNext) {
	if (multiTables)
	    printf("%s.", ci->ci_TableI->ti_TabName);
	fwrite(ci->ci_ColName, ci->ci_ColNameLen, 1, stdout);
	if (ci->ci_QNext)
	    printf(", ");
    }
    printf("\n");
}

/*
 * DSTermRange()
 *
 *	Note that each termination range falls within a single table.
 */

int
DSTermRange(Query *q)
{
#if 0
    TableI *ti = q->q_TableIQBase;	/* q->q_TermInfo */
#endif
    ColI *ci;

    printf("DATA ");

    for (ci = q->q_ColIQBase; ci; ci = ci->ci_QNext) {
	ColData *cd = ci->ci_CData;

	if (cd->cd_Data) {
	    fputc('\'', stdout);
	    DSOutputData(cd->cd_Data, cd->cd_Bytes);
	    fputc('\'', stdout);
	} else {
	    fprintf(stderr, "NULL");
	}
	if (ci->ci_QNext)
	    printf(", ");
    }
    printf("\n");
    return(1);
}

void
DSOutputData(const char *ptr, int bytes)
{
    int b;
    int i;

    for (i = b = 0; i < bytes; ++i) {
	switch(ptr[i]) {
	case '\\':
	case '%':
	case 0:
	case '\'':
	    fprintf(stdout, "%%%02x", (u_int8_t)ptr[i]);
	    break;
	default:
	    fputc(ptr[i], stdout);
	    break;
	}
    }
}

