/*
 * UTILS/LLQUERY.C		- FOR DATABASE BOOTSTRAP OPERATION ONLY
 *
 * (c)Copyright 2000-2002 Backplane, Inc.  Please refer to the COPYRIGHT
 * file at the base of the distribution tree.
 *
 * $Backplane: rdbms/utils/llquery.c,v 1.41 2002/08/20 22:06:06 dillon Exp $
 *
 * LLQUERY [-C] [-dump] Database
 */

#include "defs.h"
#include "libdbcore/dbcore-protos.h"
#include <ctype.h>

DataBase *Db;
int QuietOpt;
int EchoOpt;
int DumpOpt;
int NotDone = 1;

void addStrArray(char ***pary, int i, char *s);
void addColsArray(col_t **pcols, int i, col_t cid);
int dotsplit(char *ptr, char **ary, int count);
void DoLLQuery(vtable_t vt, char **ary, col_t *cols, int count);
int LLTermRange(Query *q);
void DumpIt(void);
static void LLRunQuery(const char *ptr);
static dbstamp_t LLCommit(dbstamp_t fts, char cmd, int how, int *commitPhase);

int
task_main(int ac, char **av)
{
    int i;
    int commitPhase = 0;
    int error = 0;
    char *dataBase = NULL;
    char *addRepPeer = NULL;
    const char *dbDir = DefaultDBDir();
    char *useName;
    dbstamp_t fts;
    int dbFlags = DBF_EXCLUSIVE;
    DBCreateOptions opts;

    bzero(&opts, sizeof(opts));

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
	case 'v':
	    DebugOpt = 1;
	    break;
	case 'd':
	    DumpOpt = 1;
	    break;
	case 'C':
	    dbFlags |= DBF_CREATE;
	    break;
	case 'P':
	    addRepPeer = (*ptr) ? ptr : av[++i];
	    break;
	case 'D':
	    dbDir = (*ptr) ? ptr : av[++i];
	    break;
	case 's':
	    dbFlags &= ~DBF_EXCLUSIVE;
	    break;
	default:
	    fprintf(stderr, "Unknown option: %s\n", ptr - 2);
	    exit(1);
	}
    }
    if (i > ac)
	fatal("last option required argument");

    if (dataBase == NULL) {
	fprintf(stderr, "Version 1.00\n");
	fprintf(stderr, "%s [-shared] [-dump] dataBase\n", av[0]);
	exit(1);
    }
    if (dataBase[0] == '/')
	dbDir = NULL;

    if ((useName = strrchr(dataBase, '/')) != NULL) {
	++useName;
    } else {
	useName = dataBase;
    }

    opts.c_Flags |= DBC_OPT_TIMESTAMP;
    opts.c_TimeStamp = timetodbstamp(time(NULL));
    Db = OpenDatabase(dbDir, dataBase, dbFlags, &opts, &error);

    if (Db == NULL) {
	fprintf(stderr, "OpenDatabase: error %d\n", error);
	exit(1);
    }
    if (!QuietOpt)
	printf("Database open %p\n", Db);

    if (DumpOpt) {
	DumpIt();
	exit(0);
    }
    if (QuietOpt == 0) {
	printf("WARNING!  Only use this program to bootstrap new databases!\n");
	fflush(stdout);
    }

    if ((fts = GetMinCTs(Db)) == 0)
	fts = 1;
    printf("FTS %016qx\n", fts);

    while (NotDone) {
	char buf[4096];
	int l;
	int i;
	int r;
	int count = 0;
	char **ary = NULL;
	col_t *cols = NULL;
	vtable_t vt;
	col_t colId;
	char *ptr;
	ColData *cd;
	RawData *rd;
	TableI *ti;

	fflush(stderr);
	if (QuietOpt == 0) {
	    printf("(%s) ", useName);
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
	    fprintf(stdout, "%s\n", buf);

	/*
	 * Skip command name and virtual table (or column id) override
	 */

	ptr = buf;
	colId = 0;
	vt = 0;
	while (*ptr && *ptr != ' ' && *ptr != '\t') {
	    if (isdigit((u_int8_t)*ptr)) {
		vt = strtol(ptr, NULL, 0);
		colId = strtol(ptr, NULL, 0);
		break;
	    }
	    ++ptr;
	}
	while (*ptr && *ptr != ' ' && *ptr != '\t') {
	    ++ptr;
	}

	/*
	 * Run command
	 */

	switch(buf[0]) {
	case '?':
	case 'h':
	    printf(
		"    h                      help\n"
		"    ?                      help\n"
		"    b                      begin transaction\n"
		"    a                      abort transaction\n"
		"    c[1,2]                 commit transaction\n"
		"    I                      show systable header\n"
		"    i[vt] col:data ...     insert raw record\n"
		"    d recoff ...           delete raw record\n"
		"    l [tableFile]          list raw record[s] in phys file\n"
		"    r[vt] col[:data] ...   issue raw query\n"
		"    +s[vt] schema          add virtual schema\n"
		"    +t[vt] schema.table    add virtual table\n"
		"    +c scm.table.col       add virtual column\n"
		"    q QUERY                issue high-level SQL query\n"
		"    q                      quit\n"
	    );
	    break;
	case 'i':
	    /*
	     * i [col:data]*
	     *
	     * XXX do not allow deletions when not in transaction
	     */
	    for (ptr = strtok(ptr, " \t\r"); ptr; ptr = strtok(NULL, " \t\r")) {
		addStrArray(&ary, count, ptr);
		addColsArray(&cols, count, strtol(ptr, NULL, 0));
		++count;
	    }
	    rd = AllocRawData(Db->db_SysTable, cols, count);
	    i = 0;
	    cd = rd->rd_ColBase;
	    while (i < count) {
		char *s;

		if ((s = strchr(ary[i], ':')) != NULL) {
		    cd->cd_Bytes = strlen(s + 1);
		    cd->cd_Data = s + 1;
		}
		++i;
		cd = cd->cd_Next;
	    }
	    InsertTableRec(Db->db_SysTable, rd, vt);
	    FreeRawData(rd);
	    break;
	case 'r':
	    /*
	     * r[vt] [col[:data]]*	(request columns [that equal data])
	     */
	    for (ptr = strtok(ptr, " \t\r"); ptr; ptr = strtok(NULL, " \t\r")) {
		addStrArray(&ary, count, ptr);
		addColsArray(&cols, count, strtol(ptr, NULL, 0));
		++count;
	    }
	    DoLLQuery(vt, ary, cols, count);
	    break;
	case 'd':
	    /*
	     * d recoff 
	     *
	     * XXX do not allow deletions when not in transaction
	     */
	    rd = AllocRawData(Db->db_SysTable, NULL, 0);
	    for (ptr = strtok(ptr, " \t\r"); ptr; ptr = strtok(NULL, " \t\r")) {
		dbpos_t recPos;
		rd->rd_Table = Db->db_SysTable;
		recPos.p_Tab = Db->db_SysTable;
		recPos.p_Ro = strtol(ptr, NULL, 0);
		DeleteTableRec(rd->rd_Table, &recPos, 0);
	    }
	    FreeRawData(rd);
	    break;
	case 'l':
	    /*
	     * l 
	     */
	    if ((ptr = strtok(ptr, " \t\r")) != NULL) {
		int error;
		Table *tab = OpenTable(Db, ptr, "dt0", NULL, &error);
		if (tab == NULL) {
		    printf("Can't open physical table file %s in %s\n", ptr, Db->db_DirPath);
		    break;
		}
		rd = AllocRawData(tab, NULL, 0);
	    } else {
		rd = AllocRawData(Db->db_SysTable, NULL, 0);
		++rd->rd_Table->ta_Refs;
	    }
	    ti = AllocPrivateTableI(rd);

	    for (
		r = GetFirstTable(ti, NULL);
		r == 0;
		r = GetNextTable(ti, NULL)
	    ) {
		for (
		    SelectBegTableRec(ti, 0); 
		    ti->ti_RanBeg.p_Ro >= 0;
		    SelectNextTableRec(ti, 0)
		) {
		    ColData *cstop;

		    cstop = ReadDataRecord(rd, &ti->ti_RanBeg, RDF_READ|RDF_ALLOC|RDF_FORCE);
		    printf("Record %08qx vt %04x sz %d flgs %02x ts %08qx hv %d: ", 
			ti->ti_RanBeg.p_Ro,
			rd->rd_Rh->rh_VTableId,
			rd->rd_Rh->rh_Size,
			rd->rd_Rh->rh_Flags,
			rd->rd_Rh->rh_Stamp,
			rd->rd_Rh->rh_Hv
		    );
#if 0
		    {
			int i;
			for (i = 0; i < rd->rd_Rh->rh_Size; ++i)
			    printf(" %02x", ((unsigned char *)rd->rd_Rh)[i]);
			printf("\n");
		    }
#endif
		    for (cd = rd->rd_ColBase; cd != cstop; cd = cd->cd_Next) {
			printf(" %d:(%d)=", 
			    (int)(col_t)cd->cd_ColId, 
			    cd->cd_Bytes
			);
			if (cd->cd_Bytes)
			    fwrite(cd->cd_Data, cd->cd_Bytes, 1, stdout);
			else
			    printf("(null)");
		    }
		    printf("\n");
		}
	    }
	    LLFreeTableI(&ti);
	    /* CloseTable(rd->rd_Table, 0); */
	    break;
	case 'b':
	    /*
	     * Begin a transaction.  Use a freeze time stamp of at least 1
	     * to avoid an assertion in dbcore.
	     */
	    Db = PushDatabase(Db, fts, &error, 0);
	    printf("Begin Transaction %016qx\n", fts);
	    break;
	case 'a':
	    if (Db->db_Parent) {
		int error = 0;

		printf("End Transaction\n");
		Db = PopDatabase(Db, &error);
		if (error)
		    printf("Abort Error %d\n", error);
		else
		    printf("Abort Succeeded\n");
		commitPhase = 0;
	    } else {
		printf("Not in transaction\n");
	    }
	    break;
	case 'u':
	    if (commitPhase) {
		printf("Undid commit phase 1\n");
		UnCommit1(Db);
		commitPhase = 0;
	    } else {
		printf("Not in commit phase 1\n");
	    }
	    break;
	case 'S':
	    SetSyncTs(Db, fts);
	    break;
	case 'c':
	case 'C':
	    if (Db->db_Parent) {
		int how = 0;

		switch(buf[1]) {
		case '1':
		    if (commitPhase == 0) {
			how = 1;
		    } else {
			printf("c1: already in phase-1\n");
			how = -1;
		    }
		    break;
		case '2':
		    if (commitPhase == 1) {
			how = 2;
		    } else {
			printf("c2: not in phase-1 , cannot do phase 2\n");
			how = -1;
		    }
		    break;
		}
		if (how < 0)
		    break;
		fts = LLCommit(fts, buf[0], how, &commitPhase);
	    } else {
		printf("Not in transaction\n");
	    }
	    break;
	case 'I':
	    {
		Table *tab = Db->db_SysTable;
		const TableFile *tf = tab->ta_Meta;

		printf("    Version:   %d\n", tf->tf_Version);
		printf("    AppendInc: %d\n", tf->tf_AppendInc);
		printf("    BlockSize: %d\n", tf->tf_BlockSize);
		printf("    FileSize:  %08qx\n", tf->tf_FileSize);
		printf("    AppendAt:  %08qx\n", tf->tf_Append);
		printf("    SyncStamp: %016qx\n", tf->tf_SyncStamp);
		printf("    NextStamp: %016qx\n", tf->tf_NextStamp);
		printf("    tf_Name:   %s\n", tf->tf_Name);
		printf("    Error:     %d\n", tf->tf_Error);
	    }
	    break;
	case '+':
	    switch(buf[1]) {
	    case 's':
		ary = zalloc(sizeof(char *) * 1);
		while (dotsplit(strtok(ptr, " \t\r"), ary, 1) == 0) {
		    SchemaI tsi;
		    int error;

		    bzero(&tsi, sizeof(tsi));
		    tsi.si_ScmName = ary[0];
		    tsi.si_DefaultPhysFile = ary[0];	/* same as schema */

		    printf("Create schema %s\t", tsi.si_ScmName);
		    fflush(stdout);
		    if ((error = LLBootstrapSchema(Db, &tsi)) == 0)
			printf("ok\n");
		    else
			printf("failed %d\n", error);
		    ptr = NULL;
		}
		zfree(ary, sizeof(char *) * 1);
		ary = NULL;
		break;
	    case 't':
		if (vt == 0) {
		    printf("Must specify virtual table id to bootstrap table\n");
		    break;
		}
		ary = zalloc(sizeof(char *) * 2);
		while (dotsplit(strtok(ptr, " \t\r"), ary, 2) == 0) {
		    int error;
		    SchemaI *si;
		    TableI tti;

		    si = LLGetSchemaI(Db, ary[0], strlen(ary[0]));
		    if (si == NULL) {
			printf("Unable to find schema %s\n", ary[0]);
			continue;
		    }
		    bzero(&tti, sizeof(tti));
		    tti.ti_TabName = ary[1];
		    tti.ti_TabNameLen = strlen(ary[1]);
		    tti.ti_VTable = vt;

		    printf("Create table\t");
		    fflush(stdout);
		    if ((error = LLBootstrapTable(Db, si, &tti)) == 0)
			printf("ok\n");
		    else
			printf("failed %d\n", error);
		    ptr = NULL;
		}
		zfree(ary, sizeof(char *) * 2);
		ary = NULL;
		break;
	    case 'c':
		if (colId == 0) {
		    printf("Must specify column id to bootstrap column\n");
		    break;
		}
		ary = zalloc(sizeof(char *) * 3);
		while (dotsplit(strtok(ptr, " \t\r"), ary, 3) == 0) {
		    Query *q = GetQuery(Db);
		    SchemaI *si;
		    TableI *ti;
		    ColI ci;
		    int error;

		    printf("Create column %s.%s.%s\t", ary[0], ary[1], ary[2]);
		    fflush(stdout);

		    si = HLGetSchemaI(q, ary[0], strlen(ary[0]));
		    if (si == NULL) {
			printf("Unable to find schema %s\n", ary[0]);
			FreeQuery(q);
			continue;
		    }
		    ti = HLGetTableI(q, si, ary[1], strlen(ary[1]), NULL, 0);
		    if (ti == NULL) {
			printf("Unable to find table %s.%s\n", ary[0], ary[1]);
			FreeQuery(q);
			continue;
		    }
		    bzero(&ci, sizeof(ci));
		    ci.ci_ColName = ary[2];
		    ci.ci_ColNameLen = strlen(ci.ci_ColName);
		    if ((error = LLBootstrapColumn(Db, ti, colId, &ci)) == 0)
			printf("ok\n");
		    else
			printf("failed creating column %s (%d)\n", ary[2], error);
		    ptr = NULL;
		}
		zfree(ary, sizeof(char *) * 3);
		ary = NULL;
		break;
	    default:
		printf("Unknown '+' command '%c'\n", buf[1]);
		break;
	    }
	    break;
	case '-':
	    switch(buf[1]) {
	    case 's':
		break;
	    case 't':
		break;
	    case 'c':
		break;
	    default:
		printf("Unknown '-' command '%c'\n", buf[1]);
		break;
	    }
	    break;
	case 'q':
	    /*
	     * Run a high level query.  By including the terminating \0
	     * the parser will accept the terminator in liu of a semicolon.
	     */
	    if (*ptr) {
		LLRunQuery(ptr);
		break;
	    }
	    /* fall through */
	case 'x':
	    NotDone = 0;
	    break;
	default:
	    printf("Unknown command '%c', h for help\n", buf[0]);
	    break;
	}

	if (ary) {
	    for (i = 0; i < count; ++i)
		free(ary[i]);
	    free(ary);
	}
	if (cols)
	    free(cols);
    }
    if (addRepPeer) {
	char *ptr;

	Db = PushDatabase(Db, fts, &error, 0);
	safe_asprintf(&ptr, "INSERT INTO sys.repgroup"
			    " ( HostName, HostId, HostType )"
			    " VALUES ( '%s', '1', 'PEER' )",
			    addRepPeer
	);
	LLRunQuery(ptr);
	free(ptr);
	fts = LLCommit(fts, 'c', 0, &commitPhase);
	SetSyncTs(Db, fts);
    }
    return(0);
}

static void
LLRunQuery(const char *ptr)
{
    Query *q = GetQuery(Db);
    token_t t;
    int type;

    type = SqlInit(&t, ptr, strlen(ptr) + 1);
    type = ParseSql(&t, q, type);
    if (type & TOKF_ERROR)
	LexPrintError(&t);
    if ((type & TOKF_ERROR) == 0) {
	q->q_TermFunc = LLTermRange;
	q->q_TermInfo = q->q_TableIQBase;
	printf("Results %d\n", RunQuery(q));
	RelQuery(q);
    } else {
	FreeQuery(q);
    }
}

static dbstamp_t
LLCommit(dbstamp_t fts, char cmd, int how, int *commitPhase)
{
    int error;

    /*
     * Note: llquery is not used in normal operation so no
     * callback sequencing is required.  i.e. there are no
     * other competing clients.
     */
    if (*commitPhase == 0) {
	int flags = (cmd == 'C') ? CCF_FORCE : 0;

	printf("Commit Transaction %016qx...", fts);
	fflush(stdout);
	if ((error = Commit1(Db, &fts, NULL, NULL, flags)) == 0) {
	    printf("%016qx...", fts);
	    *commitPhase = 1;
	} else {
	    printf("Commit Phase 1 failed with error %d\n", error);
	    Db = PopDatabase(Db, &error);
	}
    }
    if (*commitPhase == 1 && (how == 0 || how == 2)) {
	int flags = (cmd == 'C') ? CCF_FORCE : 0;

	if ((error = Commit2(Db, fts, 0, flags)) == 0) {
	    printf("Commit2 succeeded!\n");
	    ++fts;
	} else {
	    printf("Commit Phase 2 failed with error %d\n", error);
	}
	*commitPhase = 0;
	Db = PopDatabase(Db, &error);
    }
    return(fts);
}

void
addStrArray(char ***pary, int i, char *s)
{
    char **ary = realloc(*pary, (i + 1) * sizeof(char *));

    ary[i] = strdup(s);
    *pary = ary;
}

void
addColsArray(col_t **pcols, int i, col_t cid)
{
    col_t *cols = realloc(*pcols, (i + 1) * sizeof(col_t));

    cols[i] = cid;
    *pcols = cols;
}

void
DoLLQuery(vtable_t vt, char **ary, col_t *cols, int count)
{
    Query *q = GetQuery(Db);
    TableI *ti = GetTableIQuick(q, Db->db_SysTable, vt, cols, count);
    Range *r = NULL;
    int i;
    ColData *cd;

    i = 0;
    cd = ti->ti_RData->rd_ColBase;

    while (cd) {
	char *p;

	if ((p = strchr(ary[i], ':')) != NULL) {
	    ColData *cdv = GetConst(q, p + 1, strlen(p + 1));
	    r = HLAddClause(q, r, ti, cd, cdv, ROP_EQEQ, ROP_CONST);
	}
	cd = cd->cd_Next;
	++i;
    }

    /*
     * If no clauses then add a dummy one to scan the whole table
     */
    if (r == NULL)
	r = HLAddClause(q, r, ti, NULL, NULL, -1, ROP_NOP);

    q->q_TermInfo = q->q_TableIQBase;
    q->q_TermOp = QOP_SELECT;
    i = RunQuery(q);
    RelQuery(q);
    printf("result: %d\n", i);
}

/*
 * LLTermRange()
 *
 *	Note that each termination range falls within a single physical table.
 */

int
LLTermRange(Query *q)
{
    TableI *ti = q->q_TableIQBase;	/* XXX q_TermInfo */
    ColI *ci;

    printf("%08qx ", ti->ti_RanBeg.p_Ro);

    for (ci = q->q_ColIQBase; ci; ci = ci->ci_QNext) {
	ColData *cd = ci->ci_CData;

	if (cd->cd_Data == NULL)
	    continue;
	printf(" %d(%d)=", (int)(col_t)cd->cd_ColId, cd->cd_Bytes);
	if (cd->cd_Bytes) {
	    fwrite(cd->cd_Data, cd->cd_Bytes, 1, stdout);
	} else if (cd->cd_Data) {
	    ;
	} else {
	    printf("(null)");
	}
    }
    printf("\n");
    return(1);
}

void
DumpIt(void)
{
}

int
dotsplit(char *ptr, char **ary, int count)
{
    int i = 0;
    char *str;

    if (ptr == NULL)
	return(-1);

    while ((str = ptr) != NULL) {
	if ((ptr = strchr(str, '.')) != NULL)
	    *ptr++ = 0;
	ary[i++] = str;
	--count;
	if (count == 0 && ptr) {
	    printf("Too many dots!\n");
	    return(-1);
	}
    }
    if (count != 0) {
	printf("Not enough dots!\n");
	return(-1);
    }
    return(0);
}

