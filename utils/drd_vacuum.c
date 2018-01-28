/*
 * UTILS/DRD_VACUUM.C		- Remove deleted records from table(s)
 *
 * (c)Copyright 2000-2002 Backplane, Inc.  Please refer to the COPYRIGHT
 * file at the base of the distribution tree.
 *
 * $Backplane: rdbms/utils/drd_vacuum.c,v 1.20 2002/09/21 23:43:31 dillon Exp $
 *
 * DRD_VACUUM -d DATE Database[:schema]
 * DRD_VACUUM -t TIMEAMOUNT Database[:schema]
 *
 *	This program opens the specified database exclusively and regenerates
 *	the physical file associated with the schema.  It obtains a list of
 *	all tables and schemas using the physical file and extracts and sorts
 *	each one by its lowest-numbered key field (using an index) during
 *	the regeneration.
 */

#include "defs.h"
#include "libdbcore/dbcore-protos.h"
#include <libdbcore/simplequery.h>
#include <ctype.h>

int QuietOpt;
int NotDone = 1;

typedef struct ScrapVT {
    struct ScrapVT *next;
    vtable_t vtid;
} ScrapVT;

typedef struct Info {
    DataBase	*db;
    dbstamp_t	hts;
} Info;

void Vacuum(SimpleHash *sh, const char *key, ScrapVT *sbase, Info *info);
void VacuumScan(DataBase *db, const char *rfile, const char *schemaName, const char *tableName, dbstamp_t hts, TableI *ti, RawData *rd, Table *rtab, Table *wtab, vtable_t vtid, col_t colid, SimpleHash *vtidHash);

static int isDeleted(const char *s);
static int lookupVTId(SimpleHash *hash, const RecHead *rh);

static ScrapVT ScrapTerm;


int
task_main(int ac, char **av)
{
    int i;
    int didhts = 0;
    int error;
    char *dataBase = NULL;
    const char *dbDir = DefaultDBDir();
    char *schema = NULL;
    char *alloc = NULL;
    char *qry = NULL;
    DataBase *db;
    dbstamp_t hts;

    hts = dbstamp(0, 0);

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
	case 'v':
	    DebugOpt = 1;
	    break;
	case 'd':
	    didhts = 1;
	    ptr = (*ptr) ? ptr : av[++i];
	    {
		int count;
		struct tm tm = { 0 };

		count = sscanf(ptr, 
			    "%04d%02d%02d.%02d%02d%02d",
			    &tm.tm_year, &tm.tm_mon, &tm.tm_mday,
			    &tm.tm_hour, &tm.tm_min, &tm.tm_sec
			);
		if (count != 3 && count != 6) {
		    fprintf(stderr, "Bad date format, use yyyymmdd[.hhmmss]\n");
		    fprintf(stderr, "WARNING!  ALL TIMES GMT\n");
		    exit(1);
		}
		tm.tm_year -= 1900;
		tm.tm_mon -= 1;
		hts = timetodbstamp(timegm(&tm));
	    }
	    break;
	case 't':
	    didhts = 1;
	    ptr = (*ptr) ? ptr : av[++i];
	    hts = hts - timetodbstamp(strtol(ptr, NULL, 0) * 60 * 60 * 24);
	    break;
	case 'D':
	    dbDir = (*ptr) ? ptr : av[++i];
	    break;
	default:
	    fprintf(stderr, "Unknown option: %s\n", ptr - 2);
	    exit(1);
	}
    }
    if (i > ac)
	fatal("last option required argument");

    /*
     * Figure out database directory & name
     */

    if (dataBase == NULL) {
	fprintf(stderr, "Version 1.00\n");
	fprintf(stderr, "%s [-D dbdir] [-q][-v] [-d yyyymmdd[.hhmmss]] [-t keepdays] dataBase:schema\n", av[0]);
	exit(1);
    }
    if (didhts == 0) {
	fprintf(stderr, "Must specify vacuuming as-of date with -d or must\n");
	fprintf(stderr, "specify number of days to preserve with -t\n");
	exit(1);
    }
    if (dataBase[0] == '/')
	dbDir = NULL;

    /*
     * Obtain schema.  May be NULL in which case all schemas are regenerated.
     */
    if ((schema = strrchr(dataBase, ':')) != NULL) {
	*schema++ = 0;
    }

    /*
     * Open the database exclusively
     */
    error = 0;
    db = OpenDatabase(dbDir, dataBase, DBF_EXCLUSIVE, NULL, &error);

    if (db == NULL)
	fatal("OpenDatabase: error %d", error);

    /*
     * If the schema is specified then obtain the physical table file
     * from it.  
     */
    if (schema != NULL) {
	safe_replacef(&qry, "SELECT TableFile, TableName, TableVId, __timestamp FROM sys.tables"
		" WHERE SchemaName = '%s';", DBMSEscape(schema, &alloc, -1));
    } else {
	safe_replacef(&qry, "SELECT TableFile, TableName, TableVId, __timestamp FROM sys.tables;");
    }
    {
	SimpleQuery *sq;
	SimpleHash hash;
	ScrapVT *scrap;
	char **row;
	Info info = { db, hts };

	simpleHashInit(&hash);
	sq = StartSimpleQuery(db, qry);
	if (sq == NULL) {
	    fprintf(stderr, "SimpleQuery failed, cannot continue\n");
	    exit(1);
	}
	while ((row = GetSimpleQueryResult(sq)) != NULL) {
	    if ((scrap = simpleHashLookup(&hash, row[0])) == NULL) {
		simpleHashEnter(&hash, row[0], &ScrapTerm);
	    }
	    if (row[1] &&
		strncmp(row[1], "$DEL", 4) == 0 &&
		strtouq(row[3], NULL, 16) < hts
	    ) {
		ScrapVT *nscrap = zalloc(sizeof(ScrapVT));
		nscrap->vtid = strtoul(row[2], NULL, 16);
		nscrap->next = scrap;
		simpleHashEnter(&hash, row[0], nscrap);
	    }
	}
	EndSimpleQuery(sq);
	simpleHashIterate(&hash, (void *)Vacuum, &info);
	simpleHashFree(&hash, NULL);
    }
    CloseDatabase(db, 1);
    return(0);
}

void
Vacuum(SimpleHash *sh, const char *rfile, ScrapVT *sbase, Info *info)
{
    DataBase *db = info->db;
    dbstamp_t hts = info->hts;
    char *wfile;
    char *rpath;
    char *wpath;
    char *qry = NULL;
    Table *rtab;
    Table *wtab;
    TableI *ti;
    RawData *rd;
    int error;
    struct stat st;
    SimpleQuery *q1;
    char *alloc1 = NULL;
    char *alloc2 = NULL;
    char **row1;
    SimpleHash vtidHash;
    DBCreateOptions dbc;

    simpleHashInit(&vtidHash);

    safe_asprintf(&wfile, "%s.new", rfile);
    safe_asprintf(&rpath, "%s/%s.dt0", db->db_DirPath, rfile);
    safe_asprintf(&wpath, "%s/%s.dt0", db->db_DirPath, wfile);
    remove(wpath);

    error = 0;
    if (stat(rpath, &st) < 0) {
	fatal("Unable to stat physical table file %s in %s",
	    rfile, db->db_DirPath);
    }
    rtab = OpenTable(db, rfile, "dt0", NULL, &error);
    if (rtab == NULL)  {
	fatal("Unable to open physical table file %s in %s", 
	    rfile, db->db_DirPath);
    }
    InitOptionsFromTable(rtab, &dbc);
    wtab = OpenTable(db, wfile, "dt0", &dbc, &error);
    if (wtab == NULL) {
	remove(wfile);
	fatal("Unable to create physical table file %s in %s", 
	    wfile, db->db_DirPath);
    }
    if (chown(wpath, st.st_uid, st.st_gid) < 0) {
	remove(wpath);
	fatal("Unable to chown vacuum temporary file %s in %s to %d/%d",
	    wfile, 
	    db->db_DirPath,
	    (int)st.st_uid,
	    (int)st.st_gid
	);
    }
    if (chmod(wpath, st.st_mode & ALLPERMS) < 0) {
	remove(wpath);
	fatal("Unable to chmod vacuum temporary file %s in %s to %04o",
	    wfile, 
	    db->db_DirPath,
	    (int)st.st_mode & ALLPERMS
	);
    }

    rd = AllocRawData(rtab, NULL, 0);
    ti = AllocPrivateTableI(rd);
    ti->ti_ScanOneOnly = -1;

    /*
     * Locate all tables associated with the physical file and scan each
     * one.  Attempt to index by the lowest-numbered field.
     *
     * XXX keep track of vtid's and make sure we scan all of them 
     * for the physical file, even if they do not show up in sys.tables.
     */
    safe_replacef(&qry, "SELECT SchemaName, TableName, TableVId"
			" FROM sys.tables"
			" WHERE TableFile = '%s';",
			DBMSEscape(rfile, &alloc1, -1)
    );
    if (strcmp(rfile, "sys") == 0) {
	int dummy = 0;
	char buf[32];

	snprintf(buf, sizeof(buf), VT_FMT_STRING, VT_COLTABLE_COLS);
	simpleHashEnter(&vtidHash, buf, &dummy);

	VacuumScan(db, rfile, "sys", "$cols", hts, ti, rd, rtab, wtab, VT_COLTABLE_COLS, (col_t)-1, &vtidHash);
    }


    q1 = StartSimpleQuery(db, qry);
    if (q1 == NULL) {
	fprintf(stderr, "SimpleQuery Failed, cannot continue\n");
	exit(1);
    }
    while ((row1 = GetSimpleQueryResult(q1)) != NULL) {
	SimpleQuery *q2;
	char **row2;
	vtable_t vt = strtol(row1[2], NULL, 16);
	col_t hcol = (col_t)-1;
	int scrapIt = 0;
	ScrapVT *scrap;

	for (scrap = sbase; scrap != &ScrapTerm; scrap = scrap->next) {
	    if (scrap->vtid == vt)
		scrapIt = 1;
	}

	safe_replacef(&qry, "SELECT ColName, ColFlags, ColId"
			" FROM %s.%s$cols;",
			DBMSEscape(row1[0], &alloc1, -1),
			DBMSEscape(row1[1], &alloc2, -1)
	);
	q2 = StartSimpleQuery(db, qry);
	if (q2 == NULL) {
	    fprintf(stderr, "SimpleQuery Failed, cannot continue\n");
	    exit(1);
	}
	while ((row2 = GetSimpleQueryResult(q2)) != NULL) {
	    col_t col = strtol(row2[2], NULL, 16);
	    if (hcol == (col_t)-1 || (isDeleted(row2[1]) < 0 && col < hcol))
		hcol = col;
	}
	EndSimpleQuery(q2);

	/*
	 * The query may fail if this is a column table (passed vtid + 1),
	 * or has no columns.  Use the raw timstamp.
	 */
	if (hcol == (col_t)-1)
	    hcol = CID_RAW_TIMESTAMP;

	/*
	 * If the entire table is to be scrapped (table was deleted over N
	 * days ago), don't scan it.  Note that the vacuuming code does not
	 * operate on a replication group and so it cannot safely delete
	 * record in sys.tables.  It can only delete the record(s) associated
	 * with the dropped table.
	 */
	if (scrapIt == 0) {
	    VacuumScan(db, rfile, row1[0], row1[1], hts, ti,
			rd, rtab, wtab, vt + 1, hcol, &vtidHash);
	    VacuumScan(db, rfile, row1[0], row1[1], hts, ti,
			rd, rtab, wtab, vt, hcol, &vtidHash);
	} else if (vt != 0) {
	    char buf[32];
	    int dummy = 0;

	    snprintf(buf, sizeof(buf), VT_FMT_STRING, (int)vt);
	    simpleHashEnter(&vtidHash, buf, &dummy);
	    snprintf(buf, sizeof(buf), VT_FMT_STRING, (int)vt + 1);
	    simpleHashEnter(&vtidHash, buf, &dummy);
	    printf("DESTROYING DELETED TABLE: %s\n", row1[1]);
	}
    }
    EndSimpleQuery(q1);

    VacuumScan(db, rfile, "ALL", "REMAINING", hts, ti, rd, rtab, wtab, 0, (col_t)-1, &vtidHash);

    /*
     * Cleanup
     */
    SyncTableAppend(wtab);
    LLFreeTableI(&ti);
    CopyGeneration(rtab, wtab, 0);
    CloseTable(rtab, 1);
    CloseTable(wtab, 1);
    safe_free(&alloc1);
    safe_free(&alloc2);
    simpleHashFree(&vtidHash, NULL);
}

void
VacuumScan(
    DataBase *db,
    const char *rfile,
    const char *schemaName,
    const char *tableName,
    dbstamp_t hts,
    TableI *ti,
    RawData *rd,
    Table *rtab,
    Table *wtab,
    vtable_t vtid,
    col_t colid,
    SimpleHash *vtidHash
) {
    DelHash dh;
    int delCount = 0;
    int delMatchCount = 0;
    int keepCount = 0;
    int showing = 0;
    int count1;
    int count2;

    InitDelHash(&dh);
    if (vtid != 0) {
	char buf[32];
	int dummy = 0;

	snprintf(buf, sizeof(buf), VT_FMT_STRING, (int)vtid);
	simpleHashEnter(vtidHash, buf, &dummy);
    }
    if (QuietOpt == 0) {
	printf("physfile %s %s.%s vtid %04x (col %04x)",
	    rfile, 
	    schemaName,
	    tableName,
	    (int)vtid,
	    (int)colid
	);
	fflush(stdout);
    }

    ti->ti_VTable = vtid;
    if (vtid) {
	ti->ti_Index = ti->ti_Table->ta_GetTableIndex(ti->ti_Table, ti->ti_VTable, colid, ROP_EQEQ);
	ti->ti_Flags = TABRAN_INDEX|TABRAN_SYNCIDX;
	ti->ti_Index->i_SetTableRange(ti, ti->ti_Table, GetRawDataCol(rd, colid, DATATYPE_STRING), NULL, TABRAN_INDEX|TABRAN_SYNCIDX|TABRAN_INIT);
    } else {
	ti->ti_Flags = TABRAN_SLOP;
	DefaultSetTableRange(ti, ti->ti_Table, NULL, NULL, TABRAN_SLOP|TABRAN_INIT);
    }

    /*
     * Cleaning is a two-pass process.  In pass 1 we locate and record
     * all deletions occuring before the requested timestamp.  In pass 2 we
     * filter out the original records (being deleted) as well as the
     * deletion records themselves.
     *
     * We could scan the table backwards to optimize the delete hash
     * but this would cause us to rewrite the records out backwards,
     * which will break things badly, so we don't.
     */

    count1 = 0;

    for (
	SelectBegTableRec(ti, 0); 
	ti->ti_RanBeg.p_Ro >= 0;
	SelectNextTableRec(ti, 0)
    ) {
	const RecHead *rh;

	rh = rd->rd_Rh;
	++count1;

	if (QuietOpt == 0 && count1 > 16384 && count1 % 100 == 0) {
	    if (showing == 0) {
		showing = 1;
		printf("\n");
	    }
	    printf(" pass1 %d\r", count1);
	    fflush(stdout);
	}

	if (vtid) {
	    if (rh->rh_VTableId != vtid)
		continue;
	} else if (lookupVTId(vtidHash, rh) == 0) {
	    continue;
	}
	if (rh->rh_Stamp < hts) {
	    if (rh->rh_Flags & RHF_DELETE)
		SaveDelHash(&dh, &ti->ti_RanBeg,
			    rh->rh_Hv, rh->rh_Size);
	}
    }

    if (vtid) {
	ti->ti_Index->i_SetTableRange(ti, ti->ti_Table, 
		    GetRawDataCol(rd, colid, DATATYPE_STRING), NULL,
		    TABRAN_INDEX|TABRAN_INIT);
    } else {
	DefaultSetTableRange(ti, ti->ti_Table, NULL, NULL,
		    TABRAN_SLOP|TABRAN_INIT);
    }
    /*
     * Scan again removing deleted records.
     */

    count2 = 0;
    for (
	SelectBegTableRec(ti, 0); 
	ti->ti_RanBeg.p_Ro >= 0;
	SelectNextTableRec(ti, 0)
    ) {
	const RecHead *rh;

	rh = rd->rd_Rh;

	++count2;

	if (QuietOpt == 0 && count1 > 16384 && count2 % 100 == 0) {
	    if (showing == 0) {
		showing = 1;
		printf("\n");
	    }
	    printf(" pass2 %d/%d\r", count2, count1);
	    fflush(stdout);
	}

	if (vtid) {
	    if (rh->rh_VTableId != vtid)
		continue;
	} else if (lookupVTId(vtidHash, rh) == 0) {
	    continue;
	}
	if (rh->rh_Stamp < hts) {
	    if (rh->rh_Flags & RHF_DELETE) {
		++delCount;
		continue;
	    }
	    if (MatchDelHash(&dh, rh) == 0) {
		++delMatchCount;
		continue;
	    }
	}
	if (rh->rh_Flags & RHF_DELETE)
	    ++delCount;
	else
	    ++keepCount;
	WriteDataRecord(wtab, NULL, rh, rh->rh_VTableId, rh->rh_Stamp, rh->rh_UserId, rh->rh_Flags);
    }
    if (QuietOpt == 0) {
	printf(" %d/%d pruned, %d/%d remain\n",
	    delMatchCount, keepCount - delCount + delMatchCount,
	    keepCount, delCount + keepCount
	);
    }
    if (dh.dh_Count)
	printf("** WARNING, %d deletions did not match up\n", dh.dh_Count);
    CloseIndex(&ti->ti_Index, 1);
}

static int
isDeleted(const char *s)
{
    if (s && strchr(s, 'D'))
	return(0);
    return(-1);
}

static int
lookupVTId(SimpleHash *hash, const RecHead *rh)
{
    static vtable_t savevt = 0;
    static char saveBuf[32];

    if (savevt != rh->rh_VTableId)  {
	snprintf(saveBuf, sizeof(saveBuf), VT_FMT_STRING, (int)rh->rh_VTableId);
	savevt = rh->rh_VTableId;
    }
    if (simpleHashLookup(hash, saveBuf))
	return(0);
    fprintf(stderr, "    Warning: unattached record ts %016qx vtid " VT_FMT_STRING "\n", rh->rh_Stamp, rh->rh_VTableId);
    return(-1);
}

