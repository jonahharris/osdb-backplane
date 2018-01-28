/*
 * LIBDBCORE/DBLOG.C - implements logging and physical file synchronization
 *			mechanisms.
 *
 * (c)Copyright 1999-2002 Backplane, Inc.  Please refer to the COPYRIGHT
 * file at the base of the distribution tree.
 *
 * $Backplane: rdbms/libdbcore/dblog.c,v 1.9 2003/04/02 18:10:41 dillon Exp $
 */

#include "defs.h"

#define MAXLOGBUF	(64 * 1024)
#define MAXLOGFILESIZE	(2 * 1024 * 1024)	/* 16 MB per log file */

Export void findLogFileRange(const char *dirPath, u_int *begNo, u_int *endNo);
Prototype void SynchronizeTable(Table *tab);
Prototype void SynchronizeDatabase(DataBase *db, dbstamp_t cts);
Prototype void SyncTableAppend(Table *tab);
Prototype void openDataLog(DataBase *db);
Prototype void closeDataLog(DataBase *db);

/*
 * findLogFileRange() - return the range of log file indexes available.
 */
void
findLogFileRange(const char *dirPath, u_int *begNo, u_int *endNo)
{
    DIR *dir;

    if (begNo)
	*begNo = 0;
    if (endNo)
	*endNo = 0;

    if ((dir = opendir(dirPath)) != NULL) {
	struct dirent *den;
	while ((den = readdir(dir)) != NULL) {
	    char *dot;

	    if ((dot = strrchr(den->d_name, '.')) != NULL &&
		strcmp(dot, ".lg0") == 0 &&
		(dot = strchr(den->d_name, '_')) != NULL
	    ) {
		u_int count = strtoul(dot + 1, NULL, 10);
		if (begNo && *begNo > count)
		    *begNo = count;
		if (endNo && *endNo <= count)
		    *endNo = count + 1;
	    }
	}
	closedir(dir);
    }
}

/*
 * openDataLog() - create the next serialized index file
 */
void
openDataLog(DataBase *db)
{
    char *logName;

    if (db->db_DataLogCount == 0)
	findLogFileRange(db->db_DirPath, NULL, &db->db_DataLogCount);

    safe_asprintf(&logName, "%s/log_%09d.lg0",
	db->db_DirPath, db->db_DataLogCount);
    db->db_DataLogFd = open(logName, O_RDWR|O_CREAT|O_EXCL, 0660);
    DBASSERT(db->db_DataLogFd >= 0);
    safe_free(&logName);
    db->db_DataLogOff = lseek(db->db_DataLogFd, 0L, 2);
}

void
closeDataLog(DataBase *db)
{
    if (db->db_DataLogFd >= 0) {
	close(db->db_DataLogFd);
	db->db_DataLogFd = -1;
    }
}

static void
initRecord(DataBase *db, LogRecord *rec, u_int8_t cmd, int bytes)
{
    bzero(rec, bytes);
#if BYTE_ORDER == LITTLE_ENDIAN
    rec->lr_Magic = LR_MAGIC_LSB;
#else
    rec->lr_Magic = LR_MAGIC_MSB;
#endif
    rec->lr_Cmd = cmd;
    rec->lr_SeqNo = db->db_DataLogSeqNo++;
    rec->lr_Bytes = bytes;
}

static void
initLogAppendRecord(Table *tab, LogAppendRecord *lar, dboff_t offset, int flags)
{
    DataBase *db = tab->ta_Db;
    initRecord(db, &lar->lar_Head, LOG_CMD_APPEND_OFFSET, sizeof(LogAppendRecord));
    DBASSERT(tab->ta_LogFileId != 0);
    lar->lar_Head.lr_Flags |= flags;
    lar->lar_Head.lr_File = tab->ta_LogFileId;
    lar->lar_Offset = offset;
}

static void
initLogTransRecord(DataBase *db, LogTransRecord *ltr, u_int8_t cmd, dbstamp_t cts)
{
    initRecord(db, &ltr->ltr_Head, cmd, sizeof(LogTransRecord));
    ltr->ltr_Stamp = cts;
}

static void
initLogTableDataRecord(Table *tab, LogTableDataRecord *ltd, dboff_t off)
{
    DataBase *db = tab->ta_Db;
    initRecord(db, &ltd->ltd_Head, LOG_CMD_TABLE_DATA, sizeof(LogTableDataRecord));
    DBASSERT(tab->ta_LogFileId != 0);
    ltd->ltd_Head.lr_File = tab->ta_LogFileId;
    ltd->ltd_Offset = off;
}

static LogIdRecord *
allocLogIdRecord(Table *tab)
{
    int flen = strlen(tab->ta_Name) + strlen(tab->ta_Ext) + 1;
    int bytes = offsetof(LogIdRecord, lir_FileName[flen+1]);
    LogIdRecord *lir = zalloc(bytes);

    initRecord(tab->ta_Db, &lir->lir_Head, LOG_CMD_FILE_ID, bytes);
    sprintf(lir->lir_FileName, "%s.%s", tab->ta_Name, tab->ta_Ext);
    return(lir);
}

static void
freeLogRecord(LogRecord *lr)
{
    zfree(lr, lr->lr_Bytes);
}

static void
writeLogRecord(DataBase *db, LogRecord *rec)
{
    if (db->db_DataLogFd < 0)
	openDataLog(db);
    if (write(db->db_DataLogFd, rec, rec->lr_Bytes) != rec->lr_Bytes) {
	ftruncate(db->db_DataLogFd, db->db_DataLogOff);
	fsync(db->db_DataLogFd);
	fprintf(stderr, "write() failed while writing to the log\n");
	DBASSERT(0);	/* XXX */
    } else {
	db->db_DataLogOff += rec->lr_Bytes;
    }
}

static void
writeExtendedLogRecord(DataBase *db, LogRecord *rec, int rfd, dboff_t off, dboff_t bytes)
{
    int headBytes;

    if (db->db_DataLogFd < 0)
	openDataLog(db);
    headBytes = rec->lr_Bytes;
    rec->lr_Bytes += bytes;
    if (write(db->db_DataLogFd, rec, headBytes) == headBytes) {
	lseek(rfd, off, 0);
	if (bytes < 1024*1024) {
	    dboff_t pgOff = off & ~DbPgMask;
	    dboff_t pgBytes = (bytes + (off - pgOff) + DbPgMask) &
				~DbPgMask;
	    char *buf = mmap(NULL, pgBytes, PROT_READ, MAP_SHARED, rfd, pgOff);
	    int n;

	    DBASSERT(buf != MAP_FAILED);
	    n = write(db->db_DataLogFd, buf + (int)(off - pgOff), bytes);
	    munmap(buf, pgBytes);
	    if (n == bytes)
		bytes = 0;
	} else {
	    char *buf = safe_malloc(MAXLOGBUF);

	    while (bytes > 0) {
		int n = (bytes > MAXLOGBUF) ? MAXLOGBUF : (int)bytes;
		if (read(rfd, buf, n) != n)
		    break;
		if (write(db->db_DataLogFd, buf, n) != n)
		    break;
		bytes -= n;
	    }
	    free(buf);
	}
    }
    if (bytes == 0) {
	db->db_DataLogOff += rec->lr_Bytes;
    } else {
	ftruncate(db->db_DataLogFd, db->db_DataLogOff);
	fsync(db->db_DataLogFd);
	fprintf(stderr, "write() failed while writing to the log\n");
	DBASSERT(0);	/* XXX */
    }
    rec->lr_Bytes -= bytes;
}

static void
fsyncLog(DataBase *db)
{
    if (db->db_DataLogFd >= 0)
	fsync(db->db_DataLogFd);
}


/*
 * SynchronizeTable() -	Do all logging and fsync operations required to
 *			synchronize a table.  Note that the operation is
 *			not complete until SynchronizeDatabase() is called!
 *
 *	Do whatever setup is required on a table to eventually synchronize a
 *	database.  There are several run-time options here:
 *
 *	(1) Copy the new table records and append point to the log & perform
 *	    no fsyncs on either the table or the log (yet).
 *
 *	(2) FSync the new table records in the table and write just the append
 *	    point to the log.  Perform no fsyncs on the log (yet).
 *
 *	Note that this routine does NOT, I repeat, does NOT and should not write
 *	or fsync the table append offset in the table header.  That is the
 *	responsibility of SynchronizeDatabase().
 *
 *	XXX require that database be locked (it usually is) XXX
 *	XXX needs work XXX
 *	XXX deal with vacuumed databases XXX
 */
void
SynchronizeTable(Table *tab)
{
    DataBase *db = tab->ta_Db;

    if (db->db_PushType == DBPUSH_ROOT &&
	tab->ta_Append > tab->ta_Meta->tf_Append
    ) {
	int flags = 0;	/* XXX take from table header? */

	/*
	 * If no file identifier has been assigned to this table we
	 * must assign one.
	 */
	if (tab->ta_LogFileId == 0) {
	    LogIdRecord *lir;

	    tab->ta_LogFileId = db->db_NextLogFileId++;
	    lir = allocLogIdRecord(tab);
	    writeLogRecord(db, &lir->lir_Head);
	    freeLogRecord(&lir->lir_Head);
	}

	/*
	 * Log sufficient information to recover the table information,
	 * depending on the mode.
	 */
	if (flags & LRF_WITHOUT_DATA) {
	    /*
	     * Log table records without copying the table data to the log.
	     * The table file's data must be fsync'd.
	     */
	    LogAppendRecord lar;

	    tab->ta_FSync(tab);
	    initLogAppendRecord(tab, &lar, tab->ta_Append, flags);
	    writeLogRecord(db, &lar.lar_Head);
	} else {
	    /*
	     * Log table records and copy the table data to the log.
	     * The table file's data does not need to be fsync'd.
	     */
	    LogAppendRecord lar;
	    LogTableDataRecord ltd;

	    initLogTableDataRecord(tab, &ltd, tab->ta_Meta->tf_Append);
	    writeExtendedLogRecord(db, &ltd.ltd_Head, tab->ta_Fd,
		tab->ta_Meta->tf_Append,
		tab->ta_Append - tab->ta_Meta->tf_Append);
	    initLogAppendRecord(tab, &lar, tab->ta_Append, flags);
	    writeLogRecord(db, &lar.lar_Head);
	}

	if ((tab->ta_Flags & TAF_MODIFIED) == 0) {
	    tab->ta_Flags |= TAF_MODIFIED;
	    tab->ta_ModNext = db->db_ModTable;
	    db->db_ModTable = tab;
	}
    }
}

/*
 * SynchronizeDatabase() - Do all remaining processing required to 
 *			   completely commit previously synchronized items.
 *
 *	This routine is responsible for closing out the transaction in the
 *	log, fsync()ing the log, then updating the append offsets in the
 *	table headers.  The append offsets do not need to be fsync()d since
 *	they were recorded in the log.
 *
 *	This routine is also responsible for finishing up any index-related
 *	logging and sychronization.  Note that index updates typically lag
 *	the related transaction and are combined with the next transaction
 *	so the whole thing (index and table updates) can go in with a
 *	single fsync() of the log file.
 */
void
SynchronizeDatabase(DataBase *db, dbstamp_t cts)
{
    LogTransRecord ltr;
    Table *tab;
    int i;

    /*
     * Write the transaction commit record to the log and fsync the log.
     * Log operations are now complete.
     */
    if (db->db_PushType == DBPUSH_ROOT) {
	initLogTransRecord(db, &ltr, LOG_CMD_TRANS_COMMIT, cts);
	writeLogRecord(db, &ltr.ltr_Head);
	fsyncLog(db);
    }

    /*
     * Update the physical table file append points.  These writes do not 
     * have to be fsync'd.
     */
    while ((tab = db->db_ModTable) != NULL) {
	db->db_ModTable = tab->ta_ModNext;
	tab->ta_Flags &= ~TAF_MODIFIED;
	SyncTableAppend(tab);
    }

    /*
     * Rotate to the next log file if necessary.  If we rotate to a new
     * log file we have to regenerate the table and index file id's so each
     * log file can operate independantly.  Ids are regenerated if a cached
     * table or index file is removed so NextLogFileId may increment more
     * then you would expect.  If we run out of IDs (0-0x7FFFFFFF) then
     * we also have to rotate to the next log file.
     */
    if (db->db_DataLogOff > MAXLOGFILESIZE ||
	db->db_NextLogFileId > 0x3FFFFFFF
    ) {
	closeDataLog(db);
	db->db_NextLogFileId = 1;	/* XXX */
	++db->db_DataLogCount;
	openDataLog(db);
        for (i = 0; i < TAB_HSIZE; ++i) {
	    for (tab = db->db_TabHash[i]; tab; tab = tab->ta_Next)
		tab->ta_LogFileId = 0;
        }
    }
}

/*
 * SyncTableAppend() -	synchronize the persistent table append point
 *
 *	Update the tf_Append field in the physical file.  This function
 *	is called by the logging code and may also be called from llquery
 *	(for low level table bootstrapping).  This function does not need to
 *	fsync the table file since the log will already have the append 
 *	offset record.
 *
 *	This lock does not need to be thread safe.
 *
 *	XXX remove hflock's entirely and depend on single-threaded access
 *	via systable lock?
 */
void 
SyncTableAppend(Table *tab)
{
    if (tab->ta_Append != tab->ta_Meta->tf_Append) {
	if (tab->ta_Fd >= 0)
	    hflock_ex(tab->ta_Fd, 4);
	if (tab->ta_Append < tab->ta_Meta->tf_Append) {
	    tab->ta_Append = tab->ta_Meta->tf_Append;
	} else if (tab->ta_Meta->tf_Append < tab->ta_Append) {
	    tab->ta_WriteMeta(tab, offsetof(TableFile, tf_Append), 
			&tab->ta_Append, sizeof(dboff_t));
	}
	if (tab->ta_Fd >= 0)
	    hflock_un(tab->ta_Fd, 4);
    }
}

