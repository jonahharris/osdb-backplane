/*
 * UTILS/DBLOG.C
 *
 * (c)Copyright 2000-2002 Backplane, Inc.  Please refer to the COPYRIGHT
 * file at the base of the distribution tree.
 *
 *	This program can be used to recover a database from its log file.
 *
 * $Backplane: rdbms/utils/dblog.c,v 1.1 2002/10/02 21:31:35 dillon Exp $
 */

#include "defs.h"
#include <ctype.h>
#include <sys/time.h>
#include <sys/stat.h>

static void dumpLogFile(const char *dirPath, u_int index);
static void dumpRecord(LogRecordAll *all, LogRecord *track);

int UseGmt = 0;

int
main(int ac, char **av)
{
    int i;
    u_int begNo;
    u_int endNo;
    char *dbName = NULL;
    char *dirPath;

    for (i = 1; i < ac; ++i) {
	char *ptr = av[i];

	if (*ptr != '-') {
	    if (dbName) {
		fprintf(stderr, "Database name was specified twice\n");
		exit(1);
	    }
	    dbName = ptr;
	    continue;
	}
	ptr += 2;
	switch(ptr[-1]) {
	case 'D':
	    SetDefaultDBDir((*ptr) ? ptr : av[++i]);
	    break;
	default:
	    fprintf(stderr, "Unknown option: %s\n", ptr - 2);
	    exit(1);
	}
    }
    if (ac == 1 || dbName == NULL) {
	fprintf(stderr, "Version 1.00\n");
	fprintf(stderr, "%s [-D dbdir] [-d date] [-q] database\n", av[0]);
	fprintf(stderr,"    date format: ddmmmyyyy[/hh:mm:ss][zone]\n");
	fprintf(stderr,"    If date not specified, default is entire log\n");
	fprintf(stderr,"    -t  integrity test on database against logs.\n");
	fprintf(stderr,"        If date specified only tables tested and\n");
	fprintf(stderr,"        only through date.  You must still use -r\n");
	fprintf(stderr,"        to rollback the database to the date\n");
	fprintf(stderr,"    -r  recover through date (note: if not\n");
	fprintf(stderr,"        recovering to the latest indexes will be\n");
	fprintf(stderr,"        invalidated\n");
	fprintf(stderr,"    -v  summarize contents of log file(s)\n");
	exit(1);
    }

    /*
     * Scan all available log files
     */
    safe_asprintf(&dirPath, "%s/%s", DefaultDBDir(), dbName);
    findLogFileRange(dirPath, &begNo, &endNo);
    if (begNo == endNo) {
	printf("Logfiles for %s: NO LOG FILES FOUND!\n", dbName);
	exit(1);
    }
    printf("Logfiles for %s: %08x-%08x (%d total)\n",
	dbName, begNo, endNo - 1, endNo - begNo);
    while (begNo < endNo) {
	dumpLogFile(dirPath, begNo);
	++begNo;
    }
    return(0);
}

static void
initTrackingRecord(LogRecord *track)
{
    bzero(track, sizeof(LogRecord));
    track->lr_SeqNo = -1;
}

static void
dumpLogFile(const char *dirPath, u_int index)
{
    char *logFile;
    FILE *fi;
    LogRecord track;

    initTrackingRecord(&track);

    safe_asprintf(&logFile, "%s/log_%09d.lg0", dirPath, index);
    if ((fi = fopen(logFile, "r")) != NULL) {
	union {
	    LogRecordAll all;
	    char buf[1024];
	} lru;

	printf("Scanning %s\n", logFile);
	while (fread(&lru.all.a_Head, 1, sizeof(LogRecord), fi) == sizeof(LogRecord)) {
	    int extra = lru.all.a_Head.lr_Bytes - sizeof(LogRecord);
	    LogRecordAll *allPtr = &lru.all;

	    DBASSERT(extra > 0);
	    if (extra > sizeof(lru)) {
		allPtr = safe_malloc(sizeof(LogRecord) + extra);
		bcopy(&lru.all.a_Head, &allPtr->a_Head, sizeof(LogRecord));
	    }
	    if (extra) {
		if (fread(&allPtr->a_Head + 1, 1, extra, fi) != extra) {
		    printf("Last record was truncated\n");
		    break;
		}
	    }
	    dumpRecord(allPtr, &track);
	    if (allPtr != &lru.all)
		free(allPtr);
	}
	fclose(fi);
    } else {
	printf("Scanning %s: NOT FOUND\n", logFile);
    }
    safe_free(&logFile);
}

static const char *
logCmdName(u_int8_t lrCmd)
{
    switch(lrCmd) {
    case LOG_CMD_HEARTBEAT:
	return("HEART");
    case LOG_CMD_TRANS_BEGIN:
	return("BEGIN");
    case LOG_CMD_TRANS_COMMIT:
	return("COMMIT");
    case LOG_CMD_FILE_ID:
	return("FILE");
    case LOG_CMD_APPEND_OFFSET:
	return("APPEND");
    case LOG_CMD_TABLE_DATA:
	return("TDATA");
    case LOG_CMD_INDEX_DATA:
	return("IDATA");
    default:
	return("???");
    }
}

static void
dumpRecord(LogRecordAll *all, LogRecord *track)
{
    char *alloc = NULL;

    if (track->lr_SeqNo != (u_int16_t)(all->a_Head.lr_SeqNo - 1))
	printf("*SEQUENCE NUMBER NOT SEQUENTIAL\n");
    printf(" CMD=%02x F=%04x S=%04x F=%08x L=%08x %s",
	all->a_Head.lr_Cmd,
	all->a_Head.lr_Flags,
	all->a_Head.lr_SeqNo,
	all->a_Head.lr_File,
	all->a_Head.lr_Bytes,
	logCmdName(all->a_Head.lr_Cmd)
    );
    switch(all->a_Head.lr_Cmd) {
    case LOG_CMD_TRANS_BEGIN:
    case LOG_CMD_TRANS_COMMIT:
	printf("\t%s",
	    dbstamp_to_ascii(all->a_LogTrans.ltr_Stamp, UseGmt, &alloc));
	break;
    case LOG_CMD_FILE_ID:
	printf("\t\"%s\"", all->a_LogId.lir_FileName);
	break;
    }
    printf("\n");
    track->lr_SeqNo = all->a_Head.lr_SeqNo;
    safe_free(&alloc);
}

