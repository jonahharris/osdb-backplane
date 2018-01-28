/*
 * UTILS/DWAIT.C
 *
 * (c)Copyright 2000-2002 Backplane, Inc.  Please refer to the COPYRIGHT
 * file at the base of the distribution tree.
 *
 * $Backplane: rdbms/utils/dwait.c,v 1.2 2002/08/20 22:06:06 dillon Exp $
 *
 *	DWAIT	database	[timestamp]
 */

#include "defs.h"
#include <ctype.h>

database_t Db;
database_t DbI;

void
task_main(int ac, char **av)
{
    int i;
    int error;
    char *dataBase = NULL;
    char *ts = NULL;
    dbstamp_t syncTs = 0;

    for (i = 1; i < ac; ++i) {
	char *ptr = av[i];

	if (*ptr != '-') {
	    if (ts) {
		fprintf(stderr, "Unexpected argument: %s\n", ptr);
		exit(1);
	    } else if (dataBase != NULL) {
		ts = ptr;
	    } else {
		dataBase = ptr;
	    }
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
    if (dataBase == NULL) {
	fprintf(stderr, "Version 1.00\n");
	fprintf(stderr, "%s [-D dbdir] dataBase [hex_timestamp]\n", av[0]);
	exit(1);
    }

    Db = OpenCLDataBase(dataBase, &error);
    if (Db == NULL) {
	fprintf(stderr, "OpenCLDataBase() failed error %d\n", error);
	exit(1);
    }
    DbI = OpenCLInstance(Db, &syncTs, CLTYPE_RW);
    if (DbI == NULL) {
	fprintf(stderr, "OpenCLInstance() failed\n");
	exit(1);
    }

    while (ts != NULL) {
	res_t res;

	syncTs = strtouq(ts, NULL, 16);

	if ((error = SyncCLInstance(DbI, &syncTs)) > 0)
	    break;
	if (error < 0) {
	    fprintf(stderr, "SyncCLInstance: error %d\n", error);
	    break;
	}
	PushCLTrans(DbI, syncTs + 1, CPF_READONLY);
	res = QueryCLTrans(DbI, "COUNT FROM sys.repgroup WHERE HostType = 'aaaaaaaa'", &error);
	if (res)
	    FreeCLRes(res);
	AbortCLTrans(DbI);
    }
    printf("%016qx\n", syncTs);
    CloseCLInstance(DbI);
    CloseCLDataBase(Db);
}

char *
getSqlLine(iofd_t io, int to)
{
    static char *Buf;
    static int Len;
    int i;
    int sq = 0;
    int skip = 0;
    int nlerror = 0;
    int c;

    i = 0;
    while ((c = t_getc(io, to)) >= 0) {
	if (i >= Len - 1) {
	    Len = (Len) ? Len * 2 : 1024;
	    Buf = realloc(Buf, Len);
	}
	Buf[i++] = c;
	if (skip) {
	    if (skip == 1) {
		skip = 0;
		continue;
	    }
	    if (c == '\n')
		skip = 0;
	    --i;
	    continue;
	}
	if (c == '\n') {
	    if (sq && nlerror == 0) {
		nlerror = 1;
		Buf[i] = 0;
		printf("WARNING: newline embedded in quoted string on line:\n");
		printf("%s\n", Buf);
	    }
	    if (i == 1)
		--i;
	    else
		Buf[i - 1] = ' ';
	    continue;
	}
	if (c == '\\') {
	    skip = 1;
	    continue;
	}
	if (sq == 0) {
	    if (c == ';' || c == '{' || c == '}') {
		Buf[i] = 0;
		return(Buf);
	    }
	    if (c == '#') {
		skip = 2;
		--i;
	    } else if (c == '\'' || c == '"') {
		sq = c;
	    }
	} else if (sq == c) {
	    sq = 0;
	}
    }
    return(NULL);
}

