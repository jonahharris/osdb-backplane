/*
 * UTILS/DSQL.C
 *
 * (c)Copyright 2000-2002 Backplane, Inc.  Please refer to the COPYRIGHT
 * file at the base of the distribution tree.
 *
 * $Backplane: rdbms/utils/dsql.c,v 1.48 2002/08/20 22:06:06 dillon Exp $
 *
 *	DSQL ... [-T tsfile] [-D baseDir] database[:defaultSchema] 'query'
 */

#include "defs.h"
#include <ctype.h>

database_t Db;
database_t DbI;
int QuietOpt;
int EchoOpt;
int NotDone = 1;
int TabFmt = 0;
int Level;
int WaitOpt;

char *getSqlLine(iofd_t io, char **cbuf, int to);
int DoQuery(const char *buf);
dbstamp_t CommitQuery(iofd_t stdinIo, dbstamp_t fts);
const char * RemoveTabs(const char *str, char **alloc);

static
void
DisplayResult(const char *query, int rv)
{
    if (rv < 0 && EchoOpt == 0)
	printf("< %s\n", query);

    if (rv < 0)
	printf("> RESULT %d (%s)\n", rv, GetCLErrorDesc(rv));
    else if (QuietOpt == 0)
	printf("> RESULT %d\n", rv);
}

void
task_main(int ac, char **av)
{
    int i;
    int error;
    char *dataBase = NULL;
    char *useSchema;
    char *qry = NULL;
    char *tsFile = NULL;
    dbstamp_t syncTs = 0;
    dbstamp_t fts = 0;
    iofd_t stdinIo;

    for (i = 1; i < ac; ++i) {
	char *ptr = av[i];

	/*
	 * Non-option arguments are 'database' and 'query'.  We have to
	 * append a semicolon to the query if one isn't already there.
	 */
	if (*ptr != '-') {
	    if (qry) {
		fprintf(stderr, "Unexpected argument: %s\n", ptr);
		exit(1);
	    } else if (dataBase != NULL) {
		int len = strlen(ptr);
		qry = ptr;
		while (len > 0 && qry[len-1] == ' ')
		    --len;
		if (len > 0 && qry[len-1] != ';') {
		    safe_asprintf(&qry, "%s;", qry);
		}
	    } else {
		dataBase = ptr;
	    }
	    continue;
	}
	ptr += 2;
	switch(ptr[-1]) {
	case 't':
	    TabFmt = 1;
	    break;
	case 'q':
	    QuietOpt = 1;
	    break;
	case 'e':
	    EchoOpt = 1;
	    break;
	case 'D':
	    SetDefaultDBDir((*ptr) ? ptr : av[++i]);
	    break;
	case 'w':
	    WaitOpt = 1;
	    break;
	case 'T':
	    tsFile = (*ptr) ? ptr : av[++i];
	    break;
	default:
	    fprintf(stderr, "Unknown option: %s\n", ptr - 2);
	    exit(1);
	}
    }
    if (dataBase == NULL) {
	fprintf(stderr, "Version 1.00\n");
	fprintf(stderr, "%s [-T tsfile] [-D dbdir] [-e] [-q] dataBase[:schema]\n", av[0]);
	exit(1);
    }
    if (qry != NULL)
	QuietOpt = 1;

    if ((useSchema = strrchr(dataBase, ':')) != NULL)
	*useSchema++ = 0;

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

    if (tsFile) {
	int fd;

	if ((fd = open(tsFile, O_RDWR)) >= 0) {
	    (void)read(fd, &fts, sizeof(fts));
	    close(fd);
	}
    }

    if (fts == 0)
	fts = syncTs;
    if (QuietOpt == 0)
	printf("Database open fts=%016qx\n", fts);

    if (fts == 0) {
	fprintf(stderr, "FTS is 0, cannot start %s\n", av[0]);
	exit(1);
    }

    stdinIo = allocIo(0);

    if (qry) {
	PushCLTrans(DbI, fts, 0);
	++Level;
    }

#if 0
    if (qry) {
	int rv;

	PushCLTrans(DbI, fts, 0);
	if (EchoOpt)
	    printf("< %s\n", qry);
	rv = DoQuery(qry);
	fts = CommitQuery(NULL, fts);
	NotDone = 0;
	QuietOpt = 1;
	DisplayResult(qry, rv);
    }
#endif

    while (NotDone) {
	char *buf;
	int l;
	int type;
	int flags = 0;
	token_t t;

	if (!QuietOpt) {
	    printf("(%s:%s) ", dataBase, ((useSchema) ? useSchema : "?"));
	    fflush(stdout);
	}
	if ((buf = getSqlLine(stdinIo, (qry ? &qry : NULL), 0)) == NULL)
	    break;
	l = strlen(buf);

	type = SqlInit(&t, buf, l + 1);		/* include \0 terminator */

	if (EchoOpt)
	    printf("< %s\n", buf);

	switch(type) {
        case TOK_BEGINRO:
            flags = CPF_READONLY;
            /* fallthrough */
        case TOK_BEGIN:
	    for (;;) {
		type = SqlToken(&t);

                switch(type) {
                case TOK_SYNC:
                    flags |= CPF_RWSYNC;
                    continue;
                case TOK_READONLY:
                    flags |= CPF_READONLY;
                    continue;
                case TOK_STREAMING:
                    flags |= CPF_STREAM;
                    continue;
		case TOK_INT:
		    {
			char *str = safe_strndup(t.t_Data, t.t_Len);
			fts = strtouq(str, NULL, 0);
			if (!QuietOpt)
			    fprintf(stderr, "set FTS = 0x%016qx\n", fts);
			safe_free(&str);
		    }
		    continue;
                default:
                    break;
                }
                break;
            }
            if (!QuietOpt)
                printf("Begin Transaction flags=%04x\n", flags);
            PushCLTrans(DbI, fts, flags);
            ++Level;
            break;
	case TOK_COMMIT:
	    if (Level) {
		fts = CommitQuery(stdinIo, fts);
		--Level;
	    } else {
		printf("You are not in a transaction\n");
	    }
	    break;
	case TOK_ROLLBACK:
	    if (Level) {
		--Level;
		AbortCLTrans(DbI);
		printf("Rollback Succeeded\n");
	    } else {
		printf("You are not in a transaction\n");
	    }
	    break;
	case TOK_LOAD:
	    /*
	     * LOAD prefix {
	     *		remainder; remainder; remainder; remainder; ... }
	     */
	    {
		token_t t1 = t;
		token_t t2;
		char *prefix;
		char *rem;

		type = SqlToken(&t1);
		t2 = t1;
		while ((type & TOKF_ERROR) == 0 && type != TOK_OBRACE)
		    type = SqlToken(&t2);
		prefix = safe_strndup(t1.t_Data, t2.t_Data - t1.t_Data);
		while ((rem = getSqlLine(stdinIo, (qry ? &qry : NULL), 0)) != NULL) {
		    char *qry;

		    if (EchoOpt)
			printf("< %s\n", rem);
		    type = SqlInit(&t, rem, strlen(rem) + 1);
		    if (type == TOK_CBRACE)
			break;
		    switch(type) {
		    case TOK_BEGIN:
			if (!QuietOpt)
			    printf("Begin Transaction\n");
			PushCLTrans(DbI, fts, 0);
			++Level;
			break;
		    case TOK_BEGINRO:
			if (!QuietOpt)
			    printf("Begin Transaction\n");
			PushCLTrans(DbI, fts, CPF_READONLY);
			++Level;
			break;
		    case TOK_COMMIT:
			if (Level) {
			    fts = CommitQuery(stdinIo, fts);
			    --Level;
			} else {
			    printf("You are not in a transaction\n");
			}
			break;
		    case TOK_ROLLBACK:
			if (Level) {
			    --Level;
			    AbortCLTrans(DbI);
			    printf("Rollback Succeeded\n");
			} else {
			    printf("You are not in a transaction\n");
			}
			break;
		    default:
			safe_asprintf(&qry, "%s %s", prefix, rem);
			if (Level) {
			    int rv;

			    rv = DoQuery(qry);
			    DisplayResult(qry, rv);
			} else {
			    printf("You are not in a transaction\n");
			}
			safe_free(&qry);
		    }
		}
	    }
	    break;
	default:
	    if (Level) {
		int rv;
		rv = DoQuery(buf);
		DisplayResult(buf, rv);
	    } else {
		printf("You are not in a transaction\n");
	    }
	    break;
	}
    }
    if (qry) {
	if (Level) {
	    fts = CommitQuery(NULL, fts);
	    --Level;
	}
    }
    if (tsFile) {
	int fd;

	if ((fd = open(tsFile, O_RDWR|O_CREAT, 0666)) >= 0) {
	    (void)write(fd, &fts, sizeof(fts));
	    close(fd);
	}
    }
    CloseCLInstance(DbI);
    CloseCLDataBase(Db);
}

char *
getSqlLine(iofd_t io, char **cbuf, int to)
{
    static char *Buf;
    static int Len;
    int i;
    int sq = 0;
    int skip = 0;
    int nlerror = 0;
    int c;

    i = 0;
    for (;;) {
	if (cbuf) {
	    if ((c = (int)(unsigned char)**cbuf) == 0)
		c = -1;
	    else
		++*cbuf;
	} else {
	    c = t_getc(io, to);
	}
	if (c < 0)
	    break;
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

int
DoQuery(const char *buf)
{
    int rv;
    res_t res;

    res = QueryCLTrans(DbI, buf, &rv);
    if (res) {
	int cols = ResColumns(res);
	int count = 1;
	const char **row;
	int *len;
	char *alloc = NULL;

	fcntl(1, F_SETFL, 0);
	for (row = ResFirstRowL(res, &len); row; row = ResNextRowL(res, &len)) {
	    int i;

	    if (TabFmt == 0)
		printf("%-5d ", count);
	    for (i = 0; i < cols; ++i) {
		if (i) {
		    if (TabFmt)
			printf("\t");
		    else
			printf(", ");
		}
		if (row[i]) {
		    if (TabFmt)
			printf("%s", RemoveTabs(DBMSEscape(row[i], &alloc, len[i]), &alloc));
		    else
			printf("'%s'", DBMSEscape(row[i], &alloc, len[i]));
		} else {
		    printf("NULL");
		}
	    }
	    printf("\n");
	    ++count;
	}
	fcntl(1, F_SETFL, O_NONBLOCK);
	safe_free(&alloc);
	FreeCLRes(res);
    }
    return(rv);
}

dbstamp_t
CommitQuery(iofd_t stdinIo, dbstamp_t fts)
{
    int error;
    dbstamp_t commitTs = fts;

    if (!QuietOpt) {
	printf("Commit Transaction...");
	fflush(stdout);
    }
    if ((error = Commit1CLTrans(DbI, &commitTs)) == 0) {
	if (!QuietOpt) {
	    printf(" Ts %016qx...", commitTs);
	    fflush(stdout);
	}
	if (WaitOpt && stdinIo) {
	    char *buf;
	    printf("(hit return)");
	    fflush(stdout);
	    if ((buf = t_gets(stdinIo, 0)) != NULL)
		;
	}
	error = Commit2CLTrans(DbI, commitTs);
	fts = commitTs + 1;
    } else {
	AbortCLTrans(DbI);
    }
    if (error == 0) {
	if (!QuietOpt)
	    printf("Success\n");
    } else {
	printf("Commit Failed error %d\n", error);
    }
    return(fts);
}

const char *
RemoveTabs(const char *str, char **alloc)
{
    int i;

    for (i = 0; str[i]; ++i) {
	if (str[i] == '\t' || str[i] == ' ')
	    break;
    }
    if (str[i]) {
	char *ptr = malloc(strlen(str) + 1);
	for (i = 0; str[i]; ++i) {
	    if (str[i] == '\t' || str[i] == ' ')
		ptr[i] = '_';
	    else
		ptr[i] = str[i];
	}
	if (*alloc)
	    free(*alloc);
	*alloc = ptr;
	str = ptr;
    }
    return(str);
}

