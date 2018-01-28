/*
 * UTILS/DRD.C
 *
 * (c)Copyright 2000-2002 Backplane, Inc.  Please refer to the COPYRIGHT
 * file at the base of the distribution tree.
 *
 *	DRD [options] [command...]
 *		create	specifier
 *		destroy	specifier
 *		start	specifier
 *		stop	specifier
 *		link	host host
 *		unlink	host host
 *		status	[specifier]
 *
 * Specifier:
 *	db		Database on local host
 *	@[host]		Specific host (default to localhost)
 *	db@[host]	Database on host (default to localhost)
 */

#include "defs.h"
#include <sys/wait.h>
#include <sys/stat.h>
#include <ctype.h>

char *TemplateStr;
iofd_t StdinIo;

static void CheckTemplate(void);
static int Verify(const char *msg);
static void DoCmd(char **av, int ac);
static void DecodeSpecifier(char *spec, char **pdb, char **phost);
static void DoCmdCreate(char **av, int ac, char *dbName, char *hostName);
static void DoCmdDestroy(char **av, int ac, char *dbName, char *hostName);
static void DoCmdStart(char **av, int ac, char *dbName, char *hostName);
static void DoCmdStop(char **av, int ac, char *dbName, char *hostName);
static void DoCmdLink(char **av, int ac, char *dbName, char *hostName);
static void DoCmdUnLink(char **av, int ac, char *dbName, char *hostName);
static void DoCmdSetPeer(char **av, int ac, char *dbName, char *hostName);
static void DoCmdSetSnap(char **av, int ac, char *dbName, char *hostName);
static void DoCmdStatus(char **av, int ac, char *dbName, char *hostName);
static int runcmd(const char *arg0, ...);

void
task_main(int ac, char **av)
{
    int i;

    StdinIo = allocIo(0);

    if ((TemplateStr = getenv("RDBMS_TEMPLATE")) == NULL)
	TemplateStr = "drdbms_%s";

    for (i = 1; i < ac; ++i) {
	char *ptr = av[i];

	if (*ptr != '-')
	    break;
	ptr += 2;
	switch(ptr[-1]) {
	case 'q':
	    DebugOpt = 0;
	    break;
	case 'v':
	    DebugOpt = -1;
	    break;
	case 'T':
	    ptr = (*ptr ? ptr : av[++i]);
	    if (ptr == NULL) {
		dberror("Missing parameter to -D option\n");
	    } else {
		TemplateStr = ptr;
	    }
	    break;
	default:
	    dberror("Unrecognized option: %s\n", ptr - 2);
	    i = ac;	/* force fallthrough below */
	}
    }

    CheckTemplate();

    if (i < ac) {
	DoCmd(av + i, ac - i);
    } else if (i == ac) {
	char *cmd;

	printf("DRD> ");
	fflush(stdout);

	while ((cmd = t_gets(StdinIo, 0)) != NULL) {
	    char *str = cmd;
	    char *arg;

	    if ((arg = strsep(&str, " \t\r\n")) != NULL) {
		if (arg[0] != '#') {
		    do {
			av = safe_malloc(sizeof(char *));
			ac = 0;
			av[ac] = arg;
			++ac;
			av = safe_realloc(av, sizeof(char *) * (ac + 1));
		    } while ((arg = strsep(&str, " \t\r\n")) != NULL);
		    av[ac] = NULL;
		    DoCmd(av, ac);
		    free(av);
		}
	    }
	    free(cmd);
	    printf("DRD> ");
	    fflush(stdout);
	}
    }
}

static void
DoCmd(char **av, int ac)
{
    char *dbName = NULL;
    char *hostName = NULL;

    if (ac > 1)
	DecodeSpecifier(av[1], &dbName, &hostName);
    else
	DecodeSpecifier("", &dbName, &hostName);

    if (strcasecmp(av[0], "create") == 0) {
	DoCmdCreate(av, ac, dbName, hostName);
    } else if (strcasecmp(av[0], "destroy") == 0) {
	DoCmdDestroy(av, ac, dbName, hostName);
    } else if (strcasecmp(av[0], "start") == 0) {
	DoCmdStart(av, ac, dbName, hostName);
    } else if (strcasecmp(av[0], "stop") == 0) {
	DoCmdStop(av, ac, dbName, hostName);
    } else if (strcasecmp(av[0], "link") == 0) {
	DoCmdLink(av, ac, dbName, hostName);
    } else if (strcasecmp(av[0], "unlink") == 0) {
	DoCmdUnLink(av, ac, dbName, hostName);
    } else if (strcasecmp(av[0], "setpeer") == 0) {
	DoCmdSetPeer(av, ac, dbName, hostName);
    } else if (strcasecmp(av[0], "setsnap") == 0) {
	DoCmdSetSnap(av, ac, dbName, hostName);
    } else if (strcasecmp(av[0], "status") == 0) {
	DoCmdStatus(av, ac, dbName, hostName);
    } else if (strcasecmp(av[0], "?") == 0 || strcasecmp(av[0], "h") == 0) {
	printf(
	    "create @host\n"
	    "destroy @host\n"
	    "start @host\n"
	    "stop @host\n"
	    "link @host1 @host2\n"
	    "unlink @host1 @host2\n"
	    "\n"
	    "create db@host\n"
	    "destroy db@host\n"
	    "start db@host\n"
	    "stop db@host\n"
	    "setpeer db@host\n"
	    "setsnap db@host\n"
	    "status\n"
	    "\n"
	    "spec:  db, db@host, or @host\n"
	);
    } else {
	dberror("Unrecognized command: %s\n", av[0]);
    }
    safe_free(&dbName);
    safe_free(&hostName);
}

static void
DecodeSpecifier(char *spec, char **pdb, char **phost)
{
    char *db;
    char *host;
    static char MyHostName[MAXHOSTNAMELEN];

    db = spec;
    if ((host = strchr(spec, '@')) == NULL)
	host = "";
    else
	*host++ = 0;

    /*
     * If empty host then host is hostname
     */
    if (*host == 0) {
	if (MyHostName[0] == 0)
	    gethostname(MyHostName, sizeof(MyHostName) - 1);
	host = MyHostName;
    }
    *pdb = strdup(db);
    *phost = strdup(host);
}

static void 
CheckTemplate(void)
{
    const char *s;

    if ((s = strchr(TemplateStr, '%')) != NULL) {
	if (s[1] != 's')
	    fatal("Bad template, only %%s allowed");
	while ((s = strchr(s + 2, '%')) != NULL && s[1] == '%')
	    ;
	if (s)
	    fatal("Bad template, only one %%s allowed");
    } else {
	fatal("Bad template, missing %%s");
    }
}

static int
Verify(const char *msg)
{
    int r = -1;

    for (;;) {
	char *buf;

	printf("%s, Are you sure? ", msg);
	fflush(stdout);
	if ((buf = t_gets(StdinIo, 0)) == NULL)
	    break;
	if (strcasecmp(buf, "y") == 0 || strcasecmp(buf, "yes") == 0) {
	    r = 0;
	    break;
	}
	if (strcasecmp(buf, "n") == 0 || strcasecmp(buf, "no") == 0) {
	    break;
	}
	printf("yes or no please\n");
    }
    return(r);
}

static void
DoCmdCreate(char **av, int ac, char *dbName, char *hostName)
{
    char *tdir;
    struct stat st;

    asprintf(&tdir, TemplateStr, hostName);
    if (stat(tdir, &st) < 0) {
	if (mkdir(tdir, 0775) < 0)
	    dberrorsys("mkdir failed on %s", tdir);
    } else {
	dberror("directory %s already exists\n", tdir);
    }
    free(tdir);
}

static void
DoCmdDestroy(char **av, int ac, char *dbName, char *hostName)
{
    char *tdir;
    struct stat st;

    asprintf(&tdir, TemplateStr, hostName);
    if (stat(tdir, &st) < 0) {
	dberror("could not find directory %s\n", tdir);
    } else {
	if (Verify("Destroy all databases on host") == 0) {
	    dberror("XXX (not implemented)\n");
	} else {
	    dberror("Operation aborted\n");
	}
    }
    free(tdir);
}

static void
DoCmdStart(char **av, int ac, char *dbName, char *hostName)
{
    char *tdir;
    char *linkCmd;

    asprintf(&tdir, TemplateStr, hostName);
    asprintf(&linkCmd, "drd_link -D %s", TemplateStr);
    runcmd(
	"replicator",
	"-v",
	"-D", tdir,
	"-V", hostName,
	"-L", linkCmd,
	(dbName[0] ? "-b" : NULL), 
	(dbName[0] ? dbName : NULL), 
	NULL
    );
    free(linkCmd);
    free(tdir);
}

static void
DoCmdStop(char **av, int ac, char *dbName, char *hostName)
{
    char *tdir;
    char *linkCmd;

    asprintf(&tdir, TemplateStr, hostName);
    asprintf(&linkCmd, "drd_link -D %s", TemplateStr);
    runcmd(
	"replicator",
	"-v",
	"-D", tdir,
	"-V", hostName,
	"-L", linkCmd,
	(dbName[0] ? "-e" : "-S"), 
	(dbName[0] ? dbName : NULL), 
	NULL
    );
    free(linkCmd);
    free(tdir);
}

static void
DoCmdLink(char **av, int ac, char *dbName, char *hostName)
{
    char *tdir;
    char *linkCmd;
    char *host2 = av[2];

    if (av[2] == NULL) {
	dberror("command format error\n");
	return;
    }
    asprintf(&tdir, TemplateStr, hostName);
    asprintf(&linkCmd, "drd_link -D %s", TemplateStr);
    runcmd(
	"replicator",
	"-v",
	"-D", tdir,
	"-V", hostName,
	"-L", linkCmd,
	"-l", host2,
	NULL
    );
    free(linkCmd);
    free(tdir);
}

static void
DoCmdUnLink(char **av, int ac, char *dbName, char *hostName)
{
    char *tdir;
    char *linkCmd;
    char *host2 = av[2];

    if (av[2] == NULL) {
	dberror("command format error\n");
	return;
    }

    asprintf(&tdir, TemplateStr, hostName);
    asprintf(&linkCmd, "drd_link -D %s", TemplateStr);
    runcmd(
	"replicator",
	"-v",
	"-D", tdir,
	"-V", hostName,
	"-L", linkCmd,
	"-u", host2,
	NULL
    );
    free(linkCmd);
    free(tdir);
}
void 
DoCmdSetPeer(char **av, int ac, char *dbName, char *hostName)
{
    int error;
    char *tdir;
    database_t db = NULL;
    database_t dbi = NULL;
    dbstamp_t syncTs;

    asprintf(&tdir, TemplateStr, hostName);
    SetDefaultDBDir(tdir);

    if (dbName[0] == 0) {
	dberror("setpeer: Must specify full dbname@host\n");
	return;
    }

    if ((db = OpenCLDataBase(dbName, &error)) == NULL) {
	dberror("Unable to open %s@%s error %d\n", dbName, tdir, error);
    } else if ((dbi = OpenCLInstance(db, &syncTs, CLTYPE_RW)) == NULL) {
	dberror("Unable to openi %s@%s\n", dbName, tdir);
    } else {
	int status;

	BEGIN(dbi, syncTs, status) {
	    UPDATE sys.repgroup=g SET HostType = 'PEER' WHERE g.HostName = hostName;
	    if (RESULT <= 0) {
		int i;
		char *idStr = NULL;

		for (i = 1; i < 256; ++i) {
		    int count = 0;

		    if (idStr)
			free(idStr);

		    asprintf(&idStr, "%d", i);
		    SELECT g.HostName FROM sys.repgroup=g 
			WHERE g.HostId = idStr; 
		    {
			++count;
		    }
		    if (count == 0)
			break;
		}
		if (i == 256) {
		    dberror("Ran out of host identifiers!  Max 255!\n");
		    ROLLBACK;
		} else {
		    INSERT INTO sys.repgroup ( HostName, HostId, HostType )
			VALUES ( hostName, idStr, 'PEER' );
		    if (RESULT < 0) {
			dberror("Unable to insert record\n");
			ROLLBACK;
		    }
		}
		if (idStr)
		    free(idStr);
	    }
	}
	printf("Commit Status %d\n", status);
    }
    if (dbi)
	CloseCLInstance(dbi);
    if (db)
	CloseCLDataBase(db);
    free(tdir);
}

void 
DoCmdSetSnap(char **av, int ac, char *dbName, char *hostName)
{
#if 0
    char *tdir;
    database_t db;
    database_t dbi;

    asprintf(&tdir, TemplateStr, hostName);
    SetDefaultDBDir(tdir);
    free(tdir);
#endif
}

static void
DoCmdStatus(char **av, int ac, char *dbName, char *hostName)
{
    printf("(status not implemented)\n");
}

static int
runcmd(const char *arg0, ...)
{
    pid_t pid;

    if ((pid = t_fork()) == 0) {
	execvp(arg0, (char **)&arg0);
	_exit(1);
    } else if (pid < 0) {
	dberrorsys("fork failed");
	return(-1);
    } else {
	waitpid(pid, NULL, 0);
	return(0);
    }
}

