/*
 * REPLICATOR/DCREATEDB.C	- Create new database
 *
 * (c)Copyright 2000-2002 Backplane, Inc.  Please refer to the COPYRIGHT
 * file at the base of the distribution tree.
 *
 * $Backplane: rdbms/utils/dcreatedb.c,v 1.12 2003/04/02 17:34:16 dillon Exp $
 *
 * DCREATEDB [-D dbdir] database
 */

#include "defs.h"
#include "libdbcore/dbcore-protos.h"
#include <sys/wait.h>
#include <sys/time.h>
#include <sys/resource.h>
#include <ctype.h>
#include <pwd.h>

DataBase *Db;

int
task_main(int ac, char **av)
{
    int i;
    int error = 0;
    char *dataBase = NULL;
    const char *dbDir = DefaultDBDir();
    const char *path;
    DBCreateOptions opts;
    int fd;
    pid_t pid;
    char hostBuf[256];
    char *hostName;
    char *vname = NULL;

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
	case 'v':
	    DebugOpt = 1;
	    break;
	case 'V':
	    vname = (*ptr) ? ptr : av[++i];
	    break;
	case 'D':
	    dbDir = (*ptr) ? ptr : av[++i];
	    break;
	case 'B':
	    ptr = (*ptr) ? ptr : av[++i];

	    opts.c_Flags |= DBC_OPT_BLKSIZE;
	    opts.c_BlockSize = strtol(ptr, &ptr, 0);

	    switch(*ptr) {
	    case 0:
		fatal("Must specify 'k'ilobytes or 'm'egabytes");
		/* not reached */
		break;
	    case 'k':
	    case 'K':
		opts.c_BlockSize *= 1024;
		break;
	    case 'm':
	    case 'M':
		opts.c_BlockSize *= 1024 * 1024;
		break;
	    default:
		fatal("Unknown extension to blocksize option: %s", ptr);
		/* not reached */
	    }
	    if (opts.c_BlockSize < MIN_BLOCKSIZE)
		fatal("Minimum blocksize is %dK", MIN_BLOCKSIZE / 1024);
	    if (opts.c_BlockSize > MAX_BLOCKSIZE)
		fatal("Maximum blocksize is %dK", MAX_BLOCKSIZE / 1024);
	    if ((opts.c_BlockSize ^ (opts.c_BlockSize - 1)) != opts.c_BlockSize * 2 - 1)
		fatal("blocksize must be a power of 2");
	    break;
	default:
	    fprintf(stderr, "Unknown option: %s\n", ptr - 2);
	    exit(1);
	}
    }
    if (i > ac)
	fatal("last option required argument");

    if (dataBase == NULL) {
	fprintf(stderr, "Version 1.01\n");
	fprintf(stderr, "%s [-D dbbasedir] [-v] [-B blksize[k/m]] [-V vhost] database\n", av[0]);
	fprintf(stderr, "WARNING! the -V virtual host must match the -V option you give\n"
			"to the replicator that this database operates under.  You may have\n"
			"to use llquery to change sys.repgroups if you move the physical\n"
			"data to another host\n");
	exit(1);
    }
    if (dataBase[0] == '/')
	dbDir = NULL;

    opts.c_Flags |= DBC_OPT_TIMESTAMP;
    opts.c_TimeStamp = timetodbstamp(time(NULL));

    if ((Db = OpenDatabase(dbDir, dataBase, 0, NULL, &error)) != NULL) {
	CloseDatabase(Db, 1);
	fatal("dcreatedb: Database %s already exists", dataBase);
    }

    Db = OpenDatabase(dbDir, dataBase, DBF_EXCLUSIVE|DBF_CREATE, &opts, &error);

    if (Db == NULL) {
	fatal("OpenDatabase: error %d", error);
    }
    CloseDatabase(Db, 1);

    if (error)
	fatal("Unable to create database error %d", error);

    /*
     * Calculate the official host name
     */
    bzero(hostBuf, sizeof(hostBuf));
    gethostname(hostBuf, sizeof(hostBuf) - 1);

    if (vname == NULL) {
        struct passwd *pw;
        pw = getpwuid(getuid());
        if (pw == NULL)
            fatalsys("getpwuid: failed");
        vname = strdup(pw->pw_name);
        endpwent();
    }
    safe_asprintf(&hostName, "%s.%s", hostBuf, vname);

    /*
     * Run llquery to initialize the database, passing the official hostname
     * as the only PEER.
     */
    path = BACKPLANE_BASE "etc/initdb.llq";
    fd = open(path, O_RDONLY);
    if (fd < 0)
	fatal("Unable to access %s", path);
    if ((pid = t_fork()) == 0) {
	dup2(fd, 0);
	close(fd);
	execl(BACKPLANE_BASE "bin/llquery", "llquery", "-q", "-e", "-D", DefaultDBDir(), "-P",
	    hostName, dataBase, NULL);
	_exit(1);
    }
    close(fd);
    if (pid < 0) {
	error = -1;
	fatal("Unable to fork to create database");
    } else {
	int status;

	waitpid(pid, &status, 0);
	if (WEXITSTATUS(status))
	    fatal("Unable to initialize database!");
    }
    return(0);
}

