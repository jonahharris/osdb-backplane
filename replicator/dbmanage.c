/*
 * DBMANAGE.C
 *
 * (c)Copyright 2000-2002 Backplane, Inc.  Please refer to the COPYRIGHT
 * file at the base of the distribution tree.
 *
 * $Backplane: rdbms/replicator/dbmanage.c,v 1.5 2002/08/20 22:06:00 dillon Exp $
 */

#include "defs.h"

Export DBManageNode *FindDBManageNode(const char *dbname);
Export DBManageNode *CreateDBManageNode(const char *dbname);

static void DBManageClientEOFThread(DBManageNode *dbm);

static SimpleHash	DBMHash;
static int		DBMHashInit;

DBManageNode *
FindDBManageNode(const char *dbname)
{
    if (DBMHashInit == 0) {
	simpleHashInit(&DBMHash);
	DBMHashInit = 1;
    }
    return(simpleHashLookup(&DBMHash, dbname));
}

DBManageNode *
CreateDBManageNode(const char *dbname)
{
    pid_t pid;
    int cfds[2];
    int xfds[2];
    DBManageNode *dbm;

    if (DBMHashInit == 0) {
	simpleHashInit(&DBMHash);
	DBMHashInit = 1;
    }
    if (socketpair(PF_LOCAL, SOCK_STREAM, 0, cfds) < 0)
	return(NULL);
    if (socketpair(PF_LOCAL, SOCK_STREAM, 0, xfds) < 0) {
	close(cfds[0]);
	close(cfds[1]);
	return(NULL);
    }

    if ((pid = t_fork()) < 0) {
	close(cfds[0]);
	close(cfds[1]);
	close(xfds[0]);
	close(xfds[1]);
	return(NULL);
    }

    /*
     * CHILD
     */
    if (pid == 0) {
	char *debugBuf;
	char *vhostBuf;
	int ok = 1;

	safe_asprintf(&debugBuf, "-d%d", DebugOpt);
	if (VirtualHostName)
	    safe_asprintf(&vhostBuf, "-V%s", VirtualHostName);
	else
	    safe_asprintf(&vhostBuf, "-n");

	close(cfds[0]);
	close(xfds[0]);

	/*
	 * Client fd must be 3
	 * Control fd must be 4
	 */
	if (xfds[1] == 3) {
	    if ((xfds[1] = dup(xfds[1])) < 0)
		ok = 0;
	    close(3);
	}
	if (cfds[1] != 3) {
	    if (dup2(cfds[1], 3) < 0)
		ok = 0;
	    close(cfds[1]);
	}
	if (xfds[1] != 4) {
	    if (dup2(xfds[1], 4) < 0)
		ok = 0;
	    close(xfds[1]);
	}
	if (ok == 0) {
	    perror("dup() during replicator exec");
	    _exit(1);
	}
	fcntl(3, F_SETFD, 0);
	fcntl(4, F_SETFD, 0);

	/*
	 * Use EXEC to clean up all the garbage the master proc may have
	 * built up.
	 */
	execlp(Arg0, Arg0, debugBuf, "-x", "-D", DefaultDBDir(), vhostBuf, dbname, NULL);
	_exit(1);
    }

    /*
     * PARENT
     */
    dbm = zalloc(sizeof(DBManageNode));
    dbm->dm_DBName = safe_strdup(dbname);
    simpleHashEnter(&DBMHash, dbname, dbm);
    dbm->dm_Pid = pid;
    dbm->dm_ClientIo = allocIo(cfds[0]);
    dbm->dm_ControlIo = allocIo(xfds[0]);
    close(cfds[1]);
    close(xfds[1]);
    taskCreate(DBManageClientEOFThread, dbm);
    return(dbm);
}

static
void
DBManageClientEOFThread(DBManageNode *dbm)
{
    char c;
    t_read(dbm->dm_ClientIo, &c, 1, 0);

    simpleHashRemove(&DBMHash, dbm->dm_DBName);
    closeIo(dbm->dm_ClientIo);
    closeIo(dbm->dm_ControlIo);
    safe_free(&dbm->dm_DBName);
    while (waitpid(dbm->dm_Pid, NULL, 0) < 0)
	;
    zfree(dbm, sizeof(DBManageNode));
}

