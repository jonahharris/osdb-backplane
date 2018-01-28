/*
 * REPLICATOR/SYSCTL.C	- Execute replicator system control function
 *
 * (c)Copyright 2000-2002 Backplane, Inc.  Please refer to the COPYRIGHT
 * file at the base of the distribution tree.
 *
 * $Backplane: rdbms/replicator/sysctl.c,v 1.43 2003/05/09 05:57:37 dillon Exp $
 *
 */

#include "defs.h"

Prototype int SysControl(const char *dbName, const char *arg, rp_cmd_t rpcmd, char **emsg);
Prototype int StartReplicationLink(const char *dbName, const char *linkCmd, char **emsg);
Prototype void SysStartStop(const char *dbName, const char *arg, rp_cmd_t rpcmd, char **emsg, int *error);

static DBInfo *ExecDRDDatabase(const char *dbName, int *perror, char **emsg);
static void ReplicationLinkRestarter(LinkMaintain *lm);
static int StartLinkCommand(LinkInfo *l);
static int StopReplicationLink(const char *dbName, const char *linkCmd, char **emsg);
#if 0
static int StartDatabase(const char *dbName, char **emsg);
static int StopDatabase(const char *dbName, char **emsg);
#endif
static int CreateNewDb(const char *dbName, char **emsg);
static int CreatePeerDb(const char *dbName, char **emsg);
static int CreateSnapDb(const char *dbName, char **emsg);
static int DestroyDb(const char *dbName, char **emsg);
static int DowngradeDb(const char *dbName, char **emsg);
static int UpgradeDb(const char *dbName, char **emsg);
static int ValidateDBName(const char *dbName, char **emsg);

/*
 * SysControl() -	Misc.  System control commands
 *
 *	This routine implements:
 *
 *	- starting and stopping databases
 *	- adding and removing replication spanning tree linkages
 *
 *		XXX synchronous
 *
 *		XXX does not return error status / message to
 *		the client that made the request.
 */

int
SysControl(const char *dbName, const char *arg, rp_cmd_t rpcmd, char **emsg)
{
    int error = 0;

    *emsg = NULL;

    switch(rpcmd) {
    case RPCMD_SETOPT:
	{
	    char *file = NULL;
	    DebugOpt = arg ? strtol(arg, &file, 0) : 0;
	    if (file && *file == ':')
		dbsetfile(file + 1);
	    else
		dbsetfile(NULL);
	}
	break;
    case RPCMD_INFO:
	GenStatus(arg, emsg);
	break;
    case RPCMD_START_REPLICATOR:
	/* (replicator is inherently started) */
	break;
    case RPCMD_START:
    case RPCMD_STOP:
    case RPCMD_INITIAL_START:
	if ((error = ValidateDBName(dbName, emsg)) == 0) {
	    SysStartStop(dbName, arg, rpcmd, emsg, &error);
	    if (rpcmd != RPCMD_INITIAL_START)
		SaveLinkEnableFile(dbName);
	}
	break;
    case RPCMD_ADDLINK:
	/*
	 * add replication link to dbName (which is actually a hostname)
	 *
	 * XXX make sure it's a simple name, not something that can
	 * mess up the link execution command template (or create a security
	 * hole).
	 */
	if ((error = ValidateDBName(dbName, emsg)) == 0)
	    error = StartReplicationLink(dbName, arg, emsg);
	break;
    case RPCMD_REMLINK:
	/*
	 * remove replication link to dbName (which is actually a hostname)
	 */
	if ((error = ValidateDBName(dbName, emsg)) == 0)
	    error = StopReplicationLink(dbName, arg, emsg);
	break;
    case RPCMD_CREATEDB:
	if ((error = ValidateDBName(dbName, emsg)) == 0)
	    error = CreateNewDb(dbName, emsg);
	break;
    case RPCMD_CREATEPEER:
	if ((error = ValidateDBName(dbName, emsg)) == 0)
	    error = CreatePeerDb(dbName, emsg);
	break;
    case RPCMD_CREATESNAP:
	if ((error = ValidateDBName(dbName, emsg)) == 0)
	    error = CreateSnapDb(dbName, emsg);
	break;
    case RPCMD_DESTROYDB:
	if ((error = ValidateDBName(dbName, emsg)) == 0)
	    error = DestroyDb(dbName, emsg);
	break;
    case RPCMD_UPGRADEDB:
	if ((error = ValidateDBName(dbName, emsg)) == 0)
	    error = UpgradeDb(dbName, emsg);
	break;
    case RPCMD_DOWNGRADEDB:
	if ((error = ValidateDBName(dbName, emsg)) == 0)
	    error = DowngradeDb(dbName, emsg);
	break;
    default:
	break;
    }
    return(error);
}

void
SysStartStop(const char *dbName, const char *arg, rp_cmd_t rpcmd, char **emsg, int *error)
{
    LinkEnable *le;
    DBInfo *d;

    if ((le = FindLinkEnable(dbName)) == NULL)
	le = AllocLinkEnable(dbName);
    if (rpcmd == RPCMD_STOP)
	le->e_Count = 0;
    else
	le->e_Count = 2;
    sscanf(arg, "%i:%i:%i", &le->e_Count, &le->e_Pri, &le->e_Flags);

    d = FindDBInfoIfLocal(dbName, 1, error);
    if (le->e_Count > 0) {
	if (d != NULL) {
	    safe_asprintf(emsg, "Database %s already started", dbName);
	    dbwarning("DATABASE ALREADY STARTED\n");
	    *error = -1;
	} else {
	    safe_asprintf(emsg, "Starting Database %s", dbName);
	    dbinfo2("STARTING DATABASE %s\n", dbName);
	    if ((d = ExecDRDDatabase(dbName, error, emsg)) != NULL)
		StartSynchronizerThread(d);
	}
    } else {
	if (d != NULL) {
	    if (d->d_Flags & DF_NOTIFIED) {
		dbwarning("SHUTDOWN: SHUTDOWN ALREADY IN PROGRESS FOR %s\n", dbName);
		safe_asprintf(emsg, "Shutdown already in progress for %s", dbName);
		*error = -1;
	    } else {
		d->d_Flags |= DF_NOTIFIED;
		StopSynchronizerThread(d);
		safe_asprintf(emsg, "Starting Shutdown of Database %s", dbName);
		dbinfo2("SHUTDOWN: STARTING SHUTDOWN OF DATABASE %s\n", dbName);
	    }
	} else {
	    if (rpcmd != RPCMD_STOP) {
		safe_asprintf(emsg, "Database %s administratively disabled", dbName);
		dbinfo2("STARTING DATABASE %s: NOT STARTED, ADMINISTRATIVELY DISABLED\n", dbName);
	    } else {
		safe_asprintf(emsg, "Shutdown Database %s: not currently running", dbName);
		*error = -1;
		dbwarning("SHUTDOWN: DATABASE %s NOT RUNNING\n", dbName);
	    }
	}
    }
    if (d)
	DoneDBInfo(d);
}

/*
 * Returns DBInfo with its USE count bumped, or NULL
 */
static DBInfo *
ExecDRDDatabase(const char *dbName, int *perror, char **emsg)
{
    CLDataBase *cd;
    DataBase *db;
    pid_t pid;
    dbstamp_t createTs;
    int fds[2];
    char fopt[8];

    /*
     * Make sure we have access to the database, creating it
     * if necessary.
     */
    db = OpenDatabase(DefaultDBDir(), dbName, 0, NULL, perror);
    if (db == NULL) {
	safe_asprintf(emsg, "Unable to locate/access database %s", dbName);
	return(NULL);
    }
    createTs = GetDBCreateTs(db);
    CloseDatabase(db, 1);

    /*
     * Get a communications socket
     */
    if (socketpair(PF_LOCAL, SOCK_STREAM, 0, fds) < 0) {
	*perror = -1;
	safe_asprintf(emsg, "socketpair() syscall failed: %s", strerror(errno));
	return(NULL);
    }

    /*
     * Fork, check for fork failure
     */
    if ((pid = t_fork()) < 0) {
	close(fds[0]);
	close(fds[1]);
	*perror = -1;
	safe_asprintf(emsg, "fork() syscall failed: %s", strerror(errno));
	return(NULL);
    }

    /*
     * Success (parent), return database handle
     */
    if (pid > 0) {
	char *dbExtName;
	DBInfo *d;

	safe_asprintf(&dbExtName, "%s.%016qx", dbName, createTs);
	d = AllocDBInfo(dbExtName);
	cd = AllocCLDataBase(NULL, allocIo(fds[0]));
	removeNode(&cd->cd_Node);
	addTail(&d->d_CDList, &cd->cd_Node);
	++d->d_CDCount;
	d->d_Flags |= DF_LOCALRUNNING;
	safe_free(&dbExtName);
	cd->cd_Pid = pid;
	cd->cd_CreateStamp = createTs;
	close(fds[1]);
	return(d);
    }

    /*
     * Success (child), exec drd_database.  All our other descriptors will
     * be closed on exec.
     */

    close(fds[0]);
    snprintf(fopt, sizeof(fopt), "%d", fds[1]);

    if (Arg0Path[0]) {
	char *arg0;

	if (asprintf(&arg0, "%sdrd_database", Arg0Path) < 0)
	    _exit(1);
	execl(arg0, "drd_database", "-D", DefaultDBDir(), "-f", fopt, dbName, NULL);
    } else {
	execlp("drd_database", "drd_database", "-D", DefaultDBDir(), "-f", fopt, dbName, NULL);
    }
    _exit(1);
    return(NULL);	/* NOT REACHED */
}

/*
 * StartReplicationLink() -	Create a link to the destination.
 */

int
StartReplicationLink(const char *dbName, const char *linkCmd, char **emsg)
{
    LinkMaintain *lm;
    int error = 0;

    if ((lm = FindLinkMaintain(dbName, linkCmd)) != NULL) {
	if (lm->m_Flags & LMF_STOPME) {
	    lm->m_Flags &= ~LMF_STOPME;
	    SaveLinkMaintainFile(dbName);
	    safe_asprintf(emsg, "Restarting Link: \"%s\"", linkCmd);
	} else {
	    safe_asprintf(emsg, "Link Already Running: \"%s\"", linkCmd);
	    error = -1;
	}
    } else {
	lm = AllocLinkMaintain(dbName, linkCmd);
	SaveLinkMaintainFile(dbName);
	taskCreate(ReplicationLinkRestarter, lm);
	safe_asprintf(emsg, "Starting Link: \"%s\"", linkCmd);
    }
    return(error);
}

static void
ReplicationLinkRestarter(LinkMaintain *lm)
{
    dbinfo2("LMAINT %p start link maintain\n", lm);
    while ((lm->m_Flags & LMF_STOPME) == 0) {
	LinkInfo *l;

	l = AllocLinkInfo(lm->m_LinkCmd, CTYPE_UNKNOWN, NULL);
	l->l_DBName = safe_strdup(lm->m_DBName);
	lm->m_LinkInfo = l;
	++l->l_Refs;		/* so replication thread does not delete it */
	SetLinkStatus(l, "Starting up");
	if (StartLinkCommand(l) == 0) {
	    /*
	     * Send routing packet so remote replicator can route the
	     * descriptor to the right process.
	     */
	    CLAnyMsg *msg;

	    msg = BuildCLHelloMsgStr(l->l_DBName);
	    WriteCLMsg(l->l_Iow, msg, 1);
	    /* msg automatically freed */

	    ReplicationReaderThread(l);
	}
	lm->m_LinkInfo = NULL;
	FreeLinkInfo(l);
	taskSleep(10000);
    }
    FreeLinkMaintain(lm);
    dbinfo2("LMAINT %p stop link maintain\n", lm);
}

static int
StartLinkCommand(LinkInfo *l)
{
    int fds[2];
    const char *id;
    char *cmd;
    char *arg0;
    char *av[64];
    int ac;

    if (socketpair(PF_LOCAL, SOCK_STREAM, 0, fds) < 0)
	fatalsys("socketpair() failed while creating replication link");

    id = l->l_LinkCmdName;
    if (strchr(id, '@') != NULL) {
	safe_asprintf(&cmd, "ssh -x -e none -a %s " BACKPLANE_BASE "bin/drd_link", id);
    } else {
	safe_asprintf(&cmd, BACKPLANE_BASE "bin/drd_link -D %s", id);
    }

    if (l->l_Ior) {
	closeIo(l->l_Ior);
	l->l_Ior = NULL;
    }
    if (l->l_Iow) {
	closeIo(l->l_Iow);
	l->l_Iow = NULL;
    }

    fflush(stdout);
    fflush(stderr);

    /*
     * Fork, deal with fork error
     */
    if ((l->l_Pid = t_fork()) < 0) {
	free(cmd);
	close(fds[0]);
	close(fds[1]);
	return(-1);
    }

    /*
     * Success (parent)
     */
    if (l->l_Pid > 0) {
	free(cmd);
	close(fds[1]);
	l->l_Ior = allocIo(fds[0]);
	l->l_Iow = dupIo(l->l_Ior);
	return(0);
    }

    /*
     * Success (child0s)
     */

    arg0 = strtok(cmd, " \t\r\n");
    if (arg0 == NULL)
	_exit(1);

    av[0] = arg0;
    for (ac = 1; ac < arysize(av) - 3; ++ac) {
	av[ac] = strtok(NULL, " \t\r\n");
	if (av[ac] == NULL)
	    break;
    }
#if 0
    asprintf(&av[ac++], "-D %s", BaseDir);
#endif
    av[ac] = NULL;

    dup2(fds[1], 0);
    dup2(fds[1], 1);
    close(fds[0]);
    close(fds[1]);

    if (strchr(arg0, '/') != NULL)
	execv(arg0, av);
    else
	execvp(arg0, av);
    _exit(1);
    return(0);	/* NOT REACHED */
}

static int
StopReplicationLink(const char *dbName, const char *linkCmd, char **emsg)
{
    LinkMaintain *lm;
    int error = 0;

    if ((lm = FindLinkMaintain(dbName, linkCmd)) != NULL) {
	LinkInfo *l;

	lm->m_Flags |= LMF_STOPME;
	SaveLinkMaintainFile(dbName);
	if ((l = lm->m_LinkInfo) != NULL && l->l_Ior)
	    t_shutdown(l->l_Ior, SHUT_RD);
	safe_asprintf(emsg, "Stopping Link: \"%s\"", linkCmd);
    } else {
	safe_asprintf(emsg, "Link Not Running: \"%s\"", linkCmd);
	error = -1;
    }
    return(error);
}

/*
 * CreateNewDb() - create a new database.  The database must not exist
 *		   anywhere (here or on any other replicator).
 *
 *		   We become the sole peer for the new database.
 */
static int
CreateNewDb(const char *dbName, char **emsg)
{
    int error = 0;
    int fd = -1;
    DBInfo *dbi;
    DataBase *db;
    dbstamp_t createTs = timetodbstamp(time(NULL));

    if (strchr(dbName, ' ') || strchr(dbName, '\t') || strchr(dbName, '-')) {
	safe_asprintf(emsg, "Illegal database name");
	return(-1);
    }

    /*
     * The database should not already exist locally
     */
    if (error == 0) {
	if ((db = OpenDatabase(DefaultDBDir(), dbName, 0, NULL, &error)) != NULL) {
	    error = -1;
	    safe_asprintf(emsg, "Cannot create, %s already exists on %s",
		dbName,
		MyHost.h_HostName
	    );
	    CloseDatabase(db, 1);
	} else {
	    error = 0;
	}
    }

    /*
     * The database cannot exist in the spanning tree anywhere
     */
    dbi = FindDBInfo(dbName, 1, &error);
    if ((dbi && dbi->d_Refs) || error) {
	error = -1;
	if (dbi && dbi->d_Refs) {
	    safe_asprintf(emsg, "Cannot create, %s is already in the spanning tree!", dbName);
	} else {
	    safe_asprintf(emsg, "Cannot create, %s is already in the spanning tree!\nWarning, spanning tree corrupt, unassociated duplicates of %s found!", dbName, dbName);
	}
    }
    if (dbi)
	DoneDBInfo(dbi);

    /*
     * Create the database locally using the specified create timestamp.
     */
    if (error == 0) {
	DBCreateOptions opts;

	bzero(&opts, sizeof(opts));
	opts.c_Flags = DBC_OPT_TIMESTAMP;
	opts.c_TimeStamp = createTs;
	db = OpenDatabase(DefaultDBDir(), dbName, DBF_CREATE, &opts, &error);
	if (db == NULL) {
	    safe_asprintf(emsg, "Create of %s failed on %s, error %d",
		dbName,
		MyHost.h_HostName,
		error
	    );
	    error = -1;
	} else {
	    CloseDatabase(db, 1);
	}
    }

    /*
     * If this is a brand new database, initialize it using llquery and the
     * initdb.llq script file.
     */
    if (error == 0) {
	const char *path = BACKPLANE_BASE "etc/initdb.llq";
	fd = open(path, O_RDONLY);
	if (fd < 0) {
	    error = -1;
	    safe_asprintf(emsg, "Unable to open %s", path);
	}
    }
    if (error == 0) {
	pid_t pid;

	if ((pid = t_fork()) == 0) {
	    dup2(fd, 0);
	    close(fd);
	    execl(BACKPLANE_BASE "bin/llquery", "llquery", "-q", "-e", "-D", DefaultDBDir(), "-P", MyHost.h_HostName, dbName, NULL);
	    _exit(0);
	}
	close(fd);
	if (pid < 0) {
	    error = -1;
	    safe_asprintf(emsg, "Unable to fork to create database");
	} else {
	    int status;
	    waitpid(pid, &status, 0);
	    /* XXX */
	}
    }
    return(error);
}

/*
 * CreatePeerDb() - create a new database as a peer to a database that already
 *		    exists in the spanning tree.
 */
static int
CreatePeerDb(const char *dbName, char **emsg)
{
    int error = 0;
    const char *ptr;
    DBInfo *dbi;
    dbstamp_t createTs = 0;
    CLDataBase *cd;

    /*
     * The database must exist in the spanning tree somewhere
     */
    dbi = FindDBInfo(dbName, 1, &error);
    if ((dbi && dbi->d_Refs == 0) || error) {
	if (error == FDBERR_DUPLICATE) {
	    safe_asprintf(emsg, "Cannot make new peer for %s, several unassociated versions of this database exists in the spanning tree", dbName);
	    error = DBERR_REP_UNASSOCIATED_COPIES;
	} else {
	    safe_asprintf(emsg, "Cannot create new peer, I cannot locate %s in the spanning tree", dbName);
	    error = DBERR_REP_NOT_IN_TREE;
	}
    }

    /*
     * The database must have a timestamp extension
     */
    if (error == 0) {
	if ((ptr = strrchr(dbi->d_DBName, '.')) == NULL ||
	    (createTs = strtouq(ptr + 1, NULL, 16)) == 0
	) {
	    safe_asprintf(emsg, "Database %s in spanning tree has no create timestamp, cannot create peer", dbi->d_DBName);
	    error = DBERR_REP_BAD_CREATE_TS;
	}
    }
    if (dbi) {
	DoneDBInfo(dbi);
	dbi = NULL;
    }

    /*
     * The database cannot exist locally
     */
    if (error == 0) {
	DataBase *db;

	if ((db = OpenDatabase(DefaultDBDir(), dbName, 0, NULL, &error)) != NULL) {
	    safe_asprintf(emsg, "Cannot create, %s already exists on %s",
		dbName,
		MyHost.h_HostName
	    );
	    CloseDatabase(db, 1);
	    error = DBERR_REP_ALREADY_EXISTS_LOCAL;
	} else {
	    error = 0;
	}
    }

    /*
     * Open up the database in the spanning tree, make sure we have a
     * quorum.  This is kinda clunky, we are making a connection to ourselves
     * to do this.
     */
    if (error == 0 && (cd = OpenCLDataBase(dbName, &error)) != NULL) {
	CLDataBase *cd2;
	dbstamp_t syncTs;

	if ((cd2 = OpenCLInstance(cd, &syncTs, CLTYPE_RW)) != NULL) {
	    if (syncTs) {
		CLRes *res;
		char *qry;
		int hid = -1;

		PushCLTrans(cd2, syncTs, 0);

		res = QueryCLTrans(cd2, "SELECT HostName, HostId, HostType FROM sys.repgroup", &error);
		error = 0;
		if (res != NULL) {
		    const char **row;
		    for (row = ResFirstRow(res); row; row = ResNextRow(res)) {
			if (row[0] && strcmp(MyHost.h_HostName, row[0]) == 0) {
			    safe_asprintf(emsg, "Warning: %s is already listed as a peer for %s", MyHost.h_HostName, dbName);
			    hid = -2;
			    break;
			}
			if (row[1] && hid < strtol(row[1], NULL, 0))
			    hid = strtol(row[1], NULL, 0);
		    }
		    FreeCLRes(res);
		}
		if (hid == -1) {
		    safe_asprintf(emsg, "Unable to locate HostId for existing PEER in spanning tree, cannot create new peer");
		    error = DBERR_REP_CORRUPT_REPGROUP;
		} else if (hid >= 0) {
		    safe_asprintf(&qry, "INSERT INTO sys.repgroup ( HostName, HostId, HostType ) VALUES ( '%s', '%d', 'PEER' )", MyHost.h_HostName, hid + 1);
		    if ((res = QueryCLTrans(cd2, qry, &error)) != NULL)
			FreeCLRes(res);
		    if (error < 0) {
			safe_asprintf(emsg, "Unable to add %s as PEER for %s, INSERT failed error %d", MyHost.h_HostName, dbName, error);
			error = -1;
		    } else {
			safe_asprintf(emsg, "Successfully added %s as PEER for %s", MyHost.h_HostName, dbName);
			error = 0;
		    }
		}
		if (error == 0) {
		    dbstamp_t ts = 0;

		    if ((error = Commit1CLTrans(cd2, &ts)) == 0) {
			error = Commit2CLTrans(cd2, ts);
		    } else {
			AbortCLTrans(cd2);
		    }
		    if (error < 0) {
			safe_asprintf(emsg, "Accessed repgroup table in %s ok, but commit failed error %d", dbName, error);
		    }
		} else {
		    AbortCLTrans(cd2);
		}
	    } else {
		safe_asprintf(emsg, "Cannot add a new peer for %s without a quorum", dbName);
		error = DBERR_REP_FAILED_NO_QUORUM;
	    }
	    CloseCLInstance(cd2);
	} else {
	    safe_asprintf(emsg, "Unable to talk to instance of %s in spanning tree", dbName);
	    error = DBERR_REP_INSTANCE_OPEN_FAILED;
	}
	CloseCLDataBase(cd);
    } else if (error == 0) {
	safe_asprintf(emsg, "Unable to talk to database %s in spanning tree", dbName);
	error = DBERR_REP_DATABASE_OPEN_FAILED;
    }

    /*
     * Create the database locally using the timestamp obtained from the 
     * spanning tree.
     */
    if (error == 0) {
	DataBase *db;
	DBCreateOptions opts;

	bzero(&opts, sizeof(opts));
	opts.c_Flags = DBC_OPT_TIMESTAMP;
	opts.c_TimeStamp = createTs;
	db = OpenDatabase(DefaultDBDir(), dbName, DBF_CREATE, &opts, &error);
	if (db == NULL) {
	    safe_asprintf(emsg, "Create of %s failed on %s, error %d",
		dbName,
		MyHost.h_HostName,
		error
	    );
	    error = -1;
	} else {
	    CloseDatabase(db, 1);
	}
    }

    return(error);
}

/*
 * CreateSnapDb() - create a new database as a peer to a database existing
 *		    in the replication net.
 */
static int
CreateSnapDb(const char *dbName, char **emsg)
{
    int error = 0;
    DBInfo *dbi;
    dbstamp_t createTs = 0;

    /*
     * The database must exist in the spanning tree somewhere
     */
    dbi = FindDBInfo(dbName, 1, &error);
    if (dbi == NULL || dbi->d_Refs == 0 || error) {
	if (error == FDBERR_DUPLICATE) {
	    safe_asprintf(emsg, "Cannot make new snap for %s, several unassociated versions of this database exists in the spanning tree", dbName);
	    error = DBERR_REP_UNASSOCIATED_COPIES;
	} else {
	    safe_asprintf(emsg, "Cannot create new snap, I cannot locate %s in the spanning tree", dbName);
	    error = DBERR_REP_NOT_IN_TREE;
	}
    }

    /*
     * The database must have a timestamp extension
     */
    if (error == 0) {
	const char *ptr;

	if ((ptr = strrchr(dbi->d_DBName, '.')) == NULL ||
	    (createTs = strtouq(ptr + 1, NULL, 16)) == 0
	) {
	    safe_asprintf(emsg, "Database %s in spanning tree has no create timestamp, cannot create snap", dbi->d_DBName);
	    error = DBERR_REP_BAD_CREATE_TS;
	}
    }

    /*
     * The database should not already exist locally
     */
    if (error == 0) {
	DataBase *db;

	if ((db = OpenDatabase(DefaultDBDir(), dbName, 0, NULL, &error)) != NULL) {
	    error = -1;
	    safe_asprintf(emsg, "Cannot create, %s already exists on %s",
		dbName,
		MyHost.h_HostName
	    );
	    CloseDatabase(db, 1);
	} else {
	    error = 0;
	}
    }

    /*
     * Create the database locally using the timestamp and block size
     * obtained from the spanning tree.
     */
    if (error == 0) {
	DataBase *db;
	DBCreateOptions opts;

	bzero(&opts, sizeof(opts));
	opts.c_Flags = DBC_OPT_TIMESTAMP;
	opts.c_TimeStamp = createTs;
	if ((opts.c_BlockSize = dbi->d_BlockSize) != 0)
	    opts.c_Flags |= DBC_OPT_BLKSIZE;
	db = OpenDatabase(DefaultDBDir(), dbName, DBF_CREATE, &opts, &error);
	if (db == NULL) {
	    safe_asprintf(emsg, "Create of %s failed on %s, error %d",
		dbName,
		MyHost.h_HostName,
		error
	    );
	    error = -1;
	} else {
	    CloseDatabase(db, 1);
	}
    }

    if (dbi) {
	DoneDBInfo(dbi);
	dbi = NULL;
    }

    return(error);
}

/*
 * DestroyDb() -destroy a database (on the local machine only).  The database
 *		must be in a 'stopped' state to do this.
 *
 *		This function will remove the database from the replication
 *		group.
 */
static int
DestroyDb(const char *dbName, char **emsg)
{
    DBInfo *d;
    int error = 0;

    if ((d = FindDBInfoIfLocal(dbName, 1, &error)) != NULL) {
	DoneDBInfo(d);
	safe_asprintf(emsg, "Must stop (-e) database before destroying it", dbName);
	dbwarning("CANNOT DESTROY RUNNING DATABASE\n");
	return(-1);
    }
    dbinfo2("DESTROYING DATABASE %s\n", dbName);
    DeleteDatabase(DefaultDBDir(), dbName);
    return(0);
}

/*
 * DowngradeDb() - Downgrade a database from a PEER to a SNAPshot
 *
 *		This function will remove the database from the replication
 *		group, but leave it active as a snapshot.  The database must
 *		be in a 'stopped' state to do this.
 */
static int
DowngradeDb(const char *dbName, char **emsg)
{
    safe_asprintf(emsg, "DOWNGRADE not yet supported");
    return(-1);
}

/*
 * UpgradeDb() - Upgrade a database from a SNAPshot to a PEER
 *
 *		This function will upgrade a database from a snapshot to a
 *		peer, adding it to the replication group.  The database must
 *		be in a 'stopped' state to do this.
 */
static int
UpgradeDb(const char *dbName, char **emsg)
{
    safe_asprintf(emsg, "UPGRADE not yet supported");
    return(-1);
}

static int
ValidateDBName(const char *dbName, char **emsg)
{
    int i;
    char c;

    for (i = 0; dbName[i]; ++i) {
	c = dbName[i];
	if ((c >= '0' && c <= '9') ||
	    (c >= 'a' && c <= 'z') ||
	    (c >= 'A' && c <= 'Z') ||
	    (c == '_')
	) {
	    continue;
	}
	safe_asprintf(emsg, "Database name may only contain alpha-numerics and underscore");
	return(-1);
    }
    if (dbName[0] == 0) {
	safe_asprintf(emsg, "An empty database name was specified");
	return(-1);
    }
    return(0);
}

