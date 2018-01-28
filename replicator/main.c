/*
 * REPLICATOR/MAIN.C -	Replication Spanning Tree And DataBase Rendezvous 
 *			Daemon
 *
 * (c)Copyright 2000-2002 Backplane, Inc.  Please refer to the COPYRIGHT
 * file at the base of the distribution tree.
 *
 * $Backplane: rdbms/replicator/main.c,v 1.35 2002/08/20 22:06:00 dillon Exp $
 *
 *	The replicator is what all the SQL daemons connect to.  It is a set
 *	of processes and threads managing one or more databases.
 *
 *	The replicator starts-up any local databases and generates linkages
 *	to remote replicators to form a spanning tree topology across the
 *	entire replication system.  Linkages for each database are forged
 *	independantly via a fork()'d process.  
 *
 *	The master process accepts connections, determines which database
 *	they are for, and forwards the descriptors to the appropriate 
 *	forked subprocess.  The master process will fork a subprocess for
 *	each local database and when it receives a database-specific 
 *	management command (e.g. -CREATE, -PEER, -SNAP, -b, -e).
 */

#include "defs.h"

Prototype void startReplicationConnectionThread(iofd_t iofd);
Prototype void SendControlOps(iofd_t iofd, int ctlOp, const char *dbname, const char *args, int waitOpt);

Prototype char *Arg0;
Prototype char *Arg0Path;
Prototype char *LogFile;

void MainClientAcceptor(void);
void MainReplicationAcceptor(void);

static void profExit(int sigNo);
static void startupDatabases(void);
static void startClientConnectionThread(iofd_t iofd);

char *Arg0;
char *Arg0Path;
char *LogFile;

void
task_main(int ac, char **av)
{
    int i;
    int waitOpt = 0;
    int foregroundOpt = 0;
    int dbOpt = 0;
    int ctlOpt = 0;
    int restartOpt = 0;
    int serverOpt = 0;
    int funcOptCount = 0;
    const char *ctlData = NULL;
    char *dbName = NULL;
    char *udomDRDPath;
    char *udomCRDPath;
    char *ptr;
    const char *emsg;

    signal(SIGPIPE, SIG_IGN);
    signal(SIGINT, profExit);
    Arg0 = av[0];
    Arg0Path = strdup(Arg0);
    if ((ptr = strrchr(Arg0Path, '/')) != NULL)
	ptr[1] = 0;
    else
	Arg0Path[0] = 0;

    if (ac == 1) {
	fprintf(stderr, "%s [options]\n", av[0]);
	fprintf(stderr, 
	    "    -D dir          set replication base directory\n"
	    "    -V id           override userid\n"
	    "    -i              report system info/status\n"
	    "    -s              Run server\n"
	    "    -f              Run server in foreground (implies -s)\n"
	    "    -b database[:#] start database, increase to N copies\n"
	    "                    (def 2).  Extended form: 'db:#:pri:flags'\n"
	    "    -e database[:#] stop database, decrease to N (default 0)\n"
	    "    -r database     stop & restart replicator & database\n"
	    "    -l database:user@host  add replication link to remote host\n"
	    "    -l database:directory  add replication link to another\n"
	    "                           replicator running on this host\n"
	    "                           (you must use a different -V id)\n"
	    "    -u <link_string>       remove replication link\n"
	    "    -O database:level[:file] Set debugging for database\n"
	    "    -L logfile             Set the initial logfile\n"
	    "    -STOP database         stop replicator & drd for databse\n"
	    "    -CREATE database       create a new database\n"
	    "    -SNAP database         create db as SNAP to remote db\n"
	    "    -PEER database         create db as PEER to remote db\n"
	    "    -UPGRADE database      upgrade db from SNAP to PEER (1)\n"
	    "    -DOWNGRADE database    downgrade db from PEER to SNAP (1)\n"
	    "    -REMOVE database       physically delete local db\n"
	    "\n"
	    "    (note 1): these options are highly experimental\n"
	    "\n"
	);
	exit(1);
    }

    for (i = 1; i < ac; ++i) {
	char *ptr = av[i];

	if (*ptr != '-') {
	    if (dbName) {
		fprintf(stderr, "dbname specified twice\n");
	    }
	    dbName = ptr;
	    continue;
	}
	ptr += 2;
	switch(ptr[-1]) {
	case 's':
	    serverOpt = 1;
	    ++funcOptCount;
	    break;
	case 'n':	/* nop, used when execing self */
	    break;
	case 'x':
	    dbOpt = 1;	/* special exec from db manager (replicator) */
	    ++funcOptCount;
	    break;
	case 'q':
	    DebugOpt = 0;
	    break;
	case 'v':
	    DebugOpt = -1;
	    break;
	case 'd':
	    DebugOpt = *ptr ? strtol(ptr, NULL, 0) : -1;
	    break;
	case 'f':
	    foregroundOpt = 1;
	    break;
	case 'D':	/* set base directory */
	    if (*ptr == 0) {
		SetDefaultDBDir(av[++i]);
	    } else if (strncmp(ptr - 1, "DOWN", 4) == 0) {
		ctlOpt =RPCMD_DOWNGRADEDB;
		ctlData = av[++i];
	    } else {
		fatal("Illegal option: %s", ptr - 1);
	    }
	    break;
	case 'V':	/* set virtual name, else real hostname is used */
	    VirtualHostName = (*ptr) ? ptr : av[++i];
	    break;
	case 'L':	/* log file */
	    LogFile = (*ptr) ? ptr : av[++i];
	    break;
	/*
	 * Control ops - send to running replicator, get response, and exit
	 */
	case 'U':
	    if (strncmp(ptr - 1, "UPGR", 4) == 0) {
		ctlOpt = RPCMD_UPGRADEDB;
		ctlData = av[++i];
	    } else {
		fatal("Illegal option: %s", ptr - 1);
	    }
	    break;
	case 'C':
	    if (strncmp(ptr - 1, "CREA", 4) == 0) {
		ctlOpt = RPCMD_CREATEDB;
		ctlData = av[++i];
	    } else {
		fatal("Illegal option: %s", ptr - 1);
	    }
	    break;
	case 'S':
	    if (strncmp(ptr - 1, "SNAP", 4) == 0) {
		ctlOpt = RPCMD_CREATESNAP;
		ctlData = av[++i];
	    } else if (strncmp(ptr - 1, "STOP", 4) == 0) {
		ctlOpt = RPCMD_STOP_REPLICATOR;
		ctlData = av[++i];
	    } else {
		fatal("Illegal option: %s", ptr - 1);
	    }
	    break;
        case 'P':       /* peer existing database */
            if (strncmp(ptr - 1, "PEER", 4) == 0) {
                ctlOpt = RPCMD_CREATEPEER;
		ctlData = av[++i];
            } else {
                fatal("Illegal option: %s", ptr - 1);
            }
            break;
        case 'R':       /* physically remove a database (may not be active) */
            if (strncmp(ptr - 1, "REMO", 4) == 0) {
                ctlOpt = RPCMD_DESTROYDB;
		ctlData = av[++i];
            } else {
                fatal("Illegal option: %s", ptr - 1);
            }
            break;
	case 'Q':
	    ctlOpt = RPCMD_STOP_REPLICATOR;
	    ctlData = "";
	    break;
	/* 
	 * General system commands, typically routed to a particular database
	 */
	case 'l':
	    ctlOpt = RPCMD_ADDLINK;
	    ctlData = av[++i];
	    break;
	case 'u':
	    ctlOpt = RPCMD_REMLINK;
	    ctlData = av[++i];
	    break;
        case 'w':
            waitOpt = 1;
            break;
	case 'i':
	    ctlOpt = RPCMD_INFO;
	    ctlData = av[++i];
	    break;
	case 'b':
	    ctlOpt = RPCMD_START;
	    ctlData = av[++i];
	    break;
	case 'e':
	    ctlOpt = RPCMD_STOP;
	    ctlData = av[++i];
	    break;
	case 'r':
	    ctlOpt = RPCMD_STOP_REPLICATOR;
	    ctlData = av[++i];
	    restartOpt = 1;
	    break;
	case 'O':
	    ctlOpt = RPCMD_SETOPT;
	    ctlData = av[++i];
	    break;
	default:
	    fatal("Unknown option: %s", ptr - 2);
	    /* not reached */
	}
    }
    if (ctlOpt)
	++funcOptCount;
    if (i > ac)
	fatal("Expected argument to last option");
    if (funcOptCount == 0)
	fatal("No function options specified, nothing to do");
    if (funcOptCount > 1)
	fatal("Only one function option may be specified");
    if ((dbOpt && dbName == NULL) || (!dbOpt && dbName != NULL))
	fatal("Illegal use of -x / database specification");

    /************************************************************************
     *			SPECIFIC FORK FOR DATABASE CASE 
     *
     * Handle case when exec'd from master replicator.  The client UDOM 
     * descriptor is descriptor #3, the control UDOM descriptor is 
     * descriptor #4.
     */
    if (dbOpt) {
	DBManageNode *dbm = zalloc(sizeof(DBManageNode));

	if (dbName == NULL) {
	    fatal("dbName not specified in exec from master replicator!");
	}

	/*
	 * Construct DBManageNode.  Since we already forked, there is only
	 * one of these and we don't bother to put it in the hash table.
	 */

	dbm->dm_DBName = safe_strdup(dbName);
	dbm->dm_ClientIo = allocIo(3);	/* also sets close-on-exec flag */
	dbm->dm_ControlIo = allocIo(4);	/* also sets close-on-exec flag */

	InitMyHost(CTYPE_REPLIC);

	/*
	 * Forge Links for this database
	 */
	LoadLinkMaintainFile(dbm->dm_DBName);

	/*
	 * Bring up local repositories
	 */
	LoadLinkEnableFile(dbm->dm_DBName);

	/*
	 * Start a task to accept client connections
	 */
	taskCreate(MainClientMsgAcceptor, dbm);

	/*
	 * Use this task to accept replication and control connections 
	 */
	MainReplicationMsgAcceptor(dbm);
	/* not reached */
	return;
    }

    if (asprintf(&udomDRDPath, "%s/.drd_socket", DefaultDBDir()) < 0)
	fatalmem();
    if (asprintf(&udomCRDPath, "%s/.crd_socket", DefaultDBDir()) < 0)
	fatalmem();

    /************************************************************************
     *				CONTROL OP SPECIFIED
     *
     * Control ops must be forwarded over the control connection to
     * the main replication daemon.  Unlike client connections, control
     * connections will cause the main replication daemon to fork off a
     * sub-replicator as needed.
     */
    while (ctlOpt) {
	int fd;
	iofd_t iofd;
	char *dbname = safe_strdup(ctlData);
	char *args = strchr(dbname, ':');	/* "" sends to self */

	if (args)
	    *args++ = 0;
	else
	    args = "";

	if ((fd = ConnectUDomSocket(udomDRDPath, &emsg)) < 0)
	    fatalsys("Unable to connect to %s (%s)", udomDRDPath, emsg);
	iofd = allocIo(fd);
	SendControlOps(iofd, ctlOpt, dbname, args, waitOpt);

	safe_free(&dbname);

	/*
	 * If restartOpt is non-zero, we were asked to restart the replicator
	 * and database after stopping it.
	 */
	if (!restartOpt)
	    return;
	ctlOpt = RPCMD_START;
	waitOpt = 1;
	restartOpt = 0;
	printf("Restarting replicator & database for %s\n", ctlData);
	sleep(1);
    }

    /************************************************************************
     *				MASTER PARENT PROCESS
     *
     * Start two threads -- the main client acceptor, which accepts
     * connections from local clients, and the main replication 
     * acceptor, which accepts replication linkages (we just use the
     * current thread for that).
     *
     * Note that calling allocIo() also has the necessary indirect
     * effect of making the descriptor non-blocking and setting it's
     * close-on-exec flag.
     */

    if (serverOpt) {
	int fd;

	if ((fd = BuildUDomSocket(udomCRDPath, &emsg)) < 0)
	    fatalsys("Unable to bind to %s (%s)", udomCRDPath, emsg);
	LisCFd = allocIo(fd);

	if ((fd = BuildUDomSocket(udomDRDPath, &emsg)) < 0)
	    fatalsys("Unable to bind to %s (%s)", udomDRDPath, emsg);
	LisDFd = allocIo(fd);

	/*
	 * Fork off a daemon
	 */
	if (foregroundOpt == 0) {
	    if (chdir(DefaultDBDir()) < 0) {
		perror("chdir");
		exit(1);
	    }
	    if (daemon(1, 0) < 0) {
		perror("daemon");
		exit(1);
	    }
	    t_didFork();
	}
	if (LogFile)
	    freopen(LogFile, "a", stderr);

	/*
	 * Start databases
	 */
	InitMyHost(CTYPE_REPLIC);
	startupDatabases();

	/*
	 * Start a task to accept client connections
	 */
	taskCreate(MainClientAcceptor, NULL);

	/*
	 * Use this task to accept replication and control connections 
	 */
	MainReplicationAcceptor();
    } else {
	fprintf(stderr, "You didn't tell me to do anything!\n");
	exit(1);
    }
}

/*
 * MainClientAcceptor() - Accepts new client connections
 *
 *	This thread is responsible for accepting and processing
 *	new client connections.  The HELLO message is received from
 *	the client and the descriptor and message are passed to
 *	the appropriate subprocess.
 */

void
MainClientAcceptor(void)
{
    /*
     * Wait for connections and startup client rendezvous services
     * for them.  Note: allocIo() sets close-on-exec, which is what we
     * want.
     */
    for (;;) {
	iofd_t afd;
	struct sockaddr sa;
	int saLen = sizeof(sa);

	if ((afd = t_accept(LisCFd, &sa, &saLen, 0)) < 0)
	    break;
	taskCreate(startClientConnectionThread, afd);
    }
}

/*
 * This thread is responsible for taking a new client connection
 * and passing it onto the correct sub-replicator process.  Unlike
 * other operations (replication links, replicator links), the dbm
 * node must already exist for client database connections.
 */
static void
startClientConnectionThread(iofd_t iofd)
{
    CLAnyMsg *msg = ReadCLMsg(iofd);	/* UNBUFFERED.  May return NULL */
    DBManageNode *dbm;

    /*
     * If message is NULL or invalid, return an error, cleanup, and exit
     * the thread.
     */
    if (ValidCLMsgHelloWithDB(&msg->a_HelloMsg, 1) < 0) {
	FreeCLMsg(msg);
	msg = BuildCLHelloMsgStr("Badly formed HELLO message");
	msg->cma_Pkt.cp_Error = -1;
	WriteCLMsg(iofd, msg, 1);
	/* message is freed by write */
	closeIo(iofd);
	return;
    }

    /*
     * Process the hello message.  Obtain the database name and look up the
     * database management sub process.  The sub process must already exist.
     * (for replication links and control commands the sub process will be
     * created on the fly, but for client connections it must already exist).
     */
    if ((dbm = FindDBManageNode(msg->a_HelloMsg.hm_DBName)) == NULL) {
	/* dbm = CreateDBManageNode(msg->a_HelloMsg.hm_DBName); */
    }
    if (dbm == NULL) {
	FreeCLMsg(msg);
	msg = BuildCLHelloMsgStr(strerror(errno));
	msg->cma_Pkt.cp_Error = -1;
	WriteCLMsg(iofd, msg, 1);
	/* msg is freed automatically */
    } else {
	SendCLMsg(dbm->dm_ClientIo, msg, iofd);
	/* msg is freed automatically */
    }

    /*
     * Cleanup and exit task
     */
    closeIo(iofd);
}

/*
 * startupDatabases()
 *
 * Start databases.  We scan our base directory for databases to 
 * automatically start-up.  We also scan our replication link save
 * file for databases that we need to create a management instance
 * for, even if we do not host said databases ourselves.
 *
 * We have not yet created our unix domain sockets, so we shortcut
 * the command by creating a pipe to the thread function that handles
 * it.
 *
 * We use the replication.enable file to determine which databases
 * to start.  If the file does not exist we build an initial file.
 */

static void
startupDatabases(void)
{
    /*
     * Create a replicator.enable file if it does not exist
     */
    InitLinkEnableFile();

    /*
     * Load the replicator.enable file and start sub-replicators for
     * local repositories.
     */
    LoadLinkEnableFile(NULL);

    /*
     * This will force sub-replicators to be started if we were asked
     * to forge links, even if we do not have local repositories for
     * the databases in question.  Otherwise we don't bother until a 
     * remote replicator forges a link for database BLAH to us.
     */
    LoadLinkMaintainFile(NULL);
}

/*
 * MainReplicationAcceptor() -	Accepts new replication connections
 *
 *	This thread is responsible for accepting and processing new
 *	replicator/control connections.  These connections start out 
 *	with a CLMsg for identification and routing.  A client then
 *	proceeds with CLMsg's while a replication/control link proceeds
 *	with RPMsg's.
 */

void
MainReplicationAcceptor(void)
{
    /*
     * Wait for connections and startup master/slave rendezvous services
     * for them.
     */
    for (;;) {
	iofd_t afd;
	struct sockaddr sa;
	int saLen = sizeof(sa);

	if ((afd = t_accept(LisDFd, &sa, &saLen, 0)) < 0)
	    break;
	taskCreate(startReplicationConnectionThread, afd);
    }
}

void
startReplicationConnectionThread(iofd_t iofd)
{
    CLAnyMsg *msg = ReadCLMsg(iofd);	/* UNBUFFERED. may return NULL */
    DBManageNode *dbm;

    /*
     * If message is NULL or invalid, return an error, cleanup, and exit
     * the thread.
     */
    if (ValidCLMsgHelloWithDB(&msg->a_HelloMsg, 0) < 0) {
	FreeCLMsg(msg);
	msg = BuildCLHelloMsgStr("Badly formed HELLO message");
	msg->cma_Pkt.cp_Error = -1;
	WriteCLMsg(iofd, msg, 1);
	/* message is freed by write */
	closeIo(iofd);
	return;
    }

    /*
     * If the database name is empty this is a control op to be done by
     * us (the master replicator process) rather then a database-specific
     * replicator.  We leave l->l_DBName NULL to identify the distinction.
     */
    if (msg->a_HelloMsg.hm_DBName[0] == 0) {
	LinkInfo *l;

	l = AllocLinkInfo(NULL, CTYPE_UNKNOWN, iofd);
	taskCreate(ReplicationReaderThread, l);
	return;
    }

    /*
     * Process the hello message.  Obtain the database name and lookup/start
     * a database management sub process (which is a real fork)
     */
    if ((dbm = FindDBManageNode(msg->a_HelloMsg.hm_DBName)) == NULL) {
	dbm = CreateDBManageNode(msg->a_HelloMsg.hm_DBName);
    }
    if (dbm == NULL) {
	FreeCLMsg(msg);
	msg = BuildCLHelloMsgStr(strerror(errno));
	msg->cma_Pkt.cp_Error = -1;
	WriteCLMsg(iofd, msg, 1);
	/* msg is freed automatically */
    } else {
	SendCLMsg(dbm->dm_ControlIo, msg, iofd);
	/* msg is freed automatically */
    }

    /*
     * Cleanup and exit task
     */
    closeIo(iofd);
}

/*
 * SendControlOps() -	Execute management function(s) and exit
 *
 *	The dbname[] array represents link commands or database names
 *	depending on the control op.
 */

void
SendControlOps(iofd_t iofd, int ctlOp, const char *dbname, const char *args, int waitOpt)
{
    LinkInfo *l;

    /*
     * Route the connection with a CLMsg HELLO (even though this is a 
     * control op, we still need to route the connection)
     */
    {
	CLAnyMsg *msg = BuildCLHelloMsgStr(dbname);
	WriteCLMsg(iofd, msg, 1);
	/* message is freed automatically */
    }

    InitMyHost(CTYPE_CONTROL);

    l = AllocLinkInfo(NULL, CTYPE_CONTROL, iofd);

    /*
     * Send a hello
     */
    if (CmdStartupHello(l) < 0)
	fatal("control link: can't send HELLO stage 1");
    if (RecStartupHello(l) < 0)
	fatal("control link: did not receive HELLO stage 2");

    /*
     * Send control op:
     */

    for (;;) {
	char *emsg = NULL;
	int r;

	r = CmdControl(l, args, ctlOp, &emsg);
	if (emsg) {
	    printf("(%s): %s\n", dbname, emsg);
	    safe_free(&emsg);
	    if (r < 0 && waitOpt) {
		if (r == DBERR_REP_NOT_IN_TREE) {
		    printf("W");
		    fflush(stdout);
		    taskSleep(1000);
		    continue;
		}
		if (r == DBERR_REP_FAILED_NO_QUORUM) {
		    printf("W");
		    fflush(stdout);
		    taskSleep(1000);
		    continue;
		}
	    }
	} else if (r < 0) {
	    fatal("control link: lost link on %s, error %d", dbname, r);
	}
	break;
    }
    FreeLinkInfo(l);
}

static void 
profExit(int sigNo)
{
    exit(0);
}

