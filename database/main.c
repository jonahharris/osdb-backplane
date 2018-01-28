/*
 * DATABASE/MAIN.C - Manage a specific database, exec'd by replicator
 *
 * (c)Copyright 2000-2002 Backplane, Inc.  Please refer to the COPYRIGHT 
 * file at the base of the distribution tree.
 *
 * $Backplane: rdbms/database/main.c,v 1.30 2002/08/20 22:05:47 dillon Exp $
 */

#include "defs.h"

Prototype void task_main(int ac, char **av);

static void profExit(int sigNo);

/*
 * MainDatabaseSubFork() - 	Subprocess to manage a specific database
 */
void 
task_main(int ac, char **av)
{
    int i;
    int fd = -1;
    int error;
    const char *emsg;
    char *dbName = NULL;
    DataBase *db;
    CLDataBase *cd;
    CLAnyMsg *msg;

    signal(SIGPIPE, SIG_IGN);
    signal(SIGINT, profExit);

    for (i = 1; i < ac; ++i) {
	char *ptr = av[i];

	if (*ptr != '-') {
	    if (dbName) {
		dberror("Specify only one database\n");
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
	case 'f':
	    fd = strtol((*ptr ? ptr : av[++i]), NULL, 0);
	    break;
	case 'q':
	    DebugOpt = 0;
	    break;
	case 'v':
	    DebugOpt = -1;
	    break;
	default:
	    dberror("Unknown option %s\n", ptr - 2);
	    exit(1);
	}
    }
    if (dbName == NULL) {
	dberror("Database not specified\n");
	exit(1);
    }
    if (fd < 0) {
	dberror("Rendezvous descriptor not specified\n");
	dberror("NOTE!  This program should never be run manually\n");
	exit(1);
    }
    cd = AllocCLDataBase(dbName, allocIo(fd));
    db = OpenDatabase(DefaultDBDir(), cd->cd_DBName, 0, NULL, &error);
    if (db == NULL) {
	DBASSERT(error != 0);
	msg = BuildCLHelloMsgStr("Unable to open database");
	msg->cma_Pkt.cp_Error = error;
	WriteCLMsg(cd->cd_Iow, msg, 1);
	CloseCLDataBase(cd);
	return;
    }

    /*
     * XXX Do startup recovery here
     */

    error = RecoverDatabase(db, &emsg);

    if (error) {
	msg = BuildCLHelloMsgStr(emsg);
	msg->cma_Pkt.cp_Error = error;
	WriteCLMsg(cd->cd_Iow, msg, 1);
	CloseCLDataBase(cd);
	return;
    }

    /*
     * Done with recovery, send HELLO indicating that the physical database
     * is up and ready to run.  (note that we are not responsible for
     * synchronizing this database with the replication group)
     *
     * We return the most recent synchronization timestamp.  We could return
     * GetMinCTs() here for something a little closer to realtime, but we
     * do not want the caller to stall when they start running queries and
     * the caller is responsible for maintaining transactional consistency
     * anyway.
     */
    msg = BuildCLHelloMsgStr("");
    msg->a_HelloMsg.hm_SyncTs = GetSyncTs(db);
    msg->a_HelloMsg.hm_MinCTs = GetMinCTs(db);
    msg->a_HelloMsg.hm_BlockSize = GetSysBlockSize(db);
    WriteCLMsg(cd->cd_Iow, msg, 1);

    /*
     * Loop reading commands
     */

    dbinfo("%s Starting (%s)\n", av[0], cd->cd_DBName);

    /*
     * The only command we recognize is CLCMD_OPEN_INSTANCE, which opens
     * an instance of the database and returns the descriptor.
     */

    while ((msg = MReadCLMsg(cd->cd_Ior)) != NULL) {
	CLDataBase *newCd;
	int fds[2];
	iofd_t iofd;

	switch(msg->cma_Pkt.cp_Cmd) {
	case CLCMD_OPEN_INSTANCE:
	    if (socketpair(PF_LOCAL, SOCK_STREAM, 0, fds) < 0) {
		fatalsys("drd_database: socketpair() failed");
	    }
	    /*
	     * Create a new instance and a new thread to handle it.  Keep 
	     * fds[0] and send a reply containing fds[1].
	     */
	    iofd = allocIo(fds[1]);
	    msg->cma_Pkt.cp_Error = 0;
	    SendCLMsg(cd->cd_Iow, msg, iofd);
	    closeIo(iofd);

	    newCd = AllocCLInstance(cd, allocIo(fds[0]));
	    taskCreate(DatabaseInstanceThread, newCd);
	    break;
	case CLCMD_UPDATE_SYNCTS:
	    SetSyncTs(db, msg->a_StampMsg.ts_Stamp);
	    FreeCLMsg(msg);
	    break;
	case CLCMD_UPDATE_STAMPID:
	    SetDatabaseId(db, msg->a_StampMsg.ts_Stamp);
	    cd->cd_StampId = msg->a_StampMsg.ts_Stamp;
	    FreeCLMsg(msg);
	    break;
	default:
	    dberror("Illegal client packet received %d\n", msg->cma_Pkt.cp_Cmd);
	    FreeCLMsg(msg);
	    DBASSERT(0);
	    break;
	}
    }
    dbinfo("%s Exiting (%s)\n", av[0], cd->cd_DBName);
    CloseDatabase(db, 1);
    CloseCLDataBase(cd);
}

void 
profExit(int sigNo)
{
    exit(0);
}

