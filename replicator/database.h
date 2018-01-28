/*
 * REPLICATOR/DATABASE.H	- Structures tracking spanning tree state
 *
 * (c)Copyright 2000-2002 Backplane, Inc.  Please refer to the COPYRIGHT
 * file at the base of the distribution tree.
 *
 * $Backplane: rdbms/replicator/database.h,v 1.4 2002/08/20 22:06:00 dillon Exp $
 */

typedef struct DBManageNode {
    struct DBManageNode *dm_Next; /* link */
    char	*dm_DBName;	/* name of database under management */
    iofd_t	dm_ControlIo;	/* unix domain link for descriptor passing */
    iofd_t	dm_ClientIo;	/* unix domain link for descriptor passing */
    pid_t	dm_Pid;		/* pid of forked subprocess */
} DBManageNode;

#define DBMHSIZE	64
#define DBMHMASK	(DBMHSIZE - 1)

