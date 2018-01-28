/*
 * REPLICATOR/LINKMAINT.C
 *
 * (c)Copyright 2000-2002 Backplane, Inc.  Please refer to the COPYRIGHT
 * file at the base of the distribution tree.
 *
 * $Backplane: rdbms/replicator/linkmaint.c,v 1.7 2002/08/20 22:06:00 dillon Exp $
 */

#include "defs.h"
#include <sys/stat.h>

Prototype LinkMaintain *AllocLinkMaintain(const char *dbName, const char *linkCmd);
Prototype LinkMaintain *FindLinkMaintain(const char *dbName, const char *linkCmd);
Prototype void SaveLinkMaintainFile(const char *dbName);
Prototype void FreeLinkMaintain(LinkMaintain *lm);
Prototype void LoadLinkMaintainFile(const char *dbNameRestrict);
Prototype int CopyLinkMaintainFile(const char *dbName, iofd_t sio, iofd_t dio);

Prototype LinkMaintain	*LinkMaintainBase;

LinkMaintain	*LinkMaintainBase;

LinkMaintain *
AllocLinkMaintain(const char *dbName, const char *linkCmd)
{
    LinkMaintain	*lm;

    lm = zalloc(sizeof(LinkMaintain));
    lm->m_LinkCmd = strdup(linkCmd);
    lm->m_DBName = strdup(dbName);
    lm->m_Next = LinkMaintainBase;
    LinkMaintainBase = lm;
    return(lm);
}

LinkMaintain *
FindLinkMaintain(const char *dbName, const char *linkCmd)
{
    LinkMaintain	*lm;

    for (lm = LinkMaintainBase; lm; lm = lm->m_Next) {
	if (strcmp(linkCmd, lm->m_LinkCmd) == 0 &&
	    strcmp(dbName, lm->m_DBName) == 0
	) {
	    break;
	}
    }
    return(lm);
}

void 
FreeLinkMaintain(LinkMaintain *lm)
{
    LinkMaintain	**plm;

    for (plm = &LinkMaintainBase; *plm != lm; plm = &(*plm)->m_Next)
	;
    *plm = lm->m_Next;
    safe_free(&lm->m_LinkCmd);
    safe_free(&lm->m_DBName);
    zfree(lm, sizeof(LinkMaintain));
}

void
SaveLinkMaintainFile(const char *dbName)
{
    char *path1;
    char *path2;
    int fd;
    int lfd;
    iofd_t sio = NULL;

    safe_asprintf(&path1, "%s/replication.links.new", DefaultDBDir());
    safe_asprintf(&path2, "%s/replication.links", DefaultDBDir());

    while ((lfd = open(path2, O_RDWR|O_CREAT, 0640)) >= 0) {
	struct stat st1;
	struct stat st2;

	flock(lfd, LOCK_EX);
	if (stat(path2, &st1) < 0 || fstat(lfd, &st2) < 0 ||
	    st1.st_ino != st2.st_ino
	) {
	    close(lfd);
	    continue;
	}
	break;
    }
    if (lfd >= 0)
	sio = allocIo(lfd);

    if (lfd >= 0 && (fd = open(path1, O_RDWR|O_CREAT|O_TRUNC, 0640)) >= 0) {
	iofd_t dio = allocIo(fd);
	LinkMaintain *lm;
	char *ptr = NULL;

	for (lm = LinkMaintainBase; lm; lm = lm->m_Next) {
	    if ((lm->m_Flags & LMF_STOPME) == 0) {
		safe_appendf(&ptr, "%s %s\n", dbName, lm->m_LinkCmd);
	    }
	}
	if (ptr == NULL || t_write(dio, ptr, strlen(ptr), 0) == strlen(ptr)) {
	    if (dbName && CopyLinkMaintainFile(dbName, sio, dio) == 0) {
		fsync(fd);
		closeIo(dio);
		rename(path1, path2);
	    } else {
		closeIo(dio);
		remove(path1);
	    }
	} else {
	    closeIo(dio);
	    remove(path1);
	}
	safe_free(&ptr);
    }

    /*
     * clear lock on original file.
     */
    if (lfd >= 0)
	flock(lfd, LOCK_UN);
    if (sio)
	closeIo(sio);
    free(path1);
    free(path2);
}

/*
 * CopyLinkMaintainFile() - copy everything not for this particular database
 */
int
CopyLinkMaintainFile(const char *dbName, iofd_t sio, iofd_t dio)
{
    int r = 0;
    char *data;
    int len = strlen(dbName);

    sio = dupIo(sio);

    while ((data = t_gets(sio, 0)) != NULL) {
	if (strncmp(dbName, data, len) == 0 && data[len] == ' ')
	    continue;
	if (t_printf(dio, 0, "%s\n", data) != strlen(data) + 1) {
	    r = -1;
	    break;
	}
    }
    closeIo(sio);
    return(r);
}

void
LoadLinkMaintainFile(const char *dbNameRestrict)
{
    char *path;
    int fd;

    safe_asprintf(&path, "%s/replication.links", DefaultDBDir());
    if ((fd = open(path, O_RDONLY)) >= 0) {
	iofd_t io = allocIo(fd);
	char *str;

	while ((str = t_gets(io, 0)) != NULL) {
	    char *emsg = NULL;
	    char *tmp = str;
	    char *dbName = strsep(&tmp, " ");
	    char *linkCmd = dbName ? strsep(&tmp, "") : NULL;

	    if (dbName == NULL || linkCmd == NULL)
		continue;
	    if (dbNameRestrict && strcmp(dbNameRestrict, dbName) == 0) {
		StartReplicationLink(dbName, linkCmd, &emsg);
		fprintf(stderr, "%s:%s (%s)\n", dbName, linkCmd, emsg);
		safe_free(&emsg);
	    } else if (dbNameRestrict == NULL) {
		if (FindDBManageNode(dbName) == NULL)
		    CreateDBManageNode(dbName);
	    }
	}
	closeIo(io);
    }
    free(path);
}

