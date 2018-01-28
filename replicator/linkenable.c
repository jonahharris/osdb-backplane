/*
 * REPLICATOR/ENABLEMAINT.C
 *
 * (c)Copyright 2000-2002 Backplane, Inc.  Please refer to the COPYRIGHT
 * file at the base of the distribution tree.
 *
 * $Backplane: rdbms/replicator/linkenable.c,v 1.3 2003/04/02 18:28:01 dillon Exp $
 *	Maintain the replication.enable file.
 */

#include "defs.h"
#include <sys/stat.h>
#include <dirent.h>

Prototype LinkEnable *AllocLinkEnable(const char *dbName);
Prototype LinkEnable *FindLinkEnable(const char *dbName);
Prototype void SaveLinkEnableFile(const char *dbName);
Prototype void FreeLinkEnable(LinkEnable *le);
Prototype void LoadLinkEnableFile(const char *dbNameRestrict);
Prototype void InitLinkEnableFile(void);
Prototype int CopyLinkEnableFile(const char *dbName, iofd_t sio, iofd_t dio);

Prototype LinkEnable	*LinkEnableBase;

LinkEnable	*LinkEnableBase;

LinkEnable *
AllocLinkEnable(const char *dbName)
{
    LinkEnable	*le;

    le = zalloc(sizeof(LinkEnable));
    le->e_DBName = strdup(dbName);
    le->e_Next = LinkEnableBase;
    LinkEnableBase = le;

    return(le);
}

LinkEnable *
FindLinkEnable(const char *dbName)
{
    LinkEnable	*le;

    for (le = LinkEnableBase; le; le = le->e_Next) {
	if (strcmp(dbName, le->e_DBName) == 0)
	    break;
    }
    return(le);
}

void 
FreeLinkEnable(LinkEnable *le)
{
    LinkEnable	**ple;

    for (ple = &LinkEnableBase; *ple != le; ple = &(*ple)->e_Next) {
	DBASSERT(*ple != NULL);
    }
    *ple = le->e_Next;
    safe_free(&le->e_DBName);
    zfree(le, sizeof(LinkEnable));
}

void
SaveLinkEnableFile(const char *dbName)
{
    char *path1;
    char *path2;
    int fd;
    int lfd;
    iofd_t sio = NULL;

    safe_asprintf(&path1, "%s/replication.enable.new", DefaultDBDir());
    safe_asprintf(&path2, "%s/replication.enable", DefaultDBDir());

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
	LinkEnable *le;
	char *ptr = NULL;

	for (le = LinkEnableBase; le; le = le->e_Next) {
	    if (le->e_Count || le->e_Pri || le->e_Flags) {
		safe_appendf(&ptr, "%s %d %d 0x%04x\n",
		    dbName,
		    le->e_Count,
		    le->e_Pri,
		    le->e_Flags
		);
	    }
	}
	if (ptr == NULL || t_write(dio, ptr, strlen(ptr), 0) == strlen(ptr)) {
	    if (dbName && CopyLinkEnableFile(dbName, sio, dio) == 0) {
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
     * clear the interlock on the original file.  Note that other 
     * replicators can gain access the moment we rename or remove the
     * original file.
     */
    if (lfd >= 0)
	flock(lfd, LOCK_UN);
    if (sio)
	closeIo(sio);
    free(path1);
    free(path2);
}

/*
 * CopyLinkEnableFile() - copy everything not for this particular database
 */
int
CopyLinkEnableFile(const char *dbName, iofd_t sio, iofd_t dio)
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
LoadLinkEnableFile(const char *dbNameRestrict)
{
    char *path;
    int fd;

    safe_asprintf(&path, "%s/replication.enable", DefaultDBDir());
    if ((fd = open(path, O_RDONLY)) >= 0) {
	iofd_t io = allocIo(fd);
	char *str;

	while ((str = t_gets(io, 0)) != NULL) {
	    char *emsg = NULL;
	    char *tmp = str;
	    char *dbName = strsep(&tmp, " ");
	    char *countStr = dbName ? strsep(&tmp, " ") : "2";
	    char *priStr = countStr ? strsep(&tmp, " ") : "0";
	    char *flagsStr = priStr ? strsep(&tmp, " ") : "0";

	    if (dbName == NULL)
		continue;
	    if (dbNameRestrict && strcmp(dbNameRestrict, dbName) == 0) {
		char *args;

		safe_asprintf(&args, "%s:%s:%s", countStr, priStr, flagsStr);
		fprintf(stderr, "START LOCAL DATABASE %s:%s\n", dbName, args);
		SysControl(dbName, args, RPCMD_INITIAL_START, &emsg);
		safe_free(&args);
		safe_free(&emsg);
	    } else if (dbNameRestrict == NULL) {
		int fds[2];
		iofd_t iofd[2];

		if (socketpair(PF_LOCAL, SOCK_STREAM, 0, fds) < 0) {
		    perror("socketpair");
		    exit(1);
		}
		iofd[0] = allocIo(fds[0]);
		iofd[1] = allocIo(fds[1]);

		taskCreate(startReplicationConnectionThread, iofd[0]);
		SendControlOps(iofd[1], RPCMD_START_REPLICATOR, dbName, "", 1);
	    }
	}
	closeIo(io);
    }
    safe_free(&path);
}

/*
 * InitLinkEnableFile()	- Create a replication.enable file if none exists.
 */
void
InitLinkEnableFile(void)
{
    DIR *dir;
    const char *dirPath = DefaultDBDir();
    char *path1;
    char *path2;
    FILE *fo;
    struct stat st;

    safe_asprintf(&path1, "%s/replication.enable.new", dirPath);
    safe_asprintf(&path2, "%s/replication.enable", dirPath);
    if (stat(path2, &st) == 0) {
	safe_free(&path1);
	safe_free(&path2);
	return;
    }
    remove(path1);
    if ((fo = fopen(path1, "w")) == NULL) {
	safe_free(&path1);
	safe_free(&path2);
	fprintf(stderr, "Unable to create %s\n", path1);
	return;
    }

    /*
     * Startup databases
     */
    if ((dir = opendir(dirPath)) != NULL) {
	struct dirent *den;

	while ((den = readdir(dir)) != NULL) {
	    char *path;

	    if (den->d_name[0] == '.')
		continue;
	    safe_asprintf(&path, "%s/%s/sys.dt0", dirPath, den->d_name);
	    if (stat(path, &st) == 0) {
		fprintf(fo, "%s 2 0 0\n", den->d_name);
	    }
	    safe_free(&path);
	}
	closedir(dir);
    }
    fflush(fo);
    if (ferror(fo) == 0) {
	remove(path2);
	rename(path1, path2);
    }
    fclose(fo);
    safe_free(&path1);
    safe_free(&path2);
}

