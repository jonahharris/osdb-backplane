/*
 * DRECOVER.C	- Recover a database
 *
 * (c)Copyright 2000-2002 Backplane, Inc.  Please refer to the COPYRIGHT
 * file at the base of the distribution tree.
 *
 * $Backplane: rdbms/utils/drecover.c,v 1.6 2002/08/20 22:06:06 dillon Exp $
 *
 * DRECOVER [-D dbdir] database
 */

#include "defs.h"
#include "libdbcore/dbcore-protos.h"
#include <ctype.h>
#include <dirent.h>
#include <libdbcore/btree.h>

DataBase *Db;

static void RecoverIndex(const char *dirPath, const char *filePath, int fd);
static void RecoverTemporaryTableSpace(const char *dirPath, const char *filePath, int fd);
static void RecoverTableFile(const char *dirPath, const char *filePath, int fd);

static int openAndLockFile(const char *filePath);
static void closeAndUnlockFile(int fd);

int
task_main(int ac, char **av)
{
    int i;
    char *dataBase = NULL;
    const char *dbDir = DefaultDBDir();
    char *dirPath = NULL;
    DIR *dir;
    struct dirent *den;

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
	case 'D':
	    dbDir = (*ptr) ? ptr : av[++i];
	    break;
	default:
	    fprintf(stderr, "Unknown option: %s\n", ptr - 2);
	    exit(1);
	}
    }
    if (i > ac)
	fatal("last option required argument");

    if (dataBase == NULL) {
	fprintf(stderr, "Version 1.00\n");
	fprintf(stderr, "%s [-D dbbasedir] database\n", av[0]);
	exit(1);
    }
    if (dataBase[0] == '/')
	dbDir = NULL;

    /*
     * Directory path for database
     */
    if (dbDir == NULL) {
        dirPath = strdup(dataBase);
    } else {
        safe_asprintf(&dirPath, "%s/%s", dbDir, dataBase);
    }

    /*
     * Scan the directory, recover index and table files
     */
    if ((dir = opendir(dirPath)) == NULL) {
	fprintf(stderr, "Unable to find database directory at %s\n", dirPath);
	exit(1);
    }
    while ((den = readdir(dir)) != NULL) {
	const char *ptr;
	char *filePath;
	int fd;

	if ((ptr = strrchr(den->d_name, '.')) == NULL)
	    continue;
	safe_asprintf(&filePath, "%s/%s", dirPath, den->d_name);
	if (ptr[1] == 'o' && strlen(ptr + 1) == 3) {
	    if ((fd = openAndLockFile(filePath)) >= 0) {
		RecoverIndex(dirPath, filePath, fd);
		closeAndUnlockFile(fd);
	    }
	}
	if (strcmp(ptr, ".tts") == 0) {
	    if ((fd = openAndLockFile(filePath)) >= 0) {
		RecoverTemporaryTableSpace(dirPath, filePath, fd);
		closeAndUnlockFile(fd);
	    }
	}
	if (strcmp(ptr, ".dt0") == 0) {
	    if ((fd = openAndLockFile(filePath)) >= 0) {
		RecoverTableFile(dirPath, filePath, fd);
		closeAndUnlockFile(fd);
	    }
	}
	safe_free(&filePath);
    }
    closedir(dir);
    return(0);
}

static void
RecoverIndex(const char *dirPath, const char *filePath, int fd)
{
    IndexHead ih;

    if (read(fd, &ih, sizeof(ih)) != sizeof(ih)) {
	printf("Removing truncated index file");
	remove(filePath);
	return;
    }
    lseek(fd, 0L, 0);

    switch(ih.ih_Magic) {
    case BT_MAGIC:
	if (ih.ih_Version == BT_VERSION) {
	    BTreeHead bt;
	    if (read(fd, &bt, sizeof(bt)) != sizeof(bt)) {
		printf("Removing truncated btree index file");
		remove(filePath);
		break;
	    }
	    if ((bt.bt_Flags & BTF_SYNCED) == 0) {
		printf("Removing unsynchronized btree index file");
		remove(filePath);
	    } else {
		printf("FILE OK");
	    }
	} else {
	    printf("Removing version %d index file", ih.ih_Version);
	    remove(filePath);
	}
	break;
    default:
	printf("Removing unknown index file");
	remove(filePath);
	break;
    }
}

static void
RecoverTemporaryTableSpace(const char *dirPath, const char *filePath, int fd)
{
    printf("Removing unneeded tts file");
    remove(filePath);
}

static void
RecoverTableFile(const char *dirPath, const char *filePath, int fd)
{
}

static int
openAndLockFile(const char *filePath)
{
    int fd = -1;
    int retry = 1;

    if (strrchr(filePath, '/'))
	printf("%s:\t", strrchr(filePath, '/') + 1);
    else
	printf("%s:\t", filePath);
    fflush(stdout);
    while (retry) {
	retry = 0;
	if ((fd = open(filePath, O_RDWR)) < 0) {
	    printf("Unable to open R+W\n");
	} else {
	    struct stat st1;
	    struct stat st2;

	    flock(fd, LOCK_EX);
	    if (fstat(fd, &st1) < 0 || stat(filePath, &st2) < 0) {
		printf("Unable to stat\n");
	    } else if (st1.st_ino != st2.st_ino) {
		retry = 1;
		close(fd);
		fd = -1;
	    } else {
		/* we are good */
		break;
	    }
	    flock(fd, LOCK_UN);
	}
    }
    return(fd);
}

static void
closeAndUnlockFile(int fd)
{
    flock(fd, LOCK_UN);
    printf("\n");
    fflush(stdout);
}

