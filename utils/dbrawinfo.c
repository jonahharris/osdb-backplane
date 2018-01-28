/*
 * UTILS/DBRAWINFO.E
 *
 * (c)Copyright 2000-2002 Backplane, Inc.  Please refer to the COPYRIGHT
 * file at the base of the distribution tree.
 *
 *	Dump the history of the specified database to stdout
 *
 * $Backplane: rdbms/utils/dbrawinfo.c,v 1.3 2002/08/20 22:06:06 dillon Exp $
 */

#include "defs.h"
#include <ctype.h>
#include <sys/time.h>
#include <sys/stat.h>

union Head {
    BlockHead	h_Block;
    TableFile	h_Table;
};

static void Display(const char *path, struct stat *st, union Head *head);
static int UseGmt;

int
main(int ac, char **av)
{
    int i;
    int fd;
    int count = 0;
    int acount = 16;
    char **path = malloc(sizeof(char *) * acount);
    union Head head;

    for (i = 1; i < ac; ++i) {
	char *ptr = av[i];

	if (*ptr != '-') {
	    if (count == acount) {
		acount = acount * 2;
		path = realloc(path, sizeof(char *) * acount);
	    }
	    path[count++] = ptr;
	    continue;
	}
	ptr += 2;
	switch(ptr[-1]) {
	case 'D':
	    SetDefaultDBDir((*ptr) ? ptr : av[++i]);
	    break;
	case 'G':
	    if (strcmp(ptr - 1, "GMT") == 0) {
		UseGmt = 1;
		break;
	    }
	    /* fall through */
	default:
	    fprintf(stderr, "Unknown option: %s\n", ptr - 2);
	    exit(1);
	}
    }
    if (count == 0) {
	fprintf(stderr, "Version 1.00\n");
	fprintf(stderr, "%s [-D dbdir] [-q] file(s)\n", av[0]);
	fprintf(stderr, "Prints out the raw header information for the specified physical file\n");
	exit(1);
    }
    for (i = 0; i < count; ++i) {
	struct stat st;
	if ((fd = open(path[i], O_RDONLY)) < 0)
	    fatal("Unable to open physical file: %s\n", path[i]);
	if (fstat(fd, &st) < 0)
	    fatal("Unable to stat file: %s\n", path[i]);
	bzero(&head, sizeof(head));
	read(fd, &head, sizeof(head));
	Display(path[i], &st, &head);
	close(fd);
    }
    return(0);
}

static void
Display(const char *path, struct stat *st, union Head *head)
{
    char *alloc = NULL;
    dbstamp_t ts;

    printf("*** FILE %s\n", path);
    printf("BlockHeader.Magic\t0x%08x\n", head->h_Block.bh_Magic);
    printf("BlockHeader.Type\t0x%08x", head->h_Block.bh_Type);
    switch(head->h_Block.bh_Type) {
    case BH_TYPE_TABLE:
	printf(" (TABLE)\n");
	break;
    case BH_TYPE_FREE:
	printf(" (FREE)\n");
	break;
    case BH_TYPE_DATA:
	printf(" (DATA)\n");
	break;
    }

    printf("BlockHeader.CRC\t\t0x%08x\n", (unsigned int)head->h_Block.bh_CRC);

    switch(head->h_Block.bh_Type) {
    case BH_TYPE_TABLE:
	printf("Table.Version\t\t0x%08x [%d]\n",
	    (unsigned int)head->h_Table.tf_Version, head->h_Table.tf_Version);
	printf("Table.HeadSize\t\t0x%08x [%d]\n",
	    (unsigned int)head->h_Table.tf_HeadSize, head->h_Table.tf_HeadSize);
	printf("Table.AppendInc\t\t0x%08x [%d]\n", 
	    head->h_Table.tf_AppendInc, head->h_Table.tf_AppendInc);
	printf("Table.Flags\t\t0x%08x\n", (int)head->h_Table.tf_Flags);
	printf("Table.BlockSize\t\t0x%08x [%d]\n",
	    (unsigned int)head->h_Table.tf_BlockSize, head->h_Table.tf_BlockSize);
	printf("Table.DataOff\t\t0x%016llx [%lld]\n", 
	    head->h_Table.tf_DataOff, head->h_Table.tf_DataOff);
	printf("Table.FileSize\t\t0x%016llx [%lld] (Actual %016llx [%lld])\n",
	    head->h_Table.tf_FileSize, head->h_Table.tf_FileSize,
	    (long long)st->st_size, (long long)st->st_size);
	printf("Table.Append\t\t0x%016llx [%lld]\n",
	    head->h_Table.tf_Append, head->h_Table.tf_Append);
	ts = head->h_Table.tf_HistStamp;
	printf("Table.HistStamp\t\t0x%016llx (%s)\n",
	    ts, dbstamp_to_ascii(ts, UseGmt, &alloc));
	ts = head->h_Table.tf_SyncStamp;
	printf("Table.SyncStamp\t\t0x%016llx (%s)\n",
	    ts, dbstamp_to_ascii(ts, UseGmt, &alloc));
	ts = head->h_Table.tf_NextStamp;
	printf("Table.NextStamp\t\t0x%016llx (%s)\n",
	    ts, dbstamp_to_ascii(ts, UseGmt, &alloc));
	ts = head->h_Table.tf_Generation;
	printf("Table.Generation\t0x%016llx (%s)\n",
	    ts, dbstamp_to_ascii(ts, UseGmt, &alloc));
	ts = head->h_Table.tf_CreateStamp;
	printf("Table.CreateStamp\t0x%016llx (%s)\n",
	    ts, dbstamp_to_ascii(ts, UseGmt, &alloc));
	printf("Table.Name\t%s\n", head->h_Table.tf_Name);
	printf("Table.Error\t\t%d\n", head->h_Table.tf_Error);
	break;
    case BH_TYPE_FREE:
	break;
    case BH_TYPE_DATA:
	break;
    }
    printf("\n");
    safe_free(&alloc);
}

