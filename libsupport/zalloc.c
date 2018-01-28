/*
 * ZALLOC.C
 *
 * (c)Copyright 2000-2002 Backplane, Inc.  Please refer to the COPYRIGHT
 * file at the base of the distribution tree.
 *
 * $Backplane: rdbms/libsupport/zalloc.c,v 1.14 2002/08/20 22:05:55 dillon Exp $
 */

#include "defs.h"

Export void *_zalloc(int bytes);
Export void _zfree(void *ptr, int bytes);
Export void *_zalloc_debug(int bytes, const char *file, int line);
Export void _zfree_debug(void *ptr, int bytes, const char *file, int line);
Export void _zalloc_debug_dump(void);
Export int SafeFreeOpt;

int SafeFreeOpt = 0;

typedef struct MemInfo {
    Node	mi_Node;
    List	mi_List;
    const char *mi_File;
    int		mi_Line;
} MemInfo;

typedef struct MemDebug {
    Node	md_Node;
    int		md_Magic;
    int		md_Bytes;
} MemDebug;

List	MemDebugList = INITLIST(MemDebugList);

#define MD_MAGIC	0xEFC32791

/*
 * _zalloc() - (see zalloc() macro in export.h)
 *
 *	Allocate and zero the specified number of bytes.  Allocates made
 *	with zalloc() MUST be freed with zfree().  Do not mix zalloc/zfree
 *	with malloc/free.
 *
 *	Do not allow 0 (or negative) bytes to be allocated.
 */
void *
_zalloc(int bytes)
{
    void *ptr;

    DBASSERT(bytes > 0);

    if ((ptr = malloc(bytes)) == NULL)
	fatalmem();
    bzero(ptr, bytes);
    return(ptr);
}

/*
 * _zfree() - (see zfree() macro in export.h)
 *
 *	Free a structure previously allocated by zalloc().  You must free
 *	the exact number of bytes previously zalloc()'d.  Note that if 
 *	memory debugging is turned on, we track allocations precisely, 
 *	See the _*_debug functions later on in this file.
 */
void
_zfree(void *ptr, int bytes)
{
    DBASSERT(ptr != NULL);
    DBASSERT(bytes > 0);

    if (SafeFreeOpt)
	memset(ptr, -1, bytes);
    free(ptr);
}

/*
 * _zalloc_debug() -	zalloc() with tracking
 *
 *	This is used when MEMDEBUG is defined.  The allocation is explicitly
 *	tracked.
 */
void *
_zalloc_debug(int bytes, const char *file, int line)
{
    MemInfo *mi;
    MemDebug *md;
    static int dcount = 0;

    DBASSERT(bytes > 0);

    md = malloc(sizeof(MemDebug) + bytes + 1);
    if (++dcount == 10000) {
	_zalloc_debug_dump();
	dcount = 0;
    }

    for (
	mi = getHead(&MemDebugList); 
	mi; 
	mi = getListSucc(&MemDebugList, &mi->mi_Node)
    ) {
	if (mi->mi_Line == line && strcmp(file, mi->mi_File) == 0)
	    break;
    }
    if (mi == NULL) {
	mi = malloc(sizeof(MemInfo));
	bzero(mi, sizeof(MemInfo));
	initList(&mi->mi_List);
	addTail(&MemDebugList, &mi->mi_Node);
	mi->mi_Line = line;
	mi->mi_File = file;
    }
    bzero(md, sizeof(MemDebug) + bytes);
    addHead(&mi->mi_List, &md->md_Node);
    md->md_Magic = MD_MAGIC;
    md->md_Bytes = bytes;
    ((char *)(md + 1))[bytes] = 0xAA;
    return(md + 1);
}

/*
 * zfree_debug() - free zalloc'd structure, with tracking
 *
 *	This is used when MEMDEBUG is defined.  The allocation is explicitly
 *	tracked.  A tracking failure results in an assertion and core dump.
 *
 *	Do not allow NULL frees.
 */
void 
_zfree_debug(void *ptr, int bytes, const char *file, int line)
{
    MemDebug *md = (MemDebug *)ptr - 1;

    DBASSERT(ptr != NULL);

    DBASSERT(md->md_Magic == MD_MAGIC);
    DBASSERT(md->md_Bytes == bytes);
    DBASSERT(((char *)(md + 1))[bytes] == (char)0xAA);
    removeNode(&md->md_Node);
    memset(md, -1, sizeof(MemDebug) + bytes + 1);
    free(md);
}

/*
 * _zalloc_debug_dump() - dump debugging histogram on allocations
 *
 *	When debugging is enabled, a histogram dump is made to stderr
 *	every so often showing where current allocations are coming 
 *	from.
 */
void
_zalloc_debug_dump(void)
{
    MemInfo *mi;

    for (
	mi = getHead(&MemDebugList); 
	mi; 
	mi = getListSucc(&MemDebugList, &mi->mi_Node)
    ) {
	int count = 0;
	int bytes = 0;
	MemDebug *md;

	for (
	    md = getHead(&mi->mi_List); 
	    md; 
	    md = getListSucc(&mi->mi_List, &md->md_Node)
	) {
	    ++count;
	    bytes += md->md_Bytes;
	}
	if (count) {
	    fprintf(stderr, "  %s:%d\tbytes=%d\tcount=%d\n",
		mi->mi_File, mi->mi_Line, bytes, count
	    );
	}
    }
    fprintf(stderr, "\n");
}

