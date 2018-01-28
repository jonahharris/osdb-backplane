/*
 * LIBCLIENT/CLIENT.C	- Core support functions for client interface,
 *			  for user and replicator.
 *
 * (c)Copyright 2000-2002 Backplane, Inc.  Please refer to the COPYRIGHT 
 * file at the base of the distribution tree.
 *
 * $Backplane: rdbms/libclient/client.c,v 1.20 2003/04/27 20:28:08 dillon Exp $
 */

#include "defs.h"

Export database_t AllocCLDataBase(const char *dbName, iofd_t fd);
Export database_t AllocCLInstance(database_t parCd, iofd_t fd);
Export void FreeCLDataBase(database_t cd);
Export void FreeCLInstance(database_t cd);
Export database_t FindCLDataBase(const char *dbName, int abbr);
Export void RefCLDataBase(database_t cd);
Export void SetCLDataBaseName(database_t cd, const char *dbName);

Prototype void InitCLDataBase(CLDataBase *cd, iofd_t fd);

List CLDataBaseList[CLDB_HSIZE];
List CLDummyList = INITLIST(CLDummyList);

static __inline
int
cldbHash(const char *dbName)
{
    int hv = 0xAF43BC38;
    int i;

    for (i = 0; dbName[i] && dbName[i] != '.'; ++i)
	hv = (hv << 5) ^ (hv >> 23) ^ (u_int8_t)dbName[i];
    hv ^= hv >> 16;
    return(hv & CLDB_HMASK);
}

void
InitCLDataBase(CLDataBase *cd, iofd_t fd)
{
    cd->cd_Ior = fd;
    cd->cd_Iow = dupIo(fd);
    cd->cd_Refs = 1;
    cd->cd_Pid = -1;
    cd->cd_Lock = allocTLock();
#if 0
    initList(&cd->cd_ReqList);
#endif
    initList(&cd->cd_List);
    addTail(&CLDummyList, &cd->cd_Node);
}

/*
 * AllocCLDataBase() - Allocate client infrastructuer for a descriptor
 *
 *	This routine is typically used by the replication server to
 *	construct the slave side of a client connection.
 *
 */
CLDataBase *
AllocCLDataBase(const char *dbName, iofd_t fd)
{
    CLDataBase *cd;

    if (OCLLock == NULL)
	OCLLock = allocTLock();

    cd = zalloc(sizeof(CLDataBase));
    cd->cd_Type = CD_TYPE_DATABASE;
    InitCLDataBase(cd, fd);
    if (dbName)
	SetCLDataBaseName(cd, dbName);
    return(cd);
}

void
SetCLDataBaseName(CLDataBase *cd, const char *dbName)
{
    List *list;

    safe_free(&cd->cd_DBName);
    removeNode(&cd->cd_Node);
    list = &CLDataBaseList[cldbHash(dbName)];
    if (list->li_Node.no_Next == NULL)
	initList(list);
    addTail(list, &cd->cd_Node);
    cd->cd_DBName = strdup(dbName);
}

CLDataBase * 
AllocCLInstance(CLDataBase *parCd, iofd_t fd)
{
    CLDataBase *cd;

    cd = zalloc(sizeof(CLDataBase));
    InitCLDataBase(cd, fd);
    removeNode(&cd->cd_Node);
    addTail(&parCd->cd_List, &cd->cd_Node);
    cd->cd_Parent = parCd;
    cd->cd_Type = CD_TYPE_INSTANCE;
    cd->cd_StampId = parCd->cd_StampId;
    if (parCd->cd_DBName)
	cd->cd_DBName = strdup(parCd->cd_DBName);
    ++parCd->cd_Refs;
    return(cd);
}

void
FreeCLDataBase(CLDataBase *cd)
{
    DBASSERT(cd->cd_Refs == 0);

    /*
     * Abort any I/O in progress XXX
     */
    abortIo(cd->cd_Ior);
    abortIo(cd->cd_Iow);

    /*
     * Close the descriptors
     */
    closeIo(cd->cd_Ior);
    cd->cd_Ior = IOFD_NULL;

    closeIo(cd->cd_Iow);
    cd->cd_Iow = IOFD_NULL;

    freeTLock(&cd->cd_Lock);
    safe_free(&cd->cd_DBName);
    safe_free(&cd->cd_HelloHost);

    removeNode(&cd->cd_Node);
    if (cd->cd_Parent) {
	--cd->cd_Parent->cd_Refs;
	cd->cd_Parent = NULL;
    }
    zfree(cd, sizeof(CLDataBase));
}

void
FreeCLInstance(CLDataBase *cd)
{
    FreeCLDataBase(cd);
}

CLDataBase *
FindCLDataBase(const char *dbName, int abbr)
{
    List *list;
    CLDataBase *cd = NULL;
    int len = strlen(dbName);

    list = &CLDataBaseList[cldbHash(dbName)];
    if (list->li_Node.no_Next != NULL) {
	for (cd = getHead(list); cd; cd = getListSucc(list, &cd->cd_Node)) {
	    if (abbr) {
		const char *ptr;
		if ((ptr = strrchr(cd->cd_DBName, '.')) != NULL) {
		    if (len == ptr - cd->cd_DBName &&
			strncmp(cd->cd_DBName, dbName, len) == 0
		    ) {
			++cd->cd_Refs;
			break;
		    }
		}
	    } 
	    if (strcmp(dbName, cd->cd_DBName) == 0) {
		++cd->cd_Refs;
		break;
	    }
	}
    }
    return(cd);
}

void
RefCLDataBase(CLDataBase *cd)
{
    ++cd->cd_Refs;
}

