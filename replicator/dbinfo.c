/*
 * REPLICATOR/DBINFO.C
 *
 * (c)Copyright 2000-2002 Backplane, Inc.  Please refer to the COPYRIGHT
 * file at the base of the distribution tree.
 *
 * This module manages DBInfo structures, which are used to track both local
 * and remote databases.
 *
 * $Backplane: rdbms/replicator/dbinfo.c,v 1.3 2002/08/20 22:06:00 dillon Exp $
 */

#include "defs.h"
#include <pwd.h>

Prototype DBInfo *AllocDBInfo(const char *dbName);
Prototype DBInfo *FindDBInfo(const char *dbName, int abbr, int *error);
Prototype DBInfo *FindDBInfoIfLocal(const char *dbName, int abbr, int *error);
Prototype void PutDBInfo(DBInfo *d);
Prototype void DoneDBInfo(DBInfo *d);

static void destroyDBInfo(DBInfo *d);

Prototype DBInfo	*DBInfoBase;

DBInfo		*DBInfoBase;

/*
 * AllocDBInfo() -	Locate a DBInfo structure, create one if we can't
 *			find a match.
 *
 *	This function returns with the Ref count bumped.
 */

DBInfo *
AllocDBInfo(const char *dbName)
{
    DBInfo *d;

    if ((d = FindDBInfo(dbName, 0, NULL)) == NULL) {
	d = zalloc(sizeof(DBInfo));
	d->d_DBName = safe_strdup(dbName);
	d->d_Next = DBInfoBase;
	d->d_UseCount = 1;
	initList(&d->d_CIList);
	initList(&d->d_CDList);
	DBInfoBase = d;
    }
    return(d);
}

void
PutDBInfo(DBInfo *d)
{
    DBASSERT(d->d_Refs > 0);
    --d->d_Refs;
    if (d->d_UseCount == 0 && d->d_Refs == 0)
	destroyDBInfo(d);
}

void
DoneDBInfo(DBInfo *d)
{
    DBASSERT(d->d_UseCount > 0);
    --d->d_UseCount;
    if (d->d_UseCount == 0 && d->d_Refs == 0)
	destroyDBInfo(d);
}

/*
 * FindDBInfo() -	Locate the DBInfo structure associated with a 
 *			database in the replication space.
 *
 *	Locate and return a DBInfo structure.  If non-NULL, the UseCount
 *	will be bumped (release by using DoneDBInfo()).
 */
DBInfo *
FindDBInfo(const char *dbName, int abbr, int *error)
{
    DBInfo *d;
    DBInfo *dres = NULL;
    int len = strlen(dbName);

    if (error)
	*error = 0;
    for (d = DBInfoBase; d; d = d->d_Next) {
	if (abbr) {
	    const char *ptr;

	    if ((ptr = strrchr(d->d_DBName, '.')) != NULL) {
		if (ptr - d->d_DBName == len && 
		    strncmp(d->d_DBName, dbName, len) == 0
		) {
		    /*
		     * If we find duplicate databases with the same name,
		     * some bozo created a duplicate unsynchronized database
		     * in the replication group.
		     */
		    if (dres) {
			fprintf(stderr, "*** Error databases unsynchronized: %s, %s\n", dres->d_DBName, d->d_DBName);
			if (error)
			    *error = FDBERR_DUPLICATE;
			dres = NULL;
			break;
		    }
		    dres = d;
		}
	    }
	}
	if (strcmp(d->d_DBName, dbName) == 0) {
	    dres = d;
	    break;
	}
    }
    if (dres)
	++dres->d_UseCount;
    return(dres);
}

DBInfo *
FindDBInfoIfLocal(const char *dbName, int abbr, int *error)
{
    DBInfo *d;

    if ((d = FindDBInfo(dbName, abbr, error)) != NULL) {
	if ((d->d_Flags & DF_LOCALRUNNING) == 0) {
	    DoneDBInfo(d);
	    d = NULL;
	}
    }
    return(d);
}

static void
destroyDBInfo(DBInfo *d)
{
    DBInfo **pd;

    DBASSERT(d->d_RHInfoBase == NULL);
    DBASSERT(d->d_CIList.li_Node.no_Next == &d->d_CIList.li_Node);
    DBASSERT(d->d_CDList.li_Node.no_Next == &d->d_CDList.li_Node);

    for (pd = &DBInfoBase; *pd != d; pd = &(*pd)->d_Next)
	;
    *pd = d->d_Next;
    d->d_Next = NULL;
    free(d->d_DBName);
    zfree(d, sizeof(DBInfo));
}

