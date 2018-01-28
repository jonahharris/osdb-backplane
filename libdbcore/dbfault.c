/*
 * LIBDBCORE/DBFAULT.C - dummy routines to fault-out on illegal table ops
 *
 * (c)Copyright 1999-2002 Backplane, Inc.  Please refer to the COPYRIGHT
 * file at the base of the distribution tree.
 *
 * $Backplane: rdbms/libdbcore/dbfault.c,v 1.9 2002/08/20 22:05:51 dillon Exp $
 */

#include "defs.h"

Prototype void Fault_OpenTableMeta(Table *tab, DBCreateOptions *dbc, int *error);
Prototype void Fault_CloseTableMeta(Table *tab);
Prototype void Fault_CacheTableMeta(Table *tab);
Prototype int Fault_ExtendFile(Table *tab, int bytes, void *bh, int bhSize);
Prototype int Fault_WriteFile(dbpos_t *pos, void *ptr, int bytes);
Prototype void Fault_TruncFile(Table *tab, int bytes);
Prototype void Fault_CleanSlate(Table *tab);
Prototype dboff_t Default_FirstBlock(Table *tab);
Prototype dboff_t Default_NextBlock(Table *tab, const BlockHead *bh, dboff_t ro);

void 
Fault_OpenTableMeta(Table *tab, DBCreateOptions *dbc, int *error)
{
    DBASSERT(0);
}

void
Fault_CloseTableMeta(Table *tab)
{
    DBASSERT(0);
}

void
Fault_CacheTableMeta(Table *tab)
{
    DBASSERT(0);
}

int
Fault_ExtendFile(Table *tab, int bytes, void *bh, int bhSize)
{
    DBASSERT(0);
    return(0);
}

void
Fault_TruncFile(Table *tab, int bytes)
{
    DBASSERT(0);
}

int 
Fault_WriteFile(dbpos_t *pos, void *ptr, int bytes)
{
    DBASSERT(0);
    return(0);
}

void 
Fault_CleanSlate(Table *tab)
{
    DBASSERT(0);
}

dboff_t
Default_FirstBlock(Table *tab)
{
    DBASSERT(tab->ta_Meta->tf_DataOff != 0);
    return(tab->ta_Meta->tf_DataOff);
}

dboff_t
Default_NextBlock(Table *tab, const BlockHead *bh, dboff_t ro)
{
    return(ro + tab->ta_Meta->tf_BlockSize);
}

