/*
 * LIBDBCORE/DELETE.C	- Implements delete filtering
 *
 * (c)Copyright 1999-2002 Backplane, Inc.  Please refer to the COPYRIGHT
 * file at the base of the distribution tree.
 *
 * $Backplane: rdbms/libdbcore/delete.c,v 1.15 2002/08/20 22:05:51 dillon Exp $
 *
 *	Track record deletions in order to match them up against earlier
 *	record insertions, canceling out the insertion.  Deletions have
 *	the same contents as the original insertion in order to guarentee
 *	that a query will encounter both records.  Under normal circumstances
 *	all deletion records encountered in the query will have been matched
 *	up by the completion of the query.  If they aren't it is an indication
 *	of a corrupt database and we assert.  There are a few exceptions to
 *	this rule but not many.
 *
 *	Whenever possible the query scan iterates through a table backwards,
 *	making it possible to implment the table scan / delete matching in
 *	a single pass.
 */

#include "defs.h"

Prototype void InitDelHash(DelHash *dh);
Prototype void DoneDelHash(DelHash *dh);
Prototype void SaveDelHash(DelHash *dh, dbpos_t *pos, rhhash_t hv, int recSize);
Prototype int MatchDelHash(DelHash *dh, const RecHead *rh);

/*
 * DelNode/DelHash - track deletions across physical table boundries
 */

#define DHSIZE  8192
#define DHMASK  (DHSIZE-1)

typedef struct DelNode {
    struct DelNode      *dn_Next;
    struct DelHash	*dn_DelHash;
    rhhash_t            dn_Hv;
    dbpos_t             dn_Pos;         /* location of record */
    int                 dn_RecSize;     /* size of record */
} DelNode;

static DelNode	*DelHashAry[DHSIZE];

void
InitDelHash(DelHash *dh)
{
    bzero(dh, sizeof(DelHash));
}

void
DoneDelHash(DelHash *dh)
{
    /*
     * If we were interrupted then we have to scan
     * the delete hash for partial entries.  If we
     * have WHERE clauses on special header fields
     * (e.g. __timestamp), deletions may not match
     * up either.  Otherwise there had better not be
     * any partial entries.
     */
    if (dh->dh_Count && (dh->dh_Flags & (DHF_SPECIAL|DHF_INTERRUPTED))) {
	int i;
	for (i = 0; i < DHSIZE; ++i) {
	    DelNode **pdn = &DelHashAry[i];
	    DelNode *dn;

	    while ((dn = *pdn) != NULL) {
		if (dn->dn_DelHash == dh) {
		    --dh->dh_Count;
		    *pdn = dn->dn_Next;
		    zfree(dn, sizeof(DelNode));
		} else {
		    pdn = &dn->dn_Next;
		}
	    }
	}
    }
    DBASSERT(dh->dh_Count == 0);
}

void
SaveDelHash(DelHash *dh, dbpos_t *pos, rhhash_t hv, int recSize)
{
    DelNode *dn = zalloc(sizeof(DelNode));
    DelNode **pdn = &DelHashAry[hv & DHMASK];

    dn->dn_Next = *pdn;		/* XXX insert @ head not optimal */
    dn->dn_DelHash = dh;
    dn->dn_Pos = *pos;
    dn->dn_Hv = hv;
    dn->dn_RecSize = recSize;
    *pdn = dn;
    ++dh->dh_Count;
}

/*
 * MatchDelHash() - match a deletion up against the original record
 *	
 *	Note that all deletions must be matched up against their original
 *	record or the database is considered corrupt.  There is an assertion
 *	in index.c which checks.  Thus we do not need a specific DelHash
 *	close routine.
 *
 *	Note that dn_IRo is not valid as stored since other clients might
 *	insert data into the tree while we are twiddling our thumbs.
 */

int
MatchDelHash(DelHash *dh, const RecHead *rh)
{
    DelNode *dn;
    DelNode **pdn = &DelHashAry[rh->rh_Hv & DHMASK];
    int r = -1;

    while (r < 0 && (dn = *pdn) != NULL) {
	if (dn->dn_DelHash == dh &&
	    rh->rh_Hv == dn->dn_Hv &&
	    rh->rh_Size == dn->dn_RecSize
	) {
	    DataMap *dm = NULL;
	    const RecHead *orh;
	    int dataSize = rh->rh_Size - offsetof(RecHead, rh_Cols[0]);

	    orh = dn->dn_Pos.p_Tab->ta_GetDataMap(&dn->dn_Pos,&dm,rh->rh_Size);
	    if (orh && 
		orh->rh_Size == rh->rh_Size &&
		bcmp(&rh->rh_Cols[0], &orh->rh_Cols[0], dataSize) == 0
	    ) {
		r = 0;
		--dh->dh_Count;
		*pdn = dn->dn_Next;
		zfree(dn, sizeof(DelNode));
	    }
	    dm->dm_Table->ta_RelDataMap(&dm, 0);
	}
	pdn = &dn->dn_Next;	/* pdn invalid if r==0, is ok */
    }
    return(r);
}

