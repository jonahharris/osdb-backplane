/*
 * SIMPLEHASH.C
 *
 * (c)Copyright 1999-2002 Backplane, Inc.  Please refer to the COPYRIGHT
 * file at the base of the distribution tree.
 *
 * $Backplane: rdbms/libsupport/simplehash.c,v 1.22 2002/08/20 22:05:55 dillon Exp $
 */

#include "defs.h"

Export void simpleHashInit(SimpleHash *sh);
Export void simpleHashInitBig(SimpleHash *sh, int hsize);
Export void simpleHashFree(SimpleHash *sh, void (*)(void *));
Export void *simpleHashLookup(SimpleHash *sh, const char *key);
Export int  simpleHashLookupCount(SimpleHash *sh, const char *key);
Export void *simpleHashLookupRandom(SimpleHash *sh, const char **key);
Export void *simpleHashEnter(SimpleHash *sh, const char *key, void *data);
Export const char *simpleHashEnterSelf(SimpleHash *sh, const char *key);
Export void *simpleHashRemove(SimpleHash *sh, const char *key);
Export void simpleHashIterate(SimpleHash *sh, void (*callback)(SimpleHash *sh, const char *key, void *data, void *arg), void *arg);
Export void simpleHashIterate2(SimpleHash *sh, void (*callback)(SimpleHash *sh, SimpleHashNode *sn, void *arg), void *arg);
Export int simpleHashCount(SimpleHash *sh);

#define SIMPLEHASH_DEFAULTSIZE		256

/*
 * simpleHashInit() - initialize a new SimpleHash structure
 *
 *	The structure is typicalled declared on the stack or embedded in 
 *	some other structure and passed to this routine for initialization.
 */
void
simpleHashInit(SimpleHash *sh)
{
    bzero(sh, sizeof(SimpleHash));
    sh->sh_Size = SIMPLEHASH_DEFAULTSIZE;
    sh->sh_Mask = sh->sh_Size - 1;
    sh->sh_Ary  = zalloc(sizeof(SimpleHashNode *) * sh->sh_Size);
    initList(&sh->sh_List);
}

/*
 * simpleHashInitBig() - same as simpleHashInit(), but specify hash size
 *
 *	note: hash table size MUST be a power of 2.
 */

void
simpleHashInitBig(SimpleHash *sh, int hsize)
{
    if (hsize < SIMPLEHASH_DEFAULTSIZE)
	hsize = SIMPLEHASH_DEFAULTSIZE;
    else {
	/* Make sure the hash size is a power of 2. */
	int pow2 = 1;
	while (pow2 > 0 && pow2 < hsize)
	    pow2 <<= 1;
	if (pow2 < 0)
	    pow2 = INT_MAX;
	hsize = pow2;
    }

    bzero(sh, sizeof(SimpleHash));
    sh->sh_Size = hsize;
    sh->sh_Mask = sh->sh_Size - 1;
    sh->sh_Ary  = zalloc(sizeof(SimpleHashNode *) * sh->sh_Size);
    initList(&sh->sh_List);
}

/* 
 * simpleHashFree() - free all elements in a SimpleHash table.
 *
 *	An optional destructor functional may be specified.  It will be
 *	called with the auxillary data associated with the SimpleHash.
 */
void
simpleHashFree(SimpleHash *sh, void (*ds)(void *))
{
    int			tableIndex;
    SimpleHashNode	*sn;
    SimpleHashNode	*next;

    if (sh == NULL || sh->sh_Size == 0)
	return;

    for (tableIndex=0; tableIndex < sh->sh_Size; tableIndex++) {
	sn = sh->sh_Ary[tableIndex];
	while (sn != NULL) {
	    safe_free(&sn->sn_Key);
	    if (ds)
		ds(sn->sn_Data);
	    next = sn->sn_Next;
	    free(sn);
	    sn = next;
	    --sh->sh_Count;
	}
    }
    DBASSERT(sh->sh_Count == 0);
    zfree(sh->sh_Ary, sizeof(SimpleHashNode *) * sh->sh_Size);
    return;
}

/* 
 * simpleHashLookup() - Lookup an element in a SimpleHash table.
 *
 *	The auxillary data of the entry is returned given the key, or
 *	NULL if the entry could not be found.
 */
void *
simpleHashLookup(SimpleHash *sh, const char *key)
{
    SimpleHashNode *sn;

    if (sh->sh_Size == 0)
	simpleHashInit(sh);
    for (sn = sh->sh_Ary[strhash(key, strlen(key)) & sh->sh_Mask];
	sn;
	sn = sn->sn_Next
    ) {
	if (strcmp(key, sn->sn_Key) == 0)
	    return(sn->sn_Data);
    }
    return(NULL);
}

void *
simpleHashLookupRandom(SimpleHash *sh, const char **key)
{
    int hv;
    int count = 0;
    SimpleHashNode *sn = NULL;

    if (sh->sh_Size == 0 || sh->sh_Count == 0) {
	*key = NULL;
	return(NULL);
    }
    hv = random();
    for (;;) {
	hv = (hv + 1) & sh->sh_Mask;
	for (sn = sh->sh_Ary[hv]; sn; sn = sn->sn_Next)
	    ++count;
	if (count) {
	    count = random() % count;
	    for (sn = sh->sh_Ary[hv]; count--; sn = sn->sn_Next)
		;
	    DBASSERT(sn != NULL);
	    break;
	}
    }
    *key = sn->sn_Key;
    return(sn->sn_Data);
}

/* 
 * simpleHashLookupCount() - Lookup a reference count for a given key.
 *
 *	The reference count is returned if a match is found for given
 *	key, or a 0 is returned.
 */
int
simpleHashLookupCount(SimpleHash *sh, const char *key)
{
    SimpleHashNode *sn;

    if (sh->sh_Size == 0)
	simpleHashInit(sh);
    for (sn = sh->sh_Ary[strhash(key, strlen(key)) & sh->sh_Mask];
	sn;
	sn = sn->sn_Next
    ) {
	if (strcmp(key, sn->sn_Key) == 0)
	    return(sn->sn_Count);
    }
    return (0);
}

/* 
 * simpleHashEnter() - Create or replace an element in a SimpleHash table.
 *
 *	Specify the key and auxillary data pointer to enter into the hash
 *	table.  A new node is added to the hash table unless the key is
 *	already in the table, in which case the existing node's aux data is
 *	replaced with the new aux data.
 *
 *	The old aux data (or NULL if there was no old aux data) is returned.
 *
 *	NOTE!  The simple hash functions do not attempt to allocate a copy
 *	of the data, since the type of the data is not known.  If the data
 *	is temporary, the caller should allocate and pass a copy to this
 *	function rather then the original data.
 */
void *
simpleHashEnter(SimpleHash *sh, const char *key, void *data)
{
    SimpleHashNode **psn;
    SimpleHashNode *sn;
    void *olddata = NULL;

    if (sh->sh_Size == 0)
	simpleHashInit(sh);
    for (psn = &sh->sh_Ary[strhash(key, strlen(key)) & sh->sh_Mask];
	(sn = *psn) != NULL;
	psn = &sn->sn_Next
    ) {
	if (strcmp(key, sn->sn_Key) == 0)
	    break;
    }
    if (sn == NULL) {
	*psn = sn = zalloc(sizeof(SimpleHashNode));
	sn->sn_Key = safe_strdup(key);
	sn->sn_Data = data;
	sn->sn_Count = 1;
	addTail(&sh->sh_List, &sn->sn_Node);
	++sh->sh_Count;
    } else {
	olddata = sn->sn_Data;
	sn->sn_Data = data;
	removeNode(&sn->sn_Node);
	sn->sn_Count++;
	addTail(&sh->sh_List, &sn->sn_Node);
    }
    return(olddata);
}

const char *
simpleHashEnterSelf(SimpleHash *sh, const char *key)
{
    SimpleHashNode **psn;
    SimpleHashNode *sn;

    if (sh->sh_Size == 0)
	simpleHashInit(sh);
    for (psn = &sh->sh_Ary[strhash(key, strlen(key)) & sh->sh_Mask];
	(sn = *psn) != NULL;
	psn = &sn->sn_Next
    ) {
	if (strcmp(key, sn->sn_Key) == 0)
	    break;
    }
    if (sn == NULL) {
	*psn = sn = zalloc(sizeof(SimpleHashNode));
	sn->sn_Key = safe_strdup(key);
	sn->sn_Data = sn->sn_Key;
	sn->sn_Count = 0;
	addTail(&sh->sh_List, &sn->sn_Node);
	++sh->sh_Count;
    } else {
	sn->sn_Data = sn->sn_Key;
	sn->sn_Count++;
	removeNode(&sn->sn_Node);
	addTail(&sh->sh_List, &sn->sn_Node);
    }
    return(sn->sn_Data);
}

/* 
 * simpleHashRemove() - Delete an element from a SimpleHash table.
 *
 *	The specified element is located and removed, and the aux data
 *	for the element is returned.  If the element cannot be found then
 *	NULL is returned.
 */
void *
simpleHashRemove(SimpleHash *sh, const char *key)
{
    void *data = NULL;
    SimpleHashNode *sn, **psn;

    for (psn = &sh->sh_Ary[strhash(key, strlen(key)) & sh->sh_Mask];
	(sn = *psn) != NULL;
	psn = &sn->sn_Next
    ) {
	if (strcmp(key, sn->sn_Key) == 0)
	    break;
    }
    
    if (sn) {
	*psn = sn->sn_Next;
	data = sn->sn_Data;
	removeNode(&sn->sn_Node);
	safe_free(&sn->sn_Key);
	zfree(sn, sizeof(SimpleHashNode));
	--sh->sh_Count;
	DBASSERT(sh->sh_Count >= 0);
    }
    
    return(data);
}

/* 
 * simpleHashIterate() - Iterate through all elements of a hash table
 *
 *	Iterates through all elements of a hash table and calls the 
 *	specified callback function with the key and auxdata for each.
 */
void
simpleHashIterate(SimpleHash *sh, void (*callback)(SimpleHash *sh, const char *key, void *data, void *arg), void *arg)
{
    SimpleHashNode *node;
    SimpleHashNode *next;

    for (node = getHead(&sh->sh_List); node; node = next) {
	next = getListSucc(&sh->sh_List, &node->sn_Node);
	callback(sh, node->sn_Key, node->sn_Data, arg);
    }
}


/* 
 * simpleHashIterate2() - Iterate through all elements of a hash table
 *
 *	Iterates through all elements of a hash table and calls the 
 *	specified callback function with the hash node and argument.
 */
void
simpleHashIterate2(SimpleHash *sh, void (*callback)(SimpleHash *sh, SimpleHashNode *sn, void *arg), void *arg)
{
    SimpleHashNode	*node;
    SimpleHashNode	*next;

    for (node = getHead(&sh->sh_List); node; node = next) {
	next = getListSucc(&sh->sh_List, &node->sn_Node);
	(*callback)(sh, node, arg);
    }
}


/* 
 * simpleHashCount() - Return the number of elements in the hash table
 */
int
simpleHashCount(SimpleHash *sh)
{
    return(sh->sh_Count);
}

