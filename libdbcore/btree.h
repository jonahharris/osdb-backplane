/*
 * LIBDBCORE/BTREE.H	- Balanced tree node format
 *
 * (c)Copyright 2000-2002 Backplane, Inc.  Please refer to the COPYRIGHT
 * file at the base of the distribution tree.
 *
 * $Backplane: rdbms/libdbcore/btree.h,v 1.19 2002/10/03 21:12:18 dillon Exp $
 *
 * Notes on BTree node cache:
 *
 *	We cache up to half a gig of btree data globally in our mmap()'d
 *	space.
 *
 *	NOTE! Minimum data length is sizeof(dbstamp_t), usually 8 bytes
 */

#define BT_MAXELM		64		    /* elements per node */
#define BT_DATALEN		8		    /* cached key data */
#define BT_INDEXMASK		(BT_MAXELM - 1)	    /* used to align data */
#define BT_CACHESIZE		(64 * 1024)
#define BT_CACHEMASK		(BT_CACHESIZE - 1)
#define BT_MAXCACHE		(MAX_BTREE_CACHE / BT_CACHESIZE)

/*
 * The database does not require the BTree index to be in synch.  BT_SLOP
 * is the amount of slop we allow before we try to bring the BTree up to
 * date.  This improves performance for both small and large transactions.
 * However, please note that unsynchronized records are scanned sequentially
 * and too-large a SLOP can result in O(N^2) behavior (N = unindexed records).
 * We recommend a value between 512 bytes and 4K.
 */
#define BT_SLOP			(1 * 1024)

#define BTREE_HSIZE		(BT_MAXCACHE * 2)
#define BTREE_HMASK		(BTREE_HSIZE - 1)

typedef struct BTreeElm {
    dboff_t	be_Ro;		/* offset of subtree or phys tab if leaf */
    int16_t	be_Len;		/* length as stored (up to BT_DATALEN) */
    u_int16_t	be_Flags;	/* (reserved) */
    u_int8_t	be_Data[BT_DATALEN];
} BTreeElm;

#define BEF_DELETED	0x0001	/* indicates leaf element marked deleted */

/*
 * BTreeNode
 *
 */

typedef struct BTreeNode {
    dboff_t	bn_Parent;	/* offset of parent, ORd with index */
    int16_t	bn_Count;
    u_int16_t	bn_Flags;
    BTreeElm	bn_Elms[BT_MAXELM];
} BTreeNode;

#define BNF_LEAF	0x0001

typedef struct BTreeHead {
    IndexHead	bt_Head;
    dboff_t	bt_Root;	/* offset of root node */
    dboff_t	bt_TabAppend;	/* we are indexed up to this point */
    dboff_t	bt_Append;	/* append point for new records */
    dboff_t	bt_ExtAppend;	/* append point for file extension */
    dboff_t	bt_FirstElm;	/* (leaf) location of first element */
    dboff_t	bt_LastElm;	/* (leaf) location of last element */
    dbstamp_t	bt_Generation;	/* generation number */
} BTreeHead;

#define bt_Flags	bt_Head.ih_Flags
#define bt_Magic	bt_Head.ih_Magic
#define bt_Version	bt_Head.ih_Version
#define bt_HeadSize	bt_Head.ih_HeadSize

#define BTF_SYNCED	0x00000001	/* btree file is intact */
#define BTF_TEMP	0x00000002	/* temporary index */

#define BT_MAGIC	0x4255FCD2
#define BT_VERSION	2

/*
 * BTreeInsert() function flags
 */
#define BIF_FIRST	0x0001
#define BIF_LAST	0x0002

