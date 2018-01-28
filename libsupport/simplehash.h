/*
 * SIMPLEHASH.H
 *
 * (c)Copyright 1999-2002 Backplane, Inc.  Please refer to the COPYRIGHT
 * file at the base of the distribution tree.
 *
 * $Backplane: rdbms/libsupport/simplehash.h,v 1.7 2002/08/20 22:05:55 dillon Exp $
 */

typedef struct SimpleHashNode {
    Node	sn_Node;
    struct SimpleHashNode *sn_Next;
    char *sn_Key;
    void *sn_Data;
    int	sn_Count;
} SimpleHashNode;

typedef struct SimpleHash {
    SimpleHashNode **sh_Ary;
    List	sh_List;
    int		sh_Size;
    int		sh_Mask;
    int		sh_Count;
} SimpleHash;

