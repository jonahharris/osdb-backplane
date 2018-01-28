/*
 * LISTS.H
 *
 * (c)Copyright 1999-2002 Backplane, Inc.  Please refer to the COPYRIGHT
 * file at the base of the distribution tree.
 *
 * $Backplane: rdbms/libsupport/lists.h,v 1.7 2002/08/20 22:05:55 dillon Exp $
 *
 *	Simple Doubly linked lists.
 *
 *	This implementation implements a doubly linked list using a circular
 *	queue model where the list head is part of the circle.  We also
 *	support circular lists which do not have a list head (see initCNode)
 */

typedef struct Node {
    struct Node *no_Next;
    struct Node *no_Prev;
} Node;

typedef struct List {
    Node	li_Node;
} List;

#define INITLIST(list)	{ { &list.li_Node, &list.li_Node } }
#define INITCNODE(node)	{ &node, &node }

/*
 * initList() -	Initialize a new empty list
 *
 *	The list head consists of a circularly linked node, linked only
 *	to itself.
 */

static __inline
void
initList(List *list)
{
    list->li_Node.no_Next = &list->li_Node;
    list->li_Node.no_Prev = &list->li_Node;
}

/*
 * Initialize a node for use (NULL it out)
 */

static __inline
void
initNode(Node *node)
{
    node->no_Next = NULL;
    node->no_Prev = NULL;
}

/*
 * Initialize the first node in a headless circular doubly linked list of
 * nodes.  This has the same result as initList(), but operates on a node.
 */

static __inline
void
initCNode(Node *node)
{
    node->no_Next = node;
    node->no_Prev = node;
}

/*
 * Determine if the list is empty or not empty.  Generally use in
 * conditionals.
 */

static __inline
int
listIsEmpty(const List *list)
{
    return (list->li_Node.no_Next == &list->li_Node);
}

static __inline
int
listNotEmpty(const List *list)
{
    return (list->li_Node.no_Next != &list->li_Node);
}

/*
 * insertNodeBefore() - Insert a node after lnode (lnode is already
 *			in the list).
 */

static __inline
void
insertNodeAfter(Node *lnode, Node *node)
{
    DBASSERT(node->no_Prev == NULL);
    DBASSERT(node->no_Next == NULL);
    node->no_Prev = lnode;
    node->no_Next = lnode->no_Next;
    lnode->no_Next = node;
    node->no_Next->no_Prev = node;
}

/*
 * insertNodeBefore() - Insert a node prior to lnode (lnode is already
 *			in the list).
 */

static __inline
void
insertNodeBefore(Node *lnode, Node *node)
{
    DBASSERT(node->no_Prev == NULL);
    DBASSERT(node->no_Next == NULL);
    node->no_Next = lnode;
    node->no_Prev = lnode->no_Prev;
    lnode->no_Prev = node;
    node->no_Prev->no_Next = node;
}

/*
 * removeNode() - Remove a node from a linked list
 */

static __inline
void
removeNode(Node *node)
{
    DBASSERT(node->no_Prev != NULL);
    DBASSERT(node->no_Next != NULL);
    node->no_Next->no_Prev = node->no_Prev;
    node->no_Prev->no_Next = node->no_Next;
    node->no_Next = NULL;
    node->no_Prev = NULL;
}

/*
 * moveNodeAfter() - remove node and reinsert it after lnode
 */

static __inline
void
moveNodeAfter(Node *lnode, Node *node)
{
    removeNode(node);
    insertNodeAfter(lnode, node);
}

/*
 * moveNodeBefore() - remove node and reinsert it prior to lnode
 */

static __inline
void
moveNodeBefore(Node *lnode, Node *node)
{
    removeNode(node);
    insertNodeBefore(lnode, node);
}

/*
 * getSucc() - get the node following this one or the first node if at the tail
 *
 *	Warning: caller must check for the list head (if any) himself.  NULL
 *	is never returned.
 */

static __inline
void *
getSucc(const Node *node)
{
    return(node->no_Next);
}

/*
 * getPred() - get the node preceding this one or the last node if at the head
 *
 *	Warning: caller must check for the list head (if any) himself.  NULL
 *	is never returned.
 */

static __inline
void *
getPred(const Node *node)
{
    return(node->no_Prev);
}

/*
 * getSuccPred() - get node following the specified node or NULL if at tail.
 */

static __inline
void *
getListSucc(const List *list, Node *node)
{
    if ((node = node->no_Next) != &list->li_Node)
	return(node);
    else
	return(NULL);
}

/*
 * getListPred() - get node prior to specified node or NULL if at head.
 */

static __inline
void *
getListPred(const List *list, Node *node)
{
    if ((node = node->no_Prev) != &list->li_Node)
	return(node);
    else
	return(NULL);
}

/*
 * addHead() - add node to head of list
 */
static __inline
void
addHead(List *list, Node *node)
{
    insertNodeAfter(&list->li_Node, node);
}

/*
 * addHead() - add node to tail of list
 */
static __inline
void
addTail(List *list, Node *node)
{
    insertNodeBefore(&list->li_Node, node);
}

/*
 * getHead() - return first node in list or NULL if list is empty
 */
static __inline
void *
getHead(const List *list)
{
    Node *node;

    if ((node = list->li_Node.no_Next) == &list->li_Node)
	return(NULL);
    return(node);
}

/*
 * getHead() - return last node in list or NULL if list is empty
 */
static __inline
void *
getTail(const List *list)
{
    Node *node;

    if ((node = list->li_Node.no_Prev) == &list->li_Node)
	return(NULL);
    return(node);
}

/*
 * remHead() - remove and return the first node in the list, or NULL
 */
static __inline
void *
remHead(List *list)
{
    Node *node;

    if ((node = list->li_Node.no_Next) == &list->li_Node)
	return(NULL);
    removeNode(node);
    return(node);
}

/*
 * remTail() - remove and return the last node in the list, or NULL
 */
static __inline
void *
remTail(List *list)
{
    Node *node;

    if ((node = list->li_Node.no_Prev) == &list->li_Node)
	return(NULL);
    removeNode(node);
    return(node);
}

