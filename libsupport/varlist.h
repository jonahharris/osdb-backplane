/*
 * VARLIST.H
 *
 * (c)Copyright 1999-2002 Backplane, Inc.  Please refer to the COPYRIGHT
 * file at the base of the distribution tree.
 *
 * $Backplane: rdbms/libsupport/varlist.h,v 1.2 2002/08/20 22:05:55 dillon Exp $
 */

/*
 * Var structure - varlist nodes
 */
typedef struct {
    Node	va_Node;
    char	*va_Name;
    char	*va_Value;
    int		va_Size;
} Var;
