/*
 * CURSOR/QUERY.H
 *
 * (c)Copyright 2000-2002 Backplane, Inc.  Please refer to the COPYRIGHT 
 * file at the base of the distribution tree.
 *
 * $Backplane: rdbms/cursor/query.h,v 1.15 2002/08/20 22:05:45 dillon Exp $
 */

typedef struct {
    Node        columnNode;
    char        *columnName;
    u_int       columnIndex;
    int		flags;
} ColumnEntry;

typedef struct Query {
    struct Query	*parentQuery;
    List		columnList;
    List		symbolList;
    List		aliasList;
    u_int		columnCount;
    u_int		queryLevel;
    u_int		gotoId;
    int			varFlags;
    FILE		*outFile;
    char		*statusVarName;
    char		*tsVarName;
} Query;

typedef struct ColI {
    char	dummy[0];
} ColI;

#define CIF_ORDER	0x0001
#define CIF_WILD	0x0002
#define CIF_ADDCOL	0x0004 /* Add columns to column table */
#define CIF_CONST	0x0008 /* Columns must be constants */
#define CIF_ALIAS	0x0018 /* Process columns by alias */
#define CIF_PRINT	0x0020 /* Certain parser routines can output data */
#define CIF_WITHSPACE	0x0040 /* When printing, add space after end */


#define VF_COLUMNS	0x0001
#define VF_SUBLEVELS	0x0002
#define VF_SELECT	0x0004
#define VF_ROLLBACK	0x0008
#define VF_COMMIT	0x0010
