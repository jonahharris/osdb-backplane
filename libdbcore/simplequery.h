/*
 * SIMPLEQUERY.H
 *
 * (c)Copyright 1999-2002 Backplane, Inc.  Please refer to the COPYRIGHT
 * file at the base of the distribution tree.
 *
 * $Backplane: rdbms/libdbcore/simplequery.h,v 1.4 2002/08/20 22:05:52 dillon Exp $
 */

typedef struct SimpleQuery {
    DataBase	*sq_Db;
    Query	*sq_Query;
    char	*sq_QryStr;
    int		sq_Error;
    int		sq_Cols;
    int		sq_RIndex;
    int		sq_WIndex;
    int		sq_MaxIndex;
    char	***sq_Rows;
} SimpleQuery;
