/*
 * LIBDBTYPES/DATATYPE.C
 *
 * (c)Copyright 2000-2002 Backplane, Inc.  Please refer to the COPYRIGHT
 * file at the base of the distribution tree.
 *
 *	Implements string operators, validation, encoding, and decoding
 *	functions.
 *
 * $Backplane: rdbms/libdbtypes/datatype.c,v 1.2 2002/08/20 22:05:53 dillon Exp $
 */

#include "defs.h"

Export int DataTypeLookup(const char *ptr, int bytes);
Export dataop_func_t *DataTypeFuncAry[DATATYPE_ARRAY_SIZE];

dataop_func_t *DataTypeFuncAry[DATATYPE_ARRAY_SIZE] = { 
	DataTypeUnknownFuncAry,		/* DATATYPE_UNKNOWN	*/
	DataTypeStringFuncAry,		/* DATATYPE_STRING	*/
	DataTypeUnknownFuncAry		/* DATATYPE_INT		*/
};

int
DataTypeLookup(const char *ptr, int bytes)
{
    if (bytes == 7 && strncmp(ptr, "varchar", 7) == 0)
	return(DATATYPE_STRING);
    return(-1);
}

