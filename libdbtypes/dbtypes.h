/*
 * LIBDBTYPES/DBTYPES.H
 *
 * (c)Copyright 2000-2002 Backplane, Inc.  Please refer to the COPYRIGHT
 * file at the base of the distribution tree.
 *
 * $Backplane: rdbms/libdbtypes/dbtypes.h,v 1.3 2002/08/20 22:05:53 dillon Exp $
 */

/*
 * ColData - data structure used to reference a single data element
 *
 *	WARNING!  cd_Data is *NOT*, I repeat, *NOT* 0-terminated.  You
 *		  cannot pass it as a string directly to a C function.
 *
 *	WARNING!  cd_DataType may be left 0 under a number of circumstances.
 *		  Operator primitives should not use this field.
 *
 *	NOTE: NULL vs 0-length data.  cd_Data will be a NULL pointer
 *	and cd_Bytes will be 0 if NULL is being represented.  cd_Data
 *	will be a non-NULL pointer and cd_Bytes will be 0 if a 0-length
 *	string is being represented.
 */

typedef struct ColData {
    struct ColData *cd_Next;		/* next in allocated list or NULL */
    int		cd_ColId;		/* column identifier */
    int		cd_Bytes;		/* data len (terminator not included)*/
    const char	*cd_Data;		/* pointer to data */
    int		cd_Flags;		/* RDF_ALLOC only */
    int		cd_DataType;
} ColData;

typedef int (*dataop_func_t)(const ColData *lhs, const ColData *rhs);

/*
 * Fixed datatypes
 */

#define DATATYPE_UNKNOWN	0
#define DATATYPE_STRING		1
#define DATATYPE_INT		2

#define DATATYPE_ARRAY_SIZE	3

/*
 * Operators
 *
 *	WARNING! Datatype modules use static initializers for their
 *	function arrays, do not change the below numbers.
 */

#define DATAOP_UNKNOWN		0
#define DATAOP_LIKE		1
#define DATAOP_RLIKE		2
#define DATAOP_SAME		3
#define DATAOP_RSAME		4
#define DATAOP_EQEQ		5
#define DATAOP_NOTEQ		6
#define DATAOP_LT		7
#define DATAOP_LTEQ		8
#define DATAOP_GT		9
#define DATAOP_GTEQ		10

