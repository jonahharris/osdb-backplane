/*
 * LIBDBCORE/NATIVE.H
 *
 * (c)Copyright 1999-2002 Backplane, Inc.  Please refer to the COPYRIGHT
 * file at the base of the distribution tree.
 *
 * $Backplane: rdbms/libdbcore/native.h,v 1.11 2002/08/20 22:05:52 dillon Exp $
 */

#define VT_INCREMENT		4

#define VT_COLTABLE_COLS	1
#define VT_COLTABLE_RESERVED2	2
#define VT_COLTABLE_RESERVED3	3

#define VT_SYS_SCHEMA		(VT_INCREMENT*1)
#define VT_SYS_TABLE		(VT_INCREMENT*2)
#define VT_SYS_REPGROUP		(VT_INCREMENT*3)

#define VT_MIN_USER		256	/* minimum user vtable id */
#define VT_FMT_STRING		"%04x"

#define CID_RAW_TIMESTAMP	0x0001
#define CID_RAW_VTID		0x0002
#define CID_RAW_USERID		0x0003
#define CID_RAW_OPCODE		0x0004

#define CID_COOK_TIMESTAMP	0x0008
#define CID_COOK_VTID		0x0009
#define CID_COOK_USERID		0x000A
#define CID_COOK_OPCODE		0x000B
#define CID_COOK_DATESTR	0x000C

#define CID_RAW_LIMIT		0x0010	/* CID's below this are special */

#define CID_SCHEMA_NAME		0x0011
#define CID_TABLE_NAME		0x0012
#define CID_TABLE_VID		0x0013
#define CID_UNUSED_04		0x0014
#define CID_TABLE_FILE		0x0015
#define CID_HOST_NAME		0x0016
#define CID_HOST_ID		0x0017
#define CID_HOST_TYPE		0x0018

#define CID_COL_NAME		0x001B
#define CID_COL_TYPE		0x001C
#define CID_COL_FLAGS		0x001D
#define CID_COL_STATUS		0x001E
#define CID_COL_ID		0x001F
#define CID_COL_DEFAULT		0x0020

#define CID_MIN_USER		1024	/* minimum user column id */
#define COL_FMT_STRING		"%04x"

typedef struct HLResult {
    struct HLResult	*h_Next;
    int			h_AllocSize;
    char		*s_Data[0];	/* extended (first element unused) */
} HLResult;

typedef struct HLSchemaInfo {
    HLResult		h_Result;
    char		*s_SchemaName;
    char		*s_TableFile;
} HLSchemaInfo;

typedef struct HLTableInfo {
    HLResult		h_Result;
    char		*s_SchemaName;
    char		*s_TableName;
    char		*s_TableVId;
    char		*s_TableFile;
} HLTableInfo;

typedef struct HLColumnInfo {
    HLResult		h_Result;
    char		*s_ColName;
    char		*s_ColType;
    char		*s_ColFlags;
    char		*s_ColStatus;
    char		*s_ColId;
} HLColumnInfo;

