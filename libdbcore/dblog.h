/*
 * LIBDBCORE/DBLOG.H - Log file data format and headers
 *
 * (c)Copyright 1999-2002 Backplane, Inc.  Please refer to the COPYRIGHT
 * file at the base of the distribution tree.
 *
 * $Backplane: rdbms/libdbcore/dblog.h,v 1.4 2002/10/02 21:31:33 dillon Exp $
 */

/*
 * LogRecord header (note: 8 byte aligned)
 */
typedef struct {
    u_int8_t	lr_Magic;
    u_int8_t	lr_Cmd;
    u_int16_t	lr_Flags;
    u_int16_t	lr_Unused01;
    u_int16_t	lr_SeqNo;
    int32_t	lr_Unused02;
    int32_t	lr_File;	/* associated with table or index file */
    int32_t	lr_Bytes;	/* size of this record */
    int32_t	lr_RevBytes;	/* size of previous record */
} LogRecord;

#define LR_MAGIC_LSB		0xA0	/* lsb-first byte ordering */
#define LR_MAGIC_MSB		0xA1	/* msb-first byte ordering */

#define LOG_CMD_HEARTBEAT	0x01
#define LOG_CMD_TRANS_BEGIN	0x02
#define LOG_CMD_TRANS_COMMIT	0x03
#define LOG_CMD_FILE_ID		0x04	/* file id -> name info */
#define LOG_CMD_APPEND_OFFSET	0x05	/* change in table append offset */
#define LOG_CMD_TABLE_DATA	0x06	/* table record */
#define LOG_CMD_INDEX_DATA	0x07

typedef struct {
    LogRecord	lhr_Head;
    dbstamp_t	lhr_Timestamp;	/* log timestamp */
} LogHeartRecord;

typedef struct {
    LogRecord	ltr_Head;
    dbstamp_t	ltr_Stamp;	/* transaction timestamp */
    int32_t	ltr_CRC;	/* 32 bit CRC everything BEGIN thru COMMIT */
    int32_t	ltr_Unused01;
} LogTransRecord;

typedef struct {
    LogRecord	lir_Head;
    char	lir_FileName[0]; /* 0-terminated filename */
} LogIdRecord;

typedef struct {
    LogRecord	lar_Head;
    dbstamp_t	lar_Offset;	/* new append offset */
} LogAppendRecord;

typedef struct {
    LogRecord	ltd_Head;
    dbstamp_t	ltd_Offset;	/* offset of data */
    char	ltd_Data[0];
} LogTableDataRecord;

typedef struct {
    LogRecord	lid_Head;
    dbstamp_t	lid_Offset;	/* offset of data */
    int32_t	lid_OBytes;	/* # bytes of old data.  Rest is new data */
    int32_t	lid_Unused01;
    char	lid_Data[0];
} LogIndexDataRecord;

typedef union {
    LogRecord		a_Head;
    LogHeartRecord	a_LogHeart;
    LogTransRecord	a_LogTrans;
    LogIdRecord		a_LogId;
    LogAppendRecord	a_LogAppend;
    LogTableDataRecord	a_LogTableData;
    LogIndexDataRecord	a_LogIndexData;
} LogRecordAll;

#define LRF_WITHOUT_DATA	0x0001	/* associated data is not logged */

