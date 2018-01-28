/*
 * VALIDATE.C
 *
 * (c)Copyright 2000-2002 Backplane, Inc.  Please refer to the COPYRIGHT
 * file at the base of the distribution tree.
 *
 * $Backplane: rdbms/libclient/validate.c,v 1.7 2002/08/20 22:05:48 dillon Exp $
 */

#include "defs.h"

Export int ValidCLMsgHelloWithDB(CLHelloMsg *msg, int nonEmpty);

int
ValidCLMsgHelloWithDB(CLHelloMsg *msg, int nonEmpty)
{
    int dboff;
    int dataLen;

    if (msg == NULL)
	return(-1);
    if (msg->hm_Msg.cm_Pkt.cp_Cmd != CLCMD_HELLO)
	return(-1);
    dboff = offsetof(CLHelloMsg, hm_DBName[0]) - 
		offsetof(CLHelloMsg, hm_Msg.cm_Pkt);
    /* Note: dataLen includes zero terminator */
    dataLen = msg->hm_Msg.cm_Pkt.cp_Bytes - dboff;	
    if (dataLen < 1 || msg->hm_DBName[dataLen - 1])
	return(-1);
    if (nonEmpty && (dataLen == 1 || msg->hm_DBName[0] == 0))
	return(-1);
    return(0);
}

