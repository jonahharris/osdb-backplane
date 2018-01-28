/*
 * LOG.H - Support for logging
 *
 * (c)Copyright 1999-2002 Backplane, Inc.  Please refer to the COPYRIGHT
 * file at the base of the distribution tree.
 *
 * $Backplane: rdbms/libsupport/log.h,v 1.15 2003/05/09 16:42:54 dillon Exp $
 */

/* Log levels */
#define FATAL    	1
#define HIPRI	 	2
#define LOPRI	 	3
#define DEBUGPRI 	4

/* Log control flags */
#define LOGCTL_FILE	0x00000001
#define LOGCTL_SYSLOG	0x00000002
#define LOGCTL_STDOUT	0x00000004
#define LOGCTL_STDERR	0x00000008
#define LOGCTL_SIMPLE	0x00000010
#define LOGCTL_TIMESTAMP 0x00000020

