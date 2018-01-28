/*
 * LOG.C -	Logging operations
 *
 * (c)Copyright 1999-2002 Backplane, Inc.  Please refer to the COPYRIGHT
 * file at the base of the distribution tree.
 *
 * $Backplane: rdbms/libsupport/log.c,v 1.17 2003/05/09 16:42:54 dillon Exp $
 */

#include "defs.h"
#include "log.h"

static int LogLevel   = DEBUGPRI;
static int LogControl = LOGCTL_FILE;

static char *LogFile     = NULL;
static char *LogUser     = NULL;
static char *LogDatabase = NULL;

Export void LogOpen(int logLevel, int logControl, const char *logFile);
Export void LogClose(void);

Export void LogLevelSet(int logLevel);
Export void LogControlSet(int logFlag);
Export void LogUserSet(const char *);
Export void LogDatabaseSet(const char *);

Export int LogLevelGet(void);
Export int LogLevelCheck(int logLevel);
Export void LogWrite(int logLevel, const char *ctl, ...);

/* LogOpen -	Open logging
 *
 * Arguments:	logLevel	Log level priority threshold 
 *		logControl	Log configuration flags (LOGCTL_*)
 *		logFile		Log filename - can be NULL for modes that
 *				don't log to files.
 *
 * Globals:	LogLevel	Global storage for priority threshold
 *		LogControl	Global storage for configuration flags
 *		LogFile		Global storage for log filename
 *
 * Returns:	void
 *
 * Description: LogOpen sets the parameters for logging.
 *
 *		NOTE: If the syslog control flag (LOGCTL_SYSLOG) is enabled, 
 *		the log priority level provided in the logLevel argument
 *		must correspond to those used by syslogd.
 */
void
LogOpen(int logLevel, int logControl, const char *logFile)
{
    LogClose();

    LogLevel = logLevel;
    LogControl = logControl;
    if (logFile != NULL) {
	LogFile = safe_strdup(logFile);
    }
}

/* LogClose -	Close logging
 *
 * Arguments:	None		Log level priority threshold 
 * Globals:	LogFile		Global storage for log filename
 *
 * Returns:	void
 *
 * Description: LogClose stops logging and frees up any resources used
 *		by the logging subsystem.
 */
void
LogClose(void)
{
    /* Stop logging */
    LogLevel = 0;

    safe_free(&LogFile);
}

/* LogLevelGet -	Return log priority level
 *
 * Arguments:	None
 *
 * Globals:	LogLevel	Global storage for priority threshold
 *
 * Returns:	LogLevel
 *
 * Description: LogLevelGet returns the current log priority level
 */
int
LogLevelGet(void)
{
    return(LogLevel);
}

/* LogLevelSet -	Set log priority level
 *
 * Arguments:	logLevel	Log level priority threshold 
 *
 * Globals:	LogLevel	Global storage for priority threshold
 *
 * Returns:	void
 *
 * Description: LogLevelSet sets the log priority level
 */
void
LogLevelSet(int logLevel)
{
    LogLevel = logLevel;
}

/* LogLevelCheck -	Set log priority level
 *
 * Arguments:	logLevel	Log level priority threshold 
 *
 * Globals:	LogLevel	Global storage for priority threshold
 *
 * Returns:	1 if the level provided is at or above the log threshold
 *		0 if it is not
 *
 * Description: LogLevelCheck returns a boolean that indicates whether
 *		the log priority level provided is greater than or equal to
 *		the log priority threshold LogLevel.
 *
 *		LogLevelCheck performs the same comparison that LogWrite
 *		to determine whether to log a message or not. 
 */
int
LogLevelCheck(int logLevel)
{
    if (logLevel > LogLevel)
	return(0);
    return(1);
}

void 
LogDatabaseSet(const char *database)
{
    LogDatabase = safe_strdup(database);
}

void 
LogUserSet(const char *userid)
{
    LogUser = safe_strdup(userid);
}

void 
LogControlSet(int logFlag)
{
    LogControl |= logFlag;    
}

/* LogWrite -	Log a message
 *
 * Arguments:	logLevel	Log priority level of this message
 *		ctl		Arguments to log
 * Globals:	LogLevel	Global storage for priority threshold
 *		LogControl	Global storage for configuration flags
 *		LogFile		Global storage for log filename
 *
 * Returns:	void
 *
 * Description: LogWrite logs a message if its log priority (logLevel) is
 *		less than or equal to the log priority threshold LogLevel. 
 *		LogWrite checks the LogControl flag set to determine how to
 *		log the message.
 *
 *		NOTE: If the syslog control flag (LOGCTL_SYSLOG) is enabled, 
 *		the log priority level provided in the logLevel argument
 *		must correspond to those used by syslogd.
 */
void
LogWrite(int logLevel, const char *ctl, ...)
{
    va_list	va;
    char	*body;
    char 	*buf = NULL;

    /* Don't log a message unless it's priority is abobe the threshold */
    if (logLevel > LogLevel)
	return;

    va_start(va, ctl);
    safe_vasprintf(&body, ctl, va);
    va_end(va);

    if (LogControl & LOGCTL_TIMESTAMP) {
	time_t t = time(NULL);
	struct tm res;
	char tbuf[64];

	localtime_r(&t, &res);
	strftime(tbuf, sizeof(tbuf), "%d%b%H:%M:%S ", &res);
	safe_append(&buf, tbuf);
    }

    if (LogControl & LOGCTL_SIMPLE) {
	safe_appendf(&buf, "%s\n", body);
    } else {
	char *idstring;

	safe_asprintf(&idstring, "%s%s%s", !EMPTY(LogUser) ? LogUser : "",
		(!EMPTY(LogDatabase) && !EMPTY(LogUser)) ? "@" : "", 
		!EMPTY(LogDatabase) ? LogDatabase : "");

	safe_appendf(&buf, "%d %s (pid %d): %s\n",
	    logLevel,
	    idstring,
	    (int)getpid(),
	    body
	);
	
	safe_free(&idstring);
    }

    if ((LogControl & LOGCTL_FILE) && LogFile != NULL) {
	int wfd;

	if ((wfd = open(LogFile, O_CREAT|O_RDWR|O_APPEND, 0664)) >= 0) {
	    write(wfd, buf, strlen(buf));
	    close(wfd);
	}
    }

    if (LogControl & LOGCTL_SYSLOG) {
	syslog(logLevel, "%s", body);
    }

    if (LogControl & LOGCTL_STDOUT) {
	fwrite(buf, 1, strlen(buf), stdout);
    }

    if (LogControl & LOGCTL_STDERR) {
	fwrite(buf, 1, strlen(buf), stderr);
	fflush(stderr);
    }
    safe_free(&body);
    safe_free(&buf);
}
