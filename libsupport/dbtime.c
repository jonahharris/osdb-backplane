/*
 * DBTIME.C
 *
 * (c)Copyright 2000-2002 Backplane, Inc.  Please refer to the COPYRIGHT
 * file at the base of the distribution tree.
 *
 *	Database timestamp conversion functions
 */

#include "defs.h"

Export dbstamp_t ascii_to_dbstamp(const char *str);
Export const char *dbstamp_to_ascii_simple(dbstamp_t ts, int use_gmt, char **alloc);
Export const char *dbstamp_to_ascii(dbstamp_t ts, int use_gmt, char **alloc);

/*
 * ascii_to_dbstamp()	- convert ascii string to database timestamp
 *
 *	The following formats are supported:
 *
 *	0xHEX or 0XHEX 			- raw 64 bit timestamp in hex
 *	yyyy.mm.dd.hh.mm.ss.frac[G]	- date/time format 1 e.g. 2002.12.22
 *					  (all fields optional)
 *	ddmmmyyyy/hh:mm:ss.frac[G]	- date/time format 2 e.g. 23May/12:22
 *					  (year and daytime fields optional)
 *
 *	A 'g' or 'G' suffix means GMT, otherwise the time is local.
 *
 *	The database understands fractions of a second down to one 
 *	microsecond.
 *
 *	0 is returned on error.
 */
dbstamp_t
ascii_to_dbstamp(const char *str)
{
    dbstamp_t ts = 0;

    if (strncasecmp(str, "0x", 2) == 0) {
	ts = strtouq(str + 2, NULL, 16);
    } else {
	struct tm tt;
	time_t t = time(NULL);
	int frac = 0;	/* fractional seconds, in microseconds */
	int len = strlen(str);
	int n;
	int use_gmt;

	if (len && (str[len-1] == 'G' || str[len-1] == 'g')) {
	    gmtime_r(&t, &tt);
	    use_gmt = 1;
	} else {
	    localtime_r(&t, &tt);
	    use_gmt = 0;
	}

	if (strchr(str, '.') && strchr(str, '/') == NULL) {
	    /*
	     * yyyy.mm.dd.hh.mm.ss.frac
	     */
	    n = sscanf(str, "%d.%d.%d.%d.%d.%d.%n",
		&tt.tm_year,
		&tt.tm_mon,
		&tt.tm_mday,
		&tt.tm_hour,
		&tt.tm_min,
		&tt.tm_sec,
		&frac
	    );
	    if (n >= 1)
		tt.tm_year -= 1900;
	    if (n >= 2)
		--tt.tm_mon;
	} else {
	    /*
	     * ddmmmyyyy/hh:mm:ss.frac
	     */
	    static char *Months[] = { "jan", "feb", "mar", "apr", "may", "jun",
				      "jul", "aug", "sep", "oct", "nov", "dec"
				    };
	    static char mon[4] = { 0 };

	    n = sscanf(str, "%2d%3s%4d", &tt.tm_mday, mon, &tt.tm_year);
	    if (n >= 3)
		tt.tm_year -= 1900;
	    if (n >= 2) {
		int i;
		for (i = 0; i < arysize(Months); ++i) {
		    if (strcasecmp(mon, Months[i]) == 0)
			break;
		}
		if (i == arysize(Months))
		    return(0);
		tt.tm_mon = i;
	    }
	    if ((str = strchr(str, '/')) != NULL) {
		sscanf(str + 1, "%2d:%2d:%2d.%n",
		    &tt.tm_hour,
		    &tt.tm_min,
		    &tt.tm_sec,
		    &frac
		);
	    }
	}

	/*
	 * Handle fraction (index into string)
	 */
	if (frac && str[frac] == '.')
	    frac = (int)(strtod(str + frac, NULL) * 1000000.0);
	else
	    frac = 0;

	/*
	 * Convert to unix timestamp
	 */
	if (use_gmt) {
	    t = timegm(&tt);
	} else {
	    tt.tm_isdst = -1;
	    t = mktime(&tt);
	}
	ts = (dbstamp_t)t * 1000000 + frac;
    }
    return(ts);
}

const char *
dbstamp_to_ascii_simple(dbstamp_t ts, int use_gmt, char **alloc)
{
    char buf[128];

    if (ts == 0) {
	snprintf(buf, sizeof(buf), "------ zero ------");
    } else {
	time_t t = dbstamptotime(ts);
	struct tm tm;

	if (use_gmt)
	    gmtime_r(&t, &tm);
	else
	    localtime_r(&t, &tm);
	strftime(buf, sizeof(buf) - 32, "%d%b/%H:%M:%S", &tm);
	snprintf(buf + strlen(buf), 32, ".%03d(%016qx)", (int)(ts / 1000 % 1000), ts);
    }
    return(safe_replace(alloc, buf));
}

const char *
dbstamp_to_ascii(dbstamp_t ts, int use_gmt, char **alloc)
{
    char buf[128];

    if (ts == 0) {
	snprintf(buf, sizeof(buf), "------ zero ------");
    } else {
	time_t t = dbstamptotime(ts);
	struct tm tm;

	if (use_gmt)
	    gmtime_r(&t, &tm);
	else
	    localtime_r(&t, &tm);
	strftime(buf, sizeof(buf) - 32, "%d%b%Y/%H:%M:%S[%Z]", &tm);
	snprintf(buf + strlen(buf), 32, ".%03d", (int)(ts / 1000 % 1000));
    }
    return(safe_replace(alloc, buf));
}

