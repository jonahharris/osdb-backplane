/*
 * VERSION.C
 *
 * (c)Copyright 1999-2002 Backplane, Inc.  Please refer to the COPYRIGHT
 * file at the base of the distribution tree.
 *
 * $Backplane: rdbms/libsupport/version.c,v 1.4 2002/08/20 22:05:55 dillon Exp $
 */

#include "defs.h"

Export int BackplaneReleaseVersion;

/*
 * Represents the current backplane version number for programmatic
 * tests.
 */
int BackplaneReleaseVersion = BACKPLANE_VERSION;

