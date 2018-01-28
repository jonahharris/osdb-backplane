/*
 * LIBDBCORE/TYPES.H -	Basic standalone database types that things outside
 *			the database code might need.
 *
 * (c)Copyright 1999-2002 Backplane, Inc.  Please refer to the COPYRIGHT
 * file at the base of the distribution tree.
 *
 * $Backplane: rdbms/libdbcore/types.h,v 1.1 2002/09/20 21:45:05 dillon Exp $
 */

typedef int64_t		dboff_t;	/* database virtual/physical offset */
typedef u_int8_t	rhmagic_t;
typedef u_int8_t	rhflags_t;
typedef u_int16_t	vtable_t;
typedef u_int16_t	rhhash_t;
typedef u_int32_t	rhuser_t;

