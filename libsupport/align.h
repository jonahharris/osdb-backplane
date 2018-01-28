/*
 * ALIGN.H
 *
 */

#define ALIGN2(n)	(((n) + 1) & ~1)	/* note: 64 bit clean */
#define ALIGN4(n)	(((n) + 3) & ~3)	/* note: 64 bit clean */
#define ALIGN8(n)	(((n) + 7) & ~7)	/* note: 64 bit clean */
#define ALIGN128(n)	(((n) + 127) & ~127)	/* note: 64 bit clean */

#define SWAP16(n)	n = swap16(n)
#define SWAP32(n)	n = swap32(n)
#define SWAP64(n)	n = swap64(n)
#define USWAP16(n)	n = (u_int16_t)swap16((int16_t)n)
#define USWAP32(n)	n = (u_int32_t)swap32((int32_t)n)
#define USWAP64(n)	n = (u_int64_t)swap32((int64_t)n)

static __inline
int16_t
swap16(int16_t n)
{
    u_int8_t v;
    union {
	u_int8_t ary[2];
	int16_t w;
    } u;
    u.w = n;
    v = u.ary[0];
    u.ary[0] = u.ary[1];
    u.ary[1] = v;
    return(u.w);
}

static __inline
int32_t
swap32(int32_t n)
{
    u_int8_t v;
    union {
	u_int8_t ary[4];
	int32_t w;
    } u;
    u.w = n;
    v = u.ary[0];
    u.ary[0] = u.ary[3];
    u.ary[3] = v;
    v = u.ary[1];
    u.ary[1] = u.ary[2];
    u.ary[2] = v;
    return(u.w);
}

static __inline
int32_t
swap64(int64_t n)
{
    u_int8_t v;
    union {
	u_int8_t ary[8];
	int64_t w;
    } u;
    u.w = n;
    v = u.ary[0]; u.ary[0] = u.ary[7]; u.ary[7] = v;
    v = u.ary[1]; u.ary[1] = u.ary[6]; u.ary[6] = v;
    v = u.ary[2]; u.ary[2] = u.ary[5]; u.ary[5] = v;
    v = u.ary[3]; u.ary[3] = u.ary[4]; u.ary[4] = v;
    return(u.w);
}

