/*
 * AUTH.C
 *
 * (c)Copyright 1999-2002 Backplane, Inc.  Please refer to the COPYRIGHT
 * file at the base of the distribution tree.
 *
 * $Backplane: rdbms/libsupport/hex.c,v 1.6 2002/08/20 22:05:55 dillon Exp $
 */

#include "defs.h"

Export char *DataToASCII(const void *data, int dataSize);
Export void *ASCIIToData(const char *ascii, int bytes);
static __inline unsigned char hexToBin(char hexDigit);

/*
 * hexToBin - Convert a hexadecimal digit to its binary value
 *
 * Arguments:	hexDigit	A single ASCII hex digit
 * Globals:	None
 *
 * Returns:	The binary value of the input digit,
 *		or 0 if it is out of range.
 */
static __inline unsigned char 
hexToBin(char hexDigit)
{
    unsigned char binary = 0;

    if (hexDigit >= '0' && hexDigit <= '9')  
	binary = hexDigit - '0';
    else if (hexDigit >= 'A' && hexDigit <= 'F')  
	binary = hexDigit - 'A' + 10;
    else if (hexDigit >= 'a' && hexDigit <= 'f')
	binary = hexDigit - 'a' + 10;
    return(binary);
}

/* DataToASCII - Convert binary data to ASCII hex string representation
 *
 * Description:	dataToASCII() converts a buffer of binary data to
 *		ASCII hex string representation.
 *
 * Arguments:	data		Pointer to data buffer
 *		dataSize	Number of bytes to convert
 * Globals:	None
 *
 * Returns:	Pointer to dynamically allocated string containing
 *		ASCII hex representation of the data provided if successful;
 *		NULL if not
 *
 *
 *		NOTE: DataToASCII() DOES NOT account for integer byte order
 */
char *
DataToASCII(const void *data, int dataSize)
{
    const unsigned char *dataBuf;
    char *asciiStr;
    char *s;
    int i;
    char *hexTable = "0123456789ABCDEF";

    asciiStr = safe_malloc(dataSize * 2 + 1);

    /*
     * Convert binary data into a hex string
     */
    dataBuf = (const unsigned char *)data;
    for (s=asciiStr, i=0; i < dataSize; i++) {
	*s++ = hexTable[((dataBuf[i] >> 4) & 0x0F)];
	*s++ = hexTable[(dataBuf[i] & 0x0F)];
    }
    *s = '\0';
    return(asciiStr);
}

/* ASCIIToData - Convert ASCII hex string to binary data
 *
 * Description:	ASCIIToData() converts an ASCII hex string to binary data.
 *
 * Arguments:	ascii		Pointer to NULL-terminated hex string
 *		bytes		Number of bytes (post conversion)
 *
 * Globals:	None
 *
 * Returns:	Pointer to a dynamically allocated buffer containing
 *		the raw binary representation of the provided ascii-hex
 *		string.  If the supplied buffer is not large enough,
 *		the representation will be truncated.  If the buffer is
 *		too large, the trailing portion of the buffer will be zero.
 *
 *		NOTE: ASCIIToData() DOES NOT account for integer byte order
 */
void *
ASCIIToData(const char *ascii, int bytes)
{
    unsigned char *data;
    unsigned char *dataBuf;

    data = safe_malloc(bytes);
    bzero(data, bytes);

    /*
     * Convert a hex string into raw data.
     */
    dataBuf = data;
    while (ascii[0] && ascii[1]) {
	*dataBuf = hexToBin(ascii[0]) << 4;
	*dataBuf |= hexToBin(ascii[1]);
	++dataBuf;
	ascii += 2;
	if (dataBuf - data >= bytes)
	    break;
    }
    return(data);
}

