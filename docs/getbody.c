
#include <stdio.h>
#include <unistd.h>
#include <string.h>

int
main(int ac, char **av)
{
    char buf[1024];

    while (fgets(buf, sizeof(buf), stdin) != NULL) {
	if (strncasecmp(buf, "<BODY>", 6) == 0)
	    break;
    }
    while (fgets(buf, sizeof(buf), stdin) != NULL) {
	if (strncasecmp(buf, "</BODY>", 6) == 0)
	    break;
	fputs(buf, stdout);
    }
    while (fgets(buf, sizeof(buf), stdin) != NULL)
	;
    return(0);
}

