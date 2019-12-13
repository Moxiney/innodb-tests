#include <stdio.h>
#include <stdlib.h>
#include <assert.h>

#include "/usr/local/include/embedded_innodb-1.0/innodb.h"


int main() {
    ib_err_t	err;
	ib_crsr_t	crsr;
	ib_trx_t	ib_trx;
	ib_u64_t	version;

	version = ib_api_version();
	printf("API: %d.%d.%d\n",
		(int) (version >> 32),			/* Current version */
		(int) ((version >> 16)) & 0xffff,	/* Revisiion */
	       	(int) (version & 0xffff));		/* Age */

	err = ib_init();
	assert(err == DB_SUCCESS);
    return 0;
}
