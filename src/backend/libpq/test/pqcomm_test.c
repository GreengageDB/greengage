#include <stdarg.h>
#include <stddef.h>
#include <setjmp.h>
#include "cmockery.h"

#include "postgres.h"
#include "nodes/nodes.h"
#include "libpq/libpq.h"

/*
 * Mocked function of accept, because we cannot simulate accepting an
 * incoming connection in a unit test
 */
#define accept pqcomm_accept_mock
int pqcomm_accept_mock(int socket, struct sockaddr *restrict address,
		socklen_t *restrict address_len);

#include "../pqcomm.c"

/* Number of bytes requested to be sent through internal_flush */
#define TEST_NUM_BYTES 100

/* Number of seconds to set the SO_SNDTIMEO to */
#define SOCKET_TIMEOUT_SECONDS 1978

/* The mocked accept() function will return this socket when called */
static int test_accept_socket;

/*
 *  Test for internal_flush() for the case when:
 *    - requesting to send TEST_NUM_BYTES bytes
 *    - secure_write returns TEST_NUM_BYTES (send successful)
 *    - errno is not changed
 */
static void
test__internal_flush_successfulSend(void **state)
{
	int			result;

	expect_any(secure_write, port);
	expect_any(secure_write, ptr);
	expect_any(secure_write, len);
	will_return(secure_write, TEST_NUM_BYTES);

	PqSendPointer = TEST_NUM_BYTES;
	result = internal_flush();

	assert_int_equal(result,0);
	assert_int_equal(ClientConnectionLost, 0);
	assert_int_equal(InterruptPending, 0);
}

/*
 * Simulate side effects of secure_write. Sets the errno variable to val
 */
static void
_set_errno(void *val)
{
	errno = *((int *) val);
}

/*
 *  Test for internal_flush() for the case when:
 *    - secure_write returns 0 (send failed)
 *    - errno is set to EINTR
 */
static void
test__internal_flush_failedSendEINTR(void **state)
{
	int			result;

	/*
	 * In the case secure_write gets interrupted, we loop around and
	 * try the send again.
	 * In this test we simulate that, and secure_write will be called twice.
	 *
	 * First call to secure_write: returns 0 and sets errno to EINTR.
	 */
	expect_any(secure_write, port);
	expect_any(secure_write, ptr);
	expect_any(secure_write, len);
	static int errval = EINTR;
	will_return_with_sideeffect(secure_write, 0, _set_errno, &errval);

	/* Second call to secure_write: returns TEST_NUM_BYTES, i.e. success */
	expect_any(secure_write, port);
	expect_any(secure_write, ptr);
	expect_any(secure_write, len);
	will_return(secure_write, TEST_NUM_BYTES);

	PqSendPointer = TEST_NUM_BYTES;

	/* Call function under test */
	result = internal_flush();

	assert_int_equal(result,0);
	assert_int_equal(ClientConnectionLost, 0);
	assert_int_equal(InterruptPending, 0);
}

/*
 *  Test for internal_flush() for the case when:
 *    - secure_write returns 0 (send failed)
 *    - errno is set to EPIPE
 */
static void
test__internal_flush_failedSendEPIPE(void **state)
{
	int			result;

	/* Simulating that secure_write will fail, and set the errno to EPIPE */
	expect_any(secure_write, port);
	expect_any(secure_write, ptr);
	expect_any(secure_write, len);
	static int errval = EPIPE;
	will_return_with_sideeffect(secure_write, 0, _set_errno, &errval);

	/* In that case, we expect ereport(COMERROR, ...) to be called */
	expect_value(errstart, elevel, COMMERROR);
	expect_any(errstart, domain);
	will_return(errstart, false);

	PqSendPointer = TEST_NUM_BYTES;

	/* Call function under test */
	result = internal_flush();

	assert_int_equal(result,EOF);
	assert_int_equal(ClientConnectionLost, 1);
	assert_int_equal(InterruptPending, 1);
}

/*
 * This is a mocked version of the accept() system call
 * We don't actually accept an incoming connection, but we just return
 * a socket from the global variable test_accept_socket.
 */
int
pqcomm_accept_mock(int accept_sock, struct sockaddr *restrict address,
				   socklen_t *restrict address_len)
{
	return test_accept_socket;
}

/*
 * NOTE (PG17): the StreamConnection() SO_SNDTIMEO unit tests were removed here.
 * PG17 (aafc05de1bf) replaced StreamConnection(server_fd, Port*) with
 * pq_init(ClientSocket*); the GPDB SO_SNDTIMEO-on-dispatcher logic was
 * re-grafted into pq_init() (see src/backend/libpq/pqcomm.c). pq_init()
 * palloc0()s its Port and has many side effects, so it is not cleanly
 * unit-testable here; the behavior is covered by cluster/segment tests.
 * TODO: re-port these SO_SNDTIMEO checks against pq_init().
 */


/* ==================== main ==================== */
int
main(int argc, char* argv[])
{
	cmockery_parse_arguments(argc, argv);

	const UnitTest tests[] = {
		unit_test(test__internal_flush_successfulSend),
		unit_test(test__internal_flush_failedSendEINTR),
		unit_test(test__internal_flush_failedSendEPIPE)
	};

	return run_tests(tests);
}
