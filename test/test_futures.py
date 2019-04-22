import unittest

import kiwipy


class TestUtilities(unittest.TestCase):

    def test_capture_exceptions(self):
        future = kiwipy.Future()

        exception = RuntimeError()
        with kiwipy.capture_exceptions(future):
            raise exception

        self.assertIs(exception, future.exception())

    def test_copy_future(self):
        source = kiwipy.Future()
        target = kiwipy.Future()

        source.set_result("DONE")
        kiwipy.copy_future(source, target)

        self.assertEqual(target.result(), "DONE")

    def test_chain(self):
        source = kiwipy.Future()
        target = kiwipy.Future()

        # chain two futures
        kiwipy.chain(source, target)

        # set source done
        source.set_result("DONE")
        self.assertEqual(target.result(), "DONE")
