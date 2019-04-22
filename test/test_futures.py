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
        # Test copy
        source = kiwipy.Future()
        target = kiwipy.Future()

        source.set_result("DONE")
        kiwipy.copy_future(source, target)

        self.assertEqual(target.result(), "DONE")

        # Test copy already done
        source = kiwipy.Future()
        target = kiwipy.Future()
        target.set_result("Already Done")
        self.assertTrue(target.done())

        source.set_result("Source Done")
        kiwipy.copy_future(source, target)

        self.assertEqual(target.result(), "Already Done")

    def test_chain(self):
        source = kiwipy.Future()
        target = kiwipy.Future()

        # chain two futures
        kiwipy.chain(source, target)

        # set source done
        source.set_result("DONE")
        self.assertEqual(target.result(), "DONE")
