from namespace_package import rust, python
import unittest

class TestPythonMethods(unittest.TestCase):
    def test_main(self):
        self.assertTrue(python.python_func() == 15)

class TestRustMethods(unittest.TestCase):
    def test_main(self):
        self.assertTrue(rust.rust_func() == 14)
