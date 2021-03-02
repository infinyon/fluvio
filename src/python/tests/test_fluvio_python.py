from fluvio import Foo, python
import unittest

class TestPythonMethods(unittest.TestCase):
    def test_main(self):
        self.assertTrue(python.python_func() == 15)

class TestRustMethods(unittest.TestCase):
    def test_main(self):
        foo = Foo(10)
        print(foo.as_string())
        self.assertTrue(foo.as_string() == "FOO: 10")
