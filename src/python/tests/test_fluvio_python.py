from fluvio import python, Fluvio
import unittest

class TestPythonMethods(unittest.TestCase):
    def test_main(self):
        self.assertTrue(python.python_func() == 15)

class TestFluvioMethods(unittest.TestCase):
    def test_connect(self):
        fluvio = Fluvio.connect()

        producer = fluvio.topic_producer('my-topic')
        for i in range(100):
            producer.send_record("FOOBAR %s " % i, 0)

        consumer = fluvio.partition_consumer('my-topic', 0)
        stream = consumer.stream(0)
        curr = stream.next()
        while curr is not None:
            print(curr)
            if curr == "stop":
                break
            curr = stream.next()
