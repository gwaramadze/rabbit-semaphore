import time
import threading
import unittest
import yaml
from semaphore import Semaphore

# thread execution order
TEO = 0

def thread(settings, results):
    global TEO

    duration = 5

    semaphore = Semaphore(
        resource_name='testing',
        rabbitmq_username=settings['username'],
        rabbitmq_password=settings['password'],
        rabbitmq_ip=settings['ip'],
        rabbitmq_port=settings['port'],
        rabbitmq_api_port=settings['api_port'],
        rabbitmq_virtual_host=settings['virtual_host'])

    try:
        TEO += 1
        order = TEO

        start = time.time()
        semaphore.acquire()
        delta = time.time() - start

        print 'Thread {} waited {} seconds for lock. OK!'.format(order, delta)

        """First thread should acquire lock very quickly. Threads 2 and 3 are
        waiting at least 5 and 10 seconds respectively.
        """
        switch = {1: (delta, 1),
                  2: (duration, delta),
                  3: (duration*2, delta)}
        results.append(switch[order][0] < switch[order][1])

        time.sleep(duration) # accesing hypothetical resource here
    finally:
        semaphore.release()

class SemaphoreTest(unittest.TestCase):
    def setUp(self):
        with open('tests.yaml') as f:
            self.settings = yaml.load(f)
        self.semaphore = Semaphore(
            resource_name='testing',
            max_connections=1,
            rabbitmq_username=self.settings['username'],
            rabbitmq_password=self.settings['password'],
            rabbitmq_ip=self.settings['ip'],
            rabbitmq_port=self.settings['port'],
            rabbitmq_api_port=self.settings['api_port'],
            rabbitmq_virtual_host=self.settings['virtual_host'])
        time.sleep(2)

    def tearDown(self):
        self.semaphore.destroy()

    def test_initialization(self):
        self.assertTrue(self.semaphore.channel.is_open)
        self.assertEqual(self.semaphore.get_current_max(), 1)

    def test_change_limit(self):
        self.semaphore.change_limit(100)
        time.sleep(2)
        self.assertEqual(self.semaphore.get_current_max(), 100)

    def test_multithreading(self):
        results = []

        t1 = threading.Thread(target=thread, args=(self.settings, results))
        t2 = threading.Thread(target=thread, args=(self.settings, results))
        t3 = threading.Thread(target=thread, args=(self.settings, results))
        t1.start()
        t2.start()
        t3.start()
        t1.join()
        t2.join()
        t3.join()

        for result in results:
            self.assertTrue(result)

if __name__ == "__main__":
    unittest.main()
