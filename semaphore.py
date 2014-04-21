import pika
import requests

class Semaphore(object):
    def __init__(self, resource_name, rabbitmq_ip, rabbitmq_username,
                 rabbitmq_password, max_connections=None, rabbitmq_port=5672,
                 rabbitmq_virtual_host='/', rabbitmq_api_port=15672):

        self.queue = '{}.semaphore'.format(resource_name)
        self.username = rabbitmq_username
        self.password = rabbitmq_password
        self.ip = rabbitmq_ip
        self.port = rabbitmq_port
        self.api_port = rabbitmq_api_port
        self.vhost = rabbitmq_virtual_host

        credentials = pika.PlainCredentials(self.username, self.password)
        self.parameters = pika.ConnectionParameters(
            host=self.ip,
            port=self.port,
            virtual_host=self.vhost,
            credentials=credentials)

        self.connect()
        self.channel.queue_declare(queue=self.queue, durable=True)

        """Initializing without max_connections argument defaults to 1.
        Client connecting to existing semaphore is allowed to change limit.
        """
        current_max = self.get_current_max()
        if max_connections is not None:
            if max_connections != current_max:
                self.change_limit(max_connections, current_max=current_max)
        elif current_max == 0:
            self.change_limit(1, current_max)

    def __acknowledge_message(self, ch, method, properties, body):
        """This callback is used when reducing max_connections
        """
        self.channel.basic_ack(delivery_tag=method.delivery_tag)
        self.channel.stop_consuming()

    def __lock_acquired(self, ch, method, properties, body):
        """Stop consuming and return control to whatever follows acquire().
        delivery tag will be used for release.
        """
        self.delivery_tag = method.delivery_tag
        self.channel.stop_consuming()

    def __test_connection(self):
        if self.channel.is_closed:
            raise Exception('Connection closed, you need to reconnect.')
        else:
            return True

    def get_current_max(self):
        """Hacked through RabbitMQ http api. No idea how to get total
        message count from pika.
        """
        vhost = self.vhost if self.vhost != '/' else '%2F'
        url = 'http://{0.ip}:{0.api_port}/api/queues/{1}/{0.queue}'.format(self, vhost)
        result = requests.get(url, auth=(self.username, self.password)).json()
        return result["messages_ready"] + result["messages_unacknowledged"]

    def change_limit(self, max_connections, current_max=None):
        self.__test_connection()

        current_max = current_max or self.get_current_max()

        if current_max < max_connections:
            for i in xrange(max_connections - current_max):
                self.channel.basic_publish(exchange='',
                                           routing_key=self.queue,
                                           body='1')
        elif current_max > max_connections:
            for i in xrange(current_max - max_connections):
                self.channel.basic_consume(self.__acknowledge_message,
                                           queue=self.queue,
                                           arguments={'x-priority': 9})
                self.channel.start_consuming()
        else:
            """Flooding this method won't work, eg.
            semaphore.change_limit(2)
            semaphore.change_limit(1)
            semaphore.change_limit(3)
            """
            raise Exception('New resource limit is equal to old resource limit\n'
                            'You either provided wrong value or multiple '
                            'change_limit() requests in a short period od time.')

    def connect(self):
        self.connection = pika.BlockingConnection(self.parameters)
        self.channel = self.connection.channel()

    def disconnect(self):
        self.channel.close()
        self.connection.close()

    def destroy(self):
        self.channel.queue_delete(queue=self.queue)
        self.disconnect()

    def acquire(self):
        self.channel.basic_qos(prefetch_count=1)
        self.channel.basic_consume(self.__lock_acquired, queue=self.queue)
        self.channel.start_consuming()

    def release(self):
        self.channel.basic_reject(delivery_tag=self.delivery_tag)
