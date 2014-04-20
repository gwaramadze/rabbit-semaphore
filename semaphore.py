import pika
import requests

class Semaphore(object):
    def __init__(self, resource_name, rabbitmq_ip, rabbitmq_username,
                 rabbitmq_password, resource_limit=None, rabbitmq_port=5672,
                 rabbitmq_virtual_host=None, rabbitmq_api_port=15672):

        self.queue = '{}.semaphore'.format(resource_name)
        self.username = rabbitmq_username
        self.password = rabbitmq_password
        self.ip = rabbitmq_ip
        self.port = rabbitmq_port
        self.api_port = rabbitmq_api_port
        self.vhost = rabbitmq_virtual_host or '/'

        credentials = pika.PlainCredentials(self.username, self.password)
        parameters = pika.ConnectionParameters(
            host=self.ip,
            port=self.port,
            virtual_host=self.vhost,
            credentials=credentials)

        self.connection = pika.BlockingConnection(parameters)
        self.channel = self.connection.channel()

        self.queue_status = self.channel.queue_declare(queue=self.queue, durable=True)
        self.resource_limit = resource_limit
        self.sync_resource_limit()

    def sync_resource_limit(self):
        old_resource_limit = self._get_total_messages()

        if not old_resource_limit and not self.resource_limit:
            raise Exception('Semaphore is empty. Pass resource_limit argument')
        elif old_resource_limit < self.resource_limit:
            for i in xrange(self.resource_limit - old_resource_limit):
                self.channel.basic_publish(exchange='',
                                           routing_key=self.queue,
                                           body='1')
        elif old_resource_limit > self.resource_limit:
            for i in xrange(old_resource_limit - self.resource_limit):
                self.channel.basic_consume(self.__acknowledge_message,
                                           queue=self.queue,
                                           arguments={'x-priority': 9})

                self.channel.start_consuming()

    def __acknowledge_message(self, ch, method, properties, body):
        self.channel.basic_ack(delivery_tag=method.delivery_tag)
        self.channel.stop_consuming()

    def __lock_acquired(self, ch, method, properties, body):
        self.delivery_tag = method.delivery_tag
        self.channel.stop_consuming()

    def _get_total_messages(self):
        vhost = self.vhost if self.vhost != '/' else '%2F'
        url = 'http://{0.ip}:{0.api_port}/api/queues/{1}/{0.queue}'.format(self, vhost)
        result = requests.get(url, auth=(self.username, self.password)).json()
        return result["messages_ready"] + result["messages_unacknowledged"]

    def disconnect(self):
        self.connection.close()

    def acquire(self):
        self.channel.basic_qos(prefetch_count=1)
        self.channel.basic_consume(self.__lock_acquired, queue=self.queue)
        self.channel.start_consuming()

    def release(self):
        self.channel.basic_reject(delivery_tag=self.delivery_tag)
