import sys
import simpledaemon
import logging as logger
import puka
import json
import time

from torndb import Connection


class DigesterDaemon(simpledaemon.Daemon):
    default_conf = '/etc/digester.conf'
    section = 'digester'

    def conf(self, name):
        """
        Read in configurations from file and establish as part of the inner
        workings of this daemon object.
        """
        # First, make sure we have a configs list initialized.
        if not hasattr(self, 'configs'):
            self.configs = dict()

        # Next, see if the configuration we want is not available.
        if name not in self.configs:
            self.configs[name] = self.config.get(self.section, name)
            logger.debug("configuration '{0}' given value: {1}".format(name, self.configs[name]))

        return self.configs[name]

    def run(self):
        # init our varying wrappers
        self.config = self.config_parser

        rabbit_queue = self.conf('rabbitqueue')
        rabbit_requeue = self.conf('rabbitrequeue')
        rabbitmq_url = self.conf('rabbitmq_url')

        client = puka.Client(rabbitmq_url)
        promise = client.connect()
        client.wait(promise)

        promise = client.queue_declare(queue=rabbit_queue, durable=True)
        client.wait(promise)
        if rabbit_requeue:
            promise = client.queue_declare(queue=rabbit_requeue, durable=True)
            client.wait(promise)
 
        consume_promise = client.basic_consume(queue=rabbit_queue, prefetch_count=1)

        host = self.conf('mysql_host')
        user = self.conf('mysql_user')
        password = self.conf('mysql_password')
        database = self.conf('mysql_database')
        table = self.conf('mysql_table')
        mysql_query_fields = self.conf('mysql_query_fields').split(",")
        mysql_key_field = self.conf('mysql_key_field')
        mysql_field_length = self.conf('mysql_field_length')
        mysql_insert_query = self.conf('mysql_insert_query')

        mysqldb = Connection(host, database, user=user, password=password)

        while True:
            try:
                result = client.wait(consume_promise)
                payload = json.loads(result['body'])
                key = payload[mysql_key_field]

                # Do some processing to get all they key/val pairs
                for k, v in payload['body'].iteritems():
                    values = []

                    for field in mysql_query_fields:
                        if isinstance(payload[field], basestring):
                            values.append(payload[field].strip())
                        else:
                            values.append(payload[field])
                    if k == 'body':
                        values.append(key)
                    else:
                        values.append(k)
                    values.append(v)
                    logger.debug(k)
                    if len(v) > 0 and len(v) < int(mysql_field_length):
                        try:
                            mysqldb.executemany(mysql_insert_query, [values])
                        except Exception as e:
                            logger.error(e)

                client.basic_ack(result)
                
                if rabbit_requeue:
                    promise = client.basic_publish(exchange='', routing_key=rabbit_requeue, body=result['body'])
                    client.wait(promise)
            except KeyboardInterrupt as e:
                logger.error(e)
                promise = client.close()
                client.wait(promise)
                mysqldb.close()
                raise


def main():
    DigesterDaemon().main()

if __name__ == '__main__':
    main()
