import sys
import simpledaemon
import logging as logger
import puka
import json
import MySQLdb

class DigesterDaemon(simpledaemon.Daemon):

    default_conf = '/etc/digester.conf'
    section = 'digester'

    def conf(self, configname):
        """ read in configurations from file and establish as part of the inner workings of this daemon object """

        # first, make sure we have a configs list initialized
        if not hasattr(self, 'configs'):
            self.configs = dict()

        # next, see if the configuration we want is not available
        if configname not in self.configs:
            self.configs[configname] = self.config.get(self.section, configname)
            logger.debug('configuration \'%s\' given value: %s' % (configname, self.configs[configname]))

        return self.configs[configname]

    def run(self):
        # init our varying connection wrappers
        self.config = self.config_parser

        rabbit_queue = self.conf('rabbitqueue')
        rabbitmq_url = self.conf('rabbitmq_url')
       
	client = puka.Client(rabbitmq_url)
	promise = client.connect()
	client.wait(promise)

	promise = client.queue_declare(queue=rabbit_queue, durable=True)
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

        connection = MySQLdb.connect(host, user, password, database, autocommit=True)
        cursor = connection.cursor()
        while True:
  		try:
			result = client.wait(consume_promise)
                        payload = json.loads(result['body'])
                        key = payload[mysql_key_field]

                        #Do some processing to get all they key/val pairs
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
                            values.append(v)
                            logger.debug(k)
                            if len(v) > 0 and len(v) < int(mysql_field_length):
                                try:
                                    cursor.execute(mysql_insert_query, values)
                                    connection.commit()
                                except Exception as e:
                                    logger.error(e)
                                    connection.rollback()

    		        client.basic_ack(result)

		except KeyboardInterrupt as e:
                        logger.error(e)
			promise = client.close()
			client.wait(promise)
                        connection.close()
			raise
                except Exception as e:
                        connection = MySQLdb.connect(host, user, password, database, autocommit=True)
                        cursor = connection.cursor()

if __name__ == '__main__':
    DigesterDaemon().main()
