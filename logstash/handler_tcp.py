from logging.handlers import SocketHandler
from logstash import formatter
from ssl import wrap_socket


# Derive from object to force a new-style class and thus allow super() to work
# on Python 2.6
class TCPLogstashHandler(SocketHandler, object):
    """Python logging handler for Logstash. Sends events over TCP.
    :param host: The host of the logstash server.
    :param port: The port of the logstash server (default 5959).
    :param keyfile: The ssl key file (default None).
    :param certfile: The ssl certification file (default None).
    :param ca_certs: The ssl ca certificates (default None).
    :param message_type: The type of the message (default logstash).
    :param fqdn; Indicates whether to show fully qualified domain name or not (default False).
    :param version: version of logstash event schema (default is 0).
    :param tags: list of tags for a logger (default is None).
    """

    def __init__(self, host, port=5959, keyfile=None, certfile=None, ca_certs=None,
                ssl=False, message_type='logstash', tags=None, fqdn=False, version=0):
        super(TCPLogstashHandler, self).__init__(host, port)

        self.keyfile = keyfile
        self.certfile = certfile
        self.ca_certs = ca_certs
        self.ssl = ssl

        if version == 1:
            self.formatter = formatter.LogstashFormatterVersion1(message_type, tags, fqdn)
        else:
            self.formatter = formatter.LogstashFormatterVersion0(message_type, tags, fqdn)

    def makeSocket(self, timeout=1):
        socket = SocketHandler.makeSocket(self, timeout)

        if self.ssl:
          return wrap_socket(socket, keyfile=self.keyfile, certfile=self.certfile, ca_certs=self.ca_certs)

        return socket

    def makePickle(self, record):
        return self.formatter.format(record) + b'\n'
