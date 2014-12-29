#!/usr/bin/env python
import SimpleHTTPServer
import SocketServer
import logging
import cgi
import json
import socket
import sys

STATSD_METRICS = {
    "derive" : "c",
    "gauge" : "g",
    "counter": "c"
}

class ServerHandler(SimpleHTTPServer.SimpleHTTPRequestHandler):

    def do_GET(self):
        SimpleHTTPServer.SimpleHTTPRequestHandler.do_GET(self)

    def do_POST(self):
        form = cgi.FieldStorage(
            fp=self.rfile,
            headers=self.headers,
            environ={'REQUEST_METHOD':'POST',
                     'CONTENT_TYPE':self.headers['Content-Type'],
                     })

        push_to_statsd(form.value)
        SimpleHTTPServer.SimpleHTTPRequestHandler.do_GET(self)

    def log_request(code, size):
        pass

# host . plugin . plugin_instance . type . type_instance . [dsnames] : [values] | [dstypes]
def push_to_statsd(form):
    payload = create_payload(form)
    sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
    sock.sendto("\n".join(payload), (statsd_host, statsd_port))
    sock.close()

def create_payload(form):
    payload = []
    metrics = json.loads(form)
    for m in metrics:
        for n in xrange(len(m["dsnames"])):
            plugin_instance = ""
            type_instance = ""
            if len(m["plugin_instance"]) > 0:
                plugin_instance = ".{0}".format(m["plugin_instance"])
            if len(m["type_instance"]) > 0:
                type_instance = ".{0}".format(m["type_instance"])
            payload.append("{0}.{1}{2}.{3}{4}.{5}:{6}|{7}".format(m["host"], m["plugin"],
                                                           plugin_instance, m["type"],
                                                           type_instance, m["dsnames"][n],
                                                           m["values"][n], statsd_metric_type(m["dstypes"][n])))
    return payload


def statsd_metric_type(dstype):
    if dstype in STATSD_METRICS:
        return STATSD_METRICS[dstype]
    else:
        return "c"

def start_webserver(webserver_host, webserver_port):
    httpd = SocketServer.TCPServer((webserver_host, webserver_port), ServerHandler)
    logging.debug("Serving {0}:{1} to {2}:{3}".format(webserver_host, webserver_port, statsd_host, statsd_port))
    httpd.serve_forever()

def main():
  if len(sys.argv[1:]) != 4:
    print "Usage: ./collectd-statsd-proxy.py [webserverhost] [webserverport] [statsdhost] [statsdport]"
    print "Example: ./collectd-statsd-proxy.py 127.0.0.1 9000 10.12.132.1 8125"
    sys.exit(0)
    
  webserver_host  = sys.argv[1]
  webserver_port  = int(sys.argv[2])
  
  global statsd_host
  statsd_host = sys.argv[3]
  global statsd_port
  statsd_port = int(sys.argv[4])
  
  start_webserver(webserver_host, webserver_port)
        
main() 
