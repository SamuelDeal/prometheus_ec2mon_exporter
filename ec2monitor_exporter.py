#!/usr/bin/env python

"""
Collect ec2 instance data and expose them to prometheus.io via http interface

Note: the http interface is not protected so you should block it from outside using a firewall

Usage: ec2mon_exporter.py [-p PORT] [-f CONF_FILE]
"""

# Core libs
import argparse
import yaml
import json
import requests
import sys
import time
import os
import Queue
import threading
import re
import socket

# Third-party libs
from prometheus_client import start_http_server, Metric, REGISTRY, Gauge
from prometheus_client.core import GaugeMetricFamily
import boto.ec2


# global consts
DEFAULT_PORT = 9109


# Custom Error
class ExporterError(RuntimeError):
    pass


class Ec2Conn(threading.Thread):
    """
    Separated thread which will ask continuously aws for instance info

    Usage:
        conn = Ec2Conn("europe", "eu-west-1", "AKAIXXXXX", "XXXXX", {'status':"running"}
        conn.start()
        time.sleep(10)
        for data in conn.get():
            print repr(data)
         conn.stop()
    """

    def __init__(self, zone, region, aws_key, aws_secret, filter_conf):
        """
        Connection creation
        :param zone:            A zone name
        :type zone:             str
        :param region:          An Aws region (ex: "eu-west-1")
        :type region:           str
        :param aws_key:         An Aws access key (ex: "AKAIXXXX")
        :type aws_key:          str
        :param aws_secret:      An Aws access key secret
        :type aws_secret:       str
        :param filter_conf:     The configuration filter
        :type filter_conf:      dict
        """
        super(Ec2Conn, self).__init__()
        self._canceled = False
        self._zone = zone
        self._region = region
        self._aws_key = aws_key
        self._aws_secret = aws_secret
        self._tag_filters = {}
        if 'tags' in filter_conf:
            for tag, value in filter_conf['tags'].iteritems():
                self._tag_filters[tag.lower()] = {}
                if isinstance(value, basestring):
                    self._tag_filters[tag.lower()] = re.compile("^"+value+"$")
                else:
                    self._tag_filters[tag.lower()] = re.compile("^("+"|".join(value)+")$")
        if 'status' not in filter_conf:
            self._status_filter = re.compile(".*")
        elif isinstance(filter_conf['status'], basestring):
            self._status_filter = re.compile("^"+filter_conf['status']+"$")
        else:
            self._status_filter = re.compile("^("+"|".join(filter_conf['status'])+")$")
        self._data_queue = Queue.Queue()

    @property
    def zone(self):
        """ The name of the connection zone, :rtype: str """
        return self._zone

    @property
    def region(self):
        """ The Aws region of the connection, :rtype: str """
        return self._region

    def get(self):
        """ get collected informations, :yield: tuple[int, dict[str, str]] """
        try:
            while True:
                yield self._data_queue.get_nowait()
        except Queue.Empty:
            pass

    def close(self):
        """ Close the connection """
        self._canceled = True

    def stop(self):
        """ Close the connection and wait for it to finish """
        self._canceled = True
        self.join(3)

    def run(self):
        """ Thread main loop. Please do not call directly, call start() instead """
        conn = None
        try:
            conn = boto.ec2.connect_to_region(self._region,
                                              aws_access_key_id=self._aws_key,
                                              aws_secret_access_key=self._aws_secret
                                              )
            while not self._canceled:
                reservations = conn.get_all_reservations()
                instances = [i for r in reservations for i in r.instances]
                for instance in instances:
                    if self._canceled:
                        return
                    if not self._status_filter.match(instance.state):
                        continue
                    instance_tags = instance.__dict__['tags']
                    if not self._check_tags(instance_tags):
                        continue
                    tags = {"zone": self.zone, "region": self.region,
                            "machine": instance.instance_type, "ami": instance.image_id}
                    for tag, val in instance_tags.iteritems():
                        tags[tag] = val
                    tags = dict((k, v) for k, v in tags.iteritems() if v is not None)
                    self._data_queue.put_nowait((1, tags))
                if self._canceled:
                    return
                self._sleep(1)
        except KeyboardInterrupt:
            return
        except SystemExit:
            return
        finally:
            if conn:
                conn.close()

    def _check_tags(self, instance_tags):
        """
        Check if the instance is filtered
        :param instance_tags:   The list of tags of the instance
        :type instance_tags:    dict[str, str]
        :return:                False if we should skip the instance
        :rtype:                 bool
        """
        for tag, filter in self._tag_filters.iteritems():
            if tag not in instance_tags.keys():
                return False
            if not filter.match(instance_tags[tag]):
                return False
        return True

    def _sleep(self, sleep_time):
        """
        Private: Sleep for some time, but auto-cancel on stop() or close()
        :param sleep_time:  The number of seconds to sleep
        :type sleep_time:   float|int
        """
        start_sleeping = time.time()
        while not self._canceled:
            time.sleep(0.02)
            if time.time() - start_sleeping > sleep_time:
                return


class Ec2Collector(object):
    """
    Main class, collecting data for prometheus client API
    """
    def __init__(self, conf):
        """
        Create the collector instance
        :param conf:    The configuration of ec2
        :type conf:     dict
        """
        super(Ec2Collector, self).__init__()
        self._conn = []
        self._alive_metric = GaugeMetricFamily("ec2monitor_exporter_alive", "Ec2 Monitor Exporter Status", labels=['host'])
        self._metrics = {}
        for cloud in conf.keys():
            filter_conf = {}
            if 'filters' in conf[cloud].keys():
                filter_conf = conf[cloud]['filters']

            if 'access' not in conf[cloud].keys():
                raise ExporterError("Invalid config for "+str(cloud)+" config: no 'access' defined")
            if cloud == 'aws':
                for zone in conf[cloud]['access'].keys():
                    aws_key = conf[cloud]['access'][zone]['key']
                    aws_secret = conf[cloud]['access'][zone]['pwd']
                    for region in conf[cloud]['access'][zone]['region']:
                        conn = Ec2Conn(zone, region, aws_key, aws_secret, filter_conf)
                        conn.start()
                        self._conn.append(conn)
            else:
                raise ExporterError("Unknown cloud type '"+str(cloud)+"' in config file")

    def is_alive(self):
        for conn in self._conn:
            if conn.is_alive():
                return True
        return False

    def loop(self):
        """
        wait for all thread to finish
        """
        while self.is_alive():
            time.sleep(0.1)

    def collect(self):
        """
        get all the already collected metrics
        :yield:  GaugeMetricFamily
        """
        self._alive_metric.add_metric([socket.getfqdn()], 1)
        yield self._alive_metric
        for conn in self._conn:
            for val, tags in conn.get():
                frozen_tags = frozenset(sorted(tags.items(), key=lambda y: y[0]))
                if frozen_tags not in self._metrics.keys():
                    self._metrics[frozen_tags] = GaugeMetricFamily("ec2_number_instances",
                                                                   "Number of Aws EC2 instances",
                                                                   labels=sorted([x[0] for x in frozen_tags]))
                
                self._metrics[frozen_tags].add_metric([x[1] for x in sorted(frozen_tags, key=lambda y: y[0])], val)
                yield self._metrics[frozen_tags]

    def destroy(self):
        """ Close all Aws connection and wait for them to finish """
        for conn in self._conn:
            conn.close()
        for conn in self._conn:
            conn.stop()
        self._conn = {}


def main(port, config_file=None):
    """
    Main function.
    Parse config, create Aws connections to read data and create a web-server for prometheus

    :param port:            The http port the server will listen for incoming http requests
    :type port:             int
    :param config_file:     The path of the config file, optional. If none, look for config in the script folder
    :type config_file:      str|None
    :return:                The exit code
    :rtype:                 int
    """
    collector = None
    try:
        if not port:
            port = DEFAULT_PORT

        if not config_file:
            config_file = os.path.join(os.path.dirname(os.path.realpath(__file__)), "config.yml")

        if not os.path.exists(config_file):
            raise ExporterError("Unable to load config file '"+str(config_file)+"'")

        with open(config_file, "r") as cfg_fh:
            cfg_content = cfg_fh.read()

        collector = Ec2Collector(yaml.load(cfg_content))
        time.sleep(0.5)
        REGISTRY.register(collector)
        start_http_server(port)
        collector.loop()
    except KeyboardInterrupt:
        print ("\nExiting, please wait...")
        return 0
    except SystemExit:
        raise
    except ExporterError as error:
        sys.stderr.write(error.message)
        sys.stderr.write("\n")
        sys.stderr.flush()
        return 1
    finally:
        if collector is not None:
            collector.destroy()


if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument("--port", "-p", type=int, help="exporter port, default: "+str(DEFAULT_PORT))
    parser.add_argument("--config-file", "-f", help="configuration file")
    args = parser.parse_args()
    exit_code = 2
    try:
        exit_code = main(args.port, args.config_file)
    except KeyboardInterrupt: 
        exit_code = 0
        print "\nExiting, please wait..."
    except SystemExit:
        pass
    except ExporterError as e:
        sys.stderr.write(e.message)
        sys.stderr.write("\n")
        sys.stderr.flush()
        exit_code = 1
    sys.stdout.write("Bye bye\n")
    sys.stdout.flush()
    os._exit(exit_code)  # we call the brutal exit function because the web-server tends to never close

