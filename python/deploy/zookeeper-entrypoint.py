#! /usr/bin/env python

"""Zookeeper entrypoint script."""


import os
import socket


def find_zookeeper_id():
    """
    Find self zookeeper ID by comparing IP addresses
    """
    zookeeper_ips_str = os.environ.get("ZOOKEEPER_IPS")
    assert zookeeper_ips_str, "ZOOKEEPER_IPS env variable must be set"
    zookeeper_ips = zookeeper_ips_str.split(",")

    self_ip = socket.gethostbyname(socket.getfqdn())
    for idx, zookeeper_ip in enumerate(zookeeper_ips):
        if zookeeper_ip == self_ip:
            return idx + 1

    assert False, \
        "No entry for %s found in ZOOKEEPER_IPS: %s" % (self_ip, zookeeper_ips)

    return None


def write_zookeeper_id():
    """
    Write zookeeper ID to myid file
    """
    self_id = find_zookeeper_id()
    with open("/data/zookeeper/myid", "w") as f:
        f.write("%s" % self_id)


def main():
    """
    Main function
    """
    write_zookeeper_id()
    os.execv("/opt/druid/bin/supervise",
             ("/opt/druid/bin/supervise", "-c",
              "/opt/druid/conf/supervise/zookeeper.conf"))


if __name__ == '__main__':
    main()
