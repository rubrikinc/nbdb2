#!/usr/bin/env python3
"""
Utility to list AnomalyDB service resources
"""
import argparse
import json
import os
import paramiko
import sys
import subprocess
import threading
import time

from paramiko.client import SSHClient

"""
ENTER YOUR JUMPBOX IP ADDRESS HERE
"""
JUMPBOX_IP = "X.X.X.X"

args = None


aliases = {
    'zk': {
        '--cluster': 'anomalydb-druid-zookeeper',
        '--service': 'anomalydb-druid-zookeeper',
        '--cid-pattern': 'druidzookeeper-',
        '--ps-pattern': 'org.apache.zookeeper.server.quorum.QuorumPeerMain',
        '--listen-port-check': 2181
    },
    'zk-monitor': {
        '--cluster': 'anomalydb-druid-zookeeper',
        '--service': 'anomalydb-druid-zookeeper-monitor',
        '--cid-pattern': 'druidzookeepermonitor-',
        '--ps-pattern': 'nbdb/utils/zookeeper-monitor.py',
        '--listen-port-check': 0
    },
    'readapi': {
        '--cluster': 'anomalydb-druid-query',
        '--service': 'anomalydb-read-api',
        '--cid-pattern': 'anomalydb-read-api',
        '--ps-pattern': 'nbdb/api/home.py',
        '--listen-port-check': 0
    },
    'overlord': {
        '--cluster': 'anomalydb-druid-master',
        '--service': 'anomalydb-druid-overlord',
        '--cid-pattern': 'anomalydb-druid-overlord',
        '--ps-pattern': 'org.apache.druid.cli.Main server overlord',
        '--listen-port-check': 8090
    },
    'coordinator': {
        '--cluster': 'anomalydb-druid-master',
        '--service': 'anomalydb-druid-coordinator',
        '--cid-pattern': 'anomalydb-druid-coordinator',
        '--ps-pattern': 'org.apache.druid.cli.Main server coordinator',
        '--listen-port-check': 8081
    },
    'query': {
        '--cluster': 'anomalydb-druid-query',
        '--service': 'anomalydb-druid-query',
        '--cid-pattern': 'anomalydb-druid-query-',
        '--ps-pattern': 'org.apache.druid.cli.Main server broker',
        '--listen-port-check': 8082
    },
    'query-cc': {
        '--cluster': 'anomalydb-druid-query',
        '--service': 'anomalydb-druid-query_cc',
        '--cid-pattern': 'anomalydb-druid-query_cc',
        '--ps-pattern': 'org.apache.druid.cli.Main server broker',
        '--listen-port-check': 8082
    },
    'historical': {
        '--cluster': 'anomalydb-druid-historical',
        '--service': 'anomalydb-druid-historical',
        '--cid-pattern': 'anomalydb-druid-historical',
        '--ps-pattern': 'org.apache.druid.cli.Main server historical',
        '--listen-port-check': 8083
    },
    'mm-regular': {
        '--cluster': 'anomalydb-druid-middlemanager',
        '--service': 'anomalydb-druid-mm-regular',
        '--cid-pattern': 'druidmiddlemanager',
        '--ps-pattern': 'org.apache.druid.cli.Main server middleManager',
        '--listen-port-check': 8091
    },
    'mm-cc': {
        '--cluster': 'anomalydb-druid-middlemanager',
        '--service': 'anomalydb-druid-mm-crosscluster',
        '--cid-pattern': 'druidmiddlemanager',
        '--ps-pattern': 'org.apache.druid.cli.Main server middleManager',
        '--listen-port-check': 8091
    },
    'prod_no_agg-graphite-consumer-realtime': {
        '--cluster': 'anomalydb-metric-consumers',
        '--service': 'prod_no_agg-graphite-consumer-realtime',
        '--cid-pattern': 'prod_no_agg-graphite-consumer-realtime',
        '--ps-pattern': 'consumer_mode=realtime --setting_file=nbdb/config/pro'
                        'd_no_agg_graphite_consumer.yaml',
        '--listen-port-check': 0
    },
    'prod_no_agg-graphite-consumer-rollup': {
        '--cluster': 'anomalydb-metric-consumers',
        '--service': 'prod_no_agg-graphite-consumer-rollup',
        '--cid-pattern': 'prod_no_agg-graphite-consumer-rollup',
        '--ps-pattern': 'consumer_mode=rollup --setting_file=nbdb/config/prod_'
                        'no_agg_graphite_consumer.yaml',
        '--listen-port-check': 0
    },
}


def err_exit(error_message, exit_code=1):
    print(f'ERROR: {error_message}', file=sys.stderr)
    sys.exit(exit_code)


def print_json(j):
    print(json.dumps(j, skipkeys=True, indent=2))


def apply_alias(alias_name):
    if alias_name not in aliases:
        err_exit(f'Invalid alias "{alias_name}". Available aliases: '
                 f'{list(aliases)}')
    args.cluster = aliases[alias_name]['--cluster']
    args.service = aliases[alias_name]['--service']
    args.cid_pattern = aliases[alias_name]['--cid-pattern']
    args.ps_pattern = aliases[alias_name]['--ps-pattern']
    args.listen_port_check = aliases[alias_name]['--listen-port-check']


def parse_args(argv):
    parser = argparse.ArgumentParser(description='List ADB resources')

    parser.add_argument('--alias', action='store', type=str, default='',
                        help='Use args from the alias definition')
    parser.add_argument('--arn_prefix', action='store', type=str,
                        default='arn:aws:<id>:0123456789',
                        help='Specify the arn prefix for the ECS clusters')
    parser.add_argument('--cluster', action='store', type=str, default='',
                        help='Cluster name to fetch info for')
    parser.add_argument('--service', action='store', type=str, default='',
                        help='Service name to fetch info for')
    parser.add_argument('--list-aliases', action='store_true', default=False,
                        help='List all aliases')
    parser.add_argument('--list-clusters', action='store_true', default=False,
                        help='List all clusters')
    parser.add_argument('--list-services', action='store_true', default=False,
                        help='List all service names for a given cluster')
    parser.add_argument('--list-ips', action='store_true', default=False,
                        help='List EC2 private IPs for a given service')
    parser.add_argument('--cid-pattern', action='store', type=str, default='',
                        help='String pattern to grep on docker ps')
    parser.add_argument('--ps-pattern', action='store', type=str, default='',
                        help='String pattern to grep on ps -ef')
    parser.add_argument('--listen-port-check', action='store', type=int,
                        default=0, help='Check whether port is listened on')
    parser.add_argument('-v', '--verbose', action='store_true', default=False,
                        help='Print verbose info for debugging')
    if len(argv) == 0:
        parser.print_help(sys.stderr)
        sys.exit(0)
    return parser.parse_args()


def runcmd(cmd, print_err=True):
    """
    run shell command
    """
    if args.verbose:
        print(f'CMD: {cmd}')

    with subprocess.Popen(cmd, shell=True,
                          stdout=subprocess.PIPE,
                          stderr=subprocess.PIPE) as process:
        out, err = process.communicate()
        if err and print_err:
            # print to user any messages written to stderr
            print(err, file=sys.stderr)
        return str(out, 'utf-8'), str(err, 'utf-8')


def runcmd_json(cmd):
    """
    run shell command and parse output as JSON
    """
    out, err = runcmd(cmd)
    out_json = json.loads(out)
    if args.verbose:
        print(out_json)
    return out_json


def get_ssh_config(ip):
    cfg = {
        'hostname': ip,
        'timeout': 60,
        'username': 'ec2-user'
    }
    if os.path.exists(os.path.expanduser("~/.ssh/config")):
        ssh_config = paramiko.SSHConfig()
        user_config_file = os.path.expanduser("~/.ssh/config")
        with open(user_config_file, 'rt', encoding='utf-8') as f:
            ssh_config.parse(f)
        host_conf = ssh_config.lookup(ip)
        if host_conf:
            if 'proxycommand' in host_conf:
                cfg['sock'] = paramiko.ProxyCommand(
                    # TODO recursively read bastion IP from ssh config file
                    # host_conf['proxycommand']
                    f"ssh -W {ip}:22 ec2-user@{JUMPBOX_IP}"
                )
            if 'identityfile' in host_conf:
                cfg['key_filename'] = host_conf['identityfile']
    return cfg


results = {}


class _WorkerThread(threading.Thread):
    def __init__(self, ip):
        threading.Thread.__init__(self)
        self.m_ip = ip

    def get_ip(self):
        return self.m_ip

    def run(self):
        ssh_client = SSHClient()
        ssh_client.load_system_host_keys()
        ssh_client.set_missing_host_key_policy(paramiko.MissingHostKeyPolicy())
        ssh_client.connect(**get_ssh_config(self.m_ip))

        # root disk used %
        _, stdout, _ = ssh_client.exec_command(
            'df | grep "% /$" | awk \'{print $5}\'')
        out = stdout.read().decode('utf-8').strip()
        root_disk_use = out.strip('%\n')

        # mem available
        _, stdout, _ = ssh_client.exec_command(
            'cat /proc/meminfo | grep MemAvailable | awk \'{print $2}\'')
        out = stdout.read().decode('utf-8').strip()
        meminfo_list = list(filter(None, out.split('\n')))
        mem_avail = int(int(meminfo_list[0]) / 1000)  # KB -> MB

        # container ID
        if args.cid_pattern:
            _, stdout, _ = ssh_client.exec_command(
                f'docker ps | grep "{args.cid_pattern}"')
            out = stdout.read().decode('utf-8').strip()
            docker_ps_lines = list(filter(None, out.split('\n')))
            n_docker_ps_lines = len(docker_ps_lines)
            if n_docker_ps_lines == 0:
                cont_id = '<ERROR:none>'
            elif n_docker_ps_lines == 1:
                cont_id = docker_ps_lines[0].split()[0]
            else:
                cont_id = '<ERROR:many>'
        else:
            cont_id = '<NA>'

        # pid, stime, time
        if args.ps_pattern:
            _, stdout, _ = ssh_client.exec_command(
                f'ps -ef | grep -E "{args.ps_pattern}" | grep -v "grep -E" | '
                f'awk \'{{print $2,$5,$7}}\'')
            out = stdout.read().decode('utf-8').strip()
            tokens = out.split()
            pid, stime, tm = tokens if len(tokens) == 3 else ['<ERROR>'] * 3
        else:
            pid, stime, tm = ['<NA>'] * 3

        # port check
        if args.listen_port_check:
            _, stdout, _ = ssh_client.exec_command(
                f'sudo netstat -ntlp | grep :{args.listen_port_check}')
            out = stdout.read().decode('utf-8').strip()
            netstat_lines = list(filter(None, out.split('\n')))
            n_netstat_lines = len(netstat_lines)
            if n_netstat_lines == 0:
                port_check_status = 'DOWN'
            elif n_netstat_lines == 1:
                port_check_status = 'LISTEN'
            else:
                port_check_status = 'Two processes???'
        else:
            port_check_status = '<NA>'

        # print a row for this EC2 instance
        results[self.m_ip] = ('%-18s%-10s%-12s%-16s%-12s%-12s%-15s%-10s' %
                              (self.m_ip, root_disk_use, mem_avail, cont_id,
                               pid, stime, tm, port_check_status))


def get_clusters():
    cluster_arns = runcmd_json('aws ecs list-clusters')
    cluster_names = []
    for cluster_arn in cluster_arns['clusterArns']:
        cluster_names.append(cluster_arn.split('/')[-1])
    return sorted(cluster_names)


def get_services(cluster_name: str):
    if not cluster_name:
        err_exit('Missing --cluster arg')

    service_arns = runcmd_json(f'aws ecs list-services --cluster '
                               f'{args.arn_prefix}:cluster/{cluster_name}')
    service_names = []
    for service_arn in service_arns['serviceArns']:
        service_names.append(service_arn.split('/')[-1])
    return sorted(service_names)


def chunks(lst, n):
    """Yield successive n-sized chunks from lst."""
    for i in range(0, len(lst), n):
        yield lst[i:i + n]


def get_service_info(cluster_name: str, service_name: str):
    cluster = args.arn_prefix + ':cluster/' + cluster_name

    service_info = {
        'cluster': cluster,
        'service': service_name
    }
    list_tasks = runcmd_json(f'aws ecs list-tasks '
                             f'--cluster {cluster} '
                             f'--service-name {service_name}')

    task_count = len(list_tasks["taskArns"])
    service_info['task_count'] = task_count
    service_info['ci_count'] = 0
    ec2_ips_list = []

    # describe-tasks can only be run with at most 100 tasks, so do it in a loop
    for task_list in list(chunks(list_tasks["taskArns"], 100)):
        describe_tasks = runcmd_json(f'aws ecs describe-tasks '
                                     f'--cluster {cluster} '
                                     f'--tasks {" ".join(task_list)}')
        ci_arns = ' '.join([task['containerInstanceArn'] for task in
                            describe_tasks['tasks']])
        describe_cis = runcmd_json(f'aws ecs describe-container-instances '
                                   f'--cluster {cluster} '
                                   f'--container-instances {ci_arns}')
        service_info['ci_count'] += len(describe_cis['containerInstances'])
        ec2_ids = ' '.join([ci['ec2InstanceId'] for ci in
                            describe_cis['containerInstances']])
        ec2_ips_out, _ = runcmd('aws ec2 describe-instances --instance-ids '
                                '%s | grep "PrivateIpAddress" | '
                                'awk -F\':\' \'{print $2}\' | '
                                'awk -F\'"\' \'{print $2}\' | sort | uniq' %
                                ec2_ids)
        ec2_ips_list += list(filter(None, ec2_ips_out.split('\n')))

    # drop duplicate ips
    ec2_ips_list = list(set(ec2_ips_list))

    service_info["ec2_ips_count"] = len(ec2_ips_list)
    service_info["ec2_ips"] = sorted(ec2_ips_list,
                                     key=lambda ipaddr: [int(x) for x in
                                                         ipaddr.split('.')])
    return service_info


def main():
    if args.list_aliases:
        return print_json(aliases)

    if args.list_clusters:
        return print_json(get_clusters())

    if args.alias:
        apply_alias(args.alias)

    if args.list_services:
        return print_json(get_services(args.cluster))

    if not args.cluster or not args.service:
        err_exit('Both --cluster and --service args are mandatory')

    service_info = get_service_info(args.cluster, args.service)

    if args.list_ips:
        return print_json(service_info)

    print(f'\nService \'{args.service}\' has:\n'
          f'- {service_info["task_count"]} tasks\n'
          f'- {service_info["ci_count"]} container instances\n'
          f'- {service_info["ec2_ips_count"]} EC2 private IP addresses')

    n = 0
    workers = []
    for ip in service_info['ec2_ips']:
        n += 1
        worker = _WorkerThread(ip)
        worker.start()
        workers.append(worker)
        if n % 10 == 0:
            time.sleep(2)

    print('\n%-18s%-10s%-12s%-16s%-12s%-12s%-15s%-10s\n%s' %
          ('ip', 'rootfs%', 'mem_avail', 'cont_id', 'pid',
           'stime', 'time', 'port_' + str(args.listen_port_check), '-' * 109))

    for worker in workers:
        worker.join()
        print(results[worker.get_ip()])
    return 0


if __name__ == '__main__':
    args = parse_args(sys.argv[1:])
    main()
