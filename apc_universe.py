#!/usr/bin/env python

import argparse
import os
import subprocess
import HTMLParser


class _HtmlTableParser(HTMLParser.HTMLParser):
    def __init__(self):
        # HTMLParser is old-style
        HTMLParser.HTMLParser.__init__(self)

        self._current_tag = None
        self._current_data = None

        self.table_headers = []
        self.table_row = []
        self._table_data = []

    def handle_starttag(self, tag, attrs):
        self._current_tag = tag

    def handle_endtag(self, tag):
        if self._current_tag:
            if self._current_tag == 'th':
                self.table_headers.append(self._current_data)
            elif self._current_tag == 'td':
                self._table_data.append(self._current_data)
                if len(self._table_data) == len(self.table_headers):
                    self.table_row.append(self._table_data)
                    self._table_data = []
            self._current_tag = None
            self._current_data = None

    def handle_data(self, data):
        if self._current_tag:
            self._current_data = data

    def get_table(self):
        return [dict(zip(self.table_headers, row)) for row in self.table_row]


class ApceraApi(object):
    def __init__(self, apc='apc', verbose=True):
        self.apc = apc
        if verbose:
            self.stdout = None
        else:
            self.stdout = open(os.devnull, 'w')

    def _apc_output(self, cmd, table=True):
        try:
            output = subprocess.check_output([self.apc] + cmd.split() + ['--html', '--batch'])
        except subprocess.CalledProcessError:
            return None

        if table:
            parser = _HtmlTableParser()
            parser.feed(output)
            return parser.get_table()
        else:
            return output

    def _apc(self, cmd):
        if isinstance(cmd, str):
            cmd = cmd.split()
        cmd = [self.apc] + cmd + ['--batch']
        print "Calling:", ' '.join(cmd)
        return subprocess.call(cmd, stdout=self.stdout)

    def docker_run(self, instance_name, image, args=None, docker_opt='-ae'):
        docker_cmd = 'docker run {name} {docker_opt} -i {image}'.format(name=instance_name,
                                                                        docker_opt=docker_opt,
                                                                        image=image).split()
        if args:
            docker_cmd += ['-s', args]

        return self._apc(docker_cmd)

    @property
    def jobs(self):
        return [job['Name'] for job in self._apc_output('job list')]

    def job_start(self, instance_name):
        return self._apc('job start {name}'.format(name=instance_name))

    def job_delete(self, instance_name):
        return self._apc('job delete {name}'.format(name=instance_name))

    @property
    def networks(self):
        return self._apc_output('network list')

    def network_get(self, network):
        return self._apc_output('network show {network}'.format(network=network))

    def network_create(self, network):
        return self._apc('network create {network}'.format(network=network))

    def network_delete(self, network):
        return self._apc('network delete {network}'.format(network=network))

    def network_join(self, network, job):
        return self._apc('network join {network} --job {job} --discovery-address {job}'.format(network=network,
                                                                                               job=job))
    def network_route_add(self, route, job, port):
        return self._apc('route add {route} -p {port} --app {job}'.format(route=route,
                                                                          port=port,
                                                                          job=job))
    @property
    def target(self):
        # Unfortunatly target does not support html output
        for line in self._apc_output('target', table=False).split('\n'):
            if 'Targeted' in line:
                return line[line.find('https://') + len('https://'):line.find(']')]

    @property
    def namespace(self):
        # Unfortunatly target does not support html output
        for line in self._apc_output('target', table=False).split('\n'):
            if 'namespace' in line:
                return line[line.find('\"') + 1:line.rfind('\"')]



class Deployment(object):
    def __init__(self, game, instances, deployment_name, apc=ApceraApi(),
                 agent_image='jderehag/apcera-universe-starter-agent',
                 gym_image='telechong/universe.flashgames:0.20.21',
                 log_dir='/tmp/agent',
                 grpc_port='2222',
                 gym_ports=('5900', '15900')):
        self.game = game
        self.instances = instances
        self.deployment_name = deployment_name
        self.apc = apc
        self.agent_image = agent_image
        self.gym_image = gym_image
        self.log_dir = log_dir
        self.grpc_port = grpc_port
        self.gym_ports = gym_ports

    @property
    def cluster_spec(self):
        ps1 = [self.get_route('ps1') + ':' + str(self.grpc_port)]
        workers = [self.get_route('worker' + str(i)) + ':' + str(self.grpc_port) for i in range(self.instances)]
        gyms = [self.get_route('gym' + str(i)) for i in range(self.instances)]

        return {'ps': ps1, 'worker': workers, 'gym': gyms}

    @property
    def cluster_spec_flat(self):
        return ','.join(self.cluster_spec['ps'] + self.cluster_spec['worker'])

    def get_route(self, job):
        return '{job}.{namespace}{domain}'.format(job=job,
                                                   namespace='.'.join(reversed(self.apc.namespace.split('/'))),
                                                   domain=self.apc.target)

    def deploy(self):
        # Delete jobs
        for job in self.apc.jobs:
            self.apc.job_delete(job)

        # Delete + Create network
        if self.apc.network_get(self.deployment_name):
            self.apc.network_delete(self.deployment_name)
        self.apc.network_create(self.deployment_name)

        # Create + start new jobs
        self.start_instances()


    def start_instances(self):
        route = ' -r http://{}'
        for i, _ in enumerate(self.cluster_spec['gym']):
            name = 'gym' + str(i)
            ports = ' '.join(['-p ' + port for port in self.gym_ports])
            self.apc.docker_run('gym' + str(i), self.gym_image, docker_opt='{} {} -net {}'.format(ports,
                                                                                                             route.format(self.get_route(name)),
                                                                                                             self.deployment_name))

        worker_cmd = '/usr/bin/python /universe-starter-agent/worker.py '
        docker_worker_opt = '-ae -p ' + self.grpc_port
        docker_worker_opt += ' -net {}'.format(self.deployment_name)
        docker_worker_opt += route

        for i, _ in enumerate(self.cluster_spec['ps']):
            name = 'ps' + str(i)
            args = worker_cmd + '--job-name ps '
            args += '--log-dir {logdir} '.format(logdir=self.log_dir)
            args += '--env-id {game} '.format(game=self.game)
            args += '--workers {workers}'.format(workers=self.cluster_spec_flat)
            self.apc.docker_run(name, self.agent_image, docker_opt=docker_worker_opt.format(self.get_route(name)), args=args)

        for i, _ in enumerate(self.cluster_spec['worker']):
            name = 'worker' + str(i)
            args = worker_cmd + '--job-name worker '
            args += '--log-dir {logdir} '.format(logdir=self.log_dir)
            args += '--env-id {game} '.format(game=self.game)
            args += '--workers {workers} '.format(workers=self.cluster_spec_flat)
            args += '--task {id_} '.format(id_=i)
            args += '--remotes vnc://{gym}:{ports}'.format(gym=self.cluster_spec['gym'][i], ports='+'.join(self.gym_ports))
            self.apc.docker_run(name, self.agent_image, docker_opt=docker_worker_opt.format(self.get_route(name)), args=args)


def deploy(args):
    depl = Deployment(args.env_id, args.instances, args.deployment_name)
    depl.deploy()

def print_(args):
    apc = ApceraApi()
    print apc.cluster_spec


def main():
    parser = argparse.ArgumentParser(add_help=True, formatter_class=argparse.ArgumentDefaultsHelpFormatter)

    subparsers = parser.add_subparsers(help='sub-command help')

    parser.add_argument('-v', '--verbose', dest='verbose', action='store_true',
                        default=False, help='Verbose output')

    # Deploy
    parser_deploy = subparsers.add_parser('deploy', help='Deploys a cluster of RL agents')
    parser_deploy.add_argument('-i', '--instances', type=int, default=4)
    parser_deploy.add_argument('-e', '--env-id', default='flashgames.DuskDrive-v0')
    parser_deploy.add_argument('deployment_name')
    parser_deploy.set_defaults(func=deploy)

    parser_print = subparsers.add_parser('print')
    parser_print.set_defaults(func=print_)

    args = parser.parse_args()

    args.func(args)


if __name__ == '__main__':
    main()
