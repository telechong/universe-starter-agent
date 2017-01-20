#!/usr/bin/env python
from __future__ import print_function
import argparse
import os
import subprocess
from multiprocessing import Pool
from future.moves.html.parser import HTMLParser


class _HtmlTableParser(HTMLParser):
    def __init__(self):
        # HTMLParser is old-style class
        HTMLParser.__init__(self)

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
    def __init__(self, apc='apc', verbose=False):
        self.apc = apc
        if verbose:
            self.stdout = None
        else:
            self.stdout = open(os.devnull, 'w')

    def _apc_output(self, cmd, table=True):
        try:
            output = subprocess.check_output([self.apc] + cmd.split() + ['--html', '--batch']).decode("utf-8")
        except subprocess.CalledProcessError:
            return None

        if table:
            parser = _HtmlTableParser()
            parser.feed(output)
            return parser.get_table()
        else:
            return output

    def _apc(self, cmd, as_batch=True):
        if isinstance(cmd, str):
            cmd = cmd.split()
        cmd = [self.apc] + cmd

        if as_batch:
            cmd += ['--batch']

        print("\033[92m[Calling]:\033[m", ' '.join(cmd))
        return subprocess.call(cmd, stdout=self.stdout)

    def docker_run(self, instance_name, image, args=None, docker_opt='-ae', memory=None):
        memory = '-m ' + str(memory) if memory else ''
        docker_cmd = 'docker run {name} {docker_opt} {mem} -i {image}'.format(name=instance_name,
                                                                              docker_opt=docker_opt,
                                                                              mem=memory,
                                                                              image=image).split()
        if args:
            docker_cmd += ['-s', args]

        return self._apc(docker_cmd)

    @property
    def providers(self):
        return self._apc_output('provider list -ns /')

    @property
    def services(self):
        return [service['Name'] for service in self._apc_output('service list')]

    def service_create(self, service_name, provider, description=''):
        return self._apc('service create {name} --provider {provider} {desc}'.format(name=service_name,
                                                                                     provider=provider,
                                                                                     desc=description))

    def service_delete(self, service_name):
        return self._apc('service delete {name}'.format(name=service_name))

    def service_bind(self, service_name, job, custom_params=None):
        custom_params = ' -- ' + custom_params if custom_params else ''
        return self._apc('service bind {name} --batch --job {job} {custom_params}'.format(name=service_name,
                                                                                          job=job,
                                                                                          custom_params=custom_params),
                                                                                          as_batch=False)

    @property
    def jobs(self):
        return [job['Name'] for job in self._apc_output('job list')]

    def job_start(self, instance_name):
        return self._apc('job start {name}'.format(name=instance_name))

    def job_delete(self, instance_name):
        return self._apc('job delete {name}'.format(name=instance_name))

    def job_attract(self, job, to_job):
        return self._apc('job attract {job} --to {to_job} --hard'.format(job=job, to_job=to_job))

    @property
    def networks(self):
        return [nw['Network Name'] for nw in self._apc_output('network list')]

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

    def namespace_clear(self):
        for job in self.jobs:
            self.job_delete(job)

        for network in self.networks:
            self.network_delete(network)

        for service in self.services:
            self.service_delete(service)


class Deployment(object):
    def __init__(self, game, instances, deployment_name, apc=ApceraApi(),
                 agent_image='quay.io/telechong/universe-agent',
                 gym_image='quay.io/telechong/universe-flashgames',
                 log_dir='/mnt/shared',
                 grpc_port='2222',
                 gym_ports=('5900', '15900')):
        self.game = game
        # instances = [('vehicle1', 'tag1'), ('vehicle2', 'tag2')]
        self.instances = [{'gym': inst, 'worker': inst + 'worker', 'tag': tag} for inst, tag in instances]

        self.deployment_name = deployment_name
        self.apc = apc
        self.agent_image = agent_image
        self.gym_image = gym_image
        self.log_dir = log_dir
        self.grpc_port = grpc_port
        self.gym_ports = gym_ports

    @property
    def cluster_spec(self):
        ps = [self.get_discovery_address('ps0') + ':' + str(self.grpc_port)]
        workers = [self.get_discovery_address(inst['worker']) + ':' + str(self.grpc_port) for inst in self.instances]
        gyms = [self.get_discovery_address(inst['gym']) for inst in self.instances]

        return {'ps': ps, 'worker': workers, 'gym': gyms}

    @property
    def cluster_spec_flat(self):
        return ','.join(self.cluster_spec['ps'] + self.cluster_spec['worker'])

    def get_domain(self, job):
        return '{job}.{namespace}{domain}'.format(job=job,
                                                  namespace='.'.join(reversed(self.apc.namespace.split('/'))),
                                                  domain=self.apc.target)

    def get_discovery_address(self, job):
        return '{job}.apcera.local'.format(job=job)

    def deploy(self):
        self.apc.namespace_clear()

        self.apc.network_create(self.deployment_name)

        self.create_instances()

        # We need to join the network rather than adding -net to docker run
        # This is due to that we want --discovery-address (which is only available on network join
        for job in self.apc.jobs:
            self.apc.network_join(self.deployment_name, job)

        # We need to attrct jobs manually since attract is only available as a job operation rather
        # than as part of a create procedure
        for inst in self.instances:
            self.apc.job_attract(inst['worker'], inst['gym'])

        self.start_jobs()

    def create_nfs_service(self, name):
        nfs_providers = [provider for provider in self.apc.providers if provider['Type'] == 'nfs']

        assert len(nfs_providers) >= 1, 'No valid nfs providers found!'
        provider = nfs_providers[0]  # Take first valid provider
        self.apc.service_create(name, provider['Namespace'] + '::' + provider['Name'])

    def create_instances(self):
        self.create_nfs_service(self._get_nfs_service_name())

        pool = Pool(processes=(len(self.cluster_spec['gym']) +
                               len(self.cluster_spec['ps']) +
                               len(self.cluster_spec['worker'])))
        pool_args = []
        for inst in self.instances:
            name = inst['gym']
            tag = inst['tag']
            ports = ' '.join(['-p ' + port for port in self.gym_ports])
            route = ' -r http://' + self.get_domain(name)
            tag = '-ht ' + tag if tag else ''
            docker_gym_opt = '--no-start --timeout 300 {tag} {port} {route}'.format(tag=tag,
                                                                                    port=ports,
                                                                                    route=route)
            pool_args.append(dict(name=name,
                                  image=self.gym_image,
                                  args=None,
                                  docker_opt=docker_gym_opt,
                                  memory='1G',
                                  nfs_service_name=self._get_nfs_service_name(),
                                  log_dir=self.log_dir))

        worker_cmd = '/usr/bin/python /universe-starter-agent/worker.py '
        docker_worker_opt = '-ae --no-start -p {port} '.format(port=self.grpc_port)

        for i, _ in enumerate(self.cluster_spec['ps']):
            name = 'ps' + str(i)
            tb_port = '12345'
            args = '/bin/sh -c "nohup tensorboard --logdir {logdir} --port {port} & '.format(port=tb_port,
                                                                                             logdir=self.log_dir)
            args += worker_cmd + '--job-name ps '
            args += '--log-dir {logdir} '.format(logdir=self.log_dir)
            args += '--env-id {game} '.format(game=self.game)
            args += '--workers {workers}'.format(workers=self.cluster_spec_flat)
            args += '"'
            docker_ps_opt = '-p {port} -r http://{domain}'.format(domain=self.get_domain(name),
                                                                  port=tb_port)
            pool_args.append(dict(name=name,
                                  image=self.agent_image,
                                  args=args,
                                  docker_opt=docker_worker_opt + docker_ps_opt,
                                  memory='1G',
                                  nfs_service_name=self._get_nfs_service_name(),
                                  log_dir=self.log_dir))

        for i, inst in enumerate(self.instances):
            name = inst['worker']
            tag = inst['tag']
            args = worker_cmd + '--job-name worker '
            args += '--log-dir {logdir} '.format(logdir=self.log_dir)
            args += '--env-id {game} '.format(game=self.game)
            args += '--workers {workers} '.format(workers=self.cluster_spec_flat)
            args += '--task {id_} '.format(id_=i)
            args += '--remotes vnc://{gym}:{ports}'.format(gym=self.cluster_spec['gym'][i], ports='+'.join(self.gym_ports))
            docker_worker_opt += '-ht ' + tag if tag else ''
            pool_args.append(dict(name=name,
                                  image=self.agent_image,
                                  args=args,
                                  docker_opt=docker_worker_opt,
                                  memory='1G',
                                  nfs_service_name=self._get_nfs_service_name(),
                                  log_dir=self.log_dir))
        pool.map(_create_instance, pool_args)

    def start_jobs(self):
        pool1 = Pool(processes=len(self.apc.jobs))
        pool1_args = []
        pool2 = Pool(processes=len(self.apc.jobs))
        pool2_args = []

        workers = [inst['worker'] for inst in self.instances]
        for job in self.apc.jobs:
            if job not in workers:
                pool1_args.append(job)
            else:
                pool2_args.append(job)
        pool1.map(_start_apc_job, pool1_args)
        pool2.map(_start_apc_job, pool2_args)

    def _get_nfs_service_name(self):
        return self.deployment_name + '_nfs'


def _create_instance(args):
    apc = ApceraApi()
    apc.docker_run(args['name'],
                   args['image'],
                   args=args['args'],
                   docker_opt=args['docker_opt'],
                   memory=args['memory'])
    apc.service_bind(args['nfs_service_name'],
                     args['name'],
                     '--mountpath ' + args['log_dir'])


def _start_apc_job(job):
    apc = ApceraApi()
    apc.job_start(job)


def deploy(args):
    depl = Deployment(args.env_id, args.instances, args.deployment, apc=ApceraApi(verbose=args.verbose))
    depl.deploy()

def print_(args):
    apc = ApceraApi()
    print(apc.providers)

def clean(args):
    apc = ApceraApi()
    apc.namespace_clear()

class InstanceParser(argparse.Action):
    def __call__(self, parser, namespace, values, option_string=None):
        if len(values) == 1 and values[0].isdigit():
            instance_tuples = [('vehicle' + str(i), None) for i in range(int(values[0]))]
        else:
            instance_tuples = [value.split(':') for value in values]

        setattr(namespace, self.dest, instance_tuples)

def main():
    parser = argparse.ArgumentParser(add_help=True, formatter_class=argparse.ArgumentDefaultsHelpFormatter)

    subparsers = parser.add_subparsers(help='sub-command help')

    parser.add_argument('-v', '--verbose', dest='verbose', action='store_true',
                        default=False, help='Verbose output')

    # Deploy
    parser_deploy = subparsers.add_parser('deploy', help='Deploys a cluster of RL agents')
    parser_deploy.add_argument('-e', '--env-id', default='flashgames.DuskDrive-v0')
    parser_deploy.add_argument('-d', '--deployment', default='universe', help='An arbitrary deployment name')

    parser_deploy.add_argument('instances', nargs='*', default=['4'], action=InstanceParser,
                              help=('Can optionally be a number, OR '
                                    'pairs of instance names and their tags vehicle1:plano vehicle2:sj'))
    parser_deploy.set_defaults(func=deploy)

    # Print
    parser_print = subparsers.add_parser('print')
    parser_print.set_defaults(func=print_)

    # Clean
    parser_clean = subparsers.add_parser('clean')
    parser_clean.set_defaults(func=clean)


    args = parser.parse_args()

    args.func(args)


if __name__ == '__main__':
    main()
