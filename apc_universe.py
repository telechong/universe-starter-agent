
import argparse
import subprocess

class ApceraApi(object):
    def __init__(self, apc='apc'):
        self.apc = apc

    def _apc(self, cmd):
        return subprocess.call([self.apc] + cmd.split())

    def docker_run(self, instance_name, image, args='', docker_opt='-ae'):
        return self._apc('docker run {name} {docker_opt} -i {image} {args}'.format(name=instance_name,
                                                                                   docker_opt=docker_opt,
                                                                                   image=image,
                                                                                   args=args))

class Deployment(object):
    def __init__(self, game, cluster_spec, apc=ApceraApi(),
                 agent_image='jderehag/apcera-universe-starter-agent',
                 gym_image='telechong/universe-flashgames:0.20.21',
                 log_dir='/tmp/agent',
                 grpc_port='2222'):
        self.game = game
        self.log_dir = log_dir
        self.cluster_spec = cluster_spec
        self.apc = apc
        self.grpc_port = grpc_port
        self.agent_image = agent_image
        self.gym_image = gym_image
        self.jobs = []

    def _flatten_cluster_spec(self):
        # since ordering is important in --workers, we need to maintain that
        return ','.join([addr + ':' + self.grpc_port for addr in [self.cluster_spec['ps'] + self.cluster_spec['worker']]])

    def deploy(self):
        for i, _ in enumerate(self.cluster_spec['gym']):
            name = 'gym' + str(i)
            self.jobs.append(name)
            self.apc.docker_run('gym' + str(i), self.gym_image)

        worker_cmd = 'CUDA_VISIBLE_DEVICES= /usr/bin/python /universe-starter-agent/worker.py '
        docker_worker_opt = '-ae -p ' + self.grpc_port

        for _ in self.cluster_spec['ps']:
            name = 'ps'
            self.jobs.append(name)

            args = worker_cmd + '--job-name ps '
            args += '--log-dir {logdir} '.format(logdir=self.log_dir)
            args += '--end-id {game} '.format(game=self.game)
            args += '--workers {workers} '.format(workers=self._flatten_cluster_spec())
            return self.apc.docker_run(name, self.agent_image, docker_opt=docker_worker_opt, args=args)

        for i, _ in enumerate(self.cluster_spec['worker']):
            name = 'worker' + str(i)
            self.jobs.append(name)

            args = worker_cmd + '--job-name worker '
            args += '--log-dir {logdir} '.format(logdir=self.log_dir)
            args += '--end-id {game} '.format(game=self.game)
            args += '--workers {workers} '.format(workers=self._flatten_cluster_spec())
            args += '--task {id_} '.format(id_=i)
            args += '--remotes vnc://{gym}:5900+15900'.format(gym=self.cluster_spec['gym'][i])
            self.apc.docker_run(name, self.agent_image, docker_opt=docker_worker_opt, args=args)


def deploy(args):
    cluster_spec = {'ps': [], 'worker': [], 'gym': []}
    depl = Deployment(args.game, cluster_spec)
    depl.deploy()


def main():
    parser = argparse.ArgumentParser(add_help=True, formatter_class=argparse.ArgumentDefaultsHelpFormatter)

    subparsers = parser.add_subparsers(help='sub-command help')

    parser.add_argument('-v', '--verbose', dest='verbose', action='store_true',
                        default=False, help='Verbose output')

    # Deploy
    parser_deploy = subparsers.add_parser('deploy', help='Deploys a cluster of RL agents')
    parser_deploy.add_argument('-i', '--instances', type=int, default=4)
    parser_deploy.add_argument('-e', '--env-id', default='flashgames.DuskDrive-v0')
    parser_deploy.set_defaults(func=deploy)

    args = parser.parse_args()

    args.func(args)


if __name__ == '__main__':
    main()
