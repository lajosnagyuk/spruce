#!/usr/bin/env python3
"""Run bounded lifecycle traffic and namespace-local broker disruption tests."""
import argparse
import json
import re
import shlex
import subprocess
import time

parser = argparse.ArgumentParser(description=__doc__)
parser.add_argument('--namespace', required=True)
parser.add_argument('--release', default='spruce-dev')
parser.add_argument('--image', required=True)
parser.add_argument('--runner-node', default='')
parser.add_argument('--ssh-identity', default='', help='optional SSH private-key path')
parser.add_argument('--ssh-user', default='', help='required for kill cases: SSH user with sudo k3s ctr access on the nodes')
parser.add_argument('--case', choices=['baseline', 'scale', 'kill', 'kill-two'], default='baseline')
parser.add_argument('--ack', default='available')
parser.add_argument('--messages', type=int, default=500, help='messages per producer')
parser.add_argument('--rate', type=float, default=50)
args = parser.parse_args()
if not 1 <= args.messages <= 100000 or not 0 <= args.rate <= 100000:
    parser.error('invalid workload bounds')

def kubectl(*values, data=None):
    result = subprocess.run(['kubectl', '-n', args.namespace, *values], input=data,
                            text=True, capture_output=True, check=True)
    return result.stdout

namespace = json.loads(kubectl('get', 'namespace', args.namespace, '-o', 'json'))
if namespace['metadata'].get('labels', {}).get('spruce.io/test-environment') != 'true':
    parser.error('namespace must be labelled spruce.io/test-environment=true')
statefulset = json.loads(kubectl('get', 'statefulset', args.release, '-o', 'json'))
replicas = statefulset['spec']['replicas']
if args.case in ('kill', 'kill-two') and not args.ssh_user:
    parser.error('kill cases require --ssh-user for actual container SIGKILL')
if args.case != 'baseline' and replicas < 3:
    parser.error('disruption cases require at least three initial replicas')
name = f'lifecycle-{args.case}-{time.time_ns()}'
labels = {'app.kubernetes.io/name': 'spruce', 'app.kubernetes.io/instance': args.release,
          'app.kubernetes.io/component': 'test', 'spruce.io/client-access': 'true'}
job = {'apiVersion': 'batch/v1', 'kind': 'Job', 'metadata': {'name': name}, 'spec': {
    'backoffLimit': 0, 'activeDeadlineSeconds': 150, 'ttlSecondsAfterFinished': 3600,
    'template': {'metadata': {'labels': labels}, 'spec': {
        'restartPolicy': 'Never', 'automountServiceAccountToken': False,
        'containers': [{'name': 'lifecycle', 'image': args.image, 'imagePullPolicy': 'Never',
            'command': ['/spruce-lifecycle'],
            'args': ['-server', f'http://{args.release}:8080', '-ack', args.ack,
                     '-messages', str(args.messages), '-rate', str(args.rate),
                     '-timeout', '120s', '-allow-duplicates', '-allow-insecure-credentials'],
            'resources': {'requests': {'cpu': '50m', 'memory': '64Mi'},
                          'limits': {'memory': '256Mi'}},
            'env': [{'name': 'SPRUCE_TOKEN', 'valueFrom': {'secretKeyRef': {
                'name': f'{args.release}-auth', 'key': 'client-token'}}}]}]}}}}
if args.runner_node:
    job['spec']['template']['spec']['nodeSelector'] = {'kubernetes.io/hostname': args.runner_node}
print(kubectl('apply', '-f', '-', data=json.dumps(job)), flush=True)
scaled = False
fault_at = None
restored = False
deadline = time.monotonic() + 170
try:
    while time.monotonic() < deadline:
        status = json.loads(kubectl('get', 'job', name, '-o', 'json')).get('status', {})
        if status.get('succeeded') or status.get('failed'):
            print(kubectl('logs', f'job/{name}'), end='', flush=True)
            if status.get('failed'):
                raise SystemExit('lifecycle oracle failed; inspect the reported counts')
            break
        if args.case != 'baseline':
            logs = subprocess.run(['kubectl', '-n', args.namespace, 'logs', f'job/{name}'],
                                  capture_output=True, text=True).stdout
            if fault_at is None and 'subscriptions_ready=true' in logs:
                fault_at = time.monotonic() + 10
            if fault_at is not None and time.monotonic() >= fault_at and not restored:
                if args.case in ('kill', 'kill-two'):
                    targets = [f'{args.release}-1']
                    if args.case == 'kill-two': targets.append(f'{args.release}-2')
                    commands = []
                    for target in targets:
                        pod = json.loads(kubectl('get', 'pod', target, '-o', 'json'))
                        if not any(owner.get('uid') == statefulset['metadata']['uid'] for owner in pod['metadata'].get('ownerReferences', [])):
                            raise RuntimeError('target is not owned by the test StatefulSet')
                        containers = pod['status']['containerStatuses']
                        container = next(c for c in containers if c['name'] == 'spruce')
                        container_id = container['containerID'].removeprefix('containerd://')
                        if not re.fullmatch('[a-f0-9]{64}', container_id):
                            raise RuntimeError('unexpected container runtime identifier')
                        node = json.loads(kubectl('get', 'node', pod['spec']['nodeName'], '-o', 'json'))
                        address = next(a['address'] for a in node['status']['addresses'] if a['type'] == 'InternalIP')
                        command = 'sudo -n k3s ctr -n k8s.io tasks kill --signal SIGKILL ' + shlex.quote(container_id)
                        commands.append(['ssh', *(['-i', args.ssh_identity, '-o', 'IdentitiesOnly=yes'] if args.ssh_identity else []), '-o', 'BatchMode=yes', '-o', 'ConnectTimeout=5', f'{args.ssh_user}@{address}', command])
                        print(f'SIGKILL {target} container={container_id} node={pod["spec"]["nodeName"]}', flush=True)
                    processes = [subprocess.Popen(command) for command in commands]
                    if any([process.wait() != 0 for process in processes]):
                        raise RuntimeError('container SIGKILL failed')
                    restored = True
                elif not scaled:
                    # This only removes pods belonging to this test StatefulSet.
                    print(kubectl('scale', 'statefulset', args.release, '--replicas=1'), flush=True)
                    scaled = True
                elif time.monotonic() >= fault_at + 15:
                    print(kubectl('scale', 'statefulset', args.release, f'--replicas={replicas}'), flush=True)
                    restored = True
        time.sleep(.5)
    else:
        raise SystemExit('scenario deadline exceeded')
finally:
    if scaled:
        kubectl('scale', 'statefulset', args.release, f'--replicas={replicas}')
        kubectl('rollout', 'status', f'statefulset/{args.release}', '--timeout=120s')
