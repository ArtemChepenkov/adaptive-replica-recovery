#!/usr/bin/env python3
"""Measured lab calibration. Does not admit a profile from requested settings alone."""
import argparse
import csv
import hashlib
import json
import math
import os
from pathlib import Path
import re
import socket
import subprocess
import sys
import time
from datetime import datetime, timezone
from urllib.request import Request, urlopen
import uuid

ROOT = Path(__file__).resolve().parents[2]
SCRIPTS = ROOT / 'infrastructure/scripts'
PROJECT = os.environ.get('COMPOSE_PROJECT_NAME', 'replica-recovery')
NAME = re.compile(r'[a-z0-9][a-z0-9-]{0,62}\Z')
BROKER = re.compile(r'broker-[1-9][0-9]*\Z')


class ResourceFailure(RuntimeError):
    def __init__(self, response):
        self.unsupported = response.get('unsupported') is True
        super().__init__('Resource operation failed: ' + json.dumps(response))


def decode(data):
    return data.decode('utf-16-le' if data[:200].count(b'\0') > 20 else 'utf-8', errors='replace').strip('\ufeff')


def run(arguments, timeout=30, check=True):
    result = subprocess.run([str(item) for item in arguments], stdout=subprocess.PIPE,
                            stderr=subprocess.PIPE, timeout=timeout, cwd=ROOT)
    output, error = decode(result.stdout), decode(result.stderr)
    if check and result.returncode:
        raise RuntimeError(f'{arguments[0]} exited {result.returncode}: {error or output}')
    return output


def save(path, value):
    path.parent.mkdir(parents=True, exist_ok=True)
    temporary = path.with_suffix(path.suffix + '.tmp')
    temporary.write_text(json.dumps(value, indent=2, ensure_ascii=False, allow_nan=False) + '\n')
    temporary.replace(path)


def resource(operation, profile=None):
    arguments = ['bash', SCRIPTS / ('cleanup-lab.sh' if operation == 'cleanup' else 'apply-profile.sh')]
    if operation != 'cleanup':
        arguments += [operation]
        if profile is not None:
            arguments += [profile]
    result = json.loads(run(arguments, timeout=240, check=False))
    if not result.get('success'):
        raise ResourceFailure(result)
    return result


def selected_brokers(details):
    available = sorted(details['brokers'])
    requested = os.environ.get('CALIBRATION_BROKERS')
    selected = requested.split(',') if requested else available
    if not selected or any(not BROKER.fullmatch(item) or item not in available for item in selected):
        raise ValueError('CALIBRATION_BROKERS must select configured profile broker IDs')
    return sorted(set(selected))


def broker_id(name):
    if not BROKER.fullmatch(name):
        raise ValueError('Invalid broker identity')
    identifiers = run(['docker', 'ps', '--filter', f'label=com.docker.compose.project={PROJECT}',
                       '--filter', f'label=com.docker.compose.service={name}', '--format', '{{.ID}}']).split()
    if len(identifiers) != 1 or not re.fullmatch(r'[a-f0-9]{12,64}', identifiers[0]):
        raise RuntimeError(f'Expected exactly one running owned {name}')
    return identifiers[0]


def peer_for(source):
    peer = os.environ.get('CALIBRATION_PEER', 'broker-3' if source != 'broker-3' else 'broker-2')
    if peer == source:
        raise ValueError('Calibration peer must be a different broker')
    return peer, broker_id(peer)


def backbone_address(source_id, peer_id):
    source = json.loads(run(['docker', 'inspect', source_id]))[0]
    peer = json.loads(run(['docker', 'inspect', peer_id]))[0]
    common = set(source['NetworkSettings']['Networks']) & set(peer['NetworkSettings']['Networks'])
    networks = sorted(item for item in common if item.endswith('_backbone'))
    if len(networks) != 1:
        raise RuntimeError('Expected one shared Compose backbone network')
    return peer['NetworkSettings']['Networks'][networks[0]]['IPAddress']


def ping(container, address):
    output = run(['docker', 'exec', container, 'env', 'LC_ALL=C', 'ping', '-n', '-q', '-c', '40', '-i', '0.05', address], timeout=20, check=False)
    rtt = re.search(r'(?:rtt|round-trip).*?=\s*([\d.]+)/([\d.]+)/([\d.]+)', output)
    loss = re.search(r'([\d.]+)% packet loss', output)
    if not rtt or not loss:
        raise RuntimeError('Ping did not provide measured RTT/loss: ' + output)
    return {'meanRttMs': float(rtt.group(2)), 'lossPercent': float(loss.group(1)), 'raw': output}


def percentile(values, fraction):
    if not values:
        raise RuntimeError('No measured interval samples')
    return sorted(values)[max(0, math.ceil(len(values) * fraction) - 1)]


def stop_remote(container, pidfile, program, marker):
    cleanup_code = '''import os,sys,signal,time
p,program,marker=sys.argv[1:]
try:
 pid=int(open(p).read()); cmd=open('/proc/%s/cmdline'%pid,'rb').read().split(b'\\0')
 if program.encode() in cmd and any(marker.encode() in part for part in cmd):
  try: os.kill(pid,signal.SIGTERM)
  except ProcessLookupError: pass
  for attempt in range(20):
   if not os.path.exists('/proc/%s'%pid): break
   time.sleep(.05)
except (FileNotFoundError,ProcessLookupError): pass
try: os.unlink(p)
except FileNotFoundError: pass
'''
    run(['docker', 'exec', '-u', '0', container, 'python3', '-c', cleanup_code, pidfile, program, marker], timeout=10)


def fio_run(container, arguments, filename, seconds):
    pidfile = '/tmp/recovery-calibration-' + uuid.uuid4().hex + '.pid'
    try:
        return run(['docker', 'exec', '-u', '0', container, 'sh', '-c',
                    'echo $$ > "$1"; shift; exec timeout "$@"', '_', pidfile, str(seconds), *arguments], timeout=seconds + 5)
    finally:
        stop_remote(container, pidfile, 'fio', filename)


def iperf(source_id, peer_id, address, direction, seconds, output, label):
    token = uuid.uuid4().hex
    port = 5200 + int(token[:4], 16) % 20000
    remote_pid = f'/tmp/recovery-calibration-{token}.pid'
    stdout = (output / f'{label}-server.json').open('wb')
    stderr = (output / f'{label}-server.err').open('wb')
    # The remote timeout bounds a server even if this client process is interrupted.
    server = subprocess.Popen(['docker', 'exec', peer_id, 'sh', '-c',
        'echo $$ > "$1"; exec timeout "$3" iperf3 -s -1 -J -p "$2"', '_', remote_pid, str(port), str(seconds + 30)],
        stdout=stdout, stderr=stderr)
    try:
        time.sleep(0.4)
        arguments = ['docker', 'exec', source_id, 'timeout', str(seconds + 25), 'iperf3',
                     '-c', address, '-p', str(port), '-t', str(seconds), '-O', '3', '-J']
        if direction == 'ingress':
            arguments += ['-R']
        raw = run(arguments, timeout=seconds + 30)
        (output / f'{label}-client.json').write_text(raw)
        measured = json.loads(raw)
        server.wait(timeout=10)
        stdout.flush()
        received = measured.get('end', {}).get('sum_received', {})
        duration = float(received.get('seconds', 0))
        mean = float(received.get('bits_per_second', 0)) / 1_000_000
        if duration < seconds - 1 or mean <= 0 or not math.isfinite(mean):
            raise RuntimeError('iperf did not provide a complete receiving measurement')
        # Forward receiver intervals are in the server JSON, reverse receiver intervals in client JSON.
        intervals = measured if direction == 'ingress' else json.loads((output / f'{label}-server.json').read_text())
        rates = [float(item['sum']['bits_per_second']) / 1_000_000 for item in intervals.get('intervals', [])
                 if not item.get('sum', {}).get('omitted', False)]
        if any(not math.isfinite(value) or value < 0 for value in rates):
            raise RuntimeError('Invalid iperf interval sample')
        return {'meanMbit': mean, 'p95Mbit': percentile(rates, 0.95), 'runtimeSeconds': duration,
                'intervalSamples': len(rates), 'intervalSource': 'receiver'}
    finally:
        stop_remote(peer_id, remote_pid, 'iperf3', str(port))
        if server.poll() is None:
            server.terminate()
            try:
                server.wait(timeout=5)
            except subprocess.TimeoutExpired:
                server.kill()
        stdout.close()
        stderr.close()


def proof(profile, details, kind, reading):
    path = ROOT / f'results/calibration/{profile}/admission.json'
    current = {'schemaVersion': 1, 'profile': profile, 'profileSha256': details['profileSha256'],
               'hostIdentity': details['hostIdentity'], 'brokers': details['brokers']}
    if path.exists():
        previous = json.loads(path.read_text())
        if all(previous.get(key) == value for key, value in current.items()):
            current = previous
    current[kind] = reading
    resource_profile = details['profile']
    network = resource_profile.get('network', {})
    disk = resource_profile.get('disk', {})
    required = []
    if any(network.get(key) for key in ('egressMbit', 'ingressMbit', 'delayMs', 'jitterMs', 'lossPercent')):
        required.append('network')
    if any(disk.get(key) for key in ('readMiBps', 'writeMiBps')):
        required.append('disk')
    current['success'] = all(current.get(item, {}).get('success', False) for item in required) and reading['success']
    current['recordedAt'] = datetime.now(timezone.utc).isoformat()
    save(path, current)
    return path


def network_calibration(profile, output, seconds):
    details = resource('status', profile)['details']
    names = selected_brokers(details)
    resource('apply', 'normal')
    baselines, peers = {}, {}
    for name in names:
        peers[name] = peer_for(name)
        source_id = details['brokers'][name]['containerId']
        address = backbone_address(source_id, peers[name][1])
        baselines[name] = ping(source_id, address)
    resource('apply', profile)
    checks, latency = [], []
    configured = details['profile']['network']
    for name in names:
        source_id = details['brokers'][name]['containerId']
        peer, peer_id = peers[name]
        address = backbone_address(source_id, peer_id)
        observed = ping(source_id, address)
        added = float(configured.get('delayMs') or 0) * (1 + int(peer in details['brokers']))
        delta = observed['meanRttMs'] - baselines[name]['meanRttMs']
        latency.append({'broker': name, 'peer': peer, 'baselineRttMs': baselines[name]['meanRttMs'],
                        'meanRttMs': observed['meanRttMs'], 'targetAddedRttMs': added,
                        'measuredAddedRttMs': delta, 'lossPercent': observed['lossPercent'],
                        'passed': added == 0 or abs(delta - added) <= added * 0.2})
        (output / f'{name}-ping-before.txt').write_text(baselines[name]['raw'])
        (output / f'{name}-ping-after.txt').write_text(observed['raw'])
        for direction, key in [('egress', 'egressMbit'), ('ingress', 'ingressMbit')]:
            measurement = iperf(source_id, peer_id, address, direction, seconds, output, f'{name}-{direction}')
            target = configured.get(key)
            deviation = None if not target else abs(measurement['meanMbit'] - target) / target
            checks.append({'broker': name, 'direction': direction, 'targetMbit': target,
                           'deviationFraction': deviation, 'passed': deviation is None or deviation <= 0.1,
                           **measurement})
    finite = any(configured.get(key) for key in ('egressMbit', 'ingressMbit', 'delayMs', 'jitterMs', 'lossPercent'))
    complete = not finite or set(names) == set(details['brokers'])
    unverified = []
    if configured.get('jitterMs'):
        unverified.append('UNVERIFIED_JITTER_DISTRIBUTION')
    if configured.get('lossPercent'):
        unverified.append('UNVERIFIED_PACKET_LOSS_PROBABILITY')
    passed = complete and not unverified and all(item['passed'] for item in checks + latency)
    reading = {'success': passed,
               'matchedTarget': passed, 'unverifiedConstraints': unverified,
               'runtimeSeconds': seconds, 'checks': checks, 'latencyChecks': latency,
               'measuredBrokers': names, 'referenceOnly': not finite, 'timestamp': datetime.now(timezone.utc).isoformat()}
    return details, reading


def disk_calibration(profile, output, seconds):
    details = resource('status', profile)['details']
    names = selected_brokers(details)
    token = uuid.uuid4().hex
    # Kafka LogManager treats subdirectories as topic-partitions; use a regular file,
    # which it ignores, rather than creating any extra directory inside log.dirs.
    filename = f'/var/lib/kafka/data/recovery-calibration-{token}.bin'
    # Capability check before touching even the dedicated benchmark file.
    resource('apply', profile)
    resource('apply', 'normal')
    try:
        for name in names:
            identifier = details['brokers'][name]['containerId']
            create_file = '''import os,sys
p=sys.argv[1]; space=os.statvfs(os.path.dirname(p))
if space.f_bavail*space.f_frsize < 536870912: raise RuntimeError('At least 512 MiB free calibration space required')
fd=os.open(p,os.O_CREAT|os.O_EXCL|os.O_WRONLY,0o600); os.close(fd)
'''
            run(['docker', 'exec', '-u', '0', identifier, 'python3', '-c', create_file, filename])
            prepared = fio_run(identifier, ['fio', '--name=prepare', f'--filename={filename}',
                            '--size=256m', '--rw=write', '--bs=1m', '--ioengine=psync', '--direct=1', '--end_fsync=1', '--output-format=json'], filename, 120)
            (output / f'{name}-prepare.json').write_text(prepared)
        resource('apply', profile)
        checks = []
        configured = details['profile']['disk']
        for name in names:
            identifier = details['brokers'][name]['containerId']
            for operation in ('read', 'write', 'randread', 'randwrite'):
                for block_size in (4096, 1048576):
                    label = f'{name}-{operation}-{block_size}'
                    arguments = ['fio', f'--name={label}', f'--filename={filename}',
                                 '--size=256m', f'--rw={operation}', f'--bs={block_size}', '--ioengine=psync', '--iodepth=1',
                                 '--direct=1', '--time_based=1', f'--runtime={seconds}', '--ramp_time=3', '--numjobs=1',
                                 '--randrepeat=1', '--randseed=20260910', '--allow_file_create=0', '--output-format=json']
                    raw = fio_run(identifier, arguments, filename, seconds + 40)
                    (output / f'{label}.json').write_text(raw)
                    job = json.loads(raw)['jobs'][0]
                    direction = 'read' if operation.endswith('read') else 'write'
                    sample = job[direction]
                    mean = float(sample.get('bw_bytes', float(sample.get('bw', 0)) * 1024)) / 1048576
                    measured_runtime = float(sample.get('runtime', 0)) / 1000
                    target = configured.get(direction + 'MiBps')
                    deviation = None if not target else abs(mean - target) / target
                    checks.append({'broker': name, 'rw': operation, 'blockSizeBytes': block_size, 'runtimeSeconds': measured_runtime,
                                   'direct': True, 'meanMiBps': mean, 'targetMiBps': target, 'deviationFraction': deviation,
                                   'passed': job.get('error', 0) == 0 and mean > 0 and measured_runtime >= seconds - 1
                                   and (deviation is None or deviation <= 0.1)})
        finite = any(configured.get(key) for key in ('readMiBps', 'writeMiBps'))
        complete = not finite or set(names) == set(details['brokers'])
        passed = complete and len(checks) == len(names) * 8 and all(item['passed'] for item in checks)
        return details, {'success': passed, 'matchedTarget': passed, 'runtimeSeconds': seconds, 'direct': True,
                         'checks': checks, 'measuredBrokers': names, 'referenceOnly': not finite,
                         'timestamp': datetime.now(timezone.utc).isoformat()}
    finally:
        # Only our generated regular file is removed, never a Kafka partition directory.
        cleanup_code = 'import os,sys; p=sys.argv[1]; os.unlink(p) if os.path.exists(p) else None'
        for name in names:
            run(['docker', 'exec', '-u', '0', details['brokers'][name]['containerId'], 'python3', '-c', cleanup_code, filename])


def host_calibration(output):
    commands = {'cpu': ['lscpu', '--json'], 'storage': ['lsblk', '-J', '-o', 'NAME,MODEL,TYPE,SIZE,ROTA,MOUNTPOINTS'],
                'kernel': ['uname', '-a'], 'memory': ['free', '-b'], 'background': ['vmstat', '1', '3'],
                'dockerVersion': ['docker', 'version', '--format', '{{json .}}'],
                'windowsVersion': ['cmd.exe', '/c', 'ver'], 'wslVersion': ['wsl.exe', '--version'],
                'windowsHardware': ['powershell.exe', '-NoProfile', '-Command',
                    "$ErrorActionPreference='Stop';[Console]::OutputEncoding=[System.Text.UTF8Encoding]::new($false);"
                    "[pscustomobject]@{cpu=@(Get-CimInstance Win32_Processor|Select-Object Name,NumberOfCores,NumberOfLogicalProcessors);"
                    "physicalMemoryBytes=(Get-CimInstance Win32_ComputerSystem).TotalPhysicalMemory;"
                    "disks=@(Get-CimInstance Win32_DiskDrive|Select-Object Model,MediaType,Size,InterfaceType);"
                    "os=(Get-CimInstance Win32_OperatingSystem|Select-Object Caption,Version,BuildNumber)}|ConvertTo-Json -Depth 4 -Compress"],
                'java': ['java', '-version'], 'cruiseControlCommit': ['git', '-C', ROOT / 'cruise-control', 'rev-parse', 'HEAD']}
    report = {'success': True, 'recordedAt': datetime.now(timezone.utc).isoformat(), 'measurements': {}, 'unavailable': {}}
    for name, command in commands.items():
        try:
            result = subprocess.run([str(item) for item in command], stdout=subprocess.PIPE, stderr=subprocess.PIPE, timeout=15, cwd=ROOT)
            if result.returncode:
                raise RuntimeError(decode(result.stderr))
            value = decode(result.stdout or result.stderr)
            try:
                value = json.loads(value)
            except json.JSONDecodeError:
                pass
            report['measurements'][name] = value
        except (OSError, RuntimeError, subprocess.TimeoutExpired) as error:
            report['unavailable'][name] = str(error)
    try:
        docker = json.loads(run(['docker', 'info', '--format', '{{json .}}']))
        report['measurements']['dockerHost'] = {key: docker.get(key) for key in ('ID', 'ServerVersion', 'KernelVersion', 'OSType', 'OperatingSystem', 'CgroupVersion', 'Driver', 'MemTotal', 'NCPU')}
        broker = broker_id('broker-1')
        report['measurements']['kafkaVersion'] = run(['docker', 'exec', broker, '/opt/kafka/bin/kafka-topics.sh', '--version'])
        report['measurements']['brokerJavaVersion'] = decode(subprocess.run(['docker', 'exec', broker, 'java', '-version'], capture_output=True, timeout=15).stderr)
    except Exception as error:
        report['unavailable']['labVersions'] = str(error)
    report['complete'] = not report['unavailable']
    report['success'] = all(key in report['measurements'] for key in ('cpu', 'storage', 'memory', 'kernel', 'dockerVersion', 'kafkaVersion'))
    save(output / 'host.json', report)
    return report


def http(url, payload=None):
    request = Request(url, data=None if payload is None else json.dumps(payload).encode(),
                      headers={'Content-Type': 'application/json'}, method='GET' if payload is None else 'POST')
    with urlopen(request, timeout=15) as response:
        return json.load(response)


def free_port():
    with socket.socket() as sock:
        sock.bind(('127.0.0.1', 0))
        return sock.getsockname()[1]


def measured_requests(path, seconds):
    with path.open(newline='') as source:
        rows = list(csv.DictReader(source))
    if not rows:
        raise RuntimeError('No raw requests')
    start = min(int(item['timestampEpochMs']) for item in rows) + 10000
    measured = [item for item in rows if int(item['timestampEpochMs']) >= start]
    successful = [item for item in measured if item['success'] == 'true']
    elapsed = (max(int(item['timestampEpochMs']) for item in measured) - start) / 1000
    if not successful or elapsed < seconds - 12:
        raise RuntimeError('Incomplete Kafka baseline measurement')
    return {'requestCount': len(measured), 'successCount': len(successful), 'errorCount': len(measured) - len(successful),
            'produceP99Ms': percentile([float(item['latencyMicros']) / 1000 for item in successful], 0.99),
            'throughputRps': len(successful) / elapsed, 'measuredSeconds': elapsed, 'warmupExcludedSeconds': 10}


def local_binding(info, port):
    bindings = info.get('NetworkSettings', {}).get('Ports', {}).get(f'{port}/tcp') or []
    if len(bindings) != 1 or bindings[0].get('HostIp') != '127.0.0.1':
        raise RuntimeError('Expected one owned loopback port binding')
    published = str(bindings[0].get('HostPort', ''))
    if not published.isdigit() or not 1 <= int(published) <= 65535:
        raise RuntimeError('Invalid owned published port')
    return published


def kafka_preflight():
    identifiers = run(['docker', 'ps', '--filter', f'label=com.docker.compose.project={PROJECT}', '--format', '{{.ID}}']).split()
    if not identifiers or any(not re.fullmatch(r'[a-f0-9]{12,64}', item) for item in identifiers):
        raise RuntimeError('No running owned Compose services')
    instances = json.loads(run(['docker', 'inspect', *identifiers]))
    brokers, cruise = {}, []
    for info in instances:
        labels = info.get('Config', {}).get('Labels') or {}
        if labels.get('com.docker.compose.project') != PROJECT:
            raise RuntimeError('Unexpected Compose project identity')
        service = labels.get('com.docker.compose.service', '')
        if BROKER.fullmatch(service):
            if service in brokers or info.get('State', {}).get('Health', {}).get('Status') != 'healthy':
                raise RuntimeError('Broker identity duplicated or broker not healthy')
            brokers[service] = info
        elif service == 'cruise-control':
            cruise.append(info)
    if set(brokers) not in ({f'broker-{number}' for number in range(1, 5)},
                            {f'broker-{number}' for number in range(1, 7)}) or len(cruise) != 1:
        raise RuntimeError('Expected healthy Kafka topology and one owned Cruise Control')
    bootstrap = []
    for service, info in sorted(brokers.items()):
        published = local_binding(info, 9094)
        advertised = next((value.split('=', 1)[1] for value in info.get('Config', {}).get('Env', [])
                           if value.startswith('EXTERNAL_PORT=')), None)
        if advertised != published:
            raise RuntimeError('Published Kafka port differs from its advertised external listener')
        bootstrap.append('127.0.0.1:' + published)
    cruise_url = 'http://127.0.0.1:' + local_binding(cruise[0], 9090)
    executor = http(cruise_url + '/kafkacruisecontrol/state?json=true').get('ExecutorState', {}).get('state')
    if executor != 'NO_TASK_IN_PROGRESS':
        raise RuntimeError('Kafka baseline requires a verified idle Cruise Control Executor')
    reassignments = run(['docker', 'exec', brokers['broker-1']['Id'], '/opt/kafka/bin/kafka-reassign-partitions.sh',
                         '--bootstrap-server', 'localhost:9092', '--list'], timeout=30)
    if 'No partition reassignments found' not in reassignments:
        raise RuntimeError('Kafka baseline requires verified absence of partition reassignments')
    return {'bootstrapServers': ','.join(bootstrap), 'brokerIds': sorted(brokers),
            'executorState': executor, 'partitionReassignments': reassignments.strip(),
            'cruiseControlUrl': cruise_url, 'brokerContainerId': brokers['broker-1']['Id']}


def kafka_calibration(output, seconds, context=None):
    context = context or kafka_preflight()
    save(output / 'preflight.json', context)
    jars = list((ROOT / 'experiment-platform/load-generator/build/libs').glob('load-generator-*-SNAPSHOT.jar'))
    if len(jars) != 1:
        raise RuntimeError('Build :load-generator:bootJar first; expected one executable JAR')
    jar = jars[0]
    managed_java = sorted((Path.home() / '.gradle/jdks').glob('*17*/bin/java'))
    java = os.environ.get('CALIBRATION_JAVA', str(managed_java[0]) if managed_java else 'java')
    broker = context['brokerContainerId']
    inspected = json.loads(run(['docker', 'inspect', broker]))[0]
    network = next(item for item in inspected['NetworkSettings']['Networks'] if item.endswith('_backbone'))
    topic = 'task9-calibration-' + uuid.uuid4().hex
    token = uuid.uuid4().hex
    container = 'recovery-calibration-generator-' + token
    host_process = None
    api = None
    report = {'success': False, 'jarSha256': hashlib.sha256(jar.read_bytes()).hexdigest(), 'topic': topic,
              'runtimeSeconds': seconds, 'order': ['wsl', 'container'], 'runs': {},
              'interpretation': 'Client-placement comparison; not a causal estimate of total WSL2/Docker virtualization overhead.',
              'confounds': ['internal vs published Kafka listener/network route', 'sequential execution and cache/background-load drift',
                            'host process versus container cgroup/OS environment'], 'recordedAt': datetime.now(timezone.utc).isoformat()}
    payload = json.loads((ROOT / 'infrastructure/scenarios/load-smoke.json').read_text())
    payload['load'] = {'mode': 'CONSTANT', 'messageSizeBytes': 1024, 'schedule': [{'duration': f'PT{seconds}S', 'requestsPerSecond': 100}]}
    payload['seed'] = 20260910
    save(output / 'load-request.json', payload)
    resource('apply', 'normal')
    run(['docker', 'exec', broker, '/opt/kafka/bin/kafka-topics.sh', '--bootstrap-server', 'localhost:9092', '--create',
         '--topic', topic, '--partitions', '4', '--replication-factor', '3', '--config', 'min.insync.replicas=2'], timeout=60)
    try:
        for placement in ('wsl', 'container'):
            data_dir = output / placement
            data_dir.mkdir()
            port = free_port()
            api = f'http://127.0.0.1:{port}'
            log = (data_dir / 'application.log').open('wb')
            if placement == 'wsl':
                host_process = subprocess.Popen([java, '-Xmx256m', '-XX:ActiveProcessorCount=2', '-jar', str(jar),
                    f'--server.port={port}', f'--load.bootstrap-servers={context["bootstrapServers"]}',
                    f'--load.topic={topic}', f'--load.output-directory={data_dir}'], stdout=log, stderr=subprocess.STDOUT, cwd=ROOT)
            else:
                run(['docker', 'run', '--detach', '--rm', '--name', container, '--label', 'recovery.calibration.id=' + token,
                     '--network', network, '--publish', f'127.0.0.1:{port}:8081', '--memory', '512m',
                     '--mount', f'type=bind,source={jar},target=/app/load.jar,readonly',
                     '--mount', f'type=bind,source={data_dir},target=/results',
                     'eclipse-temurin:17-jre-jammy', 'java', '-Xmx256m', '-XX:ActiveProcessorCount=2', '-jar', '/app/load.jar',
                     '--server.address=0.0.0.0', '--server.port=8081', '--load.bootstrap-servers=broker-1:9092,broker-2:9092,broker-3:9092,broker-4:9092',
                     f'--load.topic={topic}', '--load.output-directory=/results'], timeout=120)
            for attempt in range(60):
                try:
                    status = http(api + '/api/load/status')
                    if status.get('cleanupComplete'):
                        break
                except (OSError, ValueError):
                    pass
                time.sleep(1)
            else:
                raise RuntimeError('Calibration Load Generator did not become ready')
            save(data_dir / 'accepted.json', http(api + '/api/load/start', payload))
            deadline = time.monotonic() + seconds + 45
            while time.monotonic() < deadline:
                status = http(api + '/api/load/status')
                if status.get('cleanupComplete') and status.get('state') in ('COMPLETED', 'FAILED'):
                    break
                time.sleep(1)
            if status.get('state') != 'COMPLETED' or not status.get('cleanupComplete'):
                raise RuntimeError('Kafka baseline load failed or cleanup incomplete')
            save(data_dir / 'completed.json', status)
            save(data_dir / 'metrics.json', http(api + '/api/load/metrics/current'))
            raw_path = Path(status['rawLogPath'])
            if placement == 'container':
                raw_path = data_dir / raw_path.relative_to('/results')
            report['runs'][placement] = measured_requests(raw_path, seconds)
            if placement == 'wsl':
                host_process.terminate()
                host_process.wait(timeout=20)
                host_process = None
            else:
                (data_dir / 'application.log').write_text(run(['docker', 'logs', container], check=False))
                run(['docker', 'stop', '--time', '20', container])
            log.close()
            api = None
        baseline, inside = report['runs']['wsl'], report['runs']['container']
        report['p99DifferenceMs'] = inside['produceP99Ms'] - baseline['produceP99Ms']
        report['throughputDifferenceRps'] = inside['throughputRps'] - baseline['throughputRps']
        report['success'] = baseline['errorCount'] == 0 and inside['errorCount'] == 0
        save(output / 'kafka-comparison.json', report)
        return report
    finally:
        if api:
            try:
                http(api + '/api/load/stop', {})
            except Exception:
                pass
        if host_process is not None and host_process.poll() is None:
            host_process.terminate()
            try:
                host_process.wait(timeout=20)
            except subprocess.TimeoutExpired:
                host_process.kill()
        inspected = run(['docker', 'inspect', container], check=False)
        if inspected.strip().startswith('['):
            instances = json.loads(inspected)
            if instances and instances[0].get('Config', {}).get('Labels', {}).get('recovery.calibration.id') == token:
                run(['docker', 'rm', '-f', container], check=False)
        run(['docker', 'exec', broker, '/opt/kafka/bin/kafka-topics.sh', '--bootstrap-server', 'localhost:9092', '--delete', '--if-exists', '--topic', topic], timeout=60, check=False)


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('kind', choices=('network', 'disk', 'host', 'kafka'))
    parser.add_argument('arguments', nargs='*')
    args = parser.parse_args()
    if not re.fullmatch(r'[a-z0-9][a-z0-9_-]{0,62}', PROJECT):
        raise ValueError('Invalid COMPOSE_PROJECT_NAME')
    profile = args.arguments[0] if args.kind in ('network', 'disk') and args.arguments else None
    if args.kind in ('network', 'disk') and (not profile or not NAME.fullmatch(profile)):
        raise ValueError('A whitelisted profile name is required')
    tail = args.arguments[1:] if profile else args.arguments
    if len(tail) > 1:
        raise ValueError('Only an optional fresh evidence directory is accepted')
    directory = tail[0] if tail else f'results/calibration/runs/{args.kind}-{profile or "host"}-{datetime.now(timezone.utc):%Y%m%dT%H%M%SZ}-{uuid.uuid4().hex[:6]}'
    output = (ROOT / directory).resolve()
    if not output.is_relative_to(ROOT):
        raise ValueError('Evidence directory must stay inside this project')
    output.mkdir(parents=True, exist_ok=False)
    seconds = int(os.environ.get('CALIBRATION_SECONDS', '60'))
    if not 60 <= seconds <= 3600:
        raise ValueError('CALIBRATION_SECONDS must be between 60 and 3600')
    details = None
    reading = None
    resources_started = False
    result = {'success': False, 'kind': args.kind, 'profile': profile, 'evidenceDirectory': str(output)}
    try:
        if args.kind == 'host':
            result.update(host_calibration(output))
        elif args.kind == 'kafka':
            context = kafka_preflight()
            resources_started = True
            result.update(kafka_calibration(output, seconds, context))
        else:
            resources_started = True
            operation = network_calibration if args.kind == 'network' else disk_calibration
            details, reading = operation(profile, output, seconds)
            save(output / 'measurement.json', reading)
            result.update(reading)
    except Exception as error:
        result['errors'] = [str(error)]
        result['unsupported'] = isinstance(error, ResourceFailure) and error.unsupported
    finally:
        if resources_started:
            try:
                result['cleanup'] = resource('cleanup')
            except Exception as error:
                result['success'] = False
                result.setdefault('errors', []).append('Cleanup unverified: ' + str(error))
        if profile:
            try:
                details = details or resource('status', profile)['details']
                section = reading if result.get('success') and reading else {
                    'success': False, 'matchedTarget': False, 'runtimeSeconds': seconds,
                    'errors': result.get('errors', ['Measured constraints did not pass admission'])}
                result['admissionPath'] = str(proof(profile, details, args.kind, section))
            except Exception as proof_error:
                result['success'] = False
                result['proofError'] = str(proof_error)
        save(output / 'summary.json', result)
    print(json.dumps(result, ensure_ascii=False, allow_nan=False))
    return 0 if result.get('success') else 1


if __name__ == '__main__':
    try:
        sys.exit(main())
    except Exception as error:
        print(json.dumps({'success': False, 'errors': [str(error)]}), ensure_ascii=False)
        sys.exit(2)
