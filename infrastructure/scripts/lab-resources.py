#!/usr/bin/env python3
"""Fixed lab operations; no user command strings or host-network/cgroup mutations."""
import hashlib
import json
import math
import os
from pathlib import Path
import re
import subprocess
import sys
import time
from datetime import datetime, timezone
import yaml

ROOT = Path(__file__).resolve().parents[2]
PROJECT = os.environ.get('COMPOSE_PROJECT_NAME', 'replica-recovery')
STATE_ROOT = ROOT / 'results/lab-state'
PROFILE_ROOT = ROOT / 'infrastructure/profiles'
ADMISSION_ROOT = ROOT / 'results/calibration'
BROKER = re.compile(r'broker-[1-6]$')
NAME = re.compile(r'[a-z0-9][a-z0-9-]{0,63}$')
ROOT_HANDLE = '9a:'
FAULT_PROFILES = {'LIMIT_NETWORK': 'network-constrained', 'ADD_LATENCY': 'high-latency',
                  'ADD_PACKET_LOSS': 'packet-loss', 'LIMIT_DISK': 'disk-constrained'}
FAULTS = {'STOP_BROKER', 'KILL_BROKER', 'RESTART_BROKER', 'ISOLATE_BROKER', 'ISOLATE_DOMAIN'} | set(FAULT_PROFILES)


def observed_now():
    return datetime.now(timezone.utc).isoformat().replace('+00:00', 'Z')


class Unsupported(RuntimeError):
    pass


def run(args, timeout=30, check=True):
    result = subprocess.run([str(arg) for arg in args], capture_output=True, text=True, timeout=timeout)
    if check and result.returncode:
        raise RuntimeError(f'{args[0]} failed ({result.returncode}): {(result.stderr or result.stdout)[-1200:].strip()}')
    return result


def docker(*args, timeout=30, check=True):
    return run(['docker', *args], timeout, check)


def execute(container, *args, timeout=30, check=True):
    if not re.fullmatch(r'[a-f0-9]{64}', container):
        raise ValueError('Invalid resolved container identity')
    return docker('exec', '--user', '0', container, *args, timeout=timeout, check=check)


def profile(name):
    if not NAME.fullmatch(name):
        raise ValueError('Invalid profile name')
    path = PROFILE_ROOT / f'{name}.yaml'
    if path.is_symlink() or not path.is_file():
        raise ValueError('Unknown profile')
    data = yaml.safe_load(path.read_text())
    if not isinstance(data, dict) or type(data.get('schemaVersion')) is not int or data.get('schemaVersion') != 1 or data.get('name') != name:
        raise ValueError('Invalid profile schema/name')
    if set(data) != {'schemaVersion', 'name', 'brokers', 'cpu', 'network', 'disk'}:
        raise ValueError('Unknown or missing profile fields')
    targets = data['brokers']
    if targets != 'all' and (not isinstance(targets, list) or not targets or len(set(targets)) != len(targets)
                             or not all(isinstance(value, str) and BROKER.fullmatch(value) for value in targets)):
        raise ValueError('brokers must be all or unique broker-1..broker-6 names')
    sections = {'cpu': {'cpus'}, 'network': {'egressMbit', 'ingressMbit', 'delayMs', 'jitterMs', 'lossPercent'},
                'disk': {'readMiBps', 'writeMiBps'}}
    for section, fields in sections.items():
        if not isinstance(data[section], dict) or set(data[section]) != fields:
            raise ValueError(f'Invalid {section} fields')
        for field, value in data[section].items():
            if value is None and field in {'egressMbit', 'ingressMbit', 'readMiBps', 'writeMiBps'}:
                continue
            if isinstance(value, bool) or not isinstance(value, (int, float)) or not math.isfinite(value):
                raise ValueError(f'{section}.{field} must be a finite number')
            if value < 0 or (field in {'cpus', 'egressMbit', 'ingressMbit', 'readMiBps', 'writeMiBps'} and value == 0):
                raise ValueError(f'{section}.{field} must be positive; null means unlimited')
    if not 0.01 <= data['cpu']['cpus'] <= 64 or not 0 <= data['network']['lossPercent'] <= 100:
        raise ValueError('CPU or loss outside supported bounds')
    if data['network']['jitterMs'] > data['network']['delayMs'] or data['network']['delayMs'] > 60000:
        raise ValueError('jitter must not exceed delay; maximum delay is 60000 ms')
    return data, hashlib.sha256(path.read_bytes()).hexdigest()


def containers(include_saved=False):
    if not re.fullmatch(r'[a-z0-9][a-z0-9_-]{0,62}', PROJECT):
        raise ValueError('Invalid Compose project name')
    ids = docker('ps', '-aq', '--filter', f'label=com.docker.compose.project={PROJECT}').stdout.split()
    if not ids:
        raise Unsupported('No containers for configured Compose project')
    configured = os.environ.get('LAB_BROKER_IDS', os.environ.get('MONITORING_BROKER_IDS', '1,2,3,4')).split(',')
    if not configured or any(not re.fullmatch(r'[1-6]', value.strip()) for value in configured):
        raise ValueError('LAB_BROKER_IDS must list broker IDs 1..6')
    allowed = {f'broker-{value.strip()}' for value in configured}
    configured_names = set(allowed)
    saved = {}
    if include_saved:
        saved = read_state()['brokers']
        allowed |= set(saved)
    result = {}
    for info in json.loads(docker('inspect', *ids).stdout):
        labels = info['Config'].get('Labels') or {}
        service = labels.get('com.docker.compose.service', '')
        if labels.get('com.docker.compose.project') != PROJECT or not BROKER.fullmatch(service) or service not in allowed:
            continue
        if service not in configured_names and saved[service].get('containerId') != info['Id']:
            continue
        if service in result:
            raise Unsupported(f'Duplicate containers for {service}')
        result[service] = info
    if not result:
        raise Unsupported('No lab broker containers')
    return result


def selected(data, all_brokers):
    names = sorted(all_brokers) if data['brokers'] == 'all' else data['brokers']
    if any(name not in all_brokers for name in names):
        raise Unsupported('Profile targets a broker absent from this topology')
    return {name: all_brokers[name] for name in names}


def metadata(data, digest, all_brokers=None):
    all_brokers = all_brokers or containers()
    targets = selected(data, all_brokers)
    result = {'profile': data, 'profileSha256': digest,
              'hostIdentity': {'dockerServerId': docker('info', '--format', '{{.ID}}').stdout.strip()}, 'brokers': {}}
    for name, info in targets.items():
        if not info['State'].get('Running'):
            old = read_state()['brokers'].get(name)
            if data['name'] != 'normal' or not old or old.get('containerId') != info['Id']:
                raise Unsupported(f'{name} is not running')
            result['brokers'][name] = {key: old[key] for key in ('containerId', 'device', 'devicePath', 'interfaces', 'ioMaxWritable', 'privateCgroup')}
            result['brokers'][name]['deferred'] = True
            continue
        container = info['Id']
        output = execute(container, 'python3', '-c',
                         'import os,json; d=os.stat("/var/lib/kafka/data").st_dev; '
                         'print(json.dumps({"device":str(os.major(d))+":"+str(os.minor(d)), '
                         '"bootId":open("/proc/sys/kernel/random/boot_id").read().strip()}))').stdout
        device = json.loads(output)
        result['hostIdentity']['bootId'] = device['bootId']
        links = json.loads(execute(container, 'ip', '-j', 'link', 'show').stdout)
        interfaces = sorted(link['ifname'] for link in links if re.fullmatch(r'eth[0-9]+', link['ifname']))
        if not interfaces:
            raise Unsupported(f'No supported network interfaces on {name}')
        uevent = execute(container, 'cat', f'/sys/dev/block/{device["device"]}/uevent', check=False)
        devname = next((line.split('=', 1)[1] for line in uevent.stdout.splitlines() if line.startswith('DEVNAME=')), None)
        if devname is not None and not re.fullmatch(r'[a-zA-Z0-9/_-]+', devname):
            raise Unsupported('Unsupported block device name')
        writable = execute(container, 'test', '-w', '/sys/fs/cgroup/io.max', check=False).returncode == 0
        private = info['HostConfig'].get('CgroupnsMode') != 'host'
        result['brokers'][name] = {'containerId': container, 'device': device['device'],
                                   'devicePath': f'/dev/{devname}' if devname else None, 'interfaces': interfaces,
                                   'ioMaxWritable': writable, 'privateCgroup': private}
    if 'bootId' not in result['hostIdentity']:
        raise Unsupported('No running broker is available to verify host identity')
    return result


def network_required(data):
    values = data['network']
    return values['egressMbit'] is not None or values['ingressMbit'] is not None or any(values[key] for key in ('delayMs', 'jitterMs', 'lossPercent'))


def disk_required(data):
    return any(value is not None for value in data['disk'].values())


def admission(details):
    data = details['profile']
    required = [kind for kind, active in [('network', network_required(data)), ('disk', disk_required(data))] if active]
    if not required:
        return
    path = ADMISSION_ROOT / data['name'] / 'admission.json'
    if path.is_symlink() or not path.is_file():
        raise Unsupported(f'Calibration admission missing for {data["name"]}')
    proof = json.loads(path.read_text())
    if proof.get('schemaVersion') != 1 or proof.get('profile') != data['name'] or proof.get('profileSha256') != details['profileSha256']:
        raise Unsupported('Calibration profile hash does not match')
    if proof.get('hostIdentity') != details['hostIdentity'] or proof.get('brokers') != details['brokers']:
        raise Unsupported('Calibration host/container/device identity does not match')
    if proof.get('success') is not True:
        raise Unsupported('Calibration admission did not pass')
    for kind in required:
        section = proof.get(kind, {})
        if section.get('success') is not True or section.get('matchedTarget') is not True or section.get('runtimeSeconds', 0) < 60:
            raise Unsupported(f'{kind} calibration did not pass')
        if kind == 'disk' and section.get('direct') is not True:
            raise Unsupported('Disk calibration requires direct I/O')
        checks = section.get('checks', [])
        for broker in details['brokers']:
            if kind == 'network':
                if section.get('unverifiedConstraints'):
                    raise Unsupported('Network profile has unverified constraints')
                for direction, field in [('egress', 'egressMbit'), ('ingress', 'ingressMbit')]:
                    target = data['network'][field]
                    if target is not None and not any(check.get('broker') == broker and check.get('direction') == direction
                                                     and check.get('targetMbit') == target and check.get('passed') is True
                                                     and check.get('runtimeSeconds', 0) >= 59 for check in checks):
                        raise Unsupported(f'Incomplete network calibration coverage: {broker}/{direction}')
                if data['network']['delayMs'] and not any(check.get('broker') == broker and check.get('passed') is True
                                                         for check in section.get('latencyChecks', [])):
                    raise Unsupported(f'Latency calibration is missing for {broker}')
            else:
                for operation in ('read', 'write', 'randread', 'randwrite'):
                    direction = 'read' if operation.endswith('read') else 'write'
                    target = data['disk'][direction + 'MiBps']
                    if target is None:
                        continue
                    for block in (4096, 1048576):
                        if not any(check.get('broker') == broker and check.get('rw') == operation
                                   and check.get('blockSizeBytes') == block and check.get('targetMiBps') == target
                                   and check.get('passed') is True and check.get('direct') is True
                                   and check.get('runtimeSeconds', 0) >= 59 for check in checks):
                            raise Unsupported(f'Incomplete disk calibration coverage: {broker}/{operation}/{block}')


def state_path():
    STATE_ROOT.mkdir(parents=True, exist_ok=True)
    return STATE_ROOT / f'{PROJECT}.json'


def read_state():
    path = state_path()
    if path.is_symlink():
        raise ValueError('Lab state cannot be a symlink')
    return json.loads(path.read_text()) if path.exists() else {'schemaVersion': 1, 'brokers': {}}


def write_state(state):
    path = state_path()
    temporary = path.with_suffix('.tmp')
    temporary.write_text(json.dumps(state, indent=2))
    temporary.replace(path)


def qdiscs(container):
    return json.loads(execute(container, 'tc', '-j', 'qdisc', 'show').stdout)


def owns_ingress(container, interface):
    filters = json.loads(execute(container, 'tc', '-j', 'filter', 'show', 'dev', interface, 'parent', 'ffff:').stdout)
    return bool(filters) and all(item.get('pref') == 49152 for item in filters)


def network_owner(container):
    state = read_state()
    return next((entry for entry in state['brokers'].values() if entry.get('containerId') == container), None)


def record_ifb_intent(container, interface):
    state = read_state()
    entry = next((entry for entry in state['brokers'].values() if entry.get('containerId') == container), None)
    if entry is None or not re.fullmatch(r'recovery-ifb[0-9]+', interface):
        raise ValueError('IFB operation requires a journaled lab container and a reserved name')
    entry['ownedIfb'] = sorted(set(entry.get('ownedIfb', [])) | {interface})
    write_state(state)


def capabilities(details, all_brokers):
    data = details['profile']
    for name, target in details['brokers'].items():
        info = all_brokers[name]
        if target.get('deferred'):
            continue
        caps = info['HostConfig'].get('CapAdd') or []
        if 'NET_ADMIN' not in caps:
            raise Unsupported(f'{name} needs NET_ADMIN in its own container namespace')
        execute(target['containerId'], 'sh', '-c', 'command -v tc; command -v fio; command -v iperf3', timeout=10)
        owner = network_owner(target['containerId'])
        for qdisc in qdiscs(target['containerId']):
            is_owned_ifb = owner is not None and qdisc.get('dev') in owner.get('ownedIfb', [])
            if qdisc.get('root') and qdisc.get('kind') != 'noqueue' and not is_owned_ifb \
                    and (qdisc.get('handle') != ROOT_HANDLE or owner is None):
                raise Unsupported(f'{name} has an unmanaged root qdisc')
            if qdisc.get('kind') == 'ingress' and (owner is None or not owns_ingress(target['containerId'], qdisc['dev'])):
                raise Unsupported(f'{name} has an unmanaged ingress qdisc')
        for link in json.loads(execute(target['containerId'], 'ip', '-j', 'link', 'show').stdout):
            if re.fullmatch(r'recovery-ifb[0-9]+', link['ifname']) and (owner is None or link['ifname'] not in owner.get('ownedIfb', [])):
                raise Unsupported(f'{name} has an unmanaged reserved IFB interface')
        if disk_required(data):
            if target['devicePath'] is None:
                raise Unsupported(f'{name} data volume device is not a physical block device')
            execute(target['containerId'], 'cat', '/sys/fs/cgroup/io.max', timeout=10)
            controllers = execute(target['containerId'], 'cat', '/sys/fs/cgroup/cgroup.controllers').stdout.split()
            if 'io' not in controllers:
                raise Unsupported(f'{name} cgroup v2 io controller is unavailable')
            if not target['privateCgroup'] or not target['ioMaxWritable']:
                raise Unsupported(f'{name} own private cgroup io.max is read-only or unavailable; disk profile unsupported')


def engine_update(container, values):
    host = os.environ.get('DOCKER_HOST')
    if not host:
        host = docker('context', 'inspect', '--format', '{{.Endpoints.docker.Host}}').stdout.strip()
    if not host.startswith('unix://'):
        raise Unsupported('Resource updates require a local Unix Docker API socket')
    socket = host[len('unix://'):]
    version = docker('version', '--format', '{{.Server.APIVersion}}').stdout.strip()
    if not re.fullmatch(r'1\.[0-9]+', version):
        raise Unsupported('Unknown Docker API version')
    response = run(['curl', '--fail-with-body', '--silent', '--show-error', '--max-time', '15',
                    '--unix-socket', socket, '-H', 'Content-Type: application/json', '-X', 'POST',
                    '--data-binary', json.dumps(values), f'http://localhost/v{version}/containers/{container}/update'])
    return json.loads(response.stdout or '{}')


def save_originals(details, all_brokers):
    state = read_state()
    for name, target in details['brokers'].items():
        old = state['brokers'].get(name)
        if old and old.get('containerId') == target['containerId']:
            continue
        host = all_brokers[name]['HostConfig']
        resources = {key: host.get(key) or (0 if key.startswith('Cpu') or key == 'NanoCpus' else [])
                     for key in ('NanoCpus', 'CpuPeriod', 'CpuQuota', 'BlkioDeviceReadBps', 'BlkioDeviceWriteBps')}
        io_max = execute(target['containerId'], 'cat', '/sys/fs/cgroup/io.max').stdout
        state['brokers'][name] = {**target, 'resources': resources, 'ioMax': io_max}
    write_state(state)


def clear_network(container, allow_unmanaged=False):
    errors = []
    owner = network_owner(container)
    owned_ifb = set(owner.get('ownedIfb', [])) if owner else set()
    for qdisc in qdiscs(container):
        if qdisc.get('dev') in owned_ifb:
            continue  # Our IFB links are removed after their ingress redirect is gone.
        if qdisc.get('root') and qdisc.get('kind') != 'noqueue':
            if (qdisc.get('handle') != ROOT_HANDLE or owner is None) and not allow_unmanaged:
                errors.append('Unmanaged root qdisc cannot be removed')
                continue
            execute(container, 'tc', 'qdisc', 'del', 'dev', qdisc['dev'], 'root')
        if qdisc.get('kind') == 'ingress':
            if owner is None or not owns_ingress(container, qdisc['dev']):
                errors.append('Unmanaged ingress qdisc cannot be removed')
                continue
            execute(container, 'tc', 'qdisc', 'del', 'dev', qdisc['dev'], 'ingress')
    if errors:
        raise Unsupported('; '.join(errors))
    for interface in owned_ifb:
        if not re.fullmatch(r'recovery-ifb[0-9]+', interface):
            raise ValueError('Invalid journaled IFB name')
        exists = execute(container, 'ip', 'link', 'show', 'dev', interface, check=False)
        if exists.returncode == 0:
            execute(container, 'ip', 'link', 'del', 'dev', interface)
    if owner is not None and owned_ifb:
        state = read_state()
        for entry in state['brokers'].values():
            if entry.get('containerId') == container:
                entry['ownedIfb'] = []
        write_state(state)


def set_network(target, network):
    container = target['containerId']
    clear_network(container)
    for interface in target['interfaces']:
        rate = network['egressMbit']
        delay = network['delayMs']
        jitter = network['jitterMs']
        loss = network['lossPercent']
        has_netem = delay or jitter or loss
        if rate is not None:
            execute(container, 'tc', 'qdisc', 'replace', 'dev', interface, 'root', 'handle', ROOT_HANDLE, 'htb', 'default', '1')
            execute(container, 'tc', 'class', 'replace', 'dev', interface, 'parent', ROOT_HANDLE, 'classid', '9a:1', 'htb',
                    'rate', f'{rate}mbit', 'ceil', f'{rate}mbit', 'burst', '32k', 'cburst', '32k')
        if has_netem:
            parent = ['parent', '9a:1', 'handle', '9b:'] if rate is not None else ['root', 'handle', ROOT_HANDLE]
            options = []
            if delay:
                options += ['delay', f'{delay}ms']
                if jitter:
                    options += [f'{jitter}ms']
            if loss:
                options += ['loss', f'{loss}%']
            execute(container, 'tc', 'qdisc', 'replace', 'dev', interface, *parent, 'netem', *options)
        ingress = network['ingressMbit']
        if ingress is not None:
            ifb = 'recovery-ifb' + interface[3:]
            record_ifb_intent(container, ifb)
            execute(container, 'ip', 'link', 'add', ifb, 'type', 'ifb')
            execute(container, 'ip', 'link', 'set', 'dev', ifb, 'up')
            execute(container, 'ip', 'link', 'set', 'dev', ifb, 'txqueuelen', '1000')
            execute(container, 'tc', 'qdisc', 'replace', 'dev', ifb, 'root', 'handle', ROOT_HANDLE, 'htb', 'default', '1')
            execute(container, 'tc', 'class', 'replace', 'dev', ifb, 'parent', ROOT_HANDLE, 'classid', '9a:1', 'htb',
                    'rate', f'{ingress}mbit', 'ceil', f'{ingress}mbit', 'burst', '32k', 'cburst', '32k')
            execute(container, 'tc', 'qdisc', 'replace', 'dev', interface, 'handle', 'ffff:', 'ingress')
            execute(container, 'tc', 'filter', 'replace', 'dev', interface, 'parent', 'ffff:', 'protocol', 'all', 'pref', '49152', 'u32',
                    'match', 'u32', '0', '0', 'action', 'mirred', 'egress', 'redirect', 'dev', ifb)


def set_disk(target, limits):
    if target['devicePath'] is None:
        if any(value is not None for value in limits.values()):
            raise Unsupported('No block device for disk profile')
        return
    raw = execute(target['containerId'], 'cat', '/sys/fs/cgroup/io.max').stdout
    line = next((line for line in raw.splitlines() if line.split()[0] == target['device']), '')
    fields = dict(value.split('=', 1) for value in line.split()[1:])
    desired = {key: str(int(limits[field] * 1048576)) if limits[field] is not None else 'max'
               for field, key in [('readMiBps', 'rbps'), ('writeMiBps', 'wbps')]}
    if any(fields.get(key, 'max') != value for key, value in desired.items()):
        if not target['privateCgroup'] or not target['ioMaxWritable']:
            raise Unsupported('Own private cgroup io.max is not writable')
        setting = target['device'] + ' ' + ' '.join(f'{key}={value}' for key, value in desired.items())
        execute(target['containerId'], 'python3', '-c',
                'import sys; open("/sys/fs/cgroup/io.max", "w").write(sys.argv[1]+"\\n")', setting)
        raw = execute(target['containerId'], 'cat', '/sys/fs/cgroup/io.max').stdout
        line = next((line for line in raw.splitlines() if line.split()[0] == target['device']), '')
        fields = dict(value.split('=', 1) for value in line.split()[1:])
    for field, key in [('readMiBps', 'rbps'), ('writeMiBps', 'wbps')]:
        expected = str(int(limits[field] * 1048576)) if limits[field] is not None else 'max'
        if fields.get(key, 'max') != expected:
            raise Unsupported('io.max does not confirm the requested limit')
    return raw


def apply_profile(name, override=None):
    data, digest = profile(name)
    all_brokers = containers()
    details = metadata(data, digest, all_brokers)
    capabilities(details, all_brokers)
    if override is not None:
        details['brokers'] = {name: details['brokers'][name] for name in override}
    save_originals(details, all_brokers)
    details['observed'] = {}
    for name, target in details['brokers'].items():
        docker('update', '--cpus', str(data['cpu']['cpus']), target['containerId'])
        nano_cpus = json.loads(docker('inspect', target['containerId']).stdout)[0]['HostConfig']['NanoCpus']
        if nano_cpus != int(data['cpu']['cpus'] * 1_000_000_000):
            raise Unsupported('Docker did not confirm the requested CPU limit')
        if target.get('deferred'):
            details['observed'][name] = {'deferred': True, 'nanoCpus': nano_cpus,
                                        'reason': 'Own stopped broker; namespace/cgroup are gone, verified after restart by cleanup'}
            continue
        set_network(target, data['network'])
        io = set_disk(target, data['disk'])
        details['observed'][name] = {'tc': execute(target['containerId'], 'tc', '-s', 'qdisc', 'show').stdout,
                                    'ioMax': io, 'nanoCpus': nano_cpus}
    return details


def healthy(info):
    return info['State'].get('Running') and info['State'].get('Health', {}).get('Status') == 'healthy'


def cleanup(phase):
    if phase not in {'all', 'reset-resources', 'start-brokers', 'health', 'reassignments'}:
        raise ValueError('Unknown cleanup operation')
    if phase == 'all':
        errors, details = [], {}
        for operation in ('reset-resources', 'start-brokers', 'health', 'reassignments'):
            try:
                details[operation] = cleanup(operation)
            except Exception as error:
                errors.append(f'{operation}: {error}')
        if errors:
            raise RuntimeError('; '.join(errors))
        return details
    all_brokers = containers(include_saved=True)
    state = read_state()
    if phase == 'reset-resources':
        errors = []
        for name, info in all_brokers.items():
            try:
                old = state['brokers'].get(name)
                if not old or old.get('containerId') != info['Id']:
                    continue
                if info['State'].get('Running'):
                    clear_network(info['Id'])
                    original_line = next((line for line in old.get('ioMax', '').splitlines() if line.split()[0] == old['device']), '')
                    original = dict(value.split('=', 1) for value in original_line.split()[1:])
                    limits = {field: int(original[key]) / 1048576 if original.get(key, 'max') != 'max' else None
                              for key, field in [('rbps', 'readMiBps'), ('wbps', 'writeMiBps')]}
                    set_disk(old, limits)
                restore = {key: old['resources'][key] for key in ('NanoCpus', 'CpuPeriod', 'CpuQuota')}
                engine_update(info['Id'], restore)
            except Exception as error:
                errors.append(f'{name}: {error}')
        if errors:
            raise RuntimeError('; '.join(errors))
        return {'restored': sorted(state['brokers'])}
    if phase == 'start-brokers':
        # A cancelled `docker stop` request may still be finishing in the daemon.
        # Persist the cleanup intent so health can restart our broker if it stops later.
        state['cleanupRestartRequested'] = True
        write_state(state)
        errors = []
        for info in all_brokers.values():
            try:
                if not info['State'].get('Running'):
                    docker('start', info['Id'])
            except Exception as error:
                errors.append(str(error))
        if errors:
            raise RuntimeError('; '.join(errors))
        return {'started': sorted(all_brokers)}
    if phase == 'health':
        for attempt in range(60):
            current = containers(include_saved=True)
            if state.get('cleanupRestartRequested'):
                for name, info in current.items():
                    old = state['brokers'].get(name)
                    if not info['State'].get('Running') and old and old.get('containerId') == info['Id']:
                        docker('start', info['Id'])
            if all(healthy(info) for info in current.values()):
                for name, info in current.items():
                    if name not in state['brokers']:
                        if any(qdisc.get('kind') != 'noqueue' for qdisc in qdiscs(info['Id'])):
                            raise Unsupported(f'Unmanaged qdisc prevents a verified baseline on {name}')
                for name, old in state['brokers'].items():
                    if name in current and old.get('containerId') == current[name]['Id']:
                        if any(qdisc.get('handle') == ROOT_HANDLE or qdisc.get('kind') == 'ingress'
                               or re.fullmatch(r'recovery-ifb[0-9]+', qdisc.get('dev', '')) for qdisc in qdiscs(old['containerId'])):
                            raise RuntimeError(f'Network profile remains on {name}')
                        now_cpu = current[name]['HostConfig'].get('NanoCpus', 0)
                        if now_cpu != old['resources']['NanoCpus']:
                            raise RuntimeError(f'CPU limit not restored on {name}')
                        raw = execute(old['containerId'], 'cat', '/sys/fs/cgroup/io.max').stdout
                        line = next((value for value in raw.splitlines() if value.split()[0] == old['device']), '')
                        fields = dict(value.split('=', 1) for value in line.split()[1:])
                        old_line = next((line for line in old.get('ioMax', '').splitlines() if line.split()[0] == old['device']), '')
                        original = dict(value.split('=', 1) for value in old_line.split()[1:])
                        for field in ('rbps', 'wbps'):
                            if fields.get(field, 'max') != original.get(field, 'max'):
                                raise RuntimeError(f'Disk limit not restored on {name}')
                return {'healthy': sorted(current)}
            time.sleep(2)
        raise RuntimeError('Brokers did not become healthy')
    first = next((info for info in all_brokers.values() if healthy(info)), None)
    if first is None:
        raise RuntimeError('No healthy broker to verify reassignments')
    output = execute(first['Id'], '/opt/kafka/bin/kafka-reassign-partitions.sh', '--bootstrap-server',
                     'localhost:9092', '--list', timeout=25).stdout
    if 'No partition reassignments found' not in output:
        raise RuntimeError('Partition reassignments remain or could not be verified')
    cleanup('health')
    write_state({'schemaVersion': 1, 'brokers': {}})
    return {'reassignments': output.strip()}


def fault(mode, action, target):
    if mode not in {'validate', 'apply'} or action not in FAULTS or not NAME.fullmatch(target):
        raise ValueError('Unknown fault action or invalid target')
    all_brokers = containers()
    if action == 'ISOLATE_DOMAIN':
        if target not in {'zone-a', 'zone-b', 'zone-c'}:
            raise ValueError('Domain must be zone-a, zone-b or zone-c')
        names = [name for name, info in all_brokers.items() if f'BROKER_RACK={target}' in info['Config']['Env']]
    else:
        if not BROKER.fullmatch(target) or target not in all_brokers:
            raise ValueError('Target broker is absent')
        names = [target]
    if not names:
        raise Unsupported('No brokers in target domain')
    if action in FAULT_PROFILES:
        data, digest = profile(FAULT_PROFILES[action])
        details = metadata(data, digest, all_brokers)
        if not set(names).issubset(details['brokers']):
            raise Unsupported('Fault target is outside the calibrated profile targets')
        capabilities(details, all_brokers)
        admission(details)
        if mode == 'validate':
            return details
        applied = apply_profile(data['name'], names)
        applied['faultObservedAt'] = observed_now()
        return applied
    data, digest = profile('normal')
    details = metadata(data, digest, all_brokers)
    details['brokers'] = {name: details['brokers'][name] for name in names}
    capabilities(details, all_brokers)
    if mode == 'validate':
        return details
    save_originals(details, all_brokers)
    observations = []
    for name in names:
        container = all_brokers[name]['Id']
        if action in {'STOP_BROKER', 'RESTART_BROKER'}:
            docker('stop', '--time', '10', container, timeout=20)
        elif action == 'KILL_BROKER':
            docker('kill', container)
        else:
            set_network(details['brokers'][name], {'egressMbit': None, 'ingressMbit': None,
                                                  'delayMs': 0, 'jitterMs': 0, 'lossPercent': 100})
        if action in {'STOP_BROKER', 'KILL_BROKER', 'RESTART_BROKER'}:
            current = json.loads(docker('inspect', container).stdout)[0]
            if current['State'].get('Running'):
                raise RuntimeError('Fault did not stop broker')
        else:
            peers = [info for peer, info in all_brokers.items() if peer not in names and info['State'].get('Running')]
            if not peers:
                raise Unsupported('No independent peer to verify isolation')
            check = execute(peers[0]['Id'], 'python3', '-c',
                            'import socket,sys; s=socket.socket(); s.settimeout(2); '
                            'sys.exit(0 if s.connect_ex((sys.argv[1],9092)) else 1)', name, timeout=5)
            if check.returncode != 0:
                raise RuntimeError('Broker remains reachable after network isolation')
        # Observe the confirmed failure before a RESTART starts the container again.
        observations.append({'broker': name, 'observedAt': observed_now()})
        if action == 'RESTART_BROKER':
            docker('start', container)
            for attempt in range(60):
                if healthy(json.loads(docker('inspect', container).stdout)[0]):
                    break
                time.sleep(2)
            else:
                raise RuntimeError('Restarted broker did not become healthy')
    return {'action': action, 'targets': names, 'observed': details,
            'faultObservedAt': observations[0]['observedAt'], 'targetObservations': observations}


def main(argv):
    if not argv:
        raise ValueError('Operation required')
    if argv[0] == 'profile':
        if len(argv) not in (2, 3) or argv[1] not in {'validate', 'apply', 'status'}:
            raise ValueError('Usage: apply-profile.sh validate|apply|status [profile]')
        if len(argv) == 2 and argv[1] != 'status':
            raise ValueError('A profile name is required for validate/apply')
        name = argv[2] if len(argv) == 3 else 'normal'
        data, digest = profile(name)
        all_brokers = containers()
        details = metadata(data, digest, all_brokers)
        if argv[1] == 'status':
            return details
        capabilities(details, all_brokers)
        if argv[1] == 'validate':
            admission(details)
            return details
        return apply_profile(name)
    if argv[0] == 'fault' and len(argv) == 4:
        return fault(*argv[1:])
    if argv[0] == 'cleanup' and len(argv) in (1, 2):
        return cleanup(argv[1] if len(argv) == 2 else 'all')
    raise ValueError('Operation is not allowed')


if __name__ == '__main__':
    try:
        details = main(sys.argv[1:])
        print(json.dumps({'success': True, 'unsupported': False, 'errors': [], 'details': details}))
    except Exception as error:
        print(json.dumps({'success': False, 'unsupported': isinstance(error, Unsupported),
                          'errors': [str(error)], 'details': {}}))
        sys.exit(2 if isinstance(error, (Unsupported, ValueError)) else 1)
