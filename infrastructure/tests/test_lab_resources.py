import importlib.util
import json
from pathlib import Path
import subprocess
import tempfile
import unittest
from unittest.mock import patch

module_path = Path(__file__).resolve().parents[1] / 'scripts/lab-resources.py'
spec = importlib.util.spec_from_file_location('lab_resources', module_path)
lab = importlib.util.module_from_spec(spec)
spec.loader.exec_module(lab)


def reply(text='', code=0):
    return subprocess.CompletedProcess([], code, text, '')


class LabResourcesTest(unittest.TestCase):
    def test_waits_until_live_kafka_peer_no_longer_reports_stopped_broker(self):
        brokers = {f'broker-{index}': {'Id': str(index) * 64, 'State': {'Running': True}}
                   for index in (1, 6)}
        live = 'ID  HOST      PORT  RACK    STATE     ENDPOINT_TYPE\n1   broker-1  9092  zone-a  unfenced  broker'
        stale = live + '\n6   broker-6  9092  zone-c  unfenced  broker'
        with patch.object(lab, 'execute', side_effect=[reply(), reply(stale), reply(live)]) as execute, \
                patch.object(lab.time, 'sleep'):
            lab.wait_for_broker_fencing(brokers, 'broker-6')
        self.assertEqual(execute.call_count, 3)
        self.assertEqual(execute.call_args.args[0], '1' * 64)

    def test_bad_profile_and_unknown_fault_are_rejected_before_docker(self):
        with patch.object(lab, 'docker') as docker:
            for name in ('../normal', 'normal;true', 'missing-profile'):
                with self.assertRaises(ValueError):
                    lab.profile(name)
            with self.assertRaises(ValueError):
                lab.fault('apply', 'RUN_SHELL', 'broker-4')
            with self.assertRaises(ValueError):
                lab.main(['profile', 'apply'])
            docker.assert_not_called()

    def test_restart_fault_observation_is_captured_before_start_and_health_wait(self):
        identifier = '4' * 64
        infos = {'broker-4': {'Id': identifier, 'State': {'Running': True}}}
        details = {'brokers': {'broker-4': {'containerId': identifier}}}
        calls, running = [], [False]
        def docker(*args, **kwargs):
            calls.append(args[0])
            if args[0] == 'start': running[0] = True
            if args[0] == 'inspect':
                return reply(json.dumps([{'State': {'Running': running[0], 'Health': {'Status': 'healthy'}}}]))
            return reply()
        timestamp = '2026-10-31T12:00:00.123456Z'
        def clock():
            calls.append('observed')
            return timestamp
        with patch.object(lab, 'containers', return_value=infos), patch.object(lab, 'metadata', return_value=details), \
                patch.object(lab, 'capabilities'), patch.object(lab, 'save_originals'), \
                patch.object(lab, 'docker', docker), patch.object(lab, 'observed_now', clock):
            result = lab.fault('apply', 'RESTART_BROKER', 'broker-4')
            self.assertEqual(result['faultObservedAt'], timestamp)
            self.assertLess(calls.index('observed'), calls.index('start'))

    def test_domain_observation_uses_first_confirmed_target_and_records_each_target(self):
        infos = {f'broker-{number}': {'Id': str(number) * 64, 'State': {'Running': True},
                  'Config': {'Env': ['BROKER_RACK=' + ('zone-a' if number in (1, 4) else 'zone-b')]}}
                 for number in (1, 2, 4)}
        details = {'brokers': {name: {'containerId': info['Id']} for name, info in infos.items()}}
        times = ['2026-10-31T12:00:00Z', '2026-10-31T12:00:02Z']
        with patch.object(lab, 'containers', return_value=infos), patch.object(lab, 'metadata', return_value=details), \
                patch.object(lab, 'capabilities'), patch.object(lab, 'save_originals'), patch.object(lab, 'set_network'), \
                patch.object(lab, 'execute', return_value=reply()), patch.object(lab, 'observed_now', side_effect=times):
            result = lab.fault('apply', 'ISOLATE_DOMAIN', 'zone-a')
            self.assertEqual(result['faultObservedAt'], times[0])
            self.assertEqual([item['observedAt'] for item in result['targetObservations']], times)

    def test_resource_fault_observation_follows_verified_profile_apply(self):
        infos = {'broker-4': {'Id': '4' * 64}}
        details = {'brokers': {'broker-4': {'containerId': '4' * 64}}}
        calls = []
        with patch.object(lab, 'containers', return_value=infos), patch.object(lab, 'metadata', return_value=details), \
                patch.object(lab, 'capabilities'), patch.object(lab, 'admission'), \
                patch.object(lab, 'apply_profile', side_effect=lambda *args: calls.append('apply-confirmed') or {}), \
                patch.object(lab, 'observed_now', side_effect=lambda: calls.append('observed') or '2026-10-31T12:00:00Z'):
            result = lab.fault('apply', 'LIMIT_NETWORK', 'broker-4')
            self.assertEqual(calls, ['apply-confirmed', 'observed'])
            self.assertEqual(result['faultObservedAt'], '2026-10-31T12:00:00Z')

    def test_dev_topology_does_not_start_old_final_members_and_pending_identity_is_exact(self):
        infos = [{'Id': str(index) * 64, 'Config': {'Labels': {
            'com.docker.compose.project': lab.PROJECT, 'com.docker.compose.service': f'broker-{index}'}}}
                 for index in range(1, 7)]
        def docker(*args, **kwargs):
            return reply(' '.join(info['Id'] for info in infos)) if args[0] == 'ps' else reply(json.dumps(infos))
        with patch.dict(lab.os.environ, {'LAB_BROKER_IDS': '1,2,3,4'}), patch.object(lab, 'docker', docker), \
                patch.object(lab, 'read_state', return_value={'brokers': {'broker-6': {'containerId': '6' * 64}}}):
            self.assertEqual(set(lab.containers()), {'broker-1', 'broker-2', 'broker-3', 'broker-4'})
            self.assertEqual(set(lab.containers(True)), {'broker-1', 'broker-2', 'broker-3', 'broker-4', 'broker-6'})
        with patch.dict(lab.os.environ, {'LAB_BROKER_IDS': '1,2,3,4'}), patch.object(lab, 'docker', docker), \
                patch.object(lab, 'read_state', return_value={'brokers': {'broker-6': {'containerId': 'a' * 64}}}):
            self.assertNotIn('broker-6', lab.containers(True))

    def test_cleanup_refuses_a_foreign_ingress_filter(self):
        with patch.object(lab, 'qdiscs', return_value=[{'kind': 'ingress', 'dev': 'eth0'}]), \
                patch.object(lab, 'owns_ingress', return_value=False), patch.object(lab, 'execute') as execute:
            with self.assertRaisesRegex(lab.Unsupported, 'Unmanaged ingress'):
                lab.clear_network('a' * 64)
            execute.assert_not_called()

    def test_ingress_uses_queued_ifb_shaping_instead_of_packet_policing(self):
        target = {'containerId': 'a' * 64, 'interfaces': ['eth0', 'eth1']}
        network = {'egressMbit': None, 'ingressMbit': 100, 'delayMs': 0, 'jitterMs': 0, 'lossPercent': 0}
        calls = []
        with patch.object(lab, 'clear_network'), \
                patch.object(lab, 'record_ifb_intent', side_effect=lambda container, interface: calls.append(('intent', interface))), \
                patch.object(lab, 'execute', side_effect=lambda container, *args, **kwargs: calls.append(args) or reply()):
            lab.set_network(target, network)
        for number in ('0', '1'):
            interface = 'recovery-ifb' + number
            self.assertLess(calls.index(('intent', interface)), calls.index(('ip', 'link', 'add', interface, 'type', 'ifb')))
            self.assertTrue(any(args[:3] == ('tc', 'class', 'replace') and interface in args for args in calls))
            self.assertTrue(any('mirred' in args and interface in args for args in calls))
        self.assertFalse(any('police' in args for args in calls))

    def test_cleanup_removes_only_journaled_ifb_after_ingress_redirect(self):
        entry = {'containerId': 'a' * 64, 'ownedIfb': ['recovery-ifb0']}
        state = {'brokers': {'broker-4': entry}}
        qdiscs = [{'kind': 'htb', 'root': True, 'dev': 'recovery-ifb0', 'handle': lab.ROOT_HANDLE},
                  {'kind': 'ingress', 'dev': 'eth0'}]
        with patch.object(lab, 'network_owner', return_value=entry), patch.object(lab, 'qdiscs', return_value=qdiscs), \
                patch.object(lab, 'owns_ingress', return_value=True), patch.object(lab, 'execute', return_value=reply()) as execute, \
                patch.object(lab, 'read_state', return_value=state), patch.object(lab, 'write_state') as write:
            lab.clear_network('a' * 64)
            self.assertEqual(execute.call_args_list[0].args[1:], ('tc', 'qdisc', 'del', 'dev', 'eth0', 'ingress'))
            self.assertEqual(execute.call_args_list[-1].args[1:], ('ip', 'link', 'del', 'dev', 'recovery-ifb0'))
            self.assertEqual(entry['ownedIfb'], [])
            write.assert_called_once()

    def test_read_only_private_cgroup_is_unsupported_without_writing_or_escalating(self):
        target = {'containerId': 'a' * 64, 'device': '8:80', 'devicePath': '/dev/sdf',
                  'privateCgroup': True, 'ioMaxWritable': False}
        with patch.object(lab, 'execute', return_value=reply()) as execute:
            with self.assertRaisesRegex(lab.Unsupported, 'not writable'):
                lab.set_disk(target, {'readMiBps': 4, 'writeMiBps': 2})
            self.assertEqual(execute.call_count, 1)
            self.assertEqual(execute.call_args.args[1:], ('cat', '/sys/fs/cgroup/io.max'))

    def test_normal_resolves_own_stopped_broker_from_journal_without_entering_dead_namespace(self):
        data, digest = lab.profile('normal')
        data['brokers'] = ['broker-2', 'broker-3']
        old = {'containerId': '2' * 64, 'device': '8:80', 'devicePath': '/dev/sdf',
               'interfaces': ['eth0'], 'ioMaxWritable': False, 'privateCgroup': True}
        infos = {'broker-2': {'Id': '2' * 64, 'State': {'Running': False}},
                 'broker-3': {'Id': '3' * 64, 'State': {'Running': True}, 'HostConfig': {'CgroupnsMode': 'private'}}}
        def execute(container, *args, **kwargs):
            self.assertEqual(container, '3' * 64)
            if args[0] == 'python3': return reply('{"device":"8:80","bootId":"boot"}')
            if args[0] == 'ip': return reply('[{"ifname":"eth0"}]')
            if args[0] == 'cat': return reply('DEVNAME=sdf\n')
            return reply(code=1)
        with patch.object(lab, 'read_state', return_value={'brokers': {'broker-2': old}}), \
                patch.object(lab, 'docker', return_value=reply('daemon')), patch.object(lab, 'execute', execute):
            details = lab.metadata(data, digest, infos)
            self.assertTrue(details['brokers']['broker-2']['deferred'])
            self.assertEqual(details['hostIdentity']['bootId'], 'boot')

    def test_journal_is_cleared_only_after_restored_resources_and_no_reassignments_are_verified(self):
        info = {'Id': '4' * 64, 'State': {'Running': True, 'Health': {'Status': 'healthy'}},
                'HostConfig': {'NanoCpus': 1000000000}}
        old = {'containerId': '4' * 64, 'device': '8:80', 'resources': {'NanoCpus': 1000000000}, 'ioMax': ''}
        with patch.object(lab, 'containers', return_value={'broker-4': info}), \
                patch.object(lab, 'read_state', return_value={'brokers': {'broker-4': old}}), \
                patch.object(lab, 'qdiscs', return_value=[]), patch.object(lab, 'execute') as execute, \
                patch.object(lab, 'write_state') as write:
            execute.side_effect = [reply('No partition reassignments found.'), reply()]
            lab.cleanup('reassignments')
            write.assert_called_once_with({'schemaVersion': 1, 'brokers': {}})
        with patch.object(lab, 'containers', return_value={'broker-4': info}), \
                patch.object(lab, 'read_state', return_value={'brokers': {'broker-4': old}}), \
                patch.object(lab, 'qdiscs', return_value=[{'handle': lab.ROOT_HANDLE}]), \
                patch.object(lab, 'execute', return_value=reply('No partition reassignments found.')), \
                patch.object(lab, 'write_state') as write:
            with self.assertRaises(RuntimeError):
                lab.cleanup('reassignments')
            write.assert_not_called()

    def test_cleanup_health_recovers_an_own_stop_that_finishes_after_start_phase(self):
        identifier = '4' * 64
        stopped = {'Id': identifier, 'State': {'Running': False}}
        healthy = {'Id': identifier, 'State': {'Running': True, 'Health': {'Status': 'healthy'}},
                   'HostConfig': {'NanoCpus': 1000000000}}
        old = {'containerId': identifier, 'device': '8:80', 'resources': {'NanoCpus': 1000000000}, 'ioMax': ''}
        state = {'brokers': {'broker-4': old}, 'cleanupRestartRequested': True}
        with patch.object(lab, 'containers', side_effect=[{'broker-4': stopped}, {'broker-4': stopped}, {'broker-4': healthy}]), \
                patch.object(lab, 'read_state', return_value=state), patch.object(lab, 'qdiscs', return_value=[]), \
                patch.object(lab, 'execute', return_value=reply()), patch.object(lab, 'docker') as docker, \
                patch.object(lab.time, 'sleep'):
            lab.cleanup('health')
            docker.assert_called_once_with('start', identifier)

    def test_writable_own_cgroup_must_confirm_actual_io_values(self):
        target = {'containerId': 'a' * 64, 'device': '8:80', 'devicePath': '/dev/sdf',
                  'privateCgroup': True, 'ioMaxWritable': True}
        with patch.object(lab, 'execute', side_effect=[reply(), reply(), reply('8:80 rbps=4194304 wbps=2097152\n')]) as execute:
            lab.set_disk(target, {'readMiBps': 4, 'writeMiBps': 2})
            self.assertIn('8:80 rbps=4194304 wbps=2097152', execute.call_args_list[1].args)
        with patch.object(lab, 'execute', side_effect=[reply(), reply(), reply()]):
            with self.assertRaisesRegex(lab.Unsupported, 'does not confirm'):
                lab.set_disk(target, {'readMiBps': 4, 'writeMiBps': 2})

    def test_calibration_admission_invalidates_changed_profiles_or_container_identity(self):
        data, digest = lab.profile('network-constrained')
        details = {'profile': data, 'profileSha256': digest, 'hostIdentity': {'bootId': 'boot'},
                   'brokers': {'broker-4': {'containerId': 'a' * 64, 'device': '8:80'}}}
        with tempfile.TemporaryDirectory() as temporary, patch.object(lab, 'ADMISSION_ROOT', Path(temporary)):
            with self.assertRaisesRegex(lab.Unsupported, 'missing'):
                lab.admission(details)
            path = Path(temporary) / data['name'] / 'admission.json'
            path.parent.mkdir()
            proof = {'schemaVersion': 1, 'profile': data['name'], **{key: details[key] for key in
                     ('profileSha256', 'hostIdentity', 'brokers')},
                     'success': True,
                     'network': {'success': True, 'matchedTarget': True, 'runtimeSeconds': 60, 'checks': [
                         {'broker': 'broker-4', 'direction': direction, 'targetMbit': 100, 'passed': True, 'runtimeSeconds': 60}
                         for direction in ('ingress', 'egress')]}}
            path.write_text(json.dumps(proof))
            lab.admission(details)
            proof['profileSha256'] = 'changed'
            path.write_text(json.dumps(proof))
            with self.assertRaisesRegex(lab.Unsupported, 'hash'):
                lab.admission(details)

    def test_success_flags_without_measured_target_coverage_do_not_admit_a_profile(self):
        data, digest = lab.profile('network-constrained')
        details = {'profile': data, 'profileSha256': digest, 'hostIdentity': {'bootId': 'boot'},
                   'brokers': {'broker-4': {'containerId': 'a' * 64, 'device': '8:80'}}}
        with tempfile.TemporaryDirectory() as temporary, patch.object(lab, 'ADMISSION_ROOT', Path(temporary)):
            path = Path(temporary) / data['name'] / 'admission.json'
            path.parent.mkdir()
            path.write_text(json.dumps({'schemaVersion': 1, 'profile': data['name'], 'success': True,
                **{key: details[key] for key in ('profileSha256', 'hostIdentity', 'brokers')},
                'network': {'success': True, 'matchedTarget': True, 'runtimeSeconds': 60, 'checks': []}}))
            with self.assertRaisesRegex(lab.Unsupported, 'coverage'):
                lab.admission(details)


if __name__ == '__main__':
    unittest.main()
