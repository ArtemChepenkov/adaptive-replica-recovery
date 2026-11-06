#!/usr/bin/env python3
"""Regression checks for calibration admission, without Docker or benchmark delays."""
import importlib.util
import io
import json
from pathlib import Path
import tempfile
import unittest
from unittest.mock import patch

MODULE = Path(__file__).resolve().parents[1] / 'scripts/lab-calibration.py'
spec = importlib.util.spec_from_file_location('lab_calibration', MODULE)
calibration = importlib.util.module_from_spec(spec)
spec.loader.exec_module(calibration)


class CalibrationAdmissionTest(unittest.TestCase):
    def measure(self, settings):
        details = {'brokers': {'broker-4': {'containerId': 'a' * 64}},
                   'profile': {'network': settings}}
        pings = [{'meanRttMs': 1, 'lossPercent': 0, 'raw': 'baseline'},
                 {'meanRttMs': 1 + settings['delayMs'], 'lossPercent': 0, 'raw': 'limited'}]
        with tempfile.TemporaryDirectory() as directory, \
                patch.object(calibration, 'resource', return_value={'details': details}), \
                patch.object(calibration, 'selected_brokers', return_value=['broker-4']), \
                patch.object(calibration, 'peer_for', return_value=('broker-3', 'b' * 64)), \
                patch.object(calibration, 'backbone_address', return_value='172.0.0.3'), \
                patch.object(calibration, 'ping', side_effect=pings), \
                patch.object(calibration, 'iperf', return_value={'meanMbit': 100, 'p95Mbit': 101, 'runtimeSeconds': 60}):
            return calibration.network_calibration('test-profile', Path(directory), 60)[1]

    def test_verified_bandwidth_and_mean_delay_can_pass(self):
        result = self.measure({'egressMbit': 100, 'ingressMbit': 100, 'delayMs': 10, 'jitterMs': 0, 'lossPercent': 0})
        self.assertTrue(result['success'])
        self.assertTrue(result['matchedTarget'])

    def test_loss_cannot_be_admitted_from_positive_throughput_alone(self):
        result = self.measure({'egressMbit': None, 'ingressMbit': None, 'delayMs': 0, 'jitterMs': 0, 'lossPercent': 1})
        self.assertFalse(result['success'])
        self.assertIn('UNVERIFIED_PACKET_LOSS_PROBABILITY', result['unverifiedConstraints'])

    def test_jitter_cannot_be_admitted_from_matching_mean_rtt(self):
        result = self.measure({'egressMbit': None, 'ingressMbit': None, 'delayMs': 10, 'jitterMs': 1, 'lossPercent': 0})
        self.assertFalse(result['matchedTarget'])
        self.assertIn('UNVERIFIED_JITTER_DISTRIBUTION', result['unverifiedConstraints'])

    def test_selected_brokers_must_be_configured_targets(self):
        with patch.dict(calibration.os.environ, {'CALIBRATION_BROKERS': 'broker-9'}):
            with self.assertRaises(ValueError):
                calibration.selected_brokers({'brokers': {'broker-4': {}}})

    def test_failed_cleanup_cannot_publish_a_successful_admission(self):
        details = {'brokers': {}, 'profile': {}}
        with tempfile.TemporaryDirectory() as directory, \
                patch.object(calibration, 'ROOT', Path(directory).resolve()), \
                patch.object(calibration.sys, 'argv', ['lab-calibration.py', 'network', 'test-profile', 'evidence']), \
                patch.object(calibration, 'network_calibration', return_value=(details, {'success': True, 'matchedTarget': True})), \
                patch.object(calibration, 'resource', side_effect=RuntimeError('cleanup failed')), \
                patch.object(calibration, 'proof', return_value=Path(directory) / 'proof.json') as publish, \
                patch.object(calibration.sys, 'stdout', io.StringIO()):
            self.assertEqual(calibration.main(), 1)
            self.assertFalse(publish.call_args.args[3]['success'])
            self.assertFalse(publish.call_args.args[3]['matchedTarget'])

    def test_unsupported_disk_is_rejected_before_creating_benchmark_file(self):
        details = {'brokers': {'broker-4': {'containerId': 'a' * 64}}, 'profile': {'disk': {}}}
        def resource(operation, profile):
            if operation == 'status':
                return {'details': details}
            raise RuntimeError('io.max is read-only')
        with tempfile.TemporaryDirectory() as directory, \
                patch.object(calibration, 'resource', side_effect=resource), \
                patch.object(calibration, 'selected_brokers', return_value=['broker-4']), \
                patch.object(calibration, 'fio_run') as benchmark:
            with self.assertRaisesRegex(RuntimeError, 'read-only'):
                calibration.disk_calibration('disk-constrained', Path(directory), 60)
            benchmark.assert_not_called()

    def kafka_instances(self):
        instances = []
        for number in range(1, 5):
            instances.append({'Id': str(number) * 64,
                'Config': {'Labels': {'com.docker.compose.project': calibration.PROJECT,
                                      'com.docker.compose.service': f'broker-{number}'},
                           'Env': [f'EXTERNAL_PORT={29091 + number}']},
                'State': {'Health': {'Status': 'healthy'}},
                'NetworkSettings': {'Ports': {'9094/tcp': [{'HostIp': '127.0.0.1', 'HostPort': str(29091 + number)}]}}})
        instances.append({'Id': 'a' * 64, 'Config': {'Labels': {
            'com.docker.compose.project': calibration.PROJECT, 'com.docker.compose.service': 'cruise-control'}},
            'NetworkSettings': {'Ports': {'9090/tcp': [{'HostIp': '127.0.0.1', 'HostPort': '29090'}]}}})
        return instances

    def test_kafka_preflight_resolves_owned_ports_instead_of_inherited_bootstrap(self):
        with patch.dict(calibration.os.environ, {'KAFKA_BOOTSTRAP_SERVERS': 'some-other-cluster:9092'}), \
                patch.object(calibration, 'run', side_effect=['1' * 12, json.dumps(self.kafka_instances()), 'No partition reassignments found']), \
                patch.object(calibration, 'http', return_value={'ExecutorState': {'state': 'NO_TASK_IN_PROGRESS'}}) as lookup:
            context = calibration.kafka_preflight()
            self.assertEqual(context['bootstrapServers'], '127.0.0.1:29092,127.0.0.1:29093,127.0.0.1:29094,127.0.0.1:29095')
            lookup.assert_called_once_with('http://127.0.0.1:29090/kafkacruisecontrol/state?json=true')

    def test_kafka_preflight_rejects_active_executor_or_reassignments(self):
        with patch.object(calibration, 'run', side_effect=['1' * 12, json.dumps(self.kafka_instances())]), \
                patch.object(calibration, 'http', return_value={'ExecutorState': {'state': 'INTER_BROKER_REPLICA_MOVEMENT_TASK_IN_PROGRESS'}}):
            with self.assertRaisesRegex(RuntimeError, 'idle Cruise Control'):
                calibration.kafka_preflight()
        with patch.object(calibration, 'run', side_effect=['1' * 12, json.dumps(self.kafka_instances()), 'Ongoing partition reassignments']), \
                patch.object(calibration, 'http', return_value={'ExecutorState': {'state': 'NO_TASK_IN_PROGRESS'}}):
            with self.assertRaisesRegex(RuntimeError, 'absence of partition reassignments'):
                calibration.kafka_preflight()

    def test_failed_kafka_preflight_does_not_change_or_cleanup_resources(self):
        with tempfile.TemporaryDirectory() as directory, \
                patch.object(calibration, 'ROOT', Path(directory).resolve()), \
                patch.object(calibration.sys, 'argv', ['lab-calibration.py', 'kafka', 'evidence']), \
                patch.object(calibration, 'kafka_preflight', side_effect=RuntimeError('Executor busy')), \
                patch.object(calibration, 'resource') as resources, \
                patch.object(calibration.sys, 'stdout', io.StringIO()):
            self.assertEqual(calibration.main(), 1)
            resources.assert_not_called()


if __name__ == '__main__':
    unittest.main()
