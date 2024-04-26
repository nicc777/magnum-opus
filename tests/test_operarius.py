import sys
import os
import hashlib
from inspect import stack

sys.path.append(os.path.dirname(os.path.realpath(__file__)) + "/../src")
print('sys.path={}'.format(sys.path))

import unittest

from magnum_opus.operarius import *

running_path = os.getcwd()
print('Current Working Path: {}'.format(running_path))


class TestLogger:   # pragma: no cover

    def __init__(self):
        super().__init__()
        self.info_lines = list()
        self.warn_lines = list()
        self.debug_lines = list()
        self.critical_lines = list()
        self.error_lines = list()
        self.all_lines_in_sequence = list()

    def info(self, message: str):
        self.info_lines.append('[LOG] INFO: {}'.format(message))
        self.all_lines_in_sequence.append(
            copy.deepcopy(self.info_lines[-1])
        )

    def warn(self, message: str):
        self.warn_lines.append('[LOG] WARNING: {}'.format(message))
        self.all_lines_in_sequence.append(
            copy.deepcopy(self.warn_lines[-1])
        )

    def warning(self, message: str):
        self.warn_lines.append('[LOG] WARNING: {}'.format(message))
        self.all_lines_in_sequence.append(
            copy.deepcopy(self.warn_lines[-1])
        )

    def debug(self, message: str):
        self.debug_lines.append('[LOG] DEBUG: {}'.format(message))
        self.all_lines_in_sequence.append(
            copy.deepcopy(self.debug_lines[-1])
        )

    def critical(self, message: str):
        self.critical_lines.append('[LOG] CRITICAL: {}'.format(message))
        self.all_lines_in_sequence.append(
            copy.deepcopy(self.critical_lines[-1])
        )

    def error(self, message: str):
        self.error_lines.append('[LOG] ERROR: {}'.format(message))
        self.all_lines_in_sequence.append(
            copy.deepcopy(self.error_lines[-1])
        )

    def reset(self):
        self.info_lines = None
        self.warn_lines = None
        self.debug_lines = None
        self.critical_lines = None
        self.error_lines = None
        self.all_lines_in_sequence = None
        self.info_lines = list()
        self.warn_lines = list()
        self.debug_lines = list()
        self.critical_lines = list()
        self.error_lines = list()
        self.all_lines_in_sequence = list()
        print('*** LOGGER RESET DONE ***')


def print_logger_lines(logger:TestLogger):  # pragma: no cover
    print('\n\n-------------------------------------------------------------------------------')
    print('\t\tLOG DUMP')
    print('\t\t-------------------')
    for line in logger.all_lines_in_sequence:
        print(line)
    print('\n_______________________________________________________________________________')


def dump_variable_store(test_class_name: str, test_method_name: str, variable_store: VariableStore):
    try:
        print('\n\n-------------------------------------------------------------------------------')
        print('\t\tVARIABLE STORE DUMP')
        print('\t\t-------------------')
        print('\t\tTest Class  : {}'.format(test_class_name))
        print('\t\tTest Method : {}'.format(test_method_name))
        print()

        # First get the max key length:
        max_key_len = 0
        for key,val in variable_store.variable_store.items():
            if len(key) > max_key_len:
                max_key_len = len(key)

        for key,val in variable_store.variable_store.items():
            final_key = '{}'.format(key)
            spaces_qty = max_key_len - len(final_key) + 1
            spaces = ' '*spaces_qty
            final_key = '{}{}: '.format(final_key, spaces)
            print('{}{}\n'.format(final_key, val))

        print('\n_______________________________________________________________________________')
    except:
        pass


def dump_events(task_id: str, variable_store: VariableStore):   # pragma: no cover
    print('\n\n-------------------------------------------------------------------------------')
    print('\t\tEVENTS for task  : {}'.format(task_id))
    print()
    event_key = '{}:PROCESSING_EVENTS'.format(task_id)
    if event_key in variable_store.variable_store:
        if variable_store.variable_store[event_key] is not None:
            if isinstance(variable_store.variable_store[event_key], list):
                for event in variable_store.variable_store[event_key]:
                    print(json.dumps(event, default=str))
    print('\n_______________________________________________________________________________')


def dump_state(task: Task, persistence: StatePersistence):   # pragma: no cover
    print('\n\n-------------------------------------------------------------------------------')
    print('\t\tSTATE for task  : {}'.format(task.task_id))
    print()
    try:
        current_state = persistence.get(object_identifier='{}:TASK_STATE'.format(task.task_id))
        print('{}'.format(json.dumps(current_state, default=str, indent=4)))
    except:
        print('No state available.')
    print('\n_______________________________________________________________________________')


test_logger = TestLogger()
logger = test_logger
override_logger(logger_class=test_logger)


class DummyTaskProcessor1(TaskProcessor):

    def __init__(self, api_version: str='DummyTaskProcessor1/v1') -> None:
        super().__init__(api_version)

    def create_action(
        self,
        task: Task,
        persistence: StatePersistence=StatePersistence(),
        variable_store: VariableStore=VariableStore(),
        task_resolved_spec: dict=dict()
    )->VariableStore:
        updated_variable_store = VariableStore()
        updated_variable_store.variable_store = copy.deepcopy(variable_store.variable_store)

        if self.create_identifier(task=task, variable_name='UNITTEST_TROW_EXCEPTION') in updated_variable_store.variable_store:
            if updated_variable_store.get_variable(variable_name=self.create_identifier(task=task, variable_name='UNITTEST_TROW_EXCEPTION')) is True:
                raise Exception('CreateAction Failed!')

        updated_variable_store.add_variable(
            variable_name=self.create_identifier(task=task, variable_name='TASK_ORIGINAL_SPEC_CHECKSUM'),
            value=hashlib.sha256(json.dumps(task.spec, default=str).encode('utf-8')).hexdigest()
        )
        updated_variable_store.add_variable(
            variable_name=self.create_identifier(task=task, variable_name='TASK_RESOLVED_SPEC_CHECKSUM'),
            value=hashlib.sha256(json.dumps(task_resolved_spec, default=str).encode('utf-8')).hexdigest()
        )

        if self.create_identifier(task=task, variable_name='PROCESSING_EVENTS') not in variable_store.variable_store:
            updated_variable_store.add_variable(
                variable_name=self.create_identifier(task=task, variable_name='PROCESSING_EVENTS'),
                value=list()
            )

        if self.create_identifier(task=task, variable_name='UNITTEST_FORCE_PROCESSING_EXCEPTION') in variable_store.variable_store:
            if variable_store.variable_store[self.create_identifier(task=task, variable_name='UNITTEST_FORCE_PROCESSING_EXCEPTION')] is not None:
                if isinstance(variable_store.variable_store[self.create_identifier(task=task, variable_name='UNITTEST_FORCE_PROCESSING_EXCEPTION')], bool):
                    if variable_store.variable_store[self.create_identifier(task=task, variable_name='UNITTEST_FORCE_PROCESSING_EXCEPTION')] is True:
                        raise Exception('Exception Forced By Unit Test Configuration')
        
        updated_variable_store.add_variable(
            variable_name=self.create_identifier(task=task, variable_name='TASK_STATE_UPDATES'),
            value={
                'resource_checksum': hashlib.sha256('CreateAction for UnitTest Completed'.encode('utf-8')).hexdigest(),
                'resolved_spec_applied': copy.deepcopy(task_resolved_spec),
                'state_changed': True,
                'is_created': True,
                'create_timestamp': int(datetime.now(timezone.utc).timestamp()),
                'raw_spec': copy.deepcopy(task.spec),
                'metadata': copy.deepcopy(task.metadata),
            }
        )

        return updated_variable_store
    
    def rollback_action(
        self,
        task: Task,
        persistence: StatePersistence=StatePersistence(),
        variable_store: VariableStore=VariableStore(),
        task_resolved_spec: dict=dict()
    )->VariableStore:
        updated_variable_store = VariableStore()
        updated_variable_store.variable_store = copy.deepcopy(variable_store.variable_store)
        if self.create_identifier(task=task, variable_name='TASK_ORIGINAL_SPEC_CHECKSUM') in updated_variable_store.variable_store:
            updated_variable_store = self.delete_action(
                task=task,
                persistence=persistence,
                variable_store=copy.deepcopy(updated_variable_store),
                task_resolved_spec=task_resolved_spec
            )
        else:
            if '__GLOBAL__:PROCESS_TASK_EXCEPTION_RAISED_FOR_ACTION' in updated_variable_store.variable_store:
                if updated_variable_store.get_variable(variable_name='__GLOBAL__:PROCESS_TASK_EXCEPTION_RAISED_FOR_ACTION') == 'CreateAction':
                    return updated_variable_store 
            updated_variable_store = self.create_action(
                task=task,
                persistence=persistence,
                variable_store=copy.deepcopy(updated_variable_store),
                task_resolved_spec=task_resolved_spec
            )
        return updated_variable_store
    
    def delete_action(
        self,
        task: Task,
        persistence: StatePersistence=StatePersistence(),
        variable_store: VariableStore=VariableStore(),
        task_resolved_spec: dict=dict()
    )->VariableStore:
        updated_variable_store = VariableStore()
        updated_variable_store.variable_store = copy.deepcopy(variable_store.variable_store)

        if self.create_identifier(task=task, variable_name='UNITTEST_TROW_EXCEPTION') in updated_variable_store.variable_store:
            if updated_variable_store.get_variable(variable_name=self.create_identifier(task=task, variable_name='UNITTEST_TROW_EXCEPTION')) is True:
                raise Exception('DeleteAction Failed!')
        
        if self.create_identifier(task=task, variable_name='TASK_ORIGINAL_SPEC_CHECKSUM') in updated_variable_store.variable_store:
            updated_variable_store.variable_store.pop(self.create_identifier(task=task, variable_name='TASK_ORIGINAL_SPEC_CHECKSUM'))
        if self.create_identifier(task=task, variable_name='TASK_RESOLVED_SPEC_CHECKSUM') in updated_variable_store.variable_store:
            updated_variable_store.variable_store.pop(self.create_identifier(task=task, variable_name='TASK_RESOLVED_SPEC_CHECKSUM'))
        
        updated_variable_store.add_variable(
            variable_name=self.create_identifier(task=task, variable_name='TASK_STATE_UPDATES'),
            value={
                'resource_checksum': None,
                'resolved_spec_applied': copy.deepcopy(task_resolved_spec),
                'state_changed': True,
                'is_created': False,
                'create_timestamp': 0,
                'raw_spec': copy.deepcopy(task.spec),
                'metadata': copy.deepcopy(task.metadata),
            }
        )

        return updated_variable_store
    
    def update_action(
        self,
        task: Task,
        persistence: StatePersistence=StatePersistence(),
        variable_store: VariableStore=VariableStore(),
        task_resolved_spec: dict=dict()
    )->VariableStore:
        updated_variable_store = VariableStore()
        updated_variable_store.variable_store = copy.deepcopy(variable_store.variable_store)

        if self.create_identifier(task=task, variable_name='UNITTEST_TROW_EXCEPTION') in updated_variable_store.variable_store:
            if updated_variable_store.get_variable(variable_name=self.create_identifier(task=task, variable_name='UNITTEST_TROW_EXCEPTION')) is True:
                raise Exception('DeleteAction Failed!')
        
        updated_variable_store.add_variable(
            variable_name=self.create_identifier(task=task, variable_name='TASK_STATE_UPDATES'),
            value={
                'resource_checksum': hashlib.sha256('UpdateAction for UnitTest Completed'.encode('utf-8')).hexdigest(),
                'resolved_spec_applied': copy.deepcopy(task_resolved_spec),
                'state_changed': True,
                'is_created': True,
                'create_timestamp': int(datetime.now(timezone.utc).timestamp()),
                'raw_spec': copy.deepcopy(task.spec),
                'metadata': copy.deepcopy(task.metadata),
            }
        )

        return updated_variable_store
    
    def describe_action(
        self,
        task: Task,
        persistence: StatePersistence=StatePersistence(),
        variable_store: VariableStore=VariableStore(),
        task_resolved_spec: dict=dict()
    )->VariableStore:
        updated_variable_store = VariableStore()
        updated_variable_store.variable_store = copy.deepcopy(variable_store.variable_store)

        if self.create_identifier(task=task, variable_name='UNITTEST_TROW_EXCEPTION') in updated_variable_store.variable_store:
            if updated_variable_store.get_variable(variable_name=self.create_identifier(task=task, variable_name='UNITTEST_TROW_EXCEPTION')) is True:
                raise Exception('UpdateAction Failed!')
        
        resource_checksum = hashlib.sha256('test_resource'.encode('utf-8')).hexdigest()
        if 'ResourceData:{}'.format(task.task_id) in variable_store.variable_store:
            resource_checksum = hashlib.sha256(
                variable_store.variable_store['ResourceData:{}'.format(task.task_id)].encode('utf-8')
            ).hexdigest()
        updated_variable_store.add_variable(
            variable_name=self.create_identifier(task=task, variable_name='TASK_DESCRIPTION_RAW'),
            value=copy.deepcopy(
                task.state.to_dict(
                    human_readable=False,
                    current_resolved_spec=task_resolved_spec,
                    current_resource_checksum=resource_checksum,
                    with_checksums=True,
                    include_applied_spec=True
                )
            )
        )
        updated_variable_store.add_variable(
            variable_name=self.create_identifier(task=task, variable_name='TASK_DESCRIPTION_HUMAN_READABLE_SUMMARY'),
            value=copy.deepcopy(
                task.state.to_dict(
                    human_readable=True,
                    current_resolved_spec=task_resolved_spec,
                    current_resource_checksum=resource_checksum,
                    with_checksums=False,
                    include_applied_spec=False
                )
            )
        )
        updated_variable_store.add_variable(
            variable_name=self.create_identifier(task=task, variable_name='TASK_DESCRIPTION_HUMAN_READABLE_EXTENDED'),
            value=copy.deepcopy(
                task.state.to_dict(
                    human_readable=True,
                    current_resolved_spec=task_resolved_spec,
                    current_resource_checksum=resource_checksum,
                    with_checksums=True,
                    include_applied_spec=False
                )
            )
        )
        return updated_variable_store
    
    def detect_drift_action(
        self,
        task: Task,
        persistence: StatePersistence=StatePersistence(),
        variable_store: VariableStore=VariableStore(),
        task_resolved_spec: dict=dict()
    )->VariableStore:
        updated_variable_store = VariableStore()
        updated_variable_store.variable_store = copy.deepcopy(variable_store.variable_store)

        if self.create_identifier(task=task, variable_name='UNITTEST_TROW_EXCEPTION') in updated_variable_store.variable_store:
            if updated_variable_store.get_variable(variable_name=self.create_identifier(task=task, variable_name='UNITTEST_TROW_EXCEPTION')) is True:
                raise Exception('DetectDriftAction Failed!')
        
        resource_checksum = hashlib.sha256('test_resource'.encode('utf-8')).hexdigest()
        if 'ResourceData:{}'.format(task.task_id) in variable_store.variable_store:
            resource_checksum = hashlib.sha256(
                variable_store.variable_store['ResourceData:{}'.format(task.task_id)].encode('utf-8')
            ).hexdigest()

        current_task_state = task.state.to_dict(
            human_readable=False,
            current_resolved_spec=task_resolved_spec,
            current_resource_checksum=resource_checksum,
            with_checksums=True,
            include_applied_spec=True
        )
        spec_drifted = False
        resource_drifted = False
        if 'IsCreated' in current_task_state:
            if isinstance(current_task_state['IsCreated'], bool):
                if current_task_state['IsCreated'] is True:
                    if 'SpecDrifted' in current_task_state and 'ResourceDrifted' in current_task_state:
                        if isinstance(current_task_state['SpecDrifted'], bool):
                            spec_drifted = current_task_state['SpecDrifted']
                        if isinstance(current_task_state['ResourceDrifted'], bool):
                            resource_drifted = current_task_state['ResourceDrifted']

        updated_variable_store.add_variable(
            variable_name=self.create_identifier(task=task, variable_name='SPEC_DRIFTED'),
            value=spec_drifted
        )
        updated_variable_store.add_variable(
            variable_name=self.create_identifier(task=task, variable_name='RESOURCE_DRIFTED'),
            value=resource_drifted
        )

        return updated_variable_store


class TestDummyTaskProcessor1(unittest.TestCase):    # pragma: no cover

    def setUp(self):
        print()
        print('-'*80)
        self.task = Task(
            api_version='DummyTaskProcessor1/v1',
            kind='DummyTaskProcessor1',
            metadata={'name': 'test-task'},
            spec={'testField': 'testValue'}
        )

    def test_create_action_01(self):
        p = DummyTaskProcessor1()
        t = copy.deepcopy(self.task)
        variable_store = p.process_task(
            task=t,
            variable_store=VariableStore(),
            action='CreateAction',
            task_resolved_spec=copy.deepcopy(t.spec)
        )

        print_logger_lines(logger=logger)
        dump_variable_store(
            test_class_name=self.__class__.__name__,
            test_method_name=stack()[0][3],
            variable_store=copy.deepcopy(variable_store)
        )
        dump_events(
            task_id=t.task_id,
            variable_store=copy.deepcopy(variable_store)
        )

        self.assertIsNotNone(variable_store)
        self.assertIsInstance(variable_store, VariableStore)
        self.assertTrue('test-task:TASK_ORIGINAL_SPEC_CHECKSUM' in variable_store.variable_store)
        self.assertTrue('test-task:TASK_RESOLVED_SPEC_CHECKSUM' in variable_store.variable_store)
        self.assertEqual(
            variable_store.variable_store['test-task:TASK_ORIGINAL_SPEC_CHECKSUM'],
            variable_store.variable_store['test-task:TASK_RESOLVED_SPEC_CHECKSUM']
        )

    def test_describe_action_01(self):
        p = DummyTaskProcessor1()
        t = copy.deepcopy(self.task)
        variable_store = VariableStore()
        resolved_spec = copy.deepcopy(self.task.spec)
        if 'ResolvedSpec:{}'.format(self.task.task_id) in variable_store.variable_store:
            resolved_spec = copy.deepcopy(variable_store.variable_store['ResolvedSpec:{}'.format(self.task.task_id)])
        variable_store = p.process_task(
            task=t,
            variable_store=copy.deepcopy(variable_store),
            action='DescribeAction',
            task_resolved_spec=resolved_spec
        )

        print_logger_lines(logger=logger)
        dump_variable_store(
            test_class_name=self.__class__.__name__,
            test_method_name=stack()[0][3],
            variable_store=variable_store
        )
        dump_events(
            task_id=t.task_id,
            variable_store=copy.deepcopy(variable_store)
        )

        self.assertIsNotNone(variable_store)
        self.assertIsInstance(variable_store, VariableStore)
        self.assertTrue('test-task:TASK_DESCRIPTION_RAW' in variable_store.variable_store)
        self.assertTrue('test-task:TASK_DESCRIPTION_HUMAN_READABLE_SUMMARY' in variable_store.variable_store)
        self.assertTrue('test-task:TASK_DESCRIPTION_HUMAN_READABLE_EXTENDED' in variable_store.variable_store)

    def test_drift_action_no_drift_detected_01(self):
        
        p = DummyTaskProcessor1()
        t = copy.deepcopy(self.task)
        t.state = TaskState(
            report_label=copy.deepcopy(t.task_id),
            manifest_spec=copy.deepcopy(t.spec),
            manifest_metadata=copy.deepcopy(t.metadata),
            applied_spec=copy.deepcopy(t.spec),
            resolved_spec=copy.deepcopy(t.spec),
            created_timestamp=1000, 
            applied_resources_checksum=hashlib.sha256('test_resource'.encode('utf-8')).hexdigest()
        )
        variable_store = VariableStore()
        variable_store.add_variable(
            variable_name='ResourceData:{}'.format(t.task_id),
            value='test_resource'
        )
        resolved_spec = copy.deepcopy(self.task.spec)
        if 'ResolvedSpec:{}'.format(self.task.task_id) in variable_store.variable_store:
            resolved_spec = copy.deepcopy(variable_store.variable_store['ResolvedSpec:{}'.format(self.task.task_id)])
        variable_store = p.process_task(
            task=t,
            variable_store=copy.deepcopy(variable_store),
            action='DetectDriftAction',
            task_resolved_spec=resolved_spec
        )

        print_logger_lines(logger=logger)
        dump_variable_store(
            test_class_name=self.__class__.__name__,
            test_method_name=stack()[0][3],
            variable_store=variable_store
        )
        dump_events(
            task_id=t.task_id,
            variable_store=copy.deepcopy(variable_store)
        )

        self.assertIsNotNone(variable_store)
        self.assertIsInstance(variable_store, VariableStore)
        self.assertTrue('test-task:SPEC_DRIFTED' in variable_store.variable_store)
        self.assertTrue('test-task:RESOURCE_DRIFTED' in variable_store.variable_store)
        self.assertIsInstance(variable_store.variable_store['test-task:SPEC_DRIFTED'], bool)
        self.assertIsInstance(variable_store.variable_store['test-task:RESOURCE_DRIFTED'], bool)
        self.assertFalse(variable_store.variable_store['test-task:SPEC_DRIFTED'])
        self.assertFalse(variable_store.variable_store['test-task:RESOURCE_DRIFTED'])


    def test_drift_action_only_resource_drift_detected_01(self):
        
        p = DummyTaskProcessor1()
        t = copy.deepcopy(self.task)
        t.state = TaskState(
            report_label=copy.deepcopy(t.task_id),
            manifest_spec=copy.deepcopy(t.spec),
            manifest_metadata=copy.deepcopy(t.metadata),
            applied_spec=copy.deepcopy(t.spec),
            resolved_spec=copy.deepcopy(t.spec),
            created_timestamp=1000, 
            applied_resources_checksum=hashlib.sha256('test_resource_original'.encode('utf-8')).hexdigest()
        )
        variable_store = VariableStore()
        variable_store.add_variable(
            variable_name='ResourceData:{}'.format(t.task_id),
            value='test_resource'
        )
        resolved_spec = copy.deepcopy(self.task.spec)
        if 'ResolvedSpec:{}'.format(self.task.task_id) in variable_store.variable_store:
            resolved_spec = copy.deepcopy(variable_store.variable_store['ResolvedSpec:{}'.format(self.task.task_id)])
        variable_store = p.process_task(
            task=t,
            variable_store=copy.deepcopy(variable_store),
            action='DetectDriftAction',
            task_resolved_spec=resolved_spec
        )

        print_logger_lines(logger=logger)
        dump_variable_store(
            test_class_name=self.__class__.__name__,
            test_method_name=stack()[0][3],
            variable_store=variable_store
        )
        dump_events(
            task_id=t.task_id,
            variable_store=copy.deepcopy(variable_store)
        )

        self.assertIsNotNone(variable_store)
        self.assertIsInstance(variable_store, VariableStore)
        self.assertTrue('test-task:SPEC_DRIFTED' in variable_store.variable_store)
        self.assertTrue('test-task:RESOURCE_DRIFTED' in variable_store.variable_store)
        self.assertIsInstance(variable_store.variable_store['test-task:SPEC_DRIFTED'], bool)
        self.assertIsInstance(variable_store.variable_store['test-task:RESOURCE_DRIFTED'], bool)
        self.assertFalse(variable_store.variable_store['test-task:SPEC_DRIFTED'])
        self.assertTrue(variable_store.variable_store['test-task:RESOURCE_DRIFTED'])

    def test_drift_action_only_spec_drift_detected_01(self):
        
        p = DummyTaskProcessor1()
        t = copy.deepcopy(self.task)
        t.state = TaskState(
            report_label=copy.deepcopy(t.task_id),
            manifest_spec=copy.deepcopy(t.spec),
            manifest_metadata=copy.deepcopy(t.metadata),
            applied_spec={'originalField': 'originalValue'},
            resolved_spec=copy.deepcopy(t.spec),
            created_timestamp=1000, 
            applied_resources_checksum=hashlib.sha256('test_resource'.encode('utf-8')).hexdigest()
        )
        variable_store = VariableStore()
        variable_store.add_variable(
            variable_name='ResourceData:{}'.format(t.task_id),
            value='test_resource'
        )
        resolved_spec = copy.deepcopy(self.task.spec)
        if 'ResolvedSpec:{}'.format(self.task.task_id) in variable_store.variable_store:
            resolved_spec = copy.deepcopy(variable_store.variable_store['ResolvedSpec:{}'.format(self.task.task_id)])
        variable_store = p.process_task(
            task=t,
            variable_store=copy.deepcopy(variable_store),
            action='DetectDriftAction',
            task_resolved_spec=resolved_spec
        )

        print_logger_lines(logger=logger)
        dump_variable_store(
            test_class_name=self.__class__.__name__,
            test_method_name=stack()[0][3],
            variable_store=variable_store
        )
        dump_events(
            task_id=t.task_id,
            variable_store=copy.deepcopy(variable_store)
        )

        self.assertIsNotNone(variable_store)
        self.assertIsInstance(variable_store, VariableStore)
        self.assertTrue('test-task:SPEC_DRIFTED' in variable_store.variable_store)
        self.assertTrue('test-task:RESOURCE_DRIFTED' in variable_store.variable_store)
        self.assertIsInstance(variable_store.variable_store['test-task:SPEC_DRIFTED'], bool)
        self.assertIsInstance(variable_store.variable_store['test-task:RESOURCE_DRIFTED'], bool)
        self.assertTrue(variable_store.variable_store['test-task:SPEC_DRIFTED'])
        self.assertFalse(variable_store.variable_store['test-task:RESOURCE_DRIFTED'])

    def test_drift_action_resource_and_spec_drift_detected_01(self):
        
        p = DummyTaskProcessor1()
        t = copy.deepcopy(self.task)
        t.state = TaskState(
            report_label=copy.deepcopy(t.task_id),
            manifest_spec=copy.deepcopy(t.spec),
            manifest_metadata=copy.deepcopy(t.metadata),
            applied_spec={'originalField': 'originalValue'},
            resolved_spec=copy.deepcopy(t.spec),
            created_timestamp=1000, 
            applied_resources_checksum=hashlib.sha256('test_resource_original'.encode('utf-8')).hexdigest()
        )
        variable_store = VariableStore()
        variable_store.add_variable(
            variable_name='ResourceData:{}'.format(t.task_id),
            value='test_resource'
        )
        resolved_spec = copy.deepcopy(self.task.spec)
        if 'ResolvedSpec:{}'.format(self.task.task_id) in variable_store.variable_store:
            resolved_spec = copy.deepcopy(variable_store.variable_store['ResolvedSpec:{}'.format(self.task.task_id)])
        variable_store = p.process_task(
            task=t,
            variable_store=copy.deepcopy(variable_store),
            action='DetectDriftAction',
            task_resolved_spec=resolved_spec
        )

        print_logger_lines(logger=logger)
        dump_variable_store(
            test_class_name=self.__class__.__name__,
            test_method_name=stack()[0][3],
            variable_store=variable_store
        )
        dump_events(
            task_id=t.task_id,
            variable_store=copy.deepcopy(variable_store)
        )

        self.assertIsNotNone(variable_store)
        self.assertIsInstance(variable_store, VariableStore)
        self.assertTrue('test-task:SPEC_DRIFTED' in variable_store.variable_store)
        self.assertTrue('test-task:RESOURCE_DRIFTED' in variable_store.variable_store)
        self.assertIsInstance(variable_store.variable_store['test-task:SPEC_DRIFTED'], bool)
        self.assertIsInstance(variable_store.variable_store['test-task:RESOURCE_DRIFTED'], bool)
        self.assertTrue(variable_store.variable_store['test-task:SPEC_DRIFTED'])
        self.assertTrue(variable_store.variable_store['test-task:RESOURCE_DRIFTED'])


class TestTaskProcessStore(unittest.TestCase):    # pragma: no cover

    def setUp(self):
        print()
        print('-'*80)
        self.task = Task(
            api_version='DummyTaskProcessor1/v1',
            kind='DummyTaskProcessor1',
            metadata={'name': 'test-task'},
            spec={'testField': 'testValue'}
        )

    def test_basic_01(self):
        task_processor_store = TaskProcessStore()
        task_processor_store.register_task_processor(task_processor=DummyTaskProcessor1())
        p = task_processor_store.get_task_processor_for_task(task=self.task)
        self.assertIsNotNone(p)
        self.assertIsInstance(p, DummyTaskProcessor1)

    def test_basic_02(self):
        task_processor_store = TaskProcessStore()
        task_processor_store.register_task_processor(task_processor=DummyTaskProcessor1())
        p = task_processor_store.get_task_processor(api_version='DummyTaskProcessor1/v1')
        self.assertIsNotNone(p)
        self.assertIsInstance(p, DummyTaskProcessor1)

    def test_basic_03(self):
        task_processor_store = TaskProcessStore()
        task_processor_store.register_task_processor(task_processor=DummyTaskProcessor1())
        with self.assertRaises(Exception):
            task_processor_store.get_task_processor(api_version='NoneExisting')

    def test_basic_04(self):
        task_processor_store = TaskProcessStore()
        task_processor_store.register_task_processor(task_processor=DummyTaskProcessor1())
        with self.assertRaises(Exception):
            task_processor_store.get_task_processor_for_task(
                task=Task(
                    api_version='NoneExisting',
                    kind='DummyKind',
                    metadata=dict(),
                    spec={
                        'key': 'value'
                    }
                )
            )


class TestTasks(unittest.TestCase):    # pragma: no cover

    def setUp(self):
        print()
        print('-'*80)
        self.task_01 = Task(
            api_version='DummyTaskProcessor1/v1',
            kind='DummyTaskProcessor1',
            metadata={'name': 'test-task-01'},
            spec={'testField': 'testValue'}
        )
        self.task_02 = Task(
            api_version='DummyTaskProcessor1/v1',
            kind='DummyTaskProcessor1',
            metadata={'name': 'test-task-02'},
            spec={'testField': 'testValue'}
        )
        self.task_03 = Task(
            api_version='DummyTaskProcessor1/v1',
            kind='DummyTaskProcessor1',
            metadata={'name': 'test-task-03'},
            spec={'testField': 'testValue'}
        )
        self.task_04 = Task(
            api_version='DummyTaskProcessor1/v1',
            kind='DummyTaskProcessor1',
            metadata={'name': 'test-task-04'},
            spec={'testField': 'testValue'}
        )
        logger.reset()

    def tearDown(self):
        self.task_01 = None
        self.task_02 = None
        return super().tearDown()

    def test_basic_task_dependency_01(self):
        # setup most basic dependency
        self.task_01.metadata['processingScope'] = [
            {
                'commands': ['command1', 'command2',],
                'contexts': ['con1','con2'],
            },
        ]
        self.task_02.metadata['dependencies'] = [
            {
                'tasks': ['test-task-01',],
            }
        ]
        tasks = Tasks()
        tasks.add_task(task=copy.deepcopy(self.task_02))
        tasks.add_task(task=copy.deepcopy(self.task_01))
        result = tasks.get_task_names_in_order(command='command1', context='con1')

        print_logger_lines(logger=logger)

        self.assertIsNotNone(result)
        self.assertIsInstance(result, list)
        self.assertEqual(len(result), 2)
        self.assertEqual(result[0], 'test-task-01')
        self.assertEqual(result[1], 'test-task-02')

    def test_basic_task_dependency_with_command_and_context_01(self):
        # setup most basic dependency
        self.task_01.metadata['processingScope'] = [
            {
                'commands': ['command1', 'command2',],
                'contexts': ['con1','con2'],
            },
        ]
        self.task_02.metadata['dependencies'] = [
            {
                'tasks': ['test-task-01',],
                'commands': ['command1', 'command2',],
                'contexts': ['con1','con2',],
            }
        ]
        self.task_03.metadata['dependencies'] = [
            {
                'tasks': ['test-task-01',],
                'commands': ['command2', 'command3',],
                'contexts': ['con2','con3'],
            }
        ]
        self.task_03.metadata['processingScopes'] = [
            {
                'commands': ['command2', 'command3',],
                'contexts': ['con2','con3'],
            },
        ]
        tasks = Tasks()
        tasks.add_task(task=copy.deepcopy(self.task_02))
        tasks.add_task(task=copy.deepcopy(self.task_03))
        tasks.add_task(task=copy.deepcopy(self.task_01))
        result = tasks.get_task_names_in_order(command='command2', context='con2')

        print_logger_lines(logger=logger)

        self.assertIsNotNone(result)
        self.assertIsInstance(result, list)
        self.assertEqual(len(result), 3)
        self.assertEqual(result[0], 'test-task-01')
        self.assertTrue('test-task-02' in result)
        self.assertTrue('test-task-03' in result)

    def test_basic_task_dependency_with_command_and_context_02(self):
        # setup most basic dependency
        self.task_01.metadata['processingScope'] = [
            {
                'commands': ['command1', 'command2',],
                'contexts': ['con1','con2'],
            },
        ]
        self.task_02.metadata['dependencies'] = [
            {
                'tasks': ['test-task-01',],
                'commands': ['command1', 'command2',],
                'contexts': ['con1','con2',],
            },
        ]
        self.task_03.metadata['dependencies'] = [
            {
                'tasks': ['test-task-01',],
                'commands': ['command2', 'command3',],
                'contexts': ['con2','con3'],
            },
        ]
        self.task_03.metadata['processingScope'] = [
            {
                'commands': ['command2', 'command3',],
                'contexts': ['con2','con3'],
            },
        ]
        tasks = Tasks()
        tasks.add_task(task=copy.deepcopy(self.task_02))
        tasks.add_task(task=copy.deepcopy(self.task_03))
        tasks.add_task(task=copy.deepcopy(self.task_01))
        result = tasks.get_task_names_in_order(command='command1', context='con1')

        print_logger_lines(logger=logger)

        self.assertIsNotNone(result)
        self.assertIsInstance(result, list)
        self.assertEqual(len(result), 2)
        self.assertEqual(result[0], 'test-task-01')
        self.assertTrue('test-task-02' in result)
        self.assertFalse('test-task-03' in result)

    def test_basic_task_dependency_with_command_and_context_03(self):
        """
            setup most basic dependencies and processing scopes.

            This test assumes the processing will be aborted because task named "test-task-01" is a dependency of 
            "test-task-02", but the dependant task is NOT scoped for processing in this command and context
        """
        self.task_01.metadata['processingScope'] = [
            {
                'commands': ['command1', 'command2',],
                'contexts': ['con1','con2'],
            },
        ]
        self.task_02.metadata['dependencies'] = [
            {
                'tasks': ['test-task-01',],
                'commands': ['command1', 'command2',],
                'contexts': ['con1','con2',],
            },
        ]
        self.task_03.metadata['dependencies'] = [
            {
                'tasks': ['test-task-01',],
                'commands': ['command2', 'command3',],
                'contexts': ['con2','con3'],
            },
        ]
        self.task_03.metadata['processingScope'] = [
            {
                'commands': ['command2', 'command3',],
                'contexts': ['con2','con3'],
            },
        ]
        tasks = Tasks()
        tasks.add_task(task=copy.deepcopy(self.task_02))
        tasks.add_task(task=copy.deepcopy(self.task_03))
        tasks.add_task(task=copy.deepcopy(self.task_01))
        result = None
        with self.assertRaises(Exception):
            result = tasks.get_task_names_in_order(command='command3', context='con3')

        print_logger_lines(logger=logger)

        self.assertIsNone(result)

    def test_task_ordering_in_multiple_daisy_chained_dependencies_01(self):
        # setup most basic dependency
        self.task_02.metadata['dependencies'] = [
            {
                'tasks': ['test-task-01','test-task-03',],
            }
        ]
        self.task_04.metadata['dependencies'] = [
            {
                'tasks': ['test-task-01','test-task-02',],
            }
        ]
        tasks = Tasks()
        tasks.add_task(task=copy.deepcopy(self.task_02))
        tasks.add_task(task=copy.deepcopy(self.task_04))
        tasks.add_task(task=copy.deepcopy(self.task_01))
        tasks.add_task(task=copy.deepcopy(self.task_03))
        result = tasks.get_task_names_in_order(command='command1', context='con1')

        print_logger_lines(logger=logger)

        self.assertIsNotNone(result)
        self.assertIsInstance(result, list)
        self.assertEqual(len(result), 4)
        self.assertEqual(result[0], 'test-task-01')
        self.assertEqual(result[1], 'test-task-03')
        self.assertEqual(result[2], 'test-task-02')
        self.assertEqual(result[3], 'test-task-04')

    def test_task_ordering_in_multiple_daisy_chained_dependencies_02(self):
        # setup most basic dependency
        self.task_01.metadata['dependencies'] = [
            {
                'tasks': ['test-task-04',],
            }
        ]
        self.task_02.metadata['dependencies'] = [
            {
                'tasks': ['test-task-01','test-task-03',],
            }
        ]
        self.task_04.metadata['dependencies'] = [
            {
                'tasks': ['test-task-03',],
            }
        ]
        tasks = Tasks()
        tasks.add_task(task=copy.deepcopy(self.task_02))
        tasks.add_task(task=copy.deepcopy(self.task_04))
        tasks.add_task(task=copy.deepcopy(self.task_01))
        tasks.add_task(task=copy.deepcopy(self.task_03))
        result = tasks.get_task_names_in_order(command='command1', context='con1')

        print_logger_lines(logger=logger)

        self.assertIsNotNone(result)
        self.assertIsInstance(result, list)
        self.assertEqual(len(result), 4)
        self.assertEqual(result[0], 'test-task-03')
        self.assertEqual(result[1], 'test-task-04')
        self.assertEqual(result[2], 'test-task-01')
        self.assertEqual(result[3], 'test-task-02')

    def test_task_multiple_dependency_scenarios_01(self):
        self.task_02.metadata['dependencies'] = [
            {
                'tasks': ['test-task-01',],
                'commands': ['command1',],
                'contexts': ['con1',],
            }
        ]
        tasks = Tasks()
        tasks.add_task(task=copy.deepcopy(self.task_02))
        tasks.add_task(task=copy.deepcopy(self.task_01))
        dependent_task_names_1 = tasks.get_task_dependencies_as_list_of_task_names(task_name='test-task-02', command='command1', context='con1')
        dependent_task_names_2 = tasks.get_task_dependencies_as_list_of_task_names(task_name='test-task-02', command='command2', context='con1')
        dependent_task_names_3 = tasks.get_task_dependencies_as_list_of_task_names(task_name='test-task-02', command='command1', context='con2')
        for result in (dependent_task_names_1, dependent_task_names_2, dependent_task_names_3,):
            self.assertIsNotNone(result)
            self.assertIsInstance(result, list)
        self.assertEqual(len(dependent_task_names_1), 1)
        self.assertEqual(len(dependent_task_names_2), 0)
        self.assertEqual(len(dependent_task_names_3), 0)
        self.assertTrue('test-task-01' in dependent_task_names_1)
        self.assertFalse('test-task-01' in dependent_task_names_2)
        self.assertFalse('test-task-01' in dependent_task_names_3)

    def test_task_multiple_dependency_scenarios_02(self):
        self.task_02.metadata['dependencies'] = [
            {
                'tasks': ['test-task-01',],
                'commands': ['command1',],
            }
        ]
        tasks = Tasks()
        tasks.add_task(task=copy.deepcopy(self.task_02))
        tasks.add_task(task=copy.deepcopy(self.task_01))
        dependent_task_names_1 = tasks.get_task_dependencies_as_list_of_task_names(task_name='test-task-02', command='command1', context='con1')
        dependent_task_names_2 = tasks.get_task_dependencies_as_list_of_task_names(task_name='test-task-02', command='command2', context='con1')
        dependent_task_names_3 = tasks.get_task_dependencies_as_list_of_task_names(task_name='test-task-02', command='command1', context='con2')
        for result in (dependent_task_names_1, dependent_task_names_2, dependent_task_names_3,):
            self.assertIsNotNone(result)
            self.assertIsInstance(result, list)
        self.assertEqual(len(dependent_task_names_1), 1)
        self.assertEqual(len(dependent_task_names_2), 0)
        self.assertEqual(len(dependent_task_names_3), 1)
        self.assertTrue('test-task-01' in dependent_task_names_1)
        self.assertFalse('test-task-01' in dependent_task_names_2)
        self.assertTrue('test-task-01' in dependent_task_names_3)

    def test_task_multiple_dependency_scenarios_03(self):
        self.task_02.metadata['dependencies'] = [
            {
                'tasks': ['test-task-01',],
                'contexts': ['con1',],
            }
        ]
        tasks = Tasks()
        tasks.add_task(task=copy.deepcopy(self.task_02))
        tasks.add_task(task=copy.deepcopy(self.task_01))
        dependent_task_names_1 = tasks.get_task_dependencies_as_list_of_task_names(task_name='test-task-02', command='command1', context='con1')
        dependent_task_names_2 = tasks.get_task_dependencies_as_list_of_task_names(task_name='test-task-02', command='command2', context='con1')
        dependent_task_names_3 = tasks.get_task_dependencies_as_list_of_task_names(task_name='test-task-02', command='command1', context='con2')
        for result in (dependent_task_names_1, dependent_task_names_2, dependent_task_names_3,):
            self.assertIsNotNone(result)
            self.assertIsInstance(result, list)
        self.assertEqual(len(dependent_task_names_1), 1)
        self.assertEqual(len(dependent_task_names_2), 1)
        self.assertEqual(len(dependent_task_names_3), 0)
        self.assertTrue('test-task-01' in dependent_task_names_1)
        self.assertTrue('test-task-01' in dependent_task_names_2)
        self.assertFalse('test-task-01' in dependent_task_names_3)

    def test_task_processing_scope_scenarios_01(self):
        self.task_01.metadata['processingScope'] = [
            {
                'commands': ['command1',],
                'contexts': ['con1',],
            }
        ]
        tasks = Tasks()
        tasks.add_task(task=copy.deepcopy(self.task_01))
        self.assertTrue(tasks.task_scoped_for_processing(task_name='test-task-01', command='command1', context='con1'))
        self.assertFalse(tasks.task_scoped_for_processing(task_name='test-task-01', command='command2', context='con1'))
        self.assertFalse(tasks.task_scoped_for_processing(task_name='test-task-01', command='command1', context='con2'))

    def test_task_processing_scope_scenarios_02(self):
        self.task_01.metadata['processingScope'] = [
            {
                'commands': ['command1',],
            }
        ]
        tasks = Tasks()
        tasks.add_task(task=copy.deepcopy(self.task_01))
        self.assertTrue(tasks.task_scoped_for_processing(task_name='test-task-01', command='command1', context='con1'))
        self.assertFalse(tasks.task_scoped_for_processing(task_name='test-task-01', command='command2', context='con1'))
        self.assertTrue(tasks.task_scoped_for_processing(task_name='test-task-01', command='command1', context='con2'))

    def test_task_processing_scope_scenarios_03(self):
        self.task_01.metadata['processingScope'] = [
            {
                'contexts': ['con1',],
            }
        ]
        tasks = Tasks()
        tasks.add_task(task=copy.deepcopy(self.task_01))
        self.assertTrue(tasks.task_scoped_for_processing(task_name='test-task-01', command='command1', context='con1'))
        self.assertTrue(tasks.task_scoped_for_processing(task_name='test-task-01', command='command2', context='con1'))
        self.assertFalse(tasks.task_scoped_for_processing(task_name='test-task-01', command='command1', context='con2'))


    def test_task_processing_scope_scenarios_04(self):
        self.task_01.metadata['processingScope'] = None
        tasks = Tasks()
        tasks.add_task(task=copy.deepcopy(self.task_01))
        self.assertTrue(tasks.task_scoped_for_processing(task_name='test-task-01', command='command1', context='con1'))
        self.assertTrue(tasks.task_scoped_for_processing(task_name='test-task-01', command='command2', context='con1'))
        self.assertTrue(tasks.task_scoped_for_processing(task_name='test-task-01', command='command1', context='con2'))

    def test_task_processing_scope_scenarios_05(self):
        self.task_01.metadata['processingScope'] = 'Invalid Type'
        tasks = Tasks()
        tasks.add_task(task=copy.deepcopy(self.task_01))
        self.assertTrue(tasks.task_scoped_for_processing(task_name='test-task-01', command='command1', context='con1'))
        self.assertTrue(tasks.task_scoped_for_processing(task_name='test-task-01', command='command2', context='con1'))
        self.assertTrue(tasks.task_scoped_for_processing(task_name='test-task-01', command='command1', context='con2'))

    def test_task_processing_scope_scenarios_06(self):
        self.task_01.metadata['processingScope'] = [
            None,
            {
                'commands': ['command2',],
                'contexts': ['con2',],
            }
        ]
        tasks = Tasks()
        tasks.add_task(task=copy.deepcopy(self.task_01))
        self.assertFalse(tasks.task_scoped_for_processing(task_name='test-task-01', command='command1', context='con1'))
        self.assertFalse(tasks.task_scoped_for_processing(task_name='test-task-01', command='command2', context='con1'))
        self.assertFalse(tasks.task_scoped_for_processing(task_name='test-task-01', command='command1', context='con2'))
        self.assertTrue(tasks.task_scoped_for_processing(task_name='test-task-01', command='command2', context='con2'))

    def test_task_processing_scope_scenarios_07(self):
        self.task_01.metadata['processingScope'] = [
            'Invalid Type',
            {
                'commands': ['command2',],
                'contexts': ['con2',],
            }
        ]
        tasks = Tasks()
        tasks.add_task(task=copy.deepcopy(self.task_01))
        self.assertFalse(tasks.task_scoped_for_processing(task_name='test-task-01', command='command1', context='con1'))
        self.assertFalse(tasks.task_scoped_for_processing(task_name='test-task-01', command='command2', context='con1'))
        self.assertFalse(tasks.task_scoped_for_processing(task_name='test-task-01', command='command1', context='con2'))
        self.assertTrue(tasks.task_scoped_for_processing(task_name='test-task-01', command='command2', context='con2'))

    def test_task_processing_scope_scenarios_08(self):
        self.task_01.metadata['processingScope'] = [
            {
                'what?': 'This will produce a TRUE result',
            },
            {
                'commands': ['command2',],
                'contexts': ['con2',],
            }
        ]
        tasks = Tasks()
        tasks.add_task(task=copy.deepcopy(self.task_01))
        self.assertTrue(tasks.task_scoped_for_processing(task_name='test-task-01', command='command1', context='con1'))
        self.assertTrue(tasks.task_scoped_for_processing(task_name='test-task-01', command='command2', context='con1'))
        self.assertTrue(tasks.task_scoped_for_processing(task_name='test-task-01', command='command1', context='con2'))
        self.assertTrue(tasks.task_scoped_for_processing(task_name='test-task-01', command='command2', context='con2'))

    def test_loop_through_tasks_01(self):
        tasks = Tasks()
        tasks.add_task(task=copy.deepcopy(self.task_01))
        tasks.add_task(task=copy.deepcopy(self.task_02))
        tasks.add_task(task=copy.deepcopy(self.task_03))
        tasks.add_task(task=copy.deepcopy(self.task_04))
        self.assertEqual(len(tasks), 4)
        task: Task
        for task in tasks:
            self.assertIsNotNone(task)
            self.assertIsInstance(task, Task)
            self.assertTrue(task.task_id.startswith('test-task-0'))

    def test_task_ordering_dependency_raises_exception_01(self):
        # setup most basic dependency
        self.task_01.metadata['processingScope'] = [
            {
                'commands': ['command1', 'command2',],
                'contexts': ['con1','con2'],
            },
        ]
        self.task_02.metadata['dependencies'] = [
            {
                'tasks': ['test-task-01',],
            }
        ]
        tasks = Tasks()
        tasks.add_task(task=copy.deepcopy(self.task_02))
        tasks.add_task(task=copy.deepcopy(self.task_01))
        combinations = (
            {
                'command': 'command1',
                'context': 'con1',
                'expectException': False
            },
            {
                'command': 'command2',
                'context': 'con1',
                'expectException': False
            },
            {
                'command': 'command1',
                'context': 'con2',
                'expectException': False
            },
            {
                'command': 'command2',
                'context': 'con2',
                'expectException': False
            },
            {
                'command': 'command3',
                'context': 'con1',
                'expectException': True
            },
            {
                'command': 'command1',
                'context': 'con3',
                'expectException': True
            },
            {
                'command': 'command3',
                'context': 'con3',
                'expectException': True
            },
        )
        for scenario in combinations:
            if scenario['expectException'] is False:
                result = tasks._task_ordering(current_processing_order=[], candidate_task_name='test-task-02',command=scenario['command'], context=scenario['context'])
                self.assertIsNotNone(result)
                self.assertIsInstance(result, list)
                self.assertEqual(len(result), 2)
                self.assertTrue('test-task-01' in result)
                self.assertTrue('test-task-02' in result)
            else:
                with self.assertRaises(Exception):
                    tasks._task_ordering(current_processing_order=[], candidate_task_name='test-task-02',command=scenario['command'], context=scenario['context'])
            print_logger_lines(logger=logger)
            logger.reset()


class TestVariousFunctions(unittest.TestCase):    # pragma: no cover

    def setUp(self):
        print()
        print('-'*80)
        logger.reset()

    def tearDown(self):
        return super().tearDown()

    def test_function_produce_column_headers_normal_01(self):
        result = produce_column_headers()
        print('RESULT:\n\n{}\n\n'.format(result))
        self.assertTrue('Manifest          Created  Created Timestamp          Spec Drifted       Resources Drifted' in result)

    def test_function_produce_column_headers_with_checksums_01(self):
        result = produce_column_headers(with_checksums=True)
        print('RESULT:\n\n{}\n\n'.format(result))
        self.assertTrue('Manifest          Created  Created Timestamp          Spec Drifted       Resources Drifted  Applied Spec CHecksum             Current Spec Checksum             Applied Resource Checksum         Current Resource Checksum' in result)

    def test_produce_column_header_horizontal_line_basic_01(self):
        result = produce_column_header_horizontal_line()
        print('RESULT:\n\n{}\n\n'.format(result))
        self.assertTrue('------------------------------------------------------------------------------------------' in result)

    def test_produce_column_header_horizontal_line_basic_02(self):
        result = produce_column_header_horizontal_line(line_char='=')
        print('RESULT:\n\n{}\n\n'.format(result))
        self.assertTrue('==========================================================================================' in result)

    def test_produce_column_header_horizontal_line_with_checksums_01(self):
        result = produce_column_header_horizontal_line(with_checksums=True)
        print('RESULT:\n\n{}\n\n'.format(result))
        self.assertTrue('----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------' in result)


class TestClassTaskState(unittest.TestCase):    # pragma: no cover

    def setUp(self):
        print()
        print('-'*80)
        logger.reset()

    def tearDown(self):
        return super().tearDown()
    
    def test_basic_init_01(self):
        task_state = TaskState()
        self.assertIsNotNone(task_state)
        self.assertIsInstance(task_state, TaskState)
        self.assertFalse(task_state.is_created)

    def test_method_update_applied_spec_existing_resource_updated_01(self):
        # Initial state of a previously created resource
        task_state = TaskState(
            manifest_spec={'field_value': 2},
            applied_spec={'field_value': 1},
            resolved_spec={'field_value': 2},
            manifest_metadata={'name': 'test-task-01'},
            report_label='test-task-01',
            created_timestamp=1000,
            applied_resources_checksum='a',
            current_resource_checksum='a'
        )
        print(str(task_state))
        drift_results_1 = task_state.to_dict(with_checksums=True)
        print('\nDRIFT DATA: {}\n\n'.format(json.dumps(drift_results_1, default=str)))
        self.assertTrue(drift_results_1['SpecDrifted'])
        self.assertEqual(drift_results_1['CreatedTimestamp'], 1000)
        self.assertEqual(drift_results_1['AppliedResourcesChecksum'], 'a')
        self.assertEqual(drift_results_1['CurrentResourceChecksum'], 'a')
        self.assertTrue(task_state.is_created)

        # Simulate a resource update
        task_state.update_applied_spec(new_applied_spec={'field_value': 2}, new_applied_resource_checksum='b', updated_timestamp=2000)
        drift_results_2 = task_state.to_dict(with_checksums=True)
        print('\nDRIFT DATA: {}\n\n'.format(json.dumps(drift_results_2, default=str)))
        self.assertFalse(drift_results_2['SpecDrifted'])
        self.assertEqual(drift_results_2['CreatedTimestamp'], 2000)
        self.assertEqual(drift_results_2['AppliedResourcesChecksum'], 'b')
        self.assertEqual(drift_results_2['CurrentResourceChecksum'], 'b')
        self.assertTrue(task_state.is_created)

    def test_method_update_applied_spec_existing_resource_deleted_01(self):
        # Initial state of a previously created resource
        task_state = TaskState(
            manifest_spec={'field_value': 1},
            applied_spec={'field_value': 1},
            resolved_spec={'field_value': 1},
            manifest_metadata={'name': 'test-task-01'},
            report_label='test-task-01',
            created_timestamp=1000,
            applied_resources_checksum='a',
            current_resource_checksum='a'
        )
        print(str(task_state))
        drift_results_1 = task_state.to_dict(with_checksums=True)
        print('\nDRIFT DATA: {}\n\n'.format(json.dumps(drift_results_1, default=str)))
        self.assertIsNone(drift_results_1['SpecDrifted'])
        self.assertEqual(drift_results_1['CreatedTimestamp'], 1000)
        self.assertEqual(drift_results_1['AppliedResourcesChecksum'], 'a')
        self.assertEqual(drift_results_1['CurrentResourceChecksum'], 'a')

        # Simulate a resource being deleted
        task_state.update_applied_spec(new_applied_spec={'field_value': 1}, new_applied_resource_checksum=None, updated_timestamp=0)
        drift_results_2 = task_state.to_dict(with_checksums=True)
        print('\nDRIFT DATA: {}\n\n'.format(json.dumps(drift_results_2, default=str)))
        self.assertFalse(drift_results_2['SpecDrifted'])
        self.assertEqual(drift_results_2['CreatedTimestamp'], None)
        self.assertEqual(drift_results_2['AppliedResourcesChecksum'], None)
        self.assertEqual(drift_results_2['CurrentResourceChecksum'], None)
        self.assertFalse(task_state.is_created)

    def test_method_column_str_basic_01(self):
        task_state = TaskState(
            manifest_spec={'field_value': 1},
            applied_spec={'field_value': 1},
            resolved_spec={'field_value': 1},
            manifest_metadata={'name': 'test-task-01'},
            report_label='test-task-01',
            created_timestamp=1000,
            applied_resources_checksum='a',
            current_resource_checksum='a'
        )
        result = task_state.column_str(
            human_readable=True,
            current_resolved_spec={'field_value': 1},
            current_resource_checksum='a'
        )
        print(produce_column_headers())
        print(produce_column_header_horizontal_line())
        print('{}'.format(result))
        self.assertTrue('test-task-01      Yes      1970-01-01 01:16:40        No                 No' in result)

    def test_method_column_str_resource_drifted_01(self):
        task_state = TaskState(
            manifest_spec={'field_value': 1},
            applied_spec={'field_value': 1},
            resolved_spec={'field_value': 1},
            manifest_metadata={'name': 'test-task-01'},
            report_label='test-task-01',
            created_timestamp=1000,
            applied_resources_checksum='a',
            current_resource_checksum='a'
        )
        result = task_state.column_str(
            human_readable=True,
            current_resolved_spec={'field_value': 1},
            current_resource_checksum='b'
        )
        print(produce_column_headers())
        print(produce_column_header_horizontal_line())
        print('{}'.format(result))
        self.assertTrue('test-task-01      Yes      1970-01-01 01:16:40        No                 Yes' in result)

    def test_method_column_str_spec_drifted_01(self):
        task_state = TaskState(
            manifest_spec={'field_value': 1},
            applied_spec={'field_value': 1},
            resolved_spec={'field_value': 1},
            manifest_metadata={'name': 'test-task-01'},
            report_label='test-task-01',
            created_timestamp=1000,
            applied_resources_checksum='a',
            current_resource_checksum='a'
        )
        result = task_state.column_str(
            human_readable=True,
            current_resolved_spec={'field_value': 2},
            current_resource_checksum='a'
        )
        print(produce_column_headers())
        print(produce_column_header_horizontal_line())
        print('{}'.format(result))
        self.assertTrue('test-task-01      Yes      1970-01-01 01:16:40        Yes                No' in result)

    def test_method_column_str_spec_and_resource_drifted_01(self):
        task_state = TaskState(
            manifest_spec={'field_value': 1},
            applied_spec={'field_value': 1},
            resolved_spec={'field_value': 1},
            manifest_metadata={'name': 'test-task-01'},
            report_label='test-task-01',
            created_timestamp=1000,
            applied_resources_checksum='a',
            current_resource_checksum='a'
        )
        result = task_state.column_str(
            human_readable=True,
            current_resolved_spec={'field_value': 2},
            current_resource_checksum='b'
        )
        print(produce_column_headers())
        print(produce_column_header_horizontal_line())
        print('{}'.format(result))
        self.assertTrue('test-task-01      Yes      1970-01-01 01:16:40        Yes                Yes' in result)

    def test_method_column_str_spec_and_resource_drifted_with_checksums_01(self):
        task_state = TaskState(
            manifest_spec={'field_value': 1},
            applied_spec={'field_value': 1},
            resolved_spec={'field_value': 1},
            manifest_metadata={'name': 'test-task-01'},
            report_label='test-task-01',
            created_timestamp=1000,
            applied_resources_checksum='a',
            current_resource_checksum='a'
        )
        result = task_state.column_str(
            human_readable=True,
            current_resolved_spec={'field_value': 2},
            current_resource_checksum='b',
            with_checksums=True
        )
        print(produce_column_headers())
        print(produce_column_header_horizontal_line())
        print('{}'.format(result))
        self.assertTrue('test-task-01      Yes      1970-01-01 01:16:40        Yes                Yes                38320c1e644eb074b24bb343e131e420  fcacd5b851d3ef945b3b11bf07ad1e17  a                                 b' in result)

    def test_method_repr(self):
        task_state = TaskState(
            manifest_spec={'field_value': 1},
            applied_spec={'field_value': 1},
            resolved_spec={'field_value': 1},
            manifest_metadata={'name': 'test-task-01'},
            report_label='test-task-01',
            created_timestamp=1000,
            applied_resources_checksum='a',
            current_resource_checksum='a'
        )
        result = repr(task_state)
        print(result)
        self.assertIsNotNone(result)
        self.assertIsInstance(result, str)


class TestClassStatePersistence(unittest.TestCase):    # pragma: no cover

    def setUp(self):
        print()
        print('-'*80)
        logger.reset()

    def tearDown(self):
        return super().tearDown()
    
    def test_load_without_exception_01(self):
        p = StatePersistence()
        self.assertFalse(p.load())

    def test_load_with_exception_01(self):
        p = StatePersistence()
        with self.assertRaises(Exception):
            p.load(on_failure=Exception('TEST FAILURE'))

    def test_update_and_get_01(self):
        p = StatePersistence()
        p.update_object_state(object_identifier='a', data={'result': 'it worked!'})
        result = p.get(object_identifier='a')
        self.assertIsNotNone(result)
        self.assertIsInstance(result, dict)
        self.assertEqual(result['result'], 'it worked!')

    def test_get_with_refresh_returns_empty_dict_01(self):
        p = StatePersistence()
        result = p.get(object_identifier='a')
        self.assertIsNotNone(result)
        self.assertIsInstance(result, dict)
        self.assertEqual(len(result), 0)

    def test_get_without_refresh_returns_empty_dict_01(self):
        p = StatePersistence()
        result = p.get(object_identifier='a', refresh_cache_if_identifier_not_found=False)
        self.assertIsNotNone(result)
        self.assertIsInstance(result, dict)
        self.assertEqual(len(result), 0)

    def test_get_with_refresh_returns_valid_dict_01(self):
        class MyStatePersistence(StatePersistence):
            def load(self, on_failure: object=False)->bool:
                self.state_cache['a'] = {'value': 1}
                return True
        
        p = MyStatePersistence(load_on_init=False)
        result = p.get(object_identifier='a')
        self.assertIsNotNone(result)
        self.assertIsInstance(result, dict)
        self.assertEqual(len(result), 1)

    def test_update_and_commit_01(self):
        p = StatePersistence()
        p.update_object_state(object_identifier='a', data={'result': 'it worked!'})
        p.commit()
        self.assertIsNotNone(p.state_cache)
        self.assertIsInstance(p.state_cache, dict)
        self.assertEqual(len(p.state_cache), 1)

class TestClassParameterValidation(unittest.TestCase):    # pragma: no cover

    def setUp(self):
        print()
        print('-'*80)
        logger.reset()

    def tearDown(self):
        return super().tearDown()
    
    def test_basic_01(self):
        pv = ParameterValidation(constraints=None)
        result = pv.validation_passed(parameters=dict())
        self.assertTrue(result)


class TestClassTaskProcessingActionParameterValidation(unittest.TestCase):    # pragma: no cover

    def setUp(self):
        print()
        print('-'*80)
        logger.reset()

    def tearDown(self):
        return super().tearDown()
    
    def test_most_basic_01(self):
        pv = TaskProcessingActionParameterValidation(constraints=None)
        result = pv.validation_passed(
            parameters={
                'Action': 'CreateAction'
            }
        )
        print_logger_lines(logger=logger)
        self.assertTrue(result)

    def test_most_basic_02(self):
        pv = TaskProcessingActionParameterValidation(
            constraints=None,
            auto_init_supported_actions=True
        ).add_command(command='command1').add_context(context='context1')
        result = pv.validation_passed(
            parameters={
                'Action': 'CreateAction',
                'Command': 'command1',
                'Context': 'context1'
            }
        )
        print_logger_lines(logger=logger)
        self.assertTrue(result)

    def test_most_basic_03(self):
        pv = TaskProcessingActionParameterValidation(
            constraints={
                'SupportedCommands': ['command1','command2',],
                'SupportedContexts': ['context1', 'context2', ],
                'SupportedActions': ['action1', 'action2',],
            },
            auto_init_supported_actions=True
        )
        result = pv.validation_passed(
            parameters={
                'Action': 'action1',
                'Command': 'command1',
                'Context': 'context1'
            }
        )
        print_logger_lines(logger=logger)
        self.assertTrue(result)

    def test_most_basic_negative_01(self):
        pv = TaskProcessingActionParameterValidation(constraints=None)
        result = pv.validation_passed(
            parameters={
                'Action': 'WrongAction'
            }
        )
        print_logger_lines(logger=logger)
        self.assertFalse(result)

    def test_most_basic_negative_02(self):
        pv = TaskProcessingActionParameterValidation(
            constraints=None,
            auto_init_supported_actions=True
        ).add_command(command='command1').add_context(context='context1')

        wrong_combinations = (
            {
                'Action': 'WrongAction',
                'Command': 'command1',
                'Context': 'context1'
            },
            {
                'Action': 'CreateAction',
                'Command': 'command99',
                'Context': 'context1'
            },
            {
                'Action': 'CreateAction',
                'Command': 'command1',
                'Context': 'context99'
            }
        )

        for wrong_parameters in wrong_combinations:
            result = pv.validation_passed(parameters=wrong_parameters)
            print_logger_lines(logger=logger)
            self.assertFalse(result)
            logger.reset()

    def test_most_basic_negative_03(self):
        pv = TaskProcessingActionParameterValidation(
            constraints={
                'SupportedCommands': ['command1','command2',],
                'SupportedContexts': ['context1', 'context2', ],
                'SupportedActions': ['action1', 'action2',],
            },
            auto_init_supported_actions=True
        )

        wrong_combinations = (
            {
                'Action': 'WrongAction',
                'Command': 'command1',
                'Context': 'context1',
            },
            {
                'Action': 'CreateAction',
                'Command': 'command99',
                'Context': 'context1',
            },
            {
                'Action': 'CreateAction',
                'Command': 'command1',
                'Context': 'context99',
            },
            {
                'Action': None,
                'Command': 'command1',
                'Context': 'context1',
            },
            {
                'Action': 'CreateAction',
                'Command': None,
                'Context': 'context1',
            },
            {
                'Action': 'CreateAction',
                'Command': 'command1',
                'Context': None,
            },
            None,
            'wrong_type',
        )

        for wrong_parameters in wrong_combinations:
            result = pv.validation_passed(parameters=wrong_parameters)
            print_logger_lines(logger=logger)
            self.assertFalse(result)
            logger.reset()


class TestClassVariableStore(unittest.TestCase):    # pragma: no cover

    def setUp(self):
        print()
        print('-'*80)
        logger.reset()

    def tearDown(self):
        return super().tearDown()
    
    def test_basic_01(self):
        v = VariableStore()
        with self.assertRaises(Exception):
            v.get_variable(variable_name='a', pop_item=False)
        with self.assertRaises(Exception):
            v.get_variable(variable_name='a', pop_item=True)
        v.add_variable(variable_name='a', value=100)
        result1 = v.get_variable(variable_name='a', pop_item=False)
        result2 = v.get_variable(variable_name='a', pop_item=True)
        with self.assertRaises(Exception):
            v.get_variable(variable_name='a', pop_item=False)
        with self.assertRaises(Exception):
            v.get_variable(variable_name='a', pop_item=True)
        for result in (result1, result2, ):
            self.assertIsNotNone(result)
            self.assertIsInstance(result, int)
            self.assertEqual(result, 100)


class TestClassTask(unittest.TestCase):    # pragma: no cover

    def setUp(self):
        print()
        print('-'*80)
        logger.reset()

    def tearDown(self):
        return super().tearDown()
    
    def test_basic_01(self):
        t = Task(
            api_version='unittest',
            kind='TestKind',
            metadata={'name': 'test'},
            spec={'name': 'value'}
        )
        self.assertIsNotNone(t)
        self.assertIsInstance(t, Task)
        self.assertEqual(t.api_version, 'unittest')
        self.assertEqual(t.kind, 'TestKind')

        self.assertIsNotNone(t.metadata)
        self.assertIsInstance(t.metadata, dict)
        self.assertEqual(len(t.metadata), 1)
        self.assertTrue('name' in t.metadata)
        self.assertEqual(t.metadata['name'], 'test')

        self.assertIsNotNone(t.spec)
        self.assertIsInstance(t.spec, dict)
        self.assertEqual(len(t.spec), 1)
        self.assertTrue('name' in t.spec)
        self.assertEqual(t.spec['name'], 'value')

    def test_invalid_dicts_create_empty_dicts_basic_01(self):
        t = Task(
            api_version='unittest',
            kind='TestKind',
            metadata=None,
            spec='invalid type'
        )
        self.assertIsNotNone(t)
        self.assertIsInstance(t, Task)
        self.assertEqual(t.api_version, 'unittest')
        self.assertEqual(t.kind, 'TestKind')

        self.assertIsNotNone(t.metadata)
        self.assertIsInstance(t.metadata, dict)
        self.assertEqual(len(t.metadata), 0)

        self.assertIsNotNone(t.spec)
        self.assertIsInstance(t.spec, dict)
        self.assertEqual(len(t.spec), 0)


class TestClassTaskProcessor(unittest.TestCase):    # pragma: no cover

    def setUp(self):
        print()
        print('-'*80)
        logger.reset()
        self.task = Task(
            api_version='DummyTaskProcessor1/v1',
            kind='DummyTaskProcessor1',
            metadata={'name': 'test-task'},
            spec={'testField': 'testValue'}
        )

    def tearDown(self):
        self.task = None
        return super().tearDown()
    
    def test_basic_create_task_01(self):
        tp = DummyTaskProcessor1()
        variable_store = tp.process_task(
            task=self.task,
            action='CreateAction',
            task_resolved_spec={'testField': 'testValue'}
        )

        print_logger_lines(logger=logger)
        dump_variable_store(
            test_class_name=self.__class__.__name__,
            test_method_name=stack()[0][3],
            variable_store=copy.deepcopy(variable_store)
        )
        dump_events(
            task_id=self.task.task_id,
            variable_store=copy.deepcopy(variable_store)
        )

        self.assertIsNotNone(variable_store)
        self.assertIsInstance(variable_store, VariableStore)

    def test_basic_create_task_force_failure_and_rollback_01(self):
        vs = VariableStore()
        tp = DummyTaskProcessor1()
        vs.add_variable(variable_name=tp.create_identifier(task=self.task, variable_name='UNITTEST_TROW_EXCEPTION'), value=True)
        with self.assertRaises(Exception):
            tp.process_task(
                task=self.task,
                action='CreateAction',
                variable_store=copy.deepcopy(vs),
                task_resolved_spec={'testField': 'testValue'}
            )

        print_logger_lines(logger=logger)

    def test_basic_update_task_01(self):
        tp = DummyTaskProcessor1()
        variable_store = tp.process_task(
            task=self.task,
            action='UpdateAction',
            task_resolved_spec={'testField': 'testValue'}
        )

        print_logger_lines(logger=logger)
        dump_variable_store(
            test_class_name=self.__class__.__name__,
            test_method_name=stack()[0][3],
            variable_store=copy.deepcopy(variable_store)
        )
        dump_events(
            task_id=self.task.task_id,
            variable_store=copy.deepcopy(variable_store)
        )

        self.assertIsNotNone(variable_store)
        self.assertIsInstance(variable_store, VariableStore)

    def test_basic_update_task_force_failure_and_rollback_01(self):
        vs = VariableStore()
        tp = DummyTaskProcessor1()
        vs.add_variable(variable_name=tp.create_identifier(task=self.task, variable_name='UNITTEST_TROW_EXCEPTION'), value=True)
        with self.assertRaises(Exception):
            tp.process_task(
                task=self.task,
                action='UpdateAction',
                variable_store=copy.deepcopy(vs),
                task_resolved_spec={'testField': 'testValue'}
            )

        print_logger_lines(logger=logger)

    def test_manual_rollback_after_create_action_01(self):
        tp = DummyTaskProcessor1()
        variable_store = tp.process_task(
            task=self.task,
            action='CreateAction',
            task_resolved_spec={'testField': 'testValue'}
        )
        variable_store = tp.process_task(
            task=self.task,
            action='RollbackAction',
            variable_store=copy.deepcopy(variable_store),
            task_resolved_spec={'testField': 'testValue'}
        )

        print_logger_lines(logger=logger)
        dump_variable_store(
            test_class_name=self.__class__.__name__,
            test_method_name=stack()[0][3],
            variable_store=copy.deepcopy(variable_store)
        )
        dump_events(
            task_id=self.task.task_id,
            variable_store=copy.deepcopy(variable_store)
        )

        self.assertIsNotNone(variable_store)
        self.assertIsInstance(variable_store, VariableStore)

    def test_default_task_processor_raises_exceptions_01(self):
        tp = TaskProcessor(api_version='unittest')
        with self.assertRaises(Exception):
            tp.process_task(
                task=self.task,
                action='CreateAction',
                task_resolved_spec={'testField': 'testValue'}
            )
        with self.assertRaises(Exception):
            tp.process_task(
                task=self.task,
                action='DeleteAction',
                task_resolved_spec={'testField': 'testValue'}
            )
        with self.assertRaises(Exception):
            tp.process_task(
                task=self.task,
                action='UpdateAction',
                task_resolved_spec={'testField': 'testValue'}
            )
        with self.assertRaises(Exception):
            tp.process_task(
                task=self.task,
                action='RollbackAction',
                task_resolved_spec={'testField': 'testValue'}
            )
        with self.assertRaises(Exception):
            tp.process_task(
                task=self.task,
                action='DescribeAction',
                task_resolved_spec={'testField': 'testValue'}
            )
        with self.assertRaises(Exception):
            tp.process_task(
                task=self.task,
                action='DetectDriftAction',
                task_resolved_spec={'testField': 'testValue'}
            )


class UnitTestExceptionThrowingHook1(Hook):

    def run(
        self,
        task: Task=None,
        parameters: dict=dict(),
        parameter_validator: ParameterValidation=ParameterValidation(constraints=None),
        persistence: StatePersistence=StatePersistence(),
        variable_store: VariableStore=VariableStore(),
        task_process_store: TaskProcessStore=TaskProcessStore()
    )->VariableStore:
        if 'LogLevel' in parameters:
            self._log(
                message='I Quit!',
                level=parameters['LogLevel']
            )
        else:
            self._log(
                message='I Quit!',
                level='error'
            )
        raise Exception('I really did quit!')
    

class TestClassUnitTestExceptionThrowingHook1(unittest.TestCase):    # pragma: no cover

    def setUp(self):
        print()
        print('-'*80)
        logger.reset()

    def tearDown(self):
        return super().tearDown()
    

    def test_info_level_01(self):
        hook = UnitTestExceptionThrowingHook1(name='unittest')
        with self.assertRaises(Exception):
            hook.run(
                parameters={'LogLevel': 'info'}
            )
        with self.assertRaises(Exception):
            hook.run(
                parameters={'LogLevel': 'debug'}
            )
        with self.assertRaises(Exception):
            hook.run(
                parameters={'LogLevel': 'warning'}
            )
        with self.assertRaises(Exception):
            hook.run(
                parameters={'LogLevel': 'critical'}
            )
        with self.assertRaises(Exception):
            hook.run()
        self.assertEqual(len(logger.critical_lines), 1)
        self.assertEqual(len(logger.info_lines), 1)
        self.assertEqual(len(logger.error_lines), 1)
        self.assertEqual(len(logger.debug_lines), 1)
        self.assertEqual(len(logger.warn_lines), 1)

    def test_Default_hook_throws_exception(self):
        h = Hook(name='unittest')
        with self.assertRaises(Exception):
            h.run()


class TestClassTaskProcessingHook(unittest.TestCase):    # pragma: no cover

    def setUp(self):
        print()
        print('-'*80)
        logger.reset()
        self.task = Task(
            api_version='DummyTaskProcessor1/v1',
            kind='DummyTaskProcessor1',
            metadata={'name': 'test-task'},
            spec={'testField': 'testValue'}
        )

    def tearDown(self):
        self.task = None
        return super().tearDown()
    
    def test_basic_01(self):
        parameters = dict()
        parameters['Action'] = 'CreateAction'
        parameters['Command'] = 'create'
        parameters['Context'] = 'unittest'
        vs = VariableStore()
        param_validator = ParameterValidation(constraints=None)
        tps = TaskProcessStore()
        tps.register_task_processor(task_processor=DummyTaskProcessor1())

        vs.add_variable(
            variable_name='ResolvedSpec:{}'.format(self.task.task_id),
            value=copy.deepcopy(self.task.spec)
        )

        tph = TaskProcessingHook()
        vs = tph.run(
            task=self.task,
            parameters=parameters,
            parameter_validator=param_validator,
            variable_store=vs,
            task_process_store=tps
        )
        
        print_logger_lines(logger=logger)
        dump_variable_store(
            test_class_name=self.__class__.__name__,
            test_method_name=stack()[0][3],
            variable_store=copy.deepcopy(vs)
        )
        dump_events(
            task_id=self.task.task_id,
            variable_store=copy.deepcopy(vs)
        )

        self.assertIsNotNone(vs)
        self.assertIsInstance(vs, VariableStore)
        self.assertTrue('test-task:TASK_ORIGINAL_SPEC_CHECKSUM' in vs.variable_store)
        self.assertTrue('test-task:TASK_RESOLVED_SPEC_CHECKSUM' in vs.variable_store)


class TestClassResolveTaskSpecVariablesHook(unittest.TestCase):    # pragma: no cover

    def setUp(self):
        print()
        print('-'*80)
        logger.reset()
        self.task_01 = Task(
            api_version='DummyTaskProcessor1/v1',
            kind='DummyTaskProcessor1',
            metadata={'name': 'test-task-1'},
            spec={
                'test1': '${}VAR:Test1:Key1:Key2{}'.format('{', '}'),
            }
        )
        self.task_02 = Task(
            api_version='DummyTaskProcessor1/v1',
            kind='DummyTaskProcessor1',
            metadata={'name': 'test-task-2'},
            spec={
                'test2': '${}VAR:Test2:Key1{}'.format('{', '}'),
            }
        )
        self.task_03 = Task(
            api_version='DummyTaskProcessor1/v1',
            kind='DummyTaskProcessor1',
            metadata={'name': 'test-task-3'},
            spec={
                'test3': [
                    '${}VAR:Test3:Key1{}'.format('{', '}'),
                    '${}VAR:Test3:Key2{}'.format('{', '}'),
                ],
            }
        )
        self.task_04 = Task(
            api_version='DummyTaskProcessor1/v1',
            kind='DummyTaskProcessor1',
            metadata={'name': 'test-task-4'},
            spec={
                'test4': {
                    'listTest4': [
                        '${}VAR:Test4:Key1{}'.format('{', '}'),
                        None,
                        'test4_ignore',
                        '${}VAR:Test4:Key2{}'.format('{', '}'),
                    ]
                },
            }
        )
        self.task_05 = Task(
            api_version='DummyTaskProcessor1/v1',
            kind='DummyTaskProcessor1',
            metadata={'name': 'test-task-5'},
            spec={
                'test5': {
                    'test5_1': {
                        'test5_1_1': '${}VAR:Test5:Key1:Key1{}'.format('{', '}'),
                        'test5_1_2': '${}VAR:Test5:Key1:Key2{}'.format('{', '}'),
                        'test5_1_3': None,
                        'test5_1_4': 'test5_1_4_ignore',
                    }
                }
            }
        )
        self.task_06 = Task(
            api_version='DummyTaskProcessor1/v1',
            kind='DummyTaskProcessor1',
            metadata={'name': 'test-task-6'},
            spec={
                'test1': '${}VAR:Test1:Key1:Key2{}'.format('{', '}'),
                'test2': '${}VAR:Test2:Key1{}'.format('{', '}'),
                'test3': [
                    '${}VAR:Test3:Key1{}'.format('{', '}'),
                    '${}VAR:Test3:Key2{}'.format('{', '}'),
                ],
                'test4': {
                    'listTest4': [
                        '${}VAR:Test4:Key1{}'.format('{', '}'),
                        None,
                        'test4_ignore',
                        '${}VAR:Test4:Key2{}'.format('{', '}'),
                    ]
                },
                'test5': {
                    'test5_1': {
                        'test5_1_1': '${}VAR:Test5:Key1:Key1{}'.format('{', '}'),
                        'test5_1_2': '${}VAR:Test5:Key1:Key2{}'.format('{', '}'),
                        'test5_1_3': None,
                        'test5_1_4': 'test5_1_4_ignore',
                    }
                }
            }
        )
        self.variable_store = VariableStore()
        self.variable_store.add_variable(variable_name='Test1:Key1:Key2', value='result_01')
        self.variable_store.add_variable(variable_name='Test2:Key1', value='result_02')
        self.variable_store.add_variable(variable_name='Test3:Key1', value='result_03')
        self.variable_store.add_variable(variable_name='Test3:Key2', value='result_04')
        self.variable_store.add_variable(variable_name='Test4:Key1', value='result_05')
        self.variable_store.add_variable(variable_name='Test4:Key2', value='result_06')
        self.variable_store.add_variable(variable_name='Test5:Key1:Key1', value='result_07')
        self.variable_store.add_variable(variable_name='Test5:Key1:Key2', value='result_08')
        

    def tearDown(self):
        self.task_01 = None
        self.task_02 = None
        self.task_03 = None
        self.task_04 = None
        self.task_05 = None
        self.task_06 = None
        self.variable_store = None
        return super().tearDown()

    def test_basic_substitution_01(self):
        hook = ResolveTaskSpecVariablesHook()
        result = hook.run(
            task=self.task_01,
            variable_store=copy.deepcopy(self.variable_store)
        )

        print('Resolved Spec: {}'.format(json.dumps(result.variable_store['ResolvedSpec:{}'.format(self.task_01.task_id)])))

        self.assertIsNotNone(result)
        self.assertIsInstance(result, VariableStore)
        self.assertTrue('ResolvedSpec:{}'.format(self.task_01.task_id) in result.variable_store)

        resolved_spec = result.variable_store['ResolvedSpec:{}'.format(self.task_01.task_id)]
        self.assertIsNotNone(resolved_spec)
        self.assertIsInstance(resolved_spec, dict)
        self.assertEqual(len(resolved_spec), 1)
        self.assertTrue('test1' in resolved_spec)
        self.assertEqual(resolved_spec['test1'], 'result_01')

    def test_basic_substitution_02(self):
        hook = ResolveTaskSpecVariablesHook()
        result = hook.run(
            task=self.task_02,
            variable_store=copy.deepcopy(self.variable_store)
        )

        print('Resolved Spec: {}'.format(json.dumps(result.variable_store['ResolvedSpec:{}'.format(self.task_02.task_id)])))

        self.assertIsNotNone(result)
        self.assertIsInstance(result, VariableStore)
        self.assertTrue('ResolvedSpec:{}'.format(self.task_02.task_id) in result.variable_store)
        
        resolved_spec = result.variable_store['ResolvedSpec:{}'.format(self.task_02.task_id)]
        self.assertIsNotNone(resolved_spec)
        self.assertIsInstance(resolved_spec, dict)
        self.assertEqual(len(resolved_spec), 1)
        self.assertTrue('test2' in resolved_spec)
        self.assertEqual(resolved_spec['test2'], 'result_02')

    def test_basic_substitution_03(self):
        hook = ResolveTaskSpecVariablesHook()
        result = hook.run(
            task=self.task_03,
            variable_store=copy.deepcopy(self.variable_store)
        )

        print('Resolved Spec: {}'.format(json.dumps(result.variable_store['ResolvedSpec:{}'.format(self.task_03.task_id)])))

        self.assertIsNotNone(result)
        self.assertIsInstance(result, VariableStore)
        self.assertTrue('ResolvedSpec:{}'.format(self.task_03.task_id) in result.variable_store)
        
        resolved_spec = result.variable_store['ResolvedSpec:{}'.format(self.task_03.task_id)]
        self.assertIsNotNone(resolved_spec)
        self.assertIsInstance(resolved_spec, dict)
        self.assertEqual(len(resolved_spec), 1)
        self.assertTrue('test3' in resolved_spec)
        v = resolved_spec['test3']
        self.assertIsNotNone(v)
        self.assertIsInstance(v, list)
        self.assertEqual(len(v), 2)
        self.assertTrue('result_03' in v)
        self.assertTrue('result_04' in v)

    def test_basic_substitution_04(self):
        hook = ResolveTaskSpecVariablesHook()
        result = hook.run(
            task=self.task_04,
            variable_store=copy.deepcopy(self.variable_store)
        )

        print('Resolved Spec: {}'.format(json.dumps(result.variable_store['ResolvedSpec:{}'.format(self.task_04.task_id)])))

        self.assertIsNotNone(result)
        self.assertIsInstance(result, VariableStore)
        self.assertTrue('ResolvedSpec:{}'.format(self.task_04.task_id) in result.variable_store)
        
        resolved_spec = result.variable_store['ResolvedSpec:{}'.format(self.task_04.task_id)]
        self.assertIsNotNone(resolved_spec)
        self.assertIsInstance(resolved_spec, dict)
        self.assertEqual(len(resolved_spec), 1)
        self.assertTrue('test4' in resolved_spec)
        
        t = resolved_spec['test4']
        self.assertIsNotNone(t)
        self.assertIsInstance(t, dict)
        self.assertEqual(len(t), 1)
        self.assertTrue('listTest4' in t)

        v = t['listTest4']
        self.assertIsNotNone(v)
        self.assertIsInstance(v, list)
        self.assertEqual(len(v), 4)
        self.assertTrue('result_05' in v)
        self.assertTrue('result_06' in v)
        self.assertTrue('test4_ignore' in v)
        self.assertIsNone(v[1])

    def test_basic_substitution_05(self):
        hook = ResolveTaskSpecVariablesHook()
        result = hook.run(
            task=self.task_05,
            variable_store=copy.deepcopy(self.variable_store)
        )

        print('Resolved Spec: {}'.format(json.dumps(result.variable_store['ResolvedSpec:{}'.format(self.task_05.task_id)])))

        self.assertIsNotNone(result)
        self.assertIsInstance(result, VariableStore)
        self.assertTrue('ResolvedSpec:{}'.format(self.task_05.task_id) in result.variable_store)
        
        resolved_spec = result.variable_store['ResolvedSpec:{}'.format(self.task_05.task_id)]
        self.assertIsNotNone(resolved_spec)
        self.assertIsInstance(resolved_spec, dict)
        self.assertEqual(len(resolved_spec), 1)
        self.assertTrue('test5' in resolved_spec)
        
        t = resolved_spec['test5']
        self.assertIsNotNone(t)
        self.assertIsInstance(t, dict)
        self.assertEqual(len(t), 1)
        self.assertTrue('test5_1' in t)

        u = t['test5_1']
        self.assertIsNotNone(u)
        self.assertIsInstance(u, dict)
        self.assertEqual(len(u), 4)
        self.assertTrue('test5_1_1' in u)
        self.assertEqual(u['test5_1_1'], 'result_07')
        self.assertTrue('test5_1_2' in u)
        self.assertEqual(u['test5_1_2'], 'result_08')
        self.assertTrue('test5_1_3' in u)
        self.assertEqual(u['test5_1_3'], None)
        self.assertTrue('test5_1_4' in u)
        self.assertEqual(u['test5_1_4'], 'test5_1_4_ignore')

    def test_basic_substitution_06(self):
        hook = ResolveTaskSpecVariablesHook()
        result = hook.run(
            task=self.task_06,
            variable_store=copy.deepcopy(self.variable_store)
        )

        print('Resolved Spec: {}'.format(json.dumps(result.variable_store['ResolvedSpec:{}'.format(self.task_06.task_id)])))

        self.assertIsNotNone(result)
        self.assertIsInstance(result, VariableStore)
        self.assertTrue('ResolvedSpec:{}'.format(self.task_06.task_id) in result.variable_store)

        resolved_spec = result.variable_store['ResolvedSpec:{}'.format(self.task_06.task_id)]

        self.assertIsNotNone(resolved_spec)
        self.assertIsInstance(resolved_spec, dict)
        self.assertEqual(len(resolved_spec), 5)

        self.assertTrue('test1' in resolved_spec)
        self.assertEqual(resolved_spec['test1'], 'result_01')

        self.assertTrue('test2' in resolved_spec)
        self.assertEqual(resolved_spec['test2'], 'result_02')

        self.assertTrue('test3' in resolved_spec)
        v = resolved_spec['test3']
        self.assertIsNotNone(v)
        self.assertIsInstance(v, list)
        self.assertEqual(len(v), 2)
        self.assertTrue('result_03' in v)
        self.assertTrue('result_04' in v)

        t1 = resolved_spec['test4']
        self.assertIsNotNone(t1)
        self.assertIsInstance(t1, dict)
        self.assertEqual(len(t1), 1)
        self.assertTrue('listTest4' in t1)

        v1 = t1['listTest4']
        self.assertIsNotNone(v1)
        self.assertIsInstance(v1, list)
        self.assertEqual(len(v1), 4)
        self.assertTrue('result_05' in v1)
        self.assertTrue('result_06' in v1)
        self.assertTrue('test4_ignore' in v1)
        self.assertIsNone(v1[1])

        t2 = resolved_spec['test5']
        self.assertIsNotNone(t2)
        self.assertIsInstance(t2, dict)
        self.assertEqual(len(t2), 1)
        self.assertTrue('test5_1' in t2)

        u = t2['test5_1']
        self.assertIsNotNone(u)
        self.assertIsInstance(u, dict)
        self.assertEqual(len(u), 4)
        self.assertTrue('test5_1_1' in u)
        self.assertEqual(u['test5_1_1'], 'result_07')
        self.assertTrue('test5_1_2' in u)
        self.assertEqual(u['test5_1_2'], 'result_08')
        self.assertTrue('test5_1_3' in u)
        self.assertEqual(u['test5_1_3'], None)
        self.assertTrue('test5_1_4' in u)
        self.assertEqual(u['test5_1_4'], 'test5_1_4_ignore')

    def test_method__is_iterable(self):
        hook = ResolveTaskSpecVariablesHook()
        self.assertFalse(hook._is_iterable(data='abc'))
        self.assertTrue(hook._is_iterable(data='abc', exclude_string=False))
        self.assertFalse(hook._is_iterable(data={'a': 1, 'b': 2}))
        self.assertTrue(hook._is_iterable(data={'a': 1, 'b': 2}, exclude_dict=False))
        self.assertFalse(hook._is_iterable(data=None))
        self.assertFalse(hook._is_iterable(data=datetime.now()))

    def test_method__lookup_value_invalid_key_01(self):
        hook = ResolveTaskSpecVariablesHook()
        with self.assertRaises(Exception):
            hook._lookup_value(
                raw_key='invalid',
                command=None,
                context=None,
                variable_store=self.variable_store,
                task=self.task_01
            )

    def test_method__analyse_data_01(self):
        hook = ResolveTaskSpecVariablesHook()
        original_data = datetime.now()
        result = hook._analyse_data(
            task=self.task_01,
            data=original_data,
            variable_store=self.variable_store,
            command=None,
            context=None
        )
        self.assertEqual(original_data, result)


class TestClassTaskPostProcessingStateUpdateHook(unittest.TestCase):    # pragma: no cover

    def setUp(self):
        print()
        print('-'*80)
        logger.reset()
        self.task = Task(
            api_version='DummyTaskProcessor1/v1',
            kind='DummyTaskProcessor1',
            metadata={'name': 'test-task'},
            spec={'testField': 'testValue'}
        )

    def tearDown(self):
        self.task = None
        return super().tearDown()
    
    def test_basic_01(self):
        persistence = StatePersistence()
        persistence.update_object_state(
            object_identifier='{}:TASK_STATE'.format(self.task.task_id),
            data=self.task.state.to_dict(
                with_checksums=True,
                include_applied_spec=True
            )
        )
        dump_state(task=self.task, persistence=persistence)
        tp = DummyTaskProcessor1()
        variable_store = VariableStore()
        variable_store = tp.process_task(
            task=self.task,
            persistence=persistence,
            variable_store=variable_store,
            action='CreateAction',
            task_resolved_spec={'testField': 'testValue'}
        )
        h = TaskPostProcessingStateUpdateHook()
        variable_store = h.run(
            task=self.task,
            persistence=persistence,
            variable_store=variable_store
        )

        print_logger_lines(logger=logger)
        dump_variable_store(
            test_class_name=self.__class__.__name__,
            test_method_name=stack()[0][3],
            variable_store=copy.deepcopy(variable_store)
        )
        dump_events(
            task_id=self.task.task_id,
            variable_store=copy.deepcopy(variable_store)
        )
        dump_state(task=self.task, persistence=persistence)

        self.assertIsNotNone(variable_store)
        self.assertIsInstance(variable_store, VariableStore)

        current_state = persistence.get(object_identifier='{}:TASK_STATE'.format(self.task.task_id))

        self.assertIsNotNone(current_state)
        self.assertIsInstance(current_state, dict)
        self.assertTrue(len(current_state) > 0)

        expected_data = {
            'Label': 'test-task',
            'IsCreated': True,
            'CreatedTimestamp': 1714105466,
            'SpecDrifted': None,
            'ResourceDrifted': False,
            'AppliedSpecChecksum': '1fe51362297a1e0dbdce5c51d5130bac42d6edc2ce219ed0bebd4ea05083a039',
            'CurrentResolvedSpecChecksum': '6b1791a6b1ebdffc9f2de2e7578c56c5d96c0601a95f2de48844c6f2f342a8b6',
            'AppliedResourcesChecksum': '00f5889a18ebd1aa27d2129ec1efcfda62c03f662b6a3746bec274e40814a4a5',
            'CurrentResourceChecksum': '00f5889a18ebd1aa27d2129ec1efcfda62c03f662b6a3746bec274e40814a4a5',
            'AppliedSpec': {
                'testField': 'testValue'
            }
        }

        for field_name, expected_data in expected_data.items():
            self.assertTrue(field_name in current_state)
            self.assertIsInstance(current_state[field_name], type(expected_data))

    def test_method__validate_data_missing_field_in_data_returns_false_01(self):
        h = TaskPostProcessingStateUpdateHook()
        data = {
            'resource_checksum': '00f5889a18ebd1aa27d2129ec1efcfda62c03f662b6a3746bec274e40814a4a5', 
            'resolved_spec_applied': {
                'testField': 'testValue'
            }, 
            'state_changed': True, 
            'is_created': True, 
            # 'create_timestamp': 1714106083, 
            'raw_spec': {
                'testField': 'testValue'
            }, 
            'metadata': {
                'name': 'test-task'
            }
        }
        self.assertFalse(h._validate_data(data=data))

    def test_method__validate_data_null_field_in_data_returns_false_01(self):
        h = TaskPostProcessingStateUpdateHook()
        data = {
            'resource_checksum': '00f5889a18ebd1aa27d2129ec1efcfda62c03f662b6a3746bec274e40814a4a5', 
            'resolved_spec_applied': {
                'testField': 'testValue'
            }, 
            'state_changed': None,  # ERROR 
            'is_created': True, 
            'create_timestamp': 1714106083, 
            'raw_spec': {
                'testField': 'testValue'
            }, 
            'metadata': {
                'name': 'test-task'
            }
        }
        self.assertFalse(h._validate_data(data=data))

    def test_method__validate_data_incorrect_field_type_in_data_returns_false_01(self):
        h = TaskPostProcessingStateUpdateHook()
        data = {
            'resource_checksum': '00f5889a18ebd1aa27d2129ec1efcfda62c03f662b6a3746bec274e40814a4a5', 
            'resolved_spec_applied': {
                'testField': 'testValue'
            }, 
            'state_changed': True, 
            'is_created': True, 
            'create_timestamp': datetime.now(), 
            'raw_spec': {
                'testField': 'testValue'
            }, 
            'metadata': {
                'name': 'test-task'
            }
        }
        self.assertFalse(h._validate_data(data=data))


if __name__ == '__main__':
    unittest.main()

