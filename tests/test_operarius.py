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
        if self.create_identifier(task=task, variable_name='TASK_ORIGINAL_SPEC_CHECKSUM') in updated_variable_store.variable_store:
            updated_variable_store.variable_store.pop(self.create_identifier(task=task, variable_name='TASK_ORIGINAL_SPEC_CHECKSUM'))
        if self.create_identifier(task=task, variable_name='TASK_RESOLVED_SPEC_CHECKSUM') in updated_variable_store.variable_store:
            updated_variable_store.variable_store.pop(self.create_identifier(task=task, variable_name='TASK_RESOLVED_SPEC_CHECKSUM'))
        return updated_variable_store
    
    def update_action(
        self,
        task: Task,
        persistence: StatePersistence=StatePersistence(),
        variable_store: VariableStore=VariableStore()
    )->VariableStore:
        updated_variable_store = VariableStore()
        updated_variable_store.variable_store = copy.deepcopy(variable_store.variable_store)
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
        self.task_01 = None
        self.task_02 = None
        return super().tearDown()
    
    def test_basic_init_01(self):
        task_state = TaskState()
        self.assertIsNotNone(task_state)
        self.assertIsInstance(task_state, TaskState)
        self.assertFalse(task_state.is_created)

    def test_method_update_applied_spec_basic_01(self):
        # update_applied_spec
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

        task_state.update_applied_spec(new_applied_spec={}, new_applied_resource_checksum='b', updated_timestamp=2000)
        drift_results_2 = task_state.to_dict(with_checksums=True)
        print('\nDRIFT DATA: {}\n\n'.format(json.dumps(drift_results_2, default=str)))
        self.assertTrue(drift_results_2['SpecDrifted'])
        self.assertEqual(drift_results_2['CreatedTimestamp'], 2000)
        self.assertEqual(drift_results_2['AppliedResourcesChecksum'], 'b')
        self.assertEqual(drift_results_2['CurrentResourceChecksum'], 'b')


if __name__ == '__main__':
    unittest.main()

