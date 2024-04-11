"""
Running only a single test method (example):

```shell
cd tests && python3 -m unittest -k "test_hello_world.TestHelloWorldScenario.test_scenario_create_resource_and_delete_resource_1" && cd ..
```

"""
import copy
import string
import random
import sys
import os
import tempfile
import shutil
from inspect import stack
from datetime import datetime, timezone

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


def random_string(string_length: int=16)->str:
    chars = string.ascii_uppercase + string.ascii_lowercase + string.digits
    random_str = ''
    while len(random_str) < string_length:
        random_str = '{}{}'.format(random_str, random.choice(chars))
    return random_str


class HelloWorldTaskProcessor(TaskProcessor):

    def __init__(self, api_version: str='hello-world/v1'):
        super().__init__(api_version)

    def _create_backup(self, output_path: str, backup_path: str):
        if os.path.exists(backup_path) is True:
            os.unlink(backup_path)
        if os.path.exists(output_path) is True:
            shutil.copy2(output_path, backup_path)
            logger.info('Backup created of file "{}" to "{}"'.format(output_path, backup_path))
        else:
            logger.warning('File "{}" does not exist and therefore no backup can be made.'.format(output_path))
        
    def _restore_backup(self, output_path: str, backup_path: str):
        if os.path.exists(output_path) is True:
            os.unlink(output_path)
        if os.path.exists(backup_path) is True:
            shutil.copy2(backup_path, output_path)
            logger.info('Backup restored from file "{}" to "{}"'.format(backup_path, output_path))
        else:
            logger.warning('File "{}" does not exist and therefore no backup can be restored.'.format(backup_path))
        
    def _delete_backup(self, backup_path: str):
        if os.path.exists(backup_path):
            try:
                os.unlink(backup_path)
                logger.info('Backup file "{}" deleted'.format(backup_path))
            except:
                logger.error('EXCEPTION: {}'.format(traceback.format_exc()))

    def _get_resource_checksum(self, file_path: str):
        if not os.path.exists(file_path):
            raise Exception('File not found: {}'.format(file_path))
        data = ''
        with open(file_path, 'r') as f:
            data = f.read()
        return hashlib.sha256(data.encode('utf-8')).hexdigest()

    def _unittest_exception_check(self, task: Task, variable_store: VariableStore):
        force_exception = False
        key = self.create_identifier(task=task, variable_name='FORCE_UNITTEST_EXCEPTION')
        if key in variable_store.variable_store:
            logger.info('Found FORCE_UNITTEST_EXCEPTION configuration')
            value = variable_store.variable_store[key]
            if value is not None:
                if isinstance(value, bool):
                    logger.info('Using FORCE_UNITTEST_EXCEPTION configuration')
                    force_exception = value
                else:
                    logger.warning('NOT using FORCE_UNITTEST_EXCEPTION configuration - value is not a Python boolean type')
            else:
                logger.warning('NOT using FORCE_UNITTEST_EXCEPTION configuration - value is NoneType')
        if force_exception is True:
            logger.info('FORCE_UNITTEST_EXCEPTION has True value - forcing Exception')
            raise Exception('Operation Failed - Failure Forced by Unit Test Configuration')
        logger.info('FORCE_UNITTEST_EXCEPTION has False value - Normal operation continues')

    def create_action(
        self,
        task: Task,
        persistence: StatePersistence=StatePersistence(),
        variable_store: VariableStore=VariableStore(),
        task_resolved_spec: dict=dict()
    )->VariableStore:
        updated_variable_store = VariableStore()
        updated_variable_store.variable_store = copy.deepcopy(variable_store.variable_store)

        output_path = None
        content = ''

        if 'outputPath' in task_resolved_spec:
            if task_resolved_spec['outputPath'] is not None:
                if isinstance(task_resolved_spec['outputPath'], str):
                    output_path = task_resolved_spec['outputPath']

        if 'content' in task_resolved_spec:
            content = '{}'.format(task_resolved_spec['content'])

        backup_path = '{}.backup'.format(output_path)

        if os.path.exists(output_path) is True:
            try:
                with open(output_path, 'r') as f:
                    backup_data = f.read()
                    updated_variable_store.add_variable(variable_name=self.create_identifier(task=task, variable_name='PREVIOUS_CONTENT'), value=backup_data)
                os.unlink(output_path)
                logger.debug('Previous instance of file found - backing up')
            except:
                logger.error('EXCEPTION: {}'.format(traceback.format_exc()))
                logger.error('Failed to create backup from previous file...')
                raise Exception('Failed to create backup from previous file...')

        if self.create_identifier(task=task, variable_name='PREVIOUS_CONTENT') in updated_variable_store.variable_store:
            if updated_variable_store.variable_store[self.create_identifier(task=task, variable_name='PREVIOUS_CONTENT')] != content:
                logger.warning('File already exists, but content differs. Old file fill be backed up to "{}"'.format(backup_path))
                if os.path.exists(output_path) is True:
                    os.rename(output_path, backup_path)
            updated_variable_store.variable_store.pop(self.create_identifier(task=task, variable_name='PREVIOUS_CONTENT'))

        self._unittest_exception_check(task=task, variable_store=variable_store)
        with open(output_path, 'w') as f:
            f.write(content)

        updated_variable_store.add_variable(
            variable_name=self.create_identifier(task=task, variable_name='TASK_STATE_UPDATES'),
            value={
                'resource_checksum': self._get_resource_checksum(file_path=output_path),
                'resolved_spec_applied': copy.deepcopy(task_resolved_spec),
                'state_changed': True,
                'is_created': True,
                'create_timestamp': datetime.now(timezone.utc),
                'raw_spec': copy.deepcopy(task.spec),
                'metadata': copy.deepcopy(task.metadata),
            }
        )

        self._delete_backup(backup_path=backup_path)

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

        output_path = None

        if 'outputPath' in task_resolved_spec:
            if task_resolved_spec['outputPath'] is not None:
                if isinstance(task_resolved_spec['outputPath'], str):
                    output_path = task_resolved_spec['outputPath']

        backup_path = '{}.backup'.format(output_path)

        if '{}:RollbackFrom'.format(task.task_id) in updated_variable_store.variable_store:
            rollback_from = updated_variable_store.variable_store['{}:RollbackFrom'.format(task.task_id)]
            if rollback_from in ('CreateAction', 'UpdateAction', 'DeleteAction',):
                try:
                    self._restore_backup(output_path=output_path, backup_path=backup_path)
                except:
                    logger.error('EXCEPTION: {}'.format(traceback.format_exc()))
                    logger.error('Failed to roll back from previous "{}"'.format(rollback_from))
                    raise Exception('Failed to roll back from previous "{}"'.format(rollback_from))

        updated_variable_store.add_variable(
            variable_name=self.create_identifier(task=task, variable_name='TASK_STATE_UPDATES'),
            value={
                'resource_checksum': None,
                'resolved_spec_applied': None,
                'state_changed': False,
                'is_created': None,
                'create_timestamp': None,
                'raw_spec': None,
                'metadata': None,
            }
        )

        self._delete_backup(backup_path=backup_path)

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

        output_path = None

        if 'outputPath' in task_resolved_spec:
            if task_resolved_spec['outputPath'] is not None:
                if isinstance(task_resolved_spec['outputPath'], str):
                    output_path = task_resolved_spec['outputPath']

        backup_path = '{}.backup'.format(output_path)

        self._create_backup(output_path=output_path, backup_path=backup_path)
        self._unittest_exception_check(task=task, variable_store=variable_store)
        if os.path.exists(output_path) is True:
            os.unlink(output_path)

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

        self._delete_backup(backup_path=backup_path)

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

        output_path = None
        content = ''

        if 'outputPath' in task_resolved_spec:
            if task_resolved_spec['outputPath'] is not None:
                if isinstance(task_resolved_spec['outputPath'], str):
                    output_path = task_resolved_spec['outputPath']

        if 'content' in task_resolved_spec:
            content = '{}'.format(task_resolved_spec['content'])

        backup_path = '{}.backup'.format(output_path)

        if os.path.exists(output_path) is False:
            raise Exception('File does not exists')
        self._create_backup(output_path=output_path, backup_path=backup_path)
        
        os.unlink(output_path)
        with open(output_path, 'w') as f:
            f.write(content)
        self._unittest_exception_check(task=task, variable_store=variable_store)

        updated_variable_store.add_variable(
            variable_name=self.create_identifier(task=task, variable_name='TASK_STATE_UPDATES'),
            value={
                'resource_checksum': self._get_resource_checksum(file_path=output_path),
                'resolved_spec_applied': copy.deepcopy(task_resolved_spec),
                'state_changed': True,
                'is_created': True,
                'create_timestamp': datetime.now(timezone.utc),
                'raw_spec': copy.deepcopy(task.spec),
                'metadata': copy.deepcopy(task.metadata),
            }
        )

        self._delete_backup(backup_path=backup_path)

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

        output_path = None

        if 'outputPath' in task_resolved_spec:
            if task_resolved_spec['outputPath'] is not None:
                if isinstance(task_resolved_spec['outputPath'], str):
                    output_path = task_resolved_spec['outputPath']

        resource_checksum = None
        if os.path.exists(output_path):
            with open(output_path, 'r') as f:
                current_resource_content = f.read()
                resource_checksum = hashlib.sha256(current_resource_content.encode('utf-8')).hexdigest()

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

        output_path = None

        if 'outputPath' in task_resolved_spec:
            if task_resolved_spec['outputPath'] is not None:
                if isinstance(task_resolved_spec['outputPath'], str):
                    output_path = task_resolved_spec['outputPath']

        resource_checksum = None
        if os.path.exists(output_path):
            task.state.is_created = True
            ctime = os.path.getctime(output_path)
            dt_obj = datetime.fromtimestamp(ctime)
            task.state.created_timestamp = int(dt_obj.timestamp())
            with open(output_path, 'r') as f:
                current_resource_content = f.read()
                task.state.applied_resources_checksum = hashlib.sha256(current_resource_content.encode('utf-8')).hexdigest()

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
    

class TestBasicHelloWorldScenario(unittest.TestCase):    # pragma: no cover

    def setUp(self):
        print()
        print('-'*80)
        self.output_path = '{}{}hello-world.txt'.format(tempfile.gettempdir(), os.sep)
        self.hello_world_processor = HelloWorldTaskProcessor()
        self.hello_world_task = Task(
            api_version='hello-world/v1',
            kind='HelloWorldV3',
            metadata={'name': 'hello-world'},
            spec={
                'outputPath': '{}{}hello-world.txt'.format(tempfile.gettempdir(), os.sep),
                'content': random_string(string_length=256)
            }
        )

    def tearDown(self):
        if os.path.exists(self.output_path) is True:
            os.unlink(self.output_path)
        self.output_path = None
        self.hello_world_task = None
        self.hello_world_processor = None
        logger.reset()

    def test_scenario_create_resource_basic_1(self):
        variable_store = VariableStore()
        variable_store.add_variable(
            variable_name='hello-world:FORCE_UNITTEST_EXCEPTION',
            value=False
        )
        variable_store = self.hello_world_processor.process_task(
            task=copy.deepcopy(self.hello_world_task),
            action='CreateAction',
            variable_store=copy.deepcopy(variable_store),
            task_resolved_spec=copy.deepcopy(self.hello_world_task.spec)
        )

        print_logger_lines(logger=logger)
        dump_variable_store(
            test_class_name=self.__class__.__name__,
            test_method_name=stack()[0][3],
            variable_store=copy.deepcopy(variable_store)
        )
        dump_events(
            task_id=self.hello_world_task.task_id,
            variable_store=copy.deepcopy(variable_store)
        )

        self.assertIsNotNone(variable_store)
        self.assertIsInstance(variable_store, VariableStore)
        self.assertTrue(os.path.exists(self.output_path))

        data = ''
        with open(self.output_path, 'r') as f:
            data = f.read()
        self.assertEqual(data, self.hello_world_task.spec['content'])

    def test_scenario_create_resource_and_delete_resource_1(self):
        # Phase 1 Test - Create Resource
        variable_store = VariableStore()
        variable_store.add_variable(
            variable_name='hello-world:FORCE_UNITTEST_EXCEPTION',
            value=False
        )
        variable_store = self.hello_world_processor.process_task(
            task=copy.deepcopy(self.hello_world_task),
            action='CreateAction',
            variable_store=copy.deepcopy(variable_store),
            task_resolved_spec=copy.deepcopy(self.hello_world_task.spec)
        )

        print_logger_lines(logger=logger)
        dump_variable_store(
            test_class_name=self.__class__.__name__,
            test_method_name=stack()[0][3],
            variable_store=copy.deepcopy(variable_store)
        )
        dump_events(
            task_id=self.hello_world_task.task_id,
            variable_store=copy.deepcopy(variable_store)
        )

        self.assertIsNotNone(variable_store)
        self.assertIsInstance(variable_store, VariableStore)
        self.assertTrue(os.path.exists(self.output_path))

        # Phase 2 Test - Delete Resource

        variable_store = VariableStore()
        variable_store.add_variable(
            variable_name='hello-world:FORCE_UNITTEST_EXCEPTION',
            value=False
        )
        variable_store = self.hello_world_processor.process_task(
            task=copy.deepcopy(self.hello_world_task),
            action='DeleteAction',
            variable_store=copy.deepcopy(variable_store),
            task_resolved_spec=copy.deepcopy(self.hello_world_task.spec)
        )

        print_logger_lines(logger=logger)
        dump_variable_store(
            test_class_name=self.__class__.__name__,
            test_method_name=stack()[0][3],
            variable_store=copy.deepcopy(variable_store)
        )
        dump_events(
            task_id=self.hello_world_task.task_id,
            variable_store=copy.deepcopy(variable_store)
        )

        self.assertIsNotNone(variable_store)
        self.assertIsInstance(variable_store, VariableStore)
        self.assertFalse(os.path.exists(self.output_path))


    def test_scenario_create_resource_and_force_rollback_via_exception_1(self):
        variable_store = VariableStore()
        variable_store.add_variable(
            variable_name='hello-world:FORCE_UNITTEST_EXCEPTION',
            value=True
        )
        self.hello_world_task.metadata['autoRollback'] = True
        with self.assertRaises(Exception):
            variable_store = self.hello_world_processor.process_task(
                task=copy.deepcopy(self.hello_world_task),
                action='CreateAction',
                variable_store=copy.deepcopy(variable_store),
                task_resolved_spec=copy.deepcopy(self.hello_world_task.spec)
            )

        print_logger_lines(logger=logger)
        dump_variable_store(
            test_class_name=self.__class__.__name__,
            test_method_name=stack()[0][3],
            variable_store=copy.deepcopy(variable_store)
        )
        dump_events(
            task_id=self.hello_world_task.task_id,
            variable_store=copy.deepcopy(variable_store)
        )

        self.assertIsNotNone(variable_store)
        self.assertIsInstance(variable_store, VariableStore)
        self.assertFalse(os.path.exists(self.output_path))

    def test_scenario_delete_resource_and_force_rollback_via_exception_1(self):
        variable_store = VariableStore()
        variable_store.add_variable(
            variable_name='hello-world:FORCE_UNITTEST_EXCEPTION',
            value=False
        )
        self.hello_world_task.metadata['autoRollback'] = True
        variable_store = self.hello_world_processor.process_task(
            task=copy.deepcopy(self.hello_world_task),
            action='CreateAction',
            variable_store=copy.deepcopy(variable_store),
            task_resolved_spec=copy.deepcopy(self.hello_world_task.spec)
        )

        self.assertIsNotNone(variable_store)
        self.assertIsInstance(variable_store, VariableStore)
        self.assertTrue(os.path.exists(self.output_path))

        variable_store.add_variable(
            variable_name='hello-world:FORCE_UNITTEST_EXCEPTION',
            value=True
        )
        with self.assertRaises(Exception):    
            variable_store = self.hello_world_processor.process_task(
                task=copy.deepcopy(self.hello_world_task),
                action='DeleteAction',
                variable_store=copy.deepcopy(variable_store),
                task_resolved_spec=copy.deepcopy(self.hello_world_task.spec)
            )
        self.assertTrue(os.path.exists(self.output_path))

        data = ''
        with open(self.output_path, 'r') as f:
            data = f.read()
        self.assertEqual(data, self.hello_world_task.spec['content'])

        print_logger_lines(logger=logger)
        dump_variable_store(
            test_class_name=self.__class__.__name__,
            test_method_name=stack()[0][3],
            variable_store=copy.deepcopy(variable_store)
        )
        dump_events(
            task_id=self.hello_world_task.task_id,
            variable_store=copy.deepcopy(variable_store)
        )


class TestHelloWorldWithResolveTaskSpecVariablesHookScenario(unittest.TestCase):    # pragma: no cover

    def setUp(self):
        print()
        print('-'*80)
        self.output_path = '{}{}hello-world.txt'.format(tempfile.gettempdir(), os.sep)
        self.hello_world_processor = HelloWorldTaskProcessor()
        self.hello_world_task = Task(
            api_version='hello-world/v1',
            kind='HelloWorldV3',
            metadata={'name': 'hello-world'},
            spec={
                'outputPath': '{}{}hello-world.txt'.format(tempfile.gettempdir(), os.sep),
                'content': '${}VAR:some-other-task:OUTPUT{}'.format('{', '}')
            }
        )

    def tearDown(self):
        if os.path.exists(self.output_path) is True:
            os.unlink(self.output_path)
        self.output_path = None
        self.hello_world_task = None
        self.hello_world_processor = None
        logger.reset()

    def test_hook_functionality_resolving_a_known_variable_1(self):
        variable_store = VariableStore()
        variable_store.add_variable(
            variable_name='some-other-task:OUTPUT',
            value='Resolved Value for Unit Testing'
        )
        hook = ResolveTaskSpecVariablesHook()
        result = hook.run(
            task=self.hello_world_task,
            parameters={
                'Command': 'apply',
                'Context': 'unittest'
            },
            variable_store=variable_store
        )
        self.assertIsNotNone(result)
        self.assertIsInstance(result, VariableStore)

        print_logger_lines(logger=logger)
        dump_variable_store(
            test_class_name=self.__class__.__name__,
            test_method_name=stack()[0][3],
            variable_store=copy.deepcopy(result)
        )
        dump_events(
            task_id=self.hello_world_task.task_id,
            variable_store=copy.deepcopy(result)
        )

        expected_resolved_spec_key = 'ResolvedSpec:{}'.format(self.hello_world_task.task_id)
        self.assertTrue(expected_resolved_spec_key in result.variable_store)
        data = result.variable_store[expected_resolved_spec_key]
        self.assertIsNotNone(data)
        self.assertIsInstance(data, dict)
        self.assertTrue('outputPath' in data)
        self.assertTrue('content' in data)
        self.assertEqual(data['outputPath'], self.hello_world_task.spec['outputPath'])
        self.assertNotEqual(data['content'], self.hello_world_task.spec['content'])
        self.assertEqual(data['content'], 'Resolved Value for Unit Testing')


if __name__ == '__main__':
    unittest.main()
