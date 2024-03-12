import copy
import tempfile
import string
import random
from pathlib import Path
import sys
import os

sys.path.append(os.path.dirname(os.path.realpath(__file__)) + "/../src")
print('sys.path={}'.format(sys.path))

import unittest

from magnum_opus.operarius import KeyValueStore, LoggerWrapper, StatePersistence, Task, TaskProcessor, Tasks

running_path = os.getcwd()
print('Current Working Path: {}'.format(running_path))


class TestLogger(LoggerWrapper):    # pragma: no cover

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
        self.info_lines = list()
        self.warn_lines = list()
        self.debug_lines = list()
        self.critical_lines = list()
        self.error_lines = list()


def random_string(string_length: int=16)->str:
    chars = string.ascii_uppercase + string.ascii_lowercase + string.digits
    random_str = ''
    while len(random_str) < string_length:
        random_str = '{}{}'.format(random_str, random.choice(chars))
    return random_str


class HelloWorldTaskProcessor(TaskProcessor):

    def __init__(self, kind: str='HelloWorld', kind_versions: list=['v1',], supported_commands: list = ['apply',], logger: LoggerWrapper = LoggerWrapper()):
        super().__init__(kind, kind_versions, supported_commands, logger)

    def process_task(self, task: Task, command: str, context: str = 'default', key_value_store: KeyValueStore = KeyValueStore(), state_persistence: StatePersistence = StatePersistence()) -> KeyValueStore:
        updated_key_Value_store = KeyValueStore()
        updated_key_Value_store.store = copy.deepcopy(key_value_store.store)
        output_file: str
        output_file = '{}{}{}.txt'.format(tempfile.gettempdir(), os.sep, random_string(string_length=32))
        if 'file' in task.spec:
            output_file = '{}'.format(task.spec['file'])
        with open(output_file, 'w') as f:
            f.write('Hello World!')
        updated_key_Value_store.save(key='hello_world_file', value=output_file)
        self.logger.info(message='Written file "{}"'.format(output_file))
        return updated_key_Value_store
    

class TestHelloWorldScenario(unittest.TestCase):    # pragma: no cover

    def setUp(self):
        print()
        print('-'*80)

    def test_run_scenario_1(self):
        values = KeyValueStore()
        logger = TestLogger()
        tasks = Tasks(key_value_store=values, logger=logger)
        tasks.register_task_processor(processor=HelloWorldTaskProcessor())
        tasks.add_task(
            task=Task(
                kind='HelloWorld',
                version='v1',
                spec={
                    'file': '{}{}{}.txt'.format(str(Path.home()), os.sep, random_string(string_length=16))
                }
            )
        )
        tasks.process_context(command='apply', context='ANY')
        values = tasks.key_value_store

        self.assertTrue('hello_world_file' in values.store)

        logger.info(message='File written to "{}".'.format(values.store['hello_world_file']))
        lines = list()
        with open(values.store['hello_world_file'], 'r') as f:
            lines = f.readlines()
        self.assertTrue(len(lines) > 0)
        self.assertTrue('Hello World!' in lines[0])

        last_log_line = logger.info_lines[-1]
        self.assertTrue(values.store['hello_world_file'] in last_log_line)

        for line in logger.all_lines_in_sequence:
            print('[LOG] >> {}'.format(line))



if __name__ == '__main__':
    unittest.main()
