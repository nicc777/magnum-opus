import copy
import tempfile
import tempfile
import shutil
import os
import random
import string

from magnum_opus.operarius import *


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
        new_variable_value = None
        content = ''

        if 'outputPath' in task_resolved_spec:
            if task_resolved_spec['outputPath'] is not None:
                if isinstance(task_resolved_spec['outputPath'], str):
                    output_path = task_resolved_spec['outputPath']

        if output_path is None:
            raise Exception('The parameter "outputPath" can not be a NoneType value')
        
        elements_of_output_path = output_path.split(os.sep)
        output_filename = elements_of_output_path[-1]
        if output_filename.startswith('new_') is False:
            new_output_filename = 'new_{}'.format(output_filename)
            new_variable_value = output_path.replace(output_filename, new_output_filename)
        else:
            new_variable_value = copy.deepcopy(output_path)


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
                'create_timestamp': int(datetime.now(timezone.utc).timestamp()),
                'raw_spec': copy.deepcopy(task.spec),
                'metadata': copy.deepcopy(task.metadata),
            }
        )
        updated_variable_store.add_variable(
            variable_name=self.create_identifier(task=task, variable_name='OUTPUT'),
            value=new_variable_value
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
    

def random_string(string_length: int=16)->str:
    chars = string.ascii_uppercase + string.ascii_lowercase + string.digits
    random_str = ''
    while len(random_str) < string_length:
        random_str = '{}{}'.format(random_str, random.choice(chars))
    return random_str


def main():
    print('Starting Hello World Example')
    output_path = '{}{}hello-world.txt'.format(tempfile.gettempdir(), os.sep)

    ###
    ### At the heart of this solution is a `Task` which defines some kind of (infrastructure) resource - in this 
    ### example, a file on the local file system
    ###
    hello_world_task = Task(
        api_version='hello-world/v1',
        kind='HelloWorldV3',
        metadata={'name': 'hello-world'},
        spec={
            'outputPath': output_path,
            'content': random_string(string_length=32)
        }
    )
    task_processor_store = TaskProcessStore()
    task_processor_store.register_task_processor(task_processor=HelloWorldTaskProcessor())
    hooks = Hooks()
    hooks.add_hook(hook=TaskProcessingHook())
    we = WorkflowExecutor(task_process_store=task_processor_store)
    we.add_workflow_step_by_hook_name(hook_name='TaskProcessingHook', hooks=hooks)
    we.add_task(task=hello_world_task)

    ###
    ### Magic Happens: The task is processed which will create a file with random text 
    ###
    variable_store = we.execute_workflow(command='create', context='test')

    ###
    ### Verify Results
    ###
    key = '{}:OUTPUT'.format(hello_world_task.task_id)
    if key in variable_store.variable_store:
        result_file = variable_store.get_variable(variable_name=key)
        print('  File "{}" successfully created'.format(result_file))
        with open(output_path, 'r') as f:
            print('    RESULT: {}'.format(f.read().strip()))
    print()
    
    print('Starting cleanup')

    ###
    ### Magic Happens: We can also cleanup previous resources that were created
    ###
    variable_store = we.execute_workflow(command='delete', context='test')
    
    if os.path.exists(output_path) is False:
        print('  Deleted file "{}"'.format(output_path))
    
    print('DONE')


if __name__ == '__main__':
    main()
    

