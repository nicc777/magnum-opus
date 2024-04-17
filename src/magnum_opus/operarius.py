from collections.abc import Sequence
import copy
import logging
import traceback
import json
import hashlib
from datetime import datetime, timezone
import re


class LocalLogger:

    def __init__(self, logger):
        self.logger = logger

    def get_logger(self):
        return self.logger


local_logger = LocalLogger(logger=logging.getLogger(__name__))
logger = local_logger.get_logger()


def override_logger(logger_class: object):
    global logger
    local_logger = LocalLogger(logger=logger_class)
    logger = None
    logger = local_logger.get_logger()


def produce_column_headers(with_checksums: bool=False, space_len: int=2)->str:
    """Produce a string of formatted column headers, ideal for `TaskState` output in human readable column format.

    Args:
        with_checksums: boolean (default=False) - If True. include checksum columns
        space_len: int (default=2). The number of spaces between column boundaries

    Returns:
        A string with the column headings
    """
    space                               = ' '*space_len
    #                                                                  Index #      Label Length    Final Field Length
    label                               = 'Manifest'                    # 0         len = 8             16
    is_created                          = 'Created'                     # 2         len = 7             7
    created_datetime                    = 'Created Timestamp'           # 4         len = 17            25
    spec_drifted                        = 'Spec Drifted'                # 6         len = 12            17
    resource_drifted                    = 'Resources Drifted'           # 8         len = 17            17
    applied_spec_checksum               = 'Applied Spec CHecksum'       # 10        len = 21            32
    current_resolved_spec_checksum      = 'Current Spec Checksum'       # 12        len = 21            32
    applied_resource_checksum           = 'Applied Resource Checksum'   # 14        len = 25            32
    spec_resource_expectation_checksum  = 'Current Resource Checksum'   # 16        len = 25            32

    report_column_header                = '{0:<16}{1}{2:<7}{3}{4:<25}{5}{6:<17}{7}{8:<17}'.format(label, space, is_created, space, created_datetime, space, spec_drifted, space, resource_drifted)
    report_column_header_with_checksums = '{0:<16}{1}{2:<7}{3}{4:<25}{5}{6:<17}{7}{8:<17}{9}{10:<32}{11}{12:<32}{13}{14:<32}{15}{16:<32}'.format(label, space, is_created, space, created_datetime, space, spec_drifted, space, resource_drifted, space, applied_spec_checksum, space, current_resolved_spec_checksum, space, applied_resource_checksum, space, spec_resource_expectation_checksum)

    if with_checksums is True:
        return report_column_header_with_checksums
    return report_column_header


def produce_column_header_horizontal_line(with_checksums: bool=False, space_len: int=2, line_char: str='-')->str:
    """Function description

    Args:
        with_checksums: boolean (default=False) - If True. include checksum columns in total length calculation
        space_len: int (default=2). The number of spaces between column boundaries, used in total length calculation
        line_char: str (default='-'). The character to be used to create the horizontal line

    Returns:
        A string with a horizontal line as a text string
    """
    short_len = 16+7+25+17+17
    long_len = short_len + 32 + 32 + 32 + 32
    final_len = short_len + (space_len*4)       # 90 characters, using defaults
    if with_checksums is True:
        final_len = long_len + (space_len*8)    # 226 characters, using defaults
    return '{}'.format(line_char)*final_len


class TaskState:
    """A helper class to track state of various elements that may help to determine what kind of changes is required
    when executing task commands in a particular environment.

    The initial values are generally set during the loading of Task state via `StatePersistence`.

    The aim of this class is to track the `Task` spec and physical resource states that will also aid in determining the
    changes required when tasks are processed.

    The class also includes the ability to produce a summary in human readable format of the current spec and resource
    states and to indicate if any drift occurred.

    There are essentially two types of drift that can occur:

    * Spec drift - where the last applied `Task` spec is different from the current spec.
    * Resource drift - where the last applied or created resources somehow differ from the current resources.

    Important to note the following about drift:

    * Even through spec drift may be present, it does not necessarily mean changes are required for the resources. Each `TaskProcessor` may approach this differently when determining if changes should be applied.    
    * The exact changes required for resources might not be known until after changes were applied by the `TaskProcessor`.

    Attributes:
        raw_spec: The current task raw spec, which may include dynamic variables which is not yet resolved.
        raw_metadata: The current task raw metadata dict
        report_label: Some label that can be printed on reports. This is usually the `Task` ID.
        created_timestamp: A Unix timestamp of the last recorded timestamp the task manifest was applied (created or updated). A value of "0" (zero) indicates that this `Task` have never been deployed (created or updated) within the specific context. It may also mean that a previous implementation was deleted.
        applied_spec: The `Task` spec, with all variables resolved, that was applied previously (if applicable).
        current_resolved_spec: The current `Task` spec with all variables resolved.
        is_created: A boolean flag to indicate if the current `Task` is currently in a "created" state given the current context.
        applied_resources_checksum: A SHA256 checksum of the last known applied `applied_spec` dict.
        current_resource_checksum: A SHA256 string of the calculated deployed/created resource state after he previous applied spec. Future resource checksum calculation should yield the same result and any change in the calculated value typically translates to some drift detection.
    """

    def __init__(
        self,
        manifest_spec: dict=dict(),
        applied_spec: dict=dict(),
        resolved_spec: dict=dict(),
        manifest_metadata: dict=dict(),
        report_label: str='',
        created_timestamp: int=0,
        applied_resources_checksum: str=None,
        current_resource_checksum: str=None
    ):
        """Initializes the instance with some optional initial attribute values.

        Args:
            manifest_spec: A dict containing the raw spec (no variables resolved)
            applied_spec: A dict with the spec as applied to create resources, with all variables resolved to their final values
            resolved_spec: A dict with the current spec with all variables resolved to their final values
            manifest_metadata: A dict with the current manifest metadata
            report_label: A string containing a label, typically the task ID
            created_timestamp: An integer with the Unix timestamp of when the resources were created. If resources were created, this value should be greater than 0
            applied_resources_checksum: A string with the SHA256 checksum after the initial resource creation process was completed
            current_resource_checksum: A string with the SHA256 checksum of the current state of running resources. A difference to the `applied_resources_checksum` could indicate some changes in the deployed resources that was not applied through task processing.
        """
        self.raw_spec = manifest_spec
        self.raw_metadata = manifest_metadata
        self.report_label = report_label
        self.created_timestamp = created_timestamp
        self.applied_spec = applied_spec
        self.current_resolved_spec = resolved_spec
        self.is_created = False
        if applied_spec is not None:
            if len(applied_spec) > 0 or created_timestamp > 0:
                self.is_created = True
        self.applied_resources_checksum = applied_resources_checksum
        self.current_resource_checksum = current_resource_checksum

    def update_applied_spec(self, new_applied_spec: dict, new_applied_resource_checksum: str, updated_timestamp: int):
        self.applied_spec = copy.deepcopy(new_applied_spec)
        self.applied_resources_checksum = copy.deepcopy(new_applied_resource_checksum)
        self.created_timestamp = updated_timestamp
        self.is_created = False
        if new_applied_spec is not None:
            if len(new_applied_spec) > 0 or updated_timestamp > 0:
                self.is_created = True

    def calculate_manifest_state_checksum(self, spec: dict=dict(), metadata: dict=dict())->str:
        """Helper class to calculate the SHA256 checksum of the provided
        dictionaries

        Args:
            spec: A dictionary
            metadata: A dictionary

        Returns:
            A string with the SHA256 checksum of the two provided dictionaries.
        """
        data = {
            'spec': spec,
            'metadata': metadata
        }
        return hashlib.sha256(json.dumps(data).encode('utf-8')).hexdigest()
    
    def to_dict(
            self,
            human_readable: bool=False,
            current_resolved_spec: dict=None,
            current_resource_checksum: str=None,
            with_checksums: bool=False,
            include_applied_spec: bool=False
        )->dict:
        """Converts the current state into a dictionary.

        Key phrases:

        * Resolved spec: A dict containing the spec where all variable placeholders have been replaced by final values.
        * Resource checksum: refers to a calculated values based on existing resources. For example, a deployed virtual machine may use a combination of hostname and network interface MAC address as inputs to generate the checksum.

        Args:
            human_readable: Boolean (default=False). If set to True, values like booleans and nulls will be converted to more appropriate english descriptive terms
            current_resolved_spec: Optional dict that will replace the current resolved spec with an updated version on which calculations will be performed (replaces `self.current_resolved_spec`). This will be used to determine the drift from the last applied spec.
            current_resource_checksum: An optional string with the SHA256 checksum of resources under management of this task
            with_checksums: In the final result, include a summary of all checksums
            include_applied_spec: In the final result, include a copy of the last applied spec.

        Returns:
            A dictionary with data representing the state. The following are examples:

            Basic output:

            ```json
            {
                "Label": "some-task-id",
                "IsCreated": false,
                "CreatedTimestamp": null,
                "SpecDrifted": false,
                "ResourceDrifted": null
            }
            ```

            Extended, human readable data:

            ```json
            {
                "Label": "some-task-id",
                "IsCreated": "Yes",
                "CreatedTimestamp": "1970-01-01 01:16:40",
                "SpecDrifted": "No",
                "ResourceDrifted": "No",
                "AppliedSpecChecksum": "83b5...e2ac10",
                "CurrentResolvedSpecChecksum": "83b5...e2ac10",
                "AppliedResourcesChecksum": "9f86...00a08",
                "CurrentResourceChecksum": "9f86...00a08"
            }
            ```
        """
        data = dict()
        data['Label'] = self.report_label
        data['IsCreated'] = self.is_created
        if human_readable is True:
            data['IsCreated'] = 'No'
            if self.is_created is True:
                data['IsCreated'] = 'Yes'
        data['CreatedTimestamp'] = None
        if self.is_created is True and self.created_timestamp > 0:
            data['CreatedTimestamp'] = self.created_timestamp
            if human_readable is True:
                data['CreatedTimestamp'] = datetime.fromtimestamp(self.created_timestamp).strftime('%Y-%m-%d %H:%M:%S %z').strip()
        elif self.is_created is False or self.created_timestamp == 0:
            if human_readable is True:
                data['CreatedTimestamp'] = '-'
        data['SpecDrifted'] = None
        if human_readable is True:
            data['SpecDrifted'] = 'Unknown'

        if current_resolved_spec is not None:
            if isinstance(current_resolved_spec, dict):
                self.current_resolved_spec = current_resolved_spec

        if current_resource_checksum is not None:
            if isinstance(current_resource_checksum, str):
                self.current_resource_checksum = current_resource_checksum

        if human_readable is True:
            data['SpecDrifted'] = 'No'
        if self.is_created is True:
            if self.current_resolved_spec is not None and self.applied_spec is not None:
                if self.calculate_manifest_state_checksum(spec=self.applied_spec) != self.calculate_manifest_state_checksum(spec=self.current_resolved_spec):
                    data['SpecDrifted'] = True
                    if human_readable is True:
                        data['SpecDrifted'] = 'Yes'
        else:
            if human_readable is True:
                data['SpecDrifted'] = 'N/A'
            else:
                data['SpecDrifted'] = None

        data['ResourceDrifted'] = None
        if human_readable is True:
            data['ResourceDrifted'] = 'Unknown'
        if self.is_created is True:
            if self.applied_resources_checksum is not None:
                data['ResourceDrifted'] = True
                if human_readable is True:
                    data['ResourceDrifted'] = 'Yes'
                if self.current_resource_checksum is not None:
                    if self.current_resource_checksum == self.applied_resources_checksum:
                        data['ResourceDrifted'] = False
                        if human_readable is True:
                            data['ResourceDrifted'] = 'No'
        else:
            if human_readable is True:
                data['ResourceDrifted'] = 'N/A'
            else:
                data['ResourceDrifted'] = None

        if with_checksums is True:
            data['AppliedSpecChecksum'] = None
            data['CurrentResolvedSpecChecksum'] = None
            data['AppliedResourcesChecksum'] = None
            data['CurrentResourceChecksum'] =None
            if human_readable is True:
                data['AppliedSpecChecksum'] = 'unavailable'
                data['CurrentResolvedSpecChecksum'] = 'unavailable'
                data['AppliedResourcesChecksum'] = 'unavailable'
                data['CurrentResourceChecksum'] = 'unavailable'

            if self.applied_spec is not None:
                if isinstance(self.applied_spec, dict) is True and self.is_created is True:
                    data['AppliedSpecChecksum'] = self.calculate_manifest_state_checksum(spec=self.applied_spec)

            if self.current_resolved_spec is not None:
                if isinstance(self.current_resolved_spec, dict) is True:
                    data['CurrentResolvedSpecChecksum'] = self.calculate_manifest_state_checksum(spec=current_resolved_spec)
            
            if self.applied_resources_checksum is not None:
                if isinstance(self.applied_resources_checksum, str) is True and self.is_created is True:
                    data['AppliedResourcesChecksum'] = self.applied_resources_checksum

            if self.current_resource_checksum is not None:
                if isinstance(self.current_resource_checksum, str) is True and self.is_created is True:
                    data['CurrentResourceChecksum'] = self.current_resource_checksum
            
        if include_applied_spec:
            data['AppliedSpec'] = self.applied_spec
        return data
    
    def _cut_str(self, input_str: str, max_len: int=32)->str:
        final_str = '{}'.format(input_str)
        if len(final_str) > max_len:
            final_str = final_str[0:max_len]
        return final_str

    def column_str(
        self,
        human_readable: bool=False,
        current_resolved_spec: dict=None,
        current_resource_checksum: str=None,
        with_checksums: bool=False,
        space_len: int=2
    )->str:
        """Based on the supplied parameters, produce a columnar output of the appropriate fields.

        To produce a short report of a single task, a typical example would be the following:

        ```python
        report = '{}\n{}\n{}'.format(
            produce_column_headers(with_checksums=True),
            produce_column_header_horizontal_line(with_checksums=True, line_char='+'),
            ts.column_str(human_readable=True, current_resolved_spec={...}, with_checksums=True)
        )
        print(report)
        ```

        To print the status of multiple `TaskState` instances:

        ```python
        list_of_task_state_instances = [...]
        print('{}\n{}\n{}'.format(
            produce_column_headers(with_checksums=True),
            produce_column_header_horizontal_line(with_checksums=True, line_char='=')
        )
        for ts in list_of_task_state_instances:
            print(ts.column_str(human_readable=True, current_resolved_spec={...}, with_checksums=True))
        ```

        Args:
            human_readable: Boolean (default=False). If set to True, values like booleans and nulls will be converted to more appropriate english descriptive terms
            current_resolved_spec: Optional dict that will replace the current resolved spec with an updated version on which calculations will be performed (replaces `self.current_resolved_spec`). This will be used to determine the drift from the last applied spec.
            current_resource_checksum: An optional string with the SHA256 checksum of resources under management of this task
            with_checksums: In the final result, include a summary of all checksums
            space_len: An integer (default `2`) for the number of space characters spaces between column boundaries.

        Returns:
            A string with the data in well formatted columns.
        """
        data = self.to_dict(human_readable=human_readable, current_resolved_spec=current_resolved_spec, current_resource_checksum=current_resource_checksum, with_checksums=with_checksums)
        label = self._cut_str(input_str=data['Label'], max_len=16)
        is_created = self._cut_str(input_str='{}'.format(data['IsCreated']), max_len=7)
        created_datetime = self._cut_str(input_str='{}'.format(data['CreatedTimestamp']), max_len=25) # max: 0000-00-00 00:00:00 +0000 (25 characters)
        spec_drifted = self._cut_str(input_str=data['SpecDrifted'], max_len=17)
        resource_drifted = self._cut_str(input_str=data['ResourceDrifted'], max_len=17)
        space = ' '*space_len
        if with_checksums is True:
            applied_spec_checksum = 'unavailable'
            current_resolved_spec_checksum ='unavailable'
            applied_resource_checksum = 'unavailable'
            current_resource_checksum = 'unavailable'
            if 'AppliedSpecChecksum' in data:
                applied_spec_checksum = data['AppliedSpecChecksum'][0:32]
            if 'CurrentResolvedSpecChecksum' in data:
                current_resolved_spec_checksum = data['CurrentResolvedSpecChecksum'][0:32]
            if 'AppliedResourcesChecksum' in data:
                applied_resource_checksum = data['AppliedResourcesChecksum'][0:32]
            if 'CurrentResourceChecksum' in data:
                current_resource_checksum = data['CurrentResourceChecksum'][0:32]
            #         0     1   2    3   4     5   6    7    8     9    10    11    12    13    14    15    16     ||||     0      1      2           3       4                5      6             7       8                9      10                     11     12                              13     14                         15     16
            return '{0:<16}{1}{2:<7}{3}{4:<25}{5}{6:<17}{7}{8:<17}{9}{10:<32}{11}{12:<32}{13}{14:<32}{15}{16:<32}'.format(label, space, is_created, space, created_datetime, space, spec_drifted, space, resource_drifted, space, applied_spec_checksum, space, current_resolved_spec_checksum, space, applied_resource_checksum, space, current_resource_checksum)
        return '{0:<16}{1}{2:<7}{3}{4:<25}{5}{6:<17}{7}{8:<17}'.format(label, space, is_created, space, created_datetime, space, spec_drifted, space, resource_drifted)
    
    def __str__(self)->str:
        return '{}\n{}\n{}'.format(
            produce_column_headers(),
            produce_column_header_horizontal_line(),
            self.column_str(human_readable=True, current_resolved_spec=self.current_resolved_spec)
        )

    def __repr__(self)->str:
        return json.dumps(self.to_dict(with_checksums=True, include_applied_spec=True))


class StatePersistence:

    """If the client requires any form of persistance, this class must be implemented with the required logic by the 
    client. 

    Without any client implementation, this class is mostly just a memory cache at runtime with no long term
    persistence.

    An instance of the `StatePersistence` class will be passed as a parameter to each `TaskProcessor` during processing.
    This can be useful for the task processing steps to determine the exact actions to take. Updated state could then be
    persisted long term for future task processing runs.

    Attributes:
        logger: An implementation of the `LoggerWrapper` class
        state_cache: A dict with the current state
        configuration: A dict holding configuration data, intended for use for client implementations of this class, for example DB credentials.
    """

    def __init__(self, configuration: dict=dict()):
        self.logger = logger
        self.state_cache = dict()
        self.configuration = configuration
        self.load()

    def load(self, on_failure: object=False)->bool:
        """This method must return all long term persisted state from some backend storage service, or local disc drive.

        A client must implement this method with the logic to retrieve persisted data.

        Args:
            on_failure: An object to return (or Exception to be thrown) on failure to retrieve the persisted data.

        Returns:
            A boolean to state the success (True) or the value of `on_failure`, provided the type of `on_failure` is not 
            an Exception.

            A simple example to throw an exception if the retrieval of persisted data failed:

            ```python
            p = StatePersistence(configuration={'path': '/data/persisted_data.json'})
            p.load(on_failure=Exception('Failed to retrieve data from "{}"'.format(p.configuration['path'])))
            ```

        Raises:
            Exception: If retrieval of data failed and `on_failure` is of type `Exception`
        """
        logger.warning('StatePersistence.retrieve_all_state_from_persistence() NOT IMPLEMENTED. Override this function in your own class for long term state storage.')
        if isinstance(on_failure, Exception):
            raise on_failure
        return on_failure

    def get(self, object_identifier: str, refresh_cache_if_identifier_not_found: bool=True)->dict:
        """Retrieves state of a given identifier from the cache.

        BY default, If the key (identifier) is not found in the cache, the cache will first be refreshed and then one 
        more attempt to retrieve the value will be made.

        It is not required by the client to override this method, unless different logic is required.

        Args:
            object_identifier: The identifier of the data to retrieve. This is the same key as is provided when calling `save_object_state()` to persist data.

        Returns:
            A dict with data is returned. If no data is found, the dict will be empty.
        """
        if object_identifier in self.state_cache:
            return copy.deepcopy(self.state_cache[object_identifier])
        elif refresh_cache_if_identifier_not_found is True:
            self.load()
            if object_identifier in self.state_cache:
                return copy.deepcopy(self.state_cache[object_identifier])
        return dict()

    def update_object_state(self, object_identifier: str, data: dict):
        """Save a dict object with a given key

        This method must ideally be overridden by the client as it needs to implement the logic of saving data long 
        term.

        When implementing the method, the current line of logic should be kept in order to refresh the local cache with
        the new data value,

        Args:
            object_identifier: The identifier of the data to retrieve.
            data: A dict with the data. The client would typically convert this to a JSON string for saving.
        """
        self.state_cache[object_identifier] = copy.deepcopy(data)

    def commit(self):
        """Save all state in one go.

        This method must ideally be overridden by the client as it needs to implement the logic of saving data long 
        term.

        The default action should the client not override this method is to loop through all items in the local cache
        and call `save_object_state()` on each one individually.
        """
        logger.warning('StatePersistence.commit() NOT IMPLEMENTED. Override this function in your own class for long term state storage.')


class ParameterValidation:

    def __init__(self, constraints: object):
        self.constraints = constraints

    def validation_passed(self, parameters: dict=dict())->bool:
        return True


class TaskProcessingActionParameterValidation(ParameterValidation):

    def __init__(self, constraints: object):
        final_constraints = dict()
        final_constraints['SupportedCommands'] = list()
        final_constraints['SupportedContexts'] = list()
        final_constraints['SupportedActions'] =  list()
        if constraints is not None:
            if isinstance(constraints, dict):
                if 'SupportedCommands' in constraints:
                    if isinstance(constraints['SupportedCommands'], list) is True:
                        for command in constraints['SupportedCommands']:
                            if command is not None:
                                if isinstance(command, str):
                                    if len(command) > 0:
                                        final_constraints['SupportedCommands'].append(command)
                if 'SupportedContexts' in constraints:
                    if isinstance(constraints['SupportedContexts'], list) is True:
                        for context in constraints['SupportedContexts']:
                            if context is not None:
                                if isinstance(context, str):
                                    if len(context) > 0:
                                        final_constraints['SupportedContexts'].append(context)
                if 'SupportedActions' in constraints:
                    if isinstance(constraints['SupportedActions'], list) is True:
                        for action in constraints['SupportedActions']:
                            if action is not None:
                                if isinstance(action, str):
                                    if len(action) > 0:
                                        final_constraints['SupportedActions'].append(action)
        if len(final_constraints['SupportedActions']) == 0:
            final_constraints['SupportedActions'] = ['CreateAction','RollbackAction','DeleteAction','UpdateAction','DescribeAction','DetectDriftAction',]
        super().__init__(final_constraints)

    def add_command(self, command: str):
        if command is not None:
            if isinstance(command, str) is True:
                if len(command) > 0:
                    self.constraints['SupportedCommands'].append(command)
        return self
    
    def add_context(self, context: str):
        if context is not None:
            if isinstance(context, str) is True:
                if len(context) > 0:
                    self.constraints['SupportedContexts'].append(context)
        return self

    def validation_passed(self, parameters: dict = dict()) -> bool:
        command = None
        context = None
        action = None
        final_command_constraints = list()
        final_context_constraints = list()
        
        if parameters is None:
            logger.warning('Parameters was NoneType - expected a dict')
            return False
        if isinstance(parameters, dict) is False:
            logger.warning('Parameters was {} - expected a dict'.format(type(parameters)))
            return False

        if 'Command' in parameters:
            if parameters['Command'] is not None:
                if isinstance(parameters['Command'], str) is True:
                    if len(parameters['Command']) > 0:
                        command = parameters['Command']
        if 'Context' in parameters:
            if parameters['Context'] is not None:
                if isinstance(parameters['Context'], str) is True:
                    if len(parameters['Context']) > 0:
                        context = parameters['Context']
        if 'Action' in parameters:
            if parameters['Action'] is not None:
                if isinstance(parameters['Action'], str) is True:
                    if len(parameters['Action']) > 0:
                        action = parameters['Action']
        if 'ResolvedSpec' not in parameters:
            logger.warning('ResolvedSpec was not present in parameters - the Task spec will be used as is')

        if len(self.constraints['SupportedCommands']) == 0:
            final_command_constraints = [copy.deepcopy(command),]
        else:
            final_command_constraints = self.constraints['SupportedCommands']
        if len(self.constraints['SupportedContexts']) == 0:
            final_context_constraints = [copy.deepcopy(context),]
        else:
            final_command_constraints = self.constraints['SupportedContexts']

        logger.debug('Final command constraints : {}'.format(final_command_constraints))
        logger.debug('Final context constraints : {}'.format(final_context_constraints))
        logger.debug('Final action constraints  : {}'.format(self.constraints['SupportedActions']))
        logger.debug('Input command             : {}'.format(command))
        logger.debug('Input context             : {}'.format(context))
        logger.debug('Input action              : {}'.format(action))

        if command not in final_command_constraints:
            for catch_all_str in ('*', 'ALL', 'ANY'):
                if command != catch_all_str:
                    logger.warning('SupportedCommands validation failed')
                    return False
        if context not in final_context_constraints:
            for catch_all_str in ('*', 'ALL', 'ANY'):
                if context != catch_all_str:
                    logger.warning('SupportedContexts validation failed')
                    return False
        if action not in self.constraints['SupportedActions']:
            logger.warning('SupportedActions validation failed')
            return False
        return True


class VariableStore:

    def __init__(self) -> None:
        self.variable_store = dict()

    def add_variable(self, variable_name: str, value: object):
        self.variable_store[variable_name] = copy.deepcopy(value)
        return self
    
    def get_variable(self, variable_name: str, pop_item: bool=False)->object:
        result = None
        if variable_name not in self.variable_store:
            raise Exception('Variable "{}" not found'.format(variable_name))
        if pop_item is True:
            result = self.variable_store.pop(variable_name)
        else:
            result = copy.deepcopy(self.variable_store[variable_name])
        return result


class Task:

    def __init__(self, api_version: str, kind: str, metadata: dict, spec: dict):
        self.api_version = api_version
        self.kind = kind
        self.metadata = self._validate_dict(input_object=metadata)
        self.spec = self._validate_dict(input_object=spec)
        self.task_id = self._create_task_id(metadata=metadata)
        self.state = TaskState(
            manifest_spec=spec,
            manifest_metadata=metadata,
            report_label=self.task_id
        )

    def _create_task_id(self, metadata: dict)->str:
        if metadata is not None:
            if isinstance(metadata, dict):
                if 'name' in metadata:
                    return metadata['name']
        return hashlib.sha256(json.dumps(self.spec, default=str).encode('utf-8')).hexdigest()[0:16]

    def _validate_dict(self, input_object: dict=dict()):
        if input_object is None:
            return dict()
        if isinstance(input_object, dict) is False:
            return dict()
        return copy.deepcopy(input_object)
    
    def auto_rollback_enabled(self)->bool:
        if 'autoRollback' in self.metadata:
            if isinstance(self.metadata['autoRollback'], bool) is True:
                return self.metadata['autoRollback']
        return False
    

class Tasks(Sequence):
    """Holds a collection of `Task` instances

    Add each `Task` with the `add_task()` method. Failing to do so may result in errors.

    Once tasks are added, the `Tasks` instance can be treated as a list:

    * `len(tasks)` will give the number of tasks
    * `for task in tasks` will loop through each task as an instance of `Task`, which will be a true copy of the actual `Task` instance

    Attributes:
        tasks: A dictionary containing the collection of `Task` instances
    """

    def __init__(self):
        """Initializes `Tasks`
        """
        self.tasks = dict()

    def add_task(self, task: Task):
        """Adds a valid `Task` instance to the collection of tasks.

        WARNING: Invalid tasks will be silently ignored.

        Args:
            task: An instance of a `Task` object

        Returns:
            None
        """
        if task is not None:
            if isinstance(task, Task):
                self.tasks[task.task_id] = dict()
                self.tasks[task.task_id]['TaskInstance'] = copy.deepcopy(task)
                self.tasks[task.task_id]['TaskDependencies'] = self._extract_task_dependencies(metadata=task.metadata)
                self.tasks[task.task_id]['TaskProcessingScopes'] = self._extract_task_processing_scopes(metadata=task.metadata)

    def get_task_instance_by_name(self, task_name: str)->Task:
        """Returns a `Task` instance matching the `task_name`

        Args:
            task_name: A string with the task name to lookup and return

        Returns:
            A true copy of the found tasks. If the task is not found, a NoneType will be returned.
        """
        if task_name in self.tasks:
            return copy.deepcopy(self.tasks[task_name]['TaskInstance'])
        
    def get_task_dependencies_as_list_of_task_names(self, task_name: str, command: str, context: str)->list:
        """Determine the dependant tasks of the given task within a certain processing scope (command and context
        combination)

        Args:
            task_name: A string with the task name to lookup and return
            command: A string with the command of the execution scope
            context: A string with the context of the execution scope

        Returns:
            A list of strings, where each string is the task name of a dependent task given the current execution scope.

        Raises:
            Exception: In scenarios where a dependant task may specifically excluded from the given scope (command and context combination)
        """
        dependencies = list()
        if task_name in self.tasks:
            task_defined_dependencies = self.tasks[task_name]['TaskDependencies']
            task_defined_dependency: dict
            for task_defined_dependency in task_defined_dependencies:
                if 'tasks' in task_defined_dependency:

                    for dependant_task_name in task_defined_dependency['tasks']:
                        if self.task_scoped_for_processing(task_name=dependant_task_name, command=command, context=context) is False:
                            logger.critical('Task "{}" depends on task "{}", but th dependant task is NOT scoped for this command and/or context. Unable to determine how to proceed - please resolve dependencies and scopes.'.format(task_name, dependant_task_name))
                            raise Exception('Task "{}" depends on task "{}", but th dependant task is NOT scoped for this command and/or context. Unable to determine how to proceed - please resolve dependencies and scopes.'.format(task_name, dependant_task_name))

                    if 'commands' not in task_defined_dependency and 'contexts' not in task_defined_dependency:
                        dependencies += task_defined_dependency['tasks']
                    elif 'commands' not in task_defined_dependency and 'contexts' in task_defined_dependency:
                        if context in task_defined_dependency['contexts']:
                            dependencies += task_defined_dependency['tasks']
                    elif 'commands' in task_defined_dependency and 'contexts' not in task_defined_dependency:
                        if command in task_defined_dependency['commands']:
                            dependencies += task_defined_dependency['tasks']
                    elif 'commands' in task_defined_dependency and 'contexts' in task_defined_dependency:
                        if command in task_defined_dependency['commands'] and context in task_defined_dependency['contexts']:
                            dependencies += task_defined_dependency['tasks']
        return dependencies

    def _extract_task_dependencies(self, metadata: dict)->list:
        """
            metadata:
              dependencies:
              - commands:
                - command1
                - command2
                contexts:
                - context1
                tasks:
                - task_id_1
                - task_id_2
              - commands:
                - command3
                - command4
                contexts:
                - context2
                - context3
                tasks:
                - task_id_5
                - task_id_6
              - tasks:          # Implies dependencies regardless of command or context
                - task_id_3
                - task_id_4
                - task_id_7
                - task_id_8
        """
        if 'dependencies' in metadata:
            if metadata['dependencies'] is not None:
                if isinstance(metadata['dependencies'], list):
                    return copy.deepcopy(metadata['dependencies'])
        return list()
    
    def _extract_task_processing_scopes(self, metadata: dict)->list:
        """
            metadata:
              processingScopes:
              - commands:
                - command1
                - command2
                contexts:
                - context1
                - context2
              - contexts:       # No commands, means any scoped command but only in the listed contexts
                - context3
                - context4
              - commands:       # No contexts, mean any context, but only for the listed commands
                - command3
                - command4
        """
        if 'processingScopes' in metadata:
            if metadata['processingScopes'] is not None:
                if isinstance(metadata['processingScopes'], list):
                    return copy.deepcopy(metadata['processingScopes'])
        return list()
    
    def task_scoped_for_processing(self, task_name: str, command: str, context: str)->bool:
        """Determine if a task is in scope for processing given the execution scope (command and context)

        Some examples of valid configurations:

        ```yaml
        metadata:
          processingScope:
          - commands:
            - command1
            - command2
            contexts:
            - context1
            - context2
          - commands:
            - command3
            contexts:
            - context1
            - context2
            - context3
          - contexts:
            - context4
        ```

        An example:

        ```python
        tasks = Tasks()
        tasks.add_task(
            task=Task(
                api_version='...',
                kind='...',
                metadata={
                    'processingScope': {
                        'commands': [...],
                        'contexts': [...],
                    }
                },
                spec={...}
            )
        )
        if tasks.task_scoped_for_processing(task_name='...', command='...', context='...'):
            ... # Process if true...
        ```

        Args:
            task_name: A string with the task name to lookup and return
            command: A string with the command of the execution scope
            context: A string with the context of the execution scope

        Returns:
            Boolean `True` if the given task is in scope for processing.
        """
        task = self.get_task_instance_by_name(task_name=task_name)
        if 'processingScope' not in task.metadata:
            logger.info('[task={}] processingScope not present in task metadata - task scoped for command "{}" and context "{}"'.format(task_name, command, context))
            return True
        if task.metadata['processingScope'] is None:
            logger.warning('[task={}] processingScope present in task metadata, but NoneType - task scoped for command "{}" and context "{}"'.format(task_name, command, context))
            return True
        if isinstance(task.metadata['processingScope'], list) is False:
            logger.warning('[task={}] processingScope present in task metadata, but not a list type - task scoped for command "{}" and context "{}"'.format(task_name, command, context))
            return True
        
        logger.debug('[task={}] Command         : {}'.format(task_name, command))
        logger.debug('[task={}] Context         : {}'.format(task_name, command))

        processing_scope: dict
        for processing_scope in task.metadata['processingScope']:

            if processing_scope is None:
                logger.warning('[task={}] Processing scope item expected to be a dict but is NoneType - skipping'.format(task_name))
                continue

            if isinstance(processing_scope, dict)is False:
                logger.warning('[task={}] Processing scope item expected to be a dict but found a "{}" - skipping'.format(task_name, type(processing_scope)))
                continue

            logger.debug('[task={}] processingScope : {}'.format(task_name, json.dumps(processing_scope, default=str)))

            if 'commands' not in processing_scope and 'contexts' not in processing_scope:
                logger.warning('[task={}] Neither command or context was defined - returning True...'.format(task_name))
                return True
            elif 'commands' in processing_scope and 'contexts' not in processing_scope:
                if command in processing_scope['commands']:
                    logger.info('[task={}] Command matches and no contexts defined - returning True'.format(task_name))
                    return True
            elif 'commands' not in processing_scope and 'contexts' in processing_scope:
                if context in processing_scope['contexts']:
                    logger.info('[task={}] No commands defined but contexts matches - returning True'.format(task_name))
                    return True
            elif 'commands' in processing_scope and 'contexts' in processing_scope:
                if command in processing_scope['commands'] and context in processing_scope['contexts']:
                    logger.info('[task={}] Command and context matches - returning True'.format(task_name))
                    return True
        return False

    def _task_ordering(self, current_processing_order: list, candidate_task_name: str, command: str, context: str)->list:
        task_names_in_preferred_processing_order = list()
        task_names_in_preferred_processing_order += copy.deepcopy(current_processing_order)

        if self.task_scoped_for_processing(task_name=candidate_task_name, command=command, context=context) is True:
            for dependant_task_name in self.get_task_dependencies_as_list_of_task_names(task_name=candidate_task_name, command=command, context=context):
                if self.task_scoped_for_processing(task_name=dependant_task_name, command=command, context=context) is True:
                    if dependant_task_name not in task_names_in_preferred_processing_order:
                        task_names_in_preferred_processing_order += self._task_ordering(
                            current_processing_order=copy.deepcopy(task_names_in_preferred_processing_order),
                            candidate_task_name=dependant_task_name,
                            command=command,
                            context=context
                        )
            if candidate_task_name not in task_names_in_preferred_processing_order:
                task_names_in_preferred_processing_order.append(candidate_task_name)

        return task_names_in_preferred_processing_order

    def get_task_names_in_order(self, command: str, context: str)->list:
        """Determines the correct order of task processing given an execution scope (command and context)

        Args:
            command: A string with the command of the execution scope
            context: A string with the context of the execution scope

        Returns:
            A list of strings, where each string is the task name. The order of the list is important as it is determined by task dependencies such that the dependant tasks are listed first.

        Raises:
            Exception: In scenarios where a dependant task may specifically excluded from the given scope (command and context combination)
        """
        task_names_in_preferred_processing_order = list()
        for task_name in list(self.tasks.keys()):
            if task_name not in task_names_in_preferred_processing_order:
                for task_name_in_order in  self._task_ordering(
                    current_processing_order=copy.deepcopy(task_names_in_preferred_processing_order),
                    candidate_task_name=task_name,
                    command=command,
                    context=context
                ):
                    if task_name_in_order not in task_names_in_preferred_processing_order:
                        task_names_in_preferred_processing_order.append(task_name_in_order)
                
        return task_names_in_preferred_processing_order
    
    def __getitem__(self, index):
        keys = list(self.tasks.keys())
        keys.sort()
        return copy.deepcopy(self.get_task_instance_by_name(task_name=keys[index]))
    
    def __len__(self) -> int:
        return len(self.tasks)


class TaskProcessor:
    """The `TaskProcessor` is a type of base class that must implement common functions to be performed on a `Task`.

    The default `TaskProcessor` must implement the following functions for each `Task`:

    | Function           | Class Method            | Description                                                                                                                                                                |
    |--------------------|-------------------------|----------------------------------------------------------------------------------------------------------------------------------------------------------------------------|
    | Create resources   | `create_action()`       | Creates new resources as defined in the `spec` of a `Task`                                                                                                                 |
    | Delete resources   | `delete_action()`       | Deletes resources as was defined in the `spec` of a `Task`                                                                                                                 |
    | Update resources   | `update_action()`       | Updates resources, typically after a previous "create" action but with an updated `spec` of a `Task`                                                                       |
    | Rollback           | `rollback_action()`     | Should be called after an exception in one of the prior actions in an attempt to get resources back to the prior state.                                                    |
    | Drift Detection    | `detect_drift_action()` | Compares the `Task` specification against the current resources associated with the task to determine if there are any deviations that may require using the update action |
    | Describe resources | `describe_action()`     | Describes resources that was created by a `Task`                                                                                                                           |

    The various function described above must by implemented by the client. When the `TaskProcessor` implementation is
    initiated, an `api_version` is supplied. And `Task` targeting this API version must be able to be processed by the
    `TaskProcessor` implementation.

    Also, the functions defined above can be directly called, but typically the `process_task()` method already contain
    the required logic to coordinate the execution of the functions and automatically call the rollback function when
    required.

    Attributes:
        api_version: A string defining the implementation's API version.
    """

    def __init__(self, api_version: str) -> None:
        self.api_version = api_version

    def create_identifier(self, task: Task, variable_name: str)->str:
        """Helper method to create a variable identifier.

        Args:
            task: A `Task` being processed
            variable_name: The variable name to use in the final identifier string

        Returns:
            A string with the identifier.
        """
        return '{}:{}'.format(task.task_id, variable_name)

    def add_event(self, variable_store: VariableStore, task: Task, event_label: str='INFO_EVENT', event_description: str='No Details Provided')->VariableStore:
        """Adds an event entry to the `VariableStore` and ensures the event is captured in a consistent format.

        The `VariableStore` instance supplied as input may already contain prior events and many other variables. The 
        new event will simply be added.

        In addition to the updated `VariableStore`, log entries of the event will also be generated.

        If the `TaskProcessor` is invoked by the `process_task()` method, some standards events will already be added
        as a `Task` is processed.

        Args:
            variable_store: An instance of `VariableStore` to which the new event will be added
            task: A `Task` being processed
            event_label: A string describing the event.
            event_description: A string describing the event.

        Returns:
            The event will be created in a consistent data structure and be added to the `VariableStore`. The updated 
            `VariableStore` is returned.

            All events for a task will have the relevant `PROCESSING_EVENTS` key, as generated by the 
            `create_identifier()` method, where the value of the `variable_name` is `PROCESSING_EVENTS`.
        """
        event_variable_name = self.create_identifier(task=task, variable_name='PROCESSING_EVENTS')
        updated_variable_store = VariableStore()
        updated_variable_store.variable_store = copy.deepcopy(variable_store.variable_store)
        if event_variable_name not in variable_store.variable_store:
            updated_variable_store = self.init_event_variable(variable_store=copy.deepcopy(updated_variable_store), task=task)
        events: list
        events = updated_variable_store.get_variable(variable_name=event_variable_name)
        event_data = {
            'EventTimestamp': datetime.now(timezone.utc),
            'EventLabel': event_label,
            'EventDescription': event_description,
            'TaskId': task.task_id,
        }
        events.append(event_data)
        logger.info('EVENT: {}'.format(json.dumps(event_data, default=str)))
        updated_variable_store = updated_variable_store.add_variable(
            variable_name=event_variable_name,
            value=copy.deepcopy(events)
        )
        return updated_variable_store

    def init_event_variable(self, variable_store: VariableStore, task: Task)->VariableStore:
        """Initializes the `VariableStore` for accepting new events for `Task` processing.

        Normally this method does not have to be called directly. When a new event is generated via a call to the 
        `add_event()` method, logic in that method will already handle the timely calling of this method. 

        Args:
            variable_store: An instance of `VariableStore` to which the new event will be added
            task: A `Task` being processed

        Returns:
            The updated `VariableStore` is returned.
        """
        updated_variable_store = VariableStore()
        updated_variable_store.variable_store = copy.deepcopy(variable_store.variable_store)
        if self.create_identifier(task=task, variable_name='PROCESSING_EVENTS') not in updated_variable_store.variable_store:
            updated_variable_store = updated_variable_store.add_variable(
                variable_name=self.create_identifier(task=task, variable_name='PROCESSING_EVENTS'),
                value=list()
            )
        return updated_variable_store

    def process_task(
        self,
        task: Task,
        persistence: StatePersistence=StatePersistence(),
        variable_store: VariableStore=VariableStore(),
        action: str='CreateAction',
        task_resolved_spec: dict=dict()
    )->VariableStore:
        variable_store = self.add_event(variable_store=copy.deepcopy(variable_store), task=task, event_label='PROCESS_TASK_CALLED', event_description='Ready For Processing')
        auto_rollback = task.auto_rollback_enabled()
        exception_raised = False
        final_exception_message = 'Unrecognized action "{}" provided'.format(action)
        if action == 'CreateAction':
            variable_store = self.add_event(variable_store=copy.deepcopy(variable_store), task=task, event_label='CREATE_ACTION_START', event_description='Start of processing')
            try:
                variable_store = self.create_action(task=task, persistence=persistence, variable_store=copy.deepcopy(variable_store), task_resolved_spec=task_resolved_spec)
                variable_store = self.add_event(variable_store=copy.deepcopy(variable_store), task=task, event_label='CREATE_ACTION_DONE', event_description='Start of processing')
                return variable_store
            except:
                exception_text = traceback.format_exc()
                logger.error('EXCEPTION: {}'.format(exception_text))
                variable_store = self.add_event(variable_store=copy.deepcopy(variable_store), task=task, event_label='CREATE_ACTION_ERROR', event_description='EXCEPTION: {}'.format(exception_text))
                exception_raised = True
                final_exception_message = 'Action "CreateAction" failed with exception - please see logs for details.'
        elif action == 'DeleteAction':
            variable_store = self.add_event(variable_store=copy.deepcopy(variable_store), task=task, event_label='DELETE_ACTION_START', event_description='Start of processing')
            try:
                variable_store = self.delete_action(task=task, persistence=persistence, variable_store=variable_store, task_resolved_spec=task_resolved_spec)
                variable_store = self.add_event(variable_store=copy.deepcopy(variable_store), task=task, event_label='DELETE_ACTION_DONE', event_description='End of processing')
                return variable_store
            except:
                exception_text = traceback.format_exc()
                logger.error('EXCEPTION: {}'.format(exception_text))
                variable_store = self.add_event(variable_store=copy.deepcopy(variable_store), task=task, event_label='DELETE_ACTION_ERROR', event_description='EXCEPTION: {}'.format(exception_text))
                exception_raised = True
                final_exception_message = 'Action "DeleteAction" failed with exception - please see logs for details.'
        elif action == 'UpdateAction':
            variable_store = self.add_event(variable_store=copy.deepcopy(variable_store), task=task, event_label='UPDATE_ACTION_START', event_description='Start of processing')
            try:
                variable_store = self.update_action(task=task, persistence=persistence, variable_store=variable_store, task_resolved_spec=task_resolved_spec)
                variable_store = self.add_event(variable_store=copy.deepcopy(variable_store), task=task, event_label='UPDATE_ACTION_DONE', event_description='End of processing')
                return variable_store
            except:
                exception_text = traceback.format_exc()
                logger.error('EXCEPTION: {}'.format(exception_text))
                variable_store = self.add_event(variable_store=copy.deepcopy(variable_store), task=task, event_label='UPDATE_ACTION_ERROR', event_description='EXCEPTION: {}'.format(exception_text))
                exception_raised = True
                final_exception_message = 'Action "UpdateAction" failed with exception - please see logs for details.'
        elif action == 'DescribeAction':
            try:
                variable_store = self.add_event(variable_store=copy.deepcopy(variable_store), task=task, event_label='DESCRIBE_ACTION_START', event_description='Start of processing')
                variable_store = self.describe_action(task=task, persistence=persistence, variable_store=variable_store, task_resolved_spec=task_resolved_spec)
                variable_store = self.add_event(variable_store=copy.deepcopy(variable_store), task=task, event_label='DESCRIBE_ACTION_DONE', event_description='End of processing')
                return variable_store
            except:
                final_exception_message = 'Action "DescribeAction" failed with exception - please see logs for details. Even if configured, no Auto Rollback was performed on this action.'
        elif action == 'DetectDriftAction':
            try:
                variable_store = self.add_event(variable_store=copy.deepcopy(variable_store), task=task, event_label='DETECT_DRIFT_ACTION_START', event_description='Start of processing')
                variable_store = self.detect_drift_action(task=task, persistence=persistence, variable_store=variable_store, task_resolved_spec=task_resolved_spec)
                variable_store = self.add_event(variable_store=copy.deepcopy(variable_store), task=task, event_label='DETECT_DRIFT_ACTION_DONE', event_description='End of processing')
                return variable_store
            except:
                final_exception_message = 'Action "DetectDriftAction" failed with exception - please see logs for details. Even if configured, no Auto Rollback was performed on this action.'
        
        if action == 'RollbackAction' and exception_raised is False:
            try:
                variable_store = self.add_event(variable_store=copy.deepcopy(variable_store), task=task, event_label='ROLLBACK_ACTION_START', event_description='Start of processing')
                variable_store = self.rollback_action(task=task, persistence=persistence, variable_store=variable_store, task_resolved_spec=task_resolved_spec)
                variable_store = self.add_event(variable_store=copy.deepcopy(variable_store), task=task, event_label='ROLLBACK_ACTION_DONE', event_description='End of processing')
                return variable_store
            except:
                logger.error('EXCEPTION: {}'.format(traceback.format_exc()))
                final_exception_message = final_exception_message = 'Action "RollbackAction" failed with exception - please see logs for details. Even if configured, no additional Rollback was performed on this action.'
        elif auto_rollback is True and action != 'RollbackAction' and exception_raised is True:
            try:
                variable_store = variable_store.add_variable(variable_name='{}:RollbackFrom'.format(task.task_id), value=action)
                variable_store = self.add_event(variable_store=copy.deepcopy(variable_store), task=task, event_label='ROLLBACK_ACTION_START', event_description='Start of processing')
                variable_store = self.rollback_action(task=task, persistence=persistence, variable_store=variable_store, task_resolved_spec=task_resolved_spec)
                variable_store = self.add_event(variable_store=copy.deepcopy(variable_store), task=task, event_label='ROLLBACK_ACTION_DONE', event_description='End of processing')
                final_exception_message = '{} Auto Rollback action was attempted.'
            except:
                logger.error('EXCEPTION: {}'.format(traceback.format_exc()))
                final_exception_message = '{} Auto Rollback action was attempted, but also raised an exception.'

        raise Exception(final_exception_message)
    
    def create_action(
        self,
        task: Task,
        persistence: StatePersistence=StatePersistence(),
        variable_store: VariableStore=VariableStore(),
        task_resolved_spec: dict=dict()
    )->VariableStore:
        raise Exception('Must be implemented/extended by client')
    
    def rollback_action(
        self,
        task: Task,
        persistence: StatePersistence=StatePersistence(),
        variable_store: VariableStore=VariableStore(),
        task_resolved_spec: dict=dict()
    )->VariableStore:
        raise Exception('Must be implemented/extended by client')
    
    def delete_action(
        self,
        task: Task,
        persistence: StatePersistence=StatePersistence(),
        variable_store: VariableStore=VariableStore(),
        task_resolved_spec: dict=dict()
    )->VariableStore:
        raise Exception('Must be implemented/extended by client')
    
    def update_action(
        self,
        task: Task,
        persistence: StatePersistence=StatePersistence(),
        variable_store: VariableStore=VariableStore(),
        task_resolved_spec: dict=dict()
    )->VariableStore:
        raise Exception('Must be implemented/extended by client')
    
    def describe_action(
        self,
        task: Task,
        persistence: StatePersistence=StatePersistence(),
        variable_store: VariableStore=VariableStore(),
        task_resolved_spec: dict=dict()
    )->VariableStore:
        raise Exception('Must be implemented/extended by client')
    
    def detect_drift_action(
        self,
        task: Task,
        persistence: StatePersistence=StatePersistence(),
        variable_store: VariableStore=VariableStore(),
        task_resolved_spec: dict=dict()
    )->VariableStore:
        raise Exception('Must be implemented/extended by client')


class TaskProcessStore:

    def __init__(self) -> None:
        self.task_processor_register = dict()

    def register_task_processor(self, task_processor: TaskProcessor):
        if task_processor.api_version not in self.task_processor_register:
            self.task_processor_register[task_processor.api_version] = copy.deepcopy(task_processor)
        return self
    
    def get_task_processor(self, api_version: str)->TaskProcessor:
        if api_version not in self.task_processor_register:
            raise Exception('No processor found for API "{}"'.format(api_version))
        return copy.deepcopy(self.task_processor_register[api_version])
    
    def get_task_processor_for_task(self, task: Task)->TaskProcessor:
        if task.api_version not in self.task_processor_register:
            raise Exception('No processor found for API "{}"'.format(task.api_version))
        return copy.deepcopy(self.task_processor_register[task.api_version])


class Hook:

    def __init__(self, name: str=None) -> None:
        self.name = self.__class__.__name__
        if name is not None:
            self.name = name

    def _log(self, message: str, task: Task=None, level: str='info'):
        task_id = 'unknown'
        if task is not None:
            if isinstance(task, Task):
                task_id = task.task_id
        final_message = 'HOOK [{}] TASK [{}] : {}'.format(self.name, task_id, message)
        if level.lower().startswith('i'):
            logger.info(final_message)
        elif level.lower().startswith('d'):
            logger.debug(final_message)
        elif level.lower().startswith('e'):
            logger.error(final_message)
        elif level.lower().startswith('c'):
            logger.critical(final_message)
        else:
            logger.warning(final_message)

    def run(
        self,
        task: Task=None,
        parameters: dict=dict(),
        parameter_validator: ParameterValidation=ParameterValidation(constraints=None),
        persistence: StatePersistence=StatePersistence(),
        variable_store: VariableStore=VariableStore(),
        task_process_store: TaskProcessStore=TaskProcessStore()
    )->VariableStore:
        raise Exception('Hook must be implemented/extended by client')


class TaskProcessingHook(Hook):

    def __init__(self, name: str='TaskProcessingHook'):
        super().__init__(name)

    def run(
        self,
        task: Task=None,
        parameters: dict=dict(),
        parameter_validator: ParameterValidation=TaskProcessingActionParameterValidation(constraints=None),
        persistence: StatePersistence=StatePersistence(),
        variable_store: VariableStore=VariableStore(),
        task_process_store: TaskProcessStore=TaskProcessStore()
    )->VariableStore:
        task_processor = task_process_store.get_task_processor_for_task(task=task)
        task_resolved_spec = copy.deepcopy(task.spec)
        if 'ResolvedSpec:{}'.format(task.task_id) in variable_store.variable_store:
            task_resolved_spec = copy.deepcopy(variable_store.variable_store['ResolvedSpec'])
        self._log(message='task_resolved_spec: {}'.format(json.dumps(task_resolved_spec, default=str)), task=task, level='debug')
        if parameter_validator.validation_passed(parameters=parameters) is True:
            variable_store = task_processor.process_task(
                task=task,
                persistence=persistence,
                variable_store=copy.deepcopy(variable_store),
                action=parameters['Action'],
                task_resolved_spec=task_resolved_spec
            )
        return copy.deepcopy(variable_store)


class ResolveTaskSpecVariablesHook(Hook):

    def __init__(self, name: str='ResolveTaskSpecVariablesHook'):
        super().__init__(name)

    def _is_iterable(self, data: object, exclude_dict: bool=True, exclude_string: bool=True)->bool:
        if data is None:
            return False
        if exclude_dict is True and isinstance(data, dict):
            return False
        if exclude_string is True and isinstance(data, str):
            return False
        try:
            iter(data)
        except TypeError as te:
            return False
        return True
    
    def _lookup_value(self, raw_key: str, command:str, context:str, variable_store: VariableStore, task: Task=None)->object:
        # Typical key in key_value_store     : MyId:a_command:a_context:SubKey1:SubKey2 (SubKey1 is required, but anything afterwards is optional)
        # Expected raw_key is something like : ${VAR:MyId:SubKey1:SubKey2}  
        result = ''
        self._log(message='       raw_key: {}'.format(raw_key), task=task, level='debug')
        if raw_key.startswith('${VAR:'):                # ${VAR:MyId:SubKey1:SubKey2}        
            key_parts = raw_key.split(':')              # ['${VAR', 'MyId', 'SubKey1', 'SubKey2}']
            self._log(message='         key_parts      : {}'.format(key_parts), task=task, level='debug')
            target_task_id = key_parts[1]               # MyId
            target_index = ':'.join(key_parts[2:])      # SubKey1:SubKey2
            target_index = target_index.replace('}', '')
            self._log(message='         target_task_id : {}'.format(target_task_id), task=task, level='debug')
            self._log(message='         target_index   : {}'.format(target_index), task=task, level='debug')
            potential_keys = list()
            potential_keys.append(
                '{}:{}:{}:{}'.format(       # MyId:a_command:a_context:SubKey1:SubKey2 - variable bound to command and context
                    target_task_id,
                    command,
                    context,
                    target_index
                )
            )
            potential_keys.append(
                '{}:{}::{}'.format(       # MyId:a_command:a_context:SubKey1:SubKey2 - variable bound to command bot not context
                    target_task_id,
                    command,
                    target_index
                )
            )
            potential_keys.append(
                '{}::{}:{}'.format(       # MyId:a_command:a_context:SubKey1:SubKey2 - variable bound to context bot not command
                    target_task_id,
                    context,
                    target_index
                )
            )
            potential_keys.append(
                '{}:{}'.format(             # MyId:SubKey1:SubKey2 - variable not bound to any command or context
                    target_task_id,
                    target_index
                )
            )

            for lookup_key_base in potential_keys:

                self._log(message='         Looking for a key that looks like "{}" in key_value_store'.format(lookup_key_base), task=task, level='debug')
                #self._log(message='           Potential keys: {}'.format(hook_name, list(variable_store.variable_store.keys())), task=task, level='debug')
                for key in list(variable_store.variable_store.keys()):
                    self._log(message='           Looking for key "{}" in "{}"'.format(lookup_key_base, key), task=task, level='debug')
                    if lookup_key_base in key:
                        self._log(message='              Resolved key "{}" to swap out for reference variable "{}"'.format(key, raw_key), task=task, level='info')
                        result = copy.deepcopy(variable_store.variable_store[key])
                    else:
                        self._log(message='              Key "{}" Not Found'.format(lookup_key_base), task=task, level='info')
        else:
            raise Exception('Oops - the raw key is not what we expected: raw_key: "{}"'.format(raw_key))
        self._log(message='         Returning final result: "{}"'.format(result), task=task, level='debug')
        return result

    def _analyse_data(self, task: Task, data: object, variable_store:VariableStore, command:str, context:str)->dict:
        self._log(message='   Analyzing data', task=task, level='info')
        self._log(message='   Inspecting object: {}'.format(data), task=task, level='debug')
        if data is None:
            return data
        modified_data = None
        if isinstance(data, str) is True:
            modified_data: str = copy.deepcopy(data)
            # matches = re.findall('(\$\{VAR:[\w|\-|\s|:|.|;|_]+\})', 'wc -l ${VAR:prompt_output_path:RESULT} > ${VAR:prompt_output_path:RESULT}_STATS && rm -vf ${VAR:prompt_output_2_path:RESULT}')
            # ['${VAR:prompt_output_path:RESULT}', '${VAR:prompt_output_path:RESULT}', '${VAR:prompt_output_2_path:RESULT}']
            matches = re.findall('(\$\{VAR:[\w|\-|\s|:|.|;|_]+\})', data)
            self._log(message='       matches: {}'.format(matches), task=task, level='debug')
            for match in matches:
                self._log(message='     Looking up value for variable placeholder "{}"'.format(match), task=task, level='debug')
                final_value = self._lookup_value(
                    raw_key=match,
                    command=command,
                    context=context,
                    variable_store=variable_store,
                    task=task
                )
                modified_data = modified_data.replace(match, final_value)
        elif isinstance(data, dict) is True:
            modified_data: dict = dict()
            for key, val in data.items():
                modified_data[key] = self._analyse_data(task=task, data=val, variable_store=variable_store, command=command, context=context)
        elif self._is_iterable(data=data) is True:
            modified_data: list = list()
            for val in data:
                modified_data.append(self._analyse_data(task=task, data=val, variable_store=variable_store, command=command, context=context))
        else:
            modified_data = copy.deepcopy(data)

        return modified_data

    def run(
        self,
        task: Task=None,
        parameters: dict=dict(),
        parameter_validator: ParameterValidation=TaskProcessingActionParameterValidation(constraints=None),
        persistence: StatePersistence=StatePersistence(),
        variable_store: VariableStore=VariableStore(),
        task_process_store: TaskProcessStore=TaskProcessStore()
    )->VariableStore:
        updated_variable_store = VariableStore()
        updated_variable_store.variable_store = copy.deepcopy(variable_store.variable_store)

        command = 'none'
        context = 'none'

        if 'Command' in parameters:
            if parameters['Command'] is not None:
                if isinstance(parameters['Command'], str) is True:
                    if len(parameters['Command']) > 0:
                        command = parameters['Command']
        if 'Context' in parameters:
            if parameters['Context'] is not None:
                if isinstance(parameters['Context'], str) is True:
                    if len(parameters['Context']) > 0:
                        context = parameters['Context']

        updated_variable_store.add_variable(
            variable_name='ResolvedSpec:{}'.format(task.task_id),
            value=self._analyse_data(
                task=task,
                data=copy.deepcopy(task.spec),
                variable_store=variable_store,
                command=command,
                context=context
            )
        ) 
        return updated_variable_store


class TaskPostProcessingStateUpdateHook(Hook):

    def __init__(self, name: str='TaskPostProcessingStateUpdateHook'):
        super().__init__(name)

    def _validate_data(self, data: dict)->bool:
        expected_data = {
            'resource_checksum': {
                'can_be_none': True,
                'type': str,
            },
            'resolved_spec_applied': {
                'can_be_none': True,
                'type': dict,
            },
            'state_changed': {
                'can_be_none': False,
                'type': bool,
            },
            'is_created': {
                'can_be_none': True,
                'type': bool,
            },
            'create_timestamp': {
                'can_be_none': True,
                'type': datetime,
            },
            'raw_spec': {
                'can_be_none': True,
                'type': dict,
            },
            'metadata': {
                'can_be_none': True,
                'type': dict,
            },
        }
        for field_key in list(expected_data.keys()):
            field_validation_rules = expected_data[field_key]
            if field_key not in data:
                logger.warning('Expected field key "{}", but it was not present'.format(field_key))
                return False
            if field_validation_rules['can_be_none'] is False and data[field_key] is None:
                logger.warning('Field key "{}" can not have a NoneType value'.format(field_key))
                return False
            if isinstance(data[field_key], field_validation_rules['type']) is False:
                logger.warning('Field key "{}" expected to be of type "{}" but found to be of type "{}"'.format(field_key, field_validation_rules['type'], type(data[field_key])))
                return False
        return True

    def run(
        self,
        task: Task=None,
        parameters: dict=dict(),
        parameter_validator: ParameterValidation=TaskProcessingActionParameterValidation(constraints=None),
        persistence: StatePersistence=StatePersistence(),
        variable_store: VariableStore=VariableStore(),
        task_process_store: TaskProcessStore=TaskProcessStore()
    )->VariableStore:
        updated_variable_store = VariableStore()
        updated_variable_store.variable_store = copy.deepcopy(variable_store.variable_store)

        vs_key = '{}:TASK_STATE_UPDATES'.format(task.task_id)
        if vs_key not in updated_variable_store.variable_store:
            logger.warning('No TASK_STATE_UPDATES variable detected - state will NOT be updated and persisted')
            return updated_variable_store
        
        data = updated_variable_store.variable_store.pop(vs_key)
        if self._validate_data(data=data) is False:
            logger.warning('TASK_STATE_UPDATES data validation FAILED - state will NOT be updated and persisted')
            return updated_variable_store
        
        if data['state_changed'] is False:
            logger.warning('No task state changed detected - state will NOT be updated and persisted')
            return updated_variable_store
        
        task.state = TaskState(
            manifest_spec=data['raw_spec'],
            applied_spec=data['resolved_spec_applied'],
            resolved_spec=data['resolved_spec_applied'],
            manifest_metadata=data['metadata'],
            report_label=task.task_id,
            created_timestamp=data['create_timestamp'],
            applied_resources_checksum=data['resource_checksum'],
            current_resource_checksum=data['resource_checksum']
        )

        persistence.update_object_state(
            object_identifier='{}:TASK_STATE'.format(task.task_id),
            data=task.state.to_dict(
                with_checksums=True,
                include_applied_spec=True
            )
        )
        persistence.commit()

        return updated_variable_store


class GeneralErrorHook(Hook):

    def __init__(self, name: str='GeneralErrorHook') -> None:
        super().__init__(name)

    def run(
        self,
        task: Task=None,
        parameters: dict=dict(),
        parameter_validator: ParameterValidation=ParameterValidation(constraints=None),
        persistence: StatePersistence=StatePersistence(),
        variable_store: VariableStore=VariableStore(),
        task_process_store: TaskProcessStore=TaskProcessStore()
    ) -> VariableStore:
        exception_message = 'An Unspecified Error Occurred'
        task_name = 'no-task-specified'
        if task is not None:
            if isinstance(task, Task) is True:
                task_name = task.task_id
                exception_message = 'An Unspecified Error Occurred - task: "{}"'.format(task_name)
        if 'ExceptionStacktrace' in parameters:
            exception_message = '** task="{}"\n**exception_message: {}'.format(task_name, parameters['ExceptionStacktrace'])
        logger.error('EXCEPTION:\n{}'.format(exception_message))


class Hooks(Sequence):

    def __init__(self) -> None:
        self.hooks = list()
        self.general_error_hook = GeneralErrorHook()
        super().__init__()

    def add_hook(self, hook: Hook):
        self.hooks.append(hook)
        return self
    
    def get_hook_by_name(self, name: str):
        hook: Hook
        for hook in self.hooks:
            if hook.name == name:
                return copy.deepcopy(hook)
        return 'not-a-hook'

    def __getitem__(self, index):
        return self.hooks[index]
    
    def __len__(self) -> int:
        return len(self.hooks)


class WorkflowExecutor:

    def __init__(
        self,
        parameter_validator: ParameterValidation=ParameterValidation(constraints=None),
        persistence: StatePersistence=StatePersistence(),
        variable_store: VariableStore=VariableStore(),
        task_process_store: TaskProcessStore=TaskProcessStore(),
        general_error_hook: Hook=GeneralErrorHook()
    ):
        self.ordered_workflow_steps = list()
        self.tasks = Tasks()
        self.general_error_hook = general_error_hook
        self.parameter_validator = parameter_validator
        self.persistence = persistence
        self.variable_store = variable_store
        self.task_process_store = task_process_store
        self.command_to_action_map = dict()
        self.persistence.load()

    def link_command_to_create_action(self, command: str):
        self.command_to_action_map[command] = 'CreateAction'
        return self

    def link_command_to_rollback_action(self, command: str):
        self.command_to_action_map[command] = 'RollbackAction'
        return self

    def link_command_to_delete_action(self, command: str):
        self.command_to_action_map[command] = 'DeleteAction'
        return self

    def link_command_to_update_action(self, command: str):
        self.command_to_action_map[command] = 'UpdateAction'
        return self

    def link_command_to_describe_action(self, command: str):
        self.command_to_action_map[command] = 'DescribeAction'
        return self

    def link_command_to_detect_drift_action(self, command: str):
        self.command_to_action_map[command] = 'DetectDriftAction'
        return self

    def add_workflow_step_by_hook_name(self, hook_name: str, hooks: Hooks):
        self.ordered_workflow_steps.append(
            hooks.get_hook_by_name(name=hook_name)
        )
        return self
    
    def add_workflow_step_by_hook_instance(self, hook: Hook):
        self.ordered_workflow_steps.append(hook)
        return self
    
    def add_task(self, task: Task):
        self.tasks.add_task(task=task)
        return self
    
    def execute_workflow(
        self,
        command: str,
        context: str
    )->VariableStore:
        if len(self.ordered_workflow_steps) == 0:
            raise Exception('No more steps to execute')
        updated_variable_store = copy.deepcopy(self.variable_store)

        if command not in self.command_to_action_map:
            raise Exception('Unrecognized command "{}"'.format(command))

        parameters = dict()
        parameters['Action'] = self.command_to_action_map[command]
        parameters['Command'] = command
        parameters['Context'] = context

        all_events = list()

        task_name: str
        for task_name in self.tasks.get_task_names_in_order(command=command, context=context):
            task = self.tasks.get_task_instance_by_name(task_name=task_name)
            hook: Hook
            for hook in self.ordered_workflow_steps:
                try:
                    updated_variable_store: VariableStore
                    updated_variable_store = hook.run(
                        task=task,
                        parameters=parameters,
                        parameter_validator=self.parameter_validator,
                        persistence=self.persistence,
                        variable_manager=self.variable_store,
                        task_process_store=self.task_process_store
                    )
                    if '{}:PROCESSING_EVENTS'.format(task.task_id) in updated_variable_store.variable_store:
                        all_events += copy.deepcopy(updated_variable_store.variable_store['{}:PROCESSING_EVENTS'.format(task.task_id)])
                except:
                    exception_stacktrace = traceback.format_exc()
                    logger.error('EXCEPTION: {}'.format(exception_stacktrace))
                    for event in all_events: logger.error('POST EXCEPTION EVENT DUMP >> {}'.format(event))
                    if self.general_error_hook is not None:
                        if isinstance(GeneralErrorHook, Hook):
                            parameters['ExceptionStacktrace'] = exception_stacktrace
                            self.general_error_hook.run(
                                task=task,
                                parameters=parameters,
                                parameter_validator=self.parameter_validator,
                                persistence=self.persistence,
                                variable_manager=self.variable_store,
                                task_process_store=self.task_process_store
                            )
                    else:
                        print(exception_stacktrace)
                    raise Exception('Failure to process hook "{}" - cannot continue'.format(hook.name))
            self.persistence.commit()
        return copy.deepcopy(updated_variable_store)

