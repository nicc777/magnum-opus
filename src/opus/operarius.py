import json
import hashlib
import copy
from collections.abc import Sequence


def keys_to_lower(data: dict):
    final_data = dict()
    for key in data.keys():
        if isinstance(data[key], dict):
            final_data[key.lower()] = keys_to_lower(data[key])
        else:
            final_data[key.lower()] = data[key]
    return final_data


class KeyValueStore:

    def __init__(self):
        self.store = dict()

    def save(self, key: str, value: object):
        self.store[key] = value


class LoggerWrapper:    # pragma: no cover

    def __init__(self):
        pass

    def info(self, message: str):
        if isinstance(message, str):
            print(message)

    def warn(self, message: str):
        self.info(message=message)

    def warning(self, message: str):
        self.info(message=message)

    def debug(self, message: str):
        self.info(message=message)

    def critical(self, message: str):
        self.info(message=message)

    def error(self, message: str):
        self.info(message=message)


class IdentifierContext:

    def __init__(self, context_type: str, context_name: str):
        self.context_type = context_type
        self.context_name = context_name

    def context(self)->str:
        return '{}:{}'.format(
            self.context_type,
            self.context_name
        )
    
    def to_dict(self)->dict:
        data = dict()
        data['ContextType'] = self.context_type
        data['ContextName'] = self.context_name
        return data
    
    def __eq__(self, __value: object) -> bool:
        try:
            if __value.context_type == self.context_type and __value.context_name == self.context_name:
                return True
        except:
            pass
        return False


class IdentifierContexts(Sequence):

    def __init__(self):
        self.identifier_contexts = list()
        self.unique_identifier_value = hashlib.sha256(json.dumps(self.identifier_contexts).encode('utf-8')).hexdigest()

    def add_identifier_context(self, identifier_context: IdentifierContext):
        duplicates = False
        if identifier_context is None:
            return
        if isinstance(identifier_context, IdentifierContext) is False:
            return
        for existing_identifier_context in self.identifier_contexts:
            if existing_identifier_context.context_type == identifier_context.context_type and existing_identifier_context.context_name == identifier_context.context_name:
                duplicates = True
        if duplicates is False:
            self.identifier_contexts.append(identifier_context)
            self.unique_identifier_value = hashlib.sha256(json.dumps(self.to_dict()).encode('utf-8')).hexdigest()

    def is_empty(self)->bool:
        if len(self.identifier_contexts) > 0:
            return False
        return True
    
    def contains_identifier_context(self, target_identifier_context: IdentifierContext)->bool:
        try:
            local_identifier_context: IdentifierContext
            for local_identifier_context in self.identifier_contexts:
                if local_identifier_context == target_identifier_context:
                    return True
        except: # pragma: no cover
            pass
        return False
    
    def to_dict(self)->dict:
        data = dict()
        data['IdentifierContexts'] = list()
        for identifier_context in self.identifier_contexts:
            data['IdentifierContexts'].append(identifier_context.to_dict())
        data['UniqueId'] = self.unique_identifier_value
        return data

    def __getitem__(self, index):
        return self.identifier_contexts[index]

    def __len__(self):
        return len(self.identifier_contexts)


class Identifier:

    def __init__(self, identifier_type: str, key: str, val: str=None, identifier_contexts: IdentifierContexts=IdentifierContexts()):
        self.identifier_type = identifier_type
        self.key = key
        self.val = val
        self.identifier_contexts = identifier_contexts
        self.unique_identifier_value = self._calc_unique_id()
        self.is_contextual_identifier = bool(len(identifier_contexts))

    def _calc_unique_id(self)->str:
        data = dict()
        data['IdentifierType'] = self.identifier_type
        data['IdentifierKey'] = self.key
        if self.val is not None:
            data['IdentifierValue'] = self.val
        data['IdentifierContexts'] = self.identifier_contexts.to_dict()
        return hashlib.sha256(json.dumps(data).encode('utf-8')).hexdigest()

    def identifier_matches_any_context(self, identifier_type: str, key: str, val: str=None, target_identifier_contexts: IdentifierContexts=IdentifierContexts())->bool:
        if self.identifier_type == identifier_type and self.key == key and self.val == val:
            if self.identifier_contexts.is_empty() is True: # This identifier (self) is not context bound, therefore the the given contexts does not matter. 
                return True
            for target_identifier_context in target_identifier_contexts:
                if self.identifier_contexts.contains_identifier_context(target_identifier_context=target_identifier_context):
                    return True
        return False
    
    def to_dict(self)->dict:
        data = dict()
        data['IdentifierType'] = self.identifier_type
        data['IdentifierKey'] = self.key
        if self.val is not None:
            data['IdentifierValue'] = self.val
        data['IdentifierContexts'] = self.identifier_contexts.to_dict()
        data['UniqueId'] = self.unique_identifier_value
        return data
    
    def __eq__(self, candidate_identifier: object) -> bool:
        key_matches = False
        val_matches = False
        context_matches = False
        try:
            if candidate_identifier.identifier_type == self.identifier_type:
                if candidate_identifier.key == self.key:
                    key_matches = True
                if candidate_identifier.val == self.val:
                    val_matches = True
                if len(candidate_identifier.identifier_contexts) == 0 and len(self.identifier_contexts) == 0:
                    context_matches = True
                else:
                    candidate_context: IdentifierContext
                    for candidate_context in candidate_identifier.identifier_contexts:
                        if self.identifier_contexts.contains_identifier_context(target_identifier_context=candidate_context) is True:
                            context_matches = True
                        if key_matches is True and val_matches is True and context_matches is True:
                            return True
        except: # pragma: no cover
            pass
        if key_matches is True and val_matches is True and context_matches is True:
            return True
        return False
    

class Identifiers(Sequence):

    def __init__(self):
        self.identifiers = list()
        self.unique_identifier_value = hashlib.sha256(json.dumps(self.identifiers).encode('utf-8')).hexdigest()

    def add_identifier(self, identifier: Identifier):
        can_add = True
        for existing_identifier in self.identifiers:
            if existing_identifier.to_dict()['UniqueId'] == identifier.to_dict()['UniqueId']:
                can_add = False
        if can_add is True:
            self.identifiers.append(identifier)

    def identifier_found(self, identifier: Identifier)->bool:
        local_identifier: Identifier
        for local_identifier in self.identifiers:
            if local_identifier == identifier:
                return True
        return False

    def identifier_matches_any_context(self, identifier_type: str, key: str, val: str=None, target_identifier_contexts: IdentifierContexts=IdentifierContexts())->bool:
        for local_identifier in self.identifiers:
            if local_identifier.identifier_matches_any_context(identifier_type=identifier_type, key=key, val=val, target_identifier_contexts=target_identifier_contexts) is True:
                return True
        return False

    def to_metadata_dict(self):
        """
            metadata:
              identifiers:                    # Non-contextual identifier
              - type: STRING                  # Example: ManifestName
                key: STRING                   # Example: my-manifest
                value: STRING|NULL            # [Optional]                  <-- Not required for type "ManifestName"
              - type: STRING                  # Example: Label
                key: STRING                   # Example: my-key
                value: STRING|NULL            # Example: my-value           <-- Required for type "Label"

              contextualIdentifiers:
              - type: STRING              # Example: ExecutionScope       <-- THEREFORE, this Manifest is scoped to 3x Environment contexts and 2x Command contexts
                key: STRING               # Example: INCLUDE              <-- or "EXCLUDE", to specifically exclude execution in a given context
                value val: STRING         # Example: Null|None
                contexts:
                - type: STRING              # Example: Environment
                  names:
                  - STRING                  # Example: sandbox
                  - STRING                  # Example: test
                  - STRING                  # Example: production
                - type: STRING              # Example: Command
                  names:
                  - STRING                  # Example: apply
                  - STRING                  # Example: delete

            Therefore, there are essentially 3x types of Identifiers in a standard Task processing context:

                * "ManifestName", which does not have contexts
                * "Label", which does not have contexts
                * "ExecutionScope", which DOES have contexts

            Any other "identifiers" (with or without contexts) must be handled/processed by the TaskProcessor Implementation as required
        """
        metadata = dict()
        identifier: Identifier
        for identifier in self.identifiers:
            if isinstance(identifier, Identifier):

                context_types = dict()
                item = dict()
                item['type'] = identifier.identifier_type
                item['key'] = identifier.key
                if identifier.val is not None:
                    item['val'] = identifier.val

                if identifier.is_contextual_identifier is True:
                    if 'contextualIdentifiers' not in metadata:
                        metadata['contextualIdentifiers'] = list()

                    item['contexts'] = list()
                    identifier_context: IdentifierContext
                    for identifier_context in identifier.identifier_contexts:
                        if identifier_context.context_type not in context_types:
                            context_types[identifier_context.context_type] = list()
                        context_types[identifier_context.context_type].append(identifier_context.context_name)

                    for context_type, context_names in context_types.items():
                        item['contexts'].append(
                            {
                                'type': context_type,
                                'names': context_names
                            }
                        )

                    metadata['contextualIdentifiers'].append(item)

                else:
                    if 'identifiers' not in metadata:
                        metadata['identifiers'] = list()
                    metadata['identifiers'].append(item)

        return metadata

    def __getitem__(self, index):
        return self.identifiers[index]

    def __len__(self):
        return len(self.identifiers)


class StatePersistence:

    def __init__(self, logger: LoggerWrapper=LoggerWrapper(), configuration: dict=dict()):
        self.logger = logger
        self.state_cache = self.retrieve_all_state_from_persistence()
        self.configuration = configuration

    def retrieve_all_state_from_persistence(self)->dict:
        self.logger.warning(message='StatePersistence.retrieve_all_state_from_persistence() NOT IMPLEMENTED. Override this function in your own class for long term state storage.')
        return dict()

    def get_object_state(self, object_identifier: str)->dict:
        if object_identifier in self.state_cache:
            return copy.deepcopy(self.state_cache[object_identifier])
        return dict()

    def save_object_state(self, object_identifier: str, data: dict):
        self.state_cache[object_identifier] = copy.deepcopy(data)

    def persist_all_state(self):
        self.logger.warning(message='StatePersistence.persist_all_state() NOT IMPLEMENTED. Override this function in your own class for long term state storage.')


# FIXME Change to class that inherits from Enum
class TaskLifecycleStage:
    TASK_PRE_REGISTER                       = 1
    TASK_PRE_REGISTER_ERROR                 = -1
    TASK_REGISTERED                         = 2
    TASK_REGISTERED_ERROR                   = -2
    TASK_PRE_PROCESSING_START               = 3
    TASK_PRE_PROCESSING_START_ERROR         = -3
    TASK_PRE_PROCESSING_COMPLETED           = 4
    TASK_PRE_PROCESSING_COMPLETED_ERROR     = -4
    TASK_PROCESSING_PRE_START               = 5
    TASK_PROCESSING_PRE_START_ERROR         = -5
    TASK_PROCESSING_POST_DONE               = 6
    TASK_PROCESSING_POST_DONE_ERROR         = -6


class TaskLifecycleStages:

    def __init__(self, init_default_stages: bool=True):
        self.stages = list()
        if init_default_stages is True:
            for i in range(1,7):
                self.stages.append(i)
                self.stages.append(i*-1)

    def register_lifecycle_stage(self, task_life_cycle_stage: int):
        if task_life_cycle_stage not in self.stages:
            self.stages.append(task_life_cycle_stage)

    def stage_registered(self, stage: int)->bool:
        if stage in self.stages:
            return True
        return False


class Hook:

    def __init__(
            self,
            name: str,
            commands: list,
            contexts: list,
            task_life_cycle_stages: TaskLifecycleStages,
            function_impl: object,  # callable object, like a function
            logger: LoggerWrapper=LoggerWrapper()
        ):
        self.name = name
        self.logger = logger
        self.commands = commands
        self.contexts = contexts
        self.task_life_cycle_stages = task_life_cycle_stages
        self.function_impl = function_impl

    def process_hook(self, command: str, context: str, task_life_cycle_stage: int, key_value_store: KeyValueStore, task: object=None, task_id: str=None, extra_parameters:dict=dict(), logger: LoggerWrapper=LoggerWrapper())->KeyValueStore:
        final_logger = self.logger
        if logger is not None:
            if isinstance(logger, LoggerWrapper):
                final_logger = logger
        if command not in self.commands or context not in self.contexts or self.task_life_cycle_stages.stage_registered(stage=task_life_cycle_stage) is False:
            return key_value_store
        try:
            final_logger.debug(
                'Hook "{}" executed on stage "{}" for task "{}" for command "{}" in context "{}"'.format(
                    self.name,
                    task_life_cycle_stage,
                    task_id,
                    command,
                    context
                )
            )
            result = self.function_impl(
                hook_name=self.name,
                task=task,
                key_value_store=key_value_store,
                command=command,
                context=context,
                task_life_cycle_stage=task_life_cycle_stage,
                extra_parameters=extra_parameters,
                logger=self.logger
            )
            if result is not None:
                if isinstance(result, KeyValueStore):
                    key_value_store.store = copy.deepcopy(result.store)
        except:
            exception_message = 'Hook "{}" failed to execute during command "{}" in context "{}" in task life cycle stage "{}"'.format(
                self.name,
                command,
                context,
                task_life_cycle_stage
            )
            final_logger.error(exception_message)
            # raise Exception(exception_message)
        return key_value_store


class Hooks:

    def __init__(self):
        self.hooks = dict()
        self.hook_registrar = dict()

    def register_hook(self, hook: Hook):
        if hook.name not in self.hook_registrar:
            self.hook_registrar[hook.name] = hook
        for context in hook.contexts:
            if context not in self.hooks:
                self.hooks[context] = dict()
            for command in hook.commands:
                if command not in self.hooks[context]:
                    self.hooks[context][command] = dict()
                if hook.name not in self.hooks[context][command]:
                    self.hooks[context][command][hook.name] = list()
                for stage in hook.task_life_cycle_stages.stages:
                    if stage not in self.hooks[context][command][hook.name]:
                        self.hooks[context][command][hook.name].append(stage)

    def process_hook(self, command: str, context: str, task_life_cycle_stage: int, key_value_store: KeyValueStore, task: object=None, task_id: str=None, extra_parameters:dict=dict(), logger: LoggerWrapper=LoggerWrapper())->KeyValueStore:
        if context in self.hooks:
            if command in self.hooks[context]:
                for hook_name, stages in self.hooks[context][command].items():
                    if hook_name in self.hook_registrar and task_life_cycle_stage in stages:
                        result = self.hook_registrar[hook_name].process_hook(
                            command=command,
                            context=context,
                            task_life_cycle_stage=task_life_cycle_stage,
                            key_value_store=key_value_store,
                            task=task,
                            task_id=task_id,
                            extra_parameters=extra_parameters,
                            logger=logger
                        )
                        if result is not None:
                            if isinstance(result, KeyValueStore):
                                key_value_store.store = copy.deepcopy(result.store)
        return key_value_store
    
    def any_hook_exists(self, command: str, context: str, task_life_cycle_stage: int)->bool:
        if context in self.hooks:
            if command in self.hooks[context]:
                for hook_name, life_cycles in self.hooks[context][command].items():
                    if task_life_cycle_stage in life_cycles:
                        return True
        return False


def build_non_contextual_identifiers(metadata: dict, current_identifiers: Identifiers=Identifiers())->Identifiers:
    """
        metadata:
          identifiers:                    # Non-contextual identifier
          - type: STRING                  # Example: ManifestName
            key: STRING                   # Example: my-manifest
            value: STRING|NULL            # [Optional]                  <-- Not required for type "ManifestName"
          - type: STRING                  # Example: Label
            key: STRING                   # Example: my-key
            value: STRING|NULL            # Example: my-value           <-- Required for type "Label"
    """

    new_identifiers = Identifiers()
    new_identifiers.identifiers = copy.deepcopy(current_identifiers.identifiers)
    new_identifiers.unique_identifier_value = copy.deepcopy(current_identifiers.unique_identifier_value)

    if 'identifiers' in metadata:
        if isinstance(metadata['identifiers'], list):
            for identifier_data in metadata['identifiers']:
                if 'type' in identifier_data and 'key' in identifier_data:
                    val = None
                    if 'val' in identifier_data:
                        val = identifier_data['val']
                    if 'value' in identifier_data:
                        val = identifier_data['value']
                    new_identifiers.add_identifier(identifier=Identifier(identifier_type=identifier_data['type'], key=identifier_data['key'], val=val))

    return new_identifiers


def build_contextual_identifiers(metadata: dict, current_identifiers: Identifiers=Identifiers())->Identifiers:
    """
        metadata:
          contextualIdentifiers:
          - type: STRING                # Example: ExecutionScope       <-- THEREFORE, this Manifest is scoped to 3x Environment contexts and 2x Command contexts
            key: STRING                 # Example: INCLUDE              <-- or "EXCLUDE", to specifically exclude execution in a given context
            value: STRING               # Example: Null|None
            contexts:
            - type: STRING              # Example: Environment
              names:
              - STRING                  # Example: sandbox
              - STRING                  # Example: test
              - STRING                  # Example: production
            - type: STRING              # Example: Command
              names:
              - STRING                  # Example: apply
              - STRING                  # Example: delete
    """

    new_identifiers = Identifiers()
    new_identifiers.identifiers = copy.deepcopy(current_identifiers.identifiers)
    new_identifiers.unique_identifier_value = copy.deepcopy(current_identifiers.unique_identifier_value)

    if 'contextualIdentifiers' in metadata:
        if isinstance(metadata['contextualIdentifiers'], list):
            for contextual_identifier_data in metadata['contextualIdentifiers']:
                if 'contexts' in contextual_identifier_data:
                    contexts = IdentifierContexts()
                    for context in contextual_identifier_data['contexts']:
                        if 'type' in context and 'names' in context:
                            if isinstance(context['type'], str) is True and isinstance(context['names'], list) is True:
                                context_type = context['type']
                                for name in context['names']:
                                    contexts.add_identifier_context(
                                        identifier_context=IdentifierContext(
                                            context_type=context_type,
                                            context_name=name
                                        )
                                    )
                if 'type' in contextual_identifier_data and 'key' in contextual_identifier_data:
                    val = None
                    if 'val' in contextual_identifier_data:         # pragma: no cover
                        val = contextual_identifier_data['val']
                    if 'value' in contextual_identifier_data:       # pragma: no cover
                        val = contextual_identifier_data['value']
                    new_identifiers.add_identifier(
                        identifier=Identifier(
                            identifier_type=contextual_identifier_data['type'],
                            key=contextual_identifier_data['key'],
                            val=val,
                            identifier_contexts=contexts
                        )
                    )

    return new_identifiers


class Task:

    def __init__(self, kind: str, version: str, spec: dict, metadata: dict=dict(), logger: LoggerWrapper=LoggerWrapper()):
        """
            Typical Manifest:

                kind: STRING                                                                    # [required]
                version: STRING                                                                 # [required]
                metadata:

                  # NEW....
                  identifiers:                    # Non-contextual identifier
                  - type: STRING                  # Example: ManifestName
                    key: STRING                   # Example: my-manifest
                    value: STRING|NULL            # [Optional]                  <-- Not required for type "ManifestName"
                  - type: STRING                  # Example: Label
                    key: STRING                   # Example: my-key
                    value: STRING|NULL            # Example: my-value           <-- Required for type "Label"

                  contextualIdentifiers:
                  - type: STRING              # Example: ExecutionScope       <-- THEREFORE, this Manifest is scoped to 3x Environment contexts and 2x Command contexts
                    key: STRING               # Example: INCLUDE              <-- or "EXCLUDE", to specifically exclude execution in a given context
                    value val: STRING         # Example: Null|None
                    contexts:
                    - type: STRING              # Example: Environment
                      names:
                      - STRING                  # Example: sandbox
                      - STRING                  # Example: test
                      - STRING                  # Example: production
                    - type: STRING              # Example: Command
                      names:
                      - STRING                  # Example: apply
                      - STRING                  # Example: delete

                  dependencies:
                  - identifierType: ManifestName|Label      # Link to a Non-contextual identifier
                    identifiers:
                    - key: STRING
                      value: STRING                         # Optional - required for identifierType "Label"


                  # DEPRECATED...
                  name: STRING                                                                  # [optional]
                  labels:                                                                       # [optional]
                    key: STRING

                  annotations:                                                                  # [optional]

                    # DEPRECATED....
                    contexts: CSV-STRING                                                        # [optional, but when supplied only commands within the defined context will be in scope for processing]
                    commands: CSV-STRING                                                        # [optional, but when supplied only commands listed here will bring the task in potential scope (dependant also on context)]
                    dependency/name: CSV-STRING                                                 # [optional. list of other task names this task depends on]
                    dependency/label/STRING(command)/STRING(label-name): STRING(label-value)    # [optional. select dependant task by label value]


                    
                          

                spec:
                  ... as required by the TaskProcessor ...
        """
        self.task_can_be_persisted = False
        self.logger = logger
        self.kind = kind
        self.version = version
        self.metadata = dict()
        self.identifiers = build_contextual_identifiers(
            metadata=metadata,
            current_identifiers=build_non_contextual_identifiers(metadata=metadata)
        )
        if metadata is not None:
            if isinstance(metadata, dict):
                self.metadata = keys_to_lower(data=metadata)
        self.spec = dict()
        if spec is not None:
            if isinstance(spec, dict):
                self.spec = keys_to_lower(data=spec)
        self.annotations = dict()
        self.task_dependencies = list()
        self.task_as_dict = dict()
        self._register_annotations()
        self._register_dependencies()
        self.task_checksum = None
        self.task_id = self._determine_task_id()
        logger.info('Task "{}" registered. Task checksum: {}'.format(self.task_id, self.task_checksum))

    def task_match_name(self, name: str)->bool:
        return self.identifiers.identifier_matches_any_context(identifier_type='ManifestName', key=name)

    def task_match_label(self, key: str, value: str)->bool:
        return self.identifiers.identifier_matches_any_context(identifier_type='Label', key=key, val=value)
    
    def task_qualifies_for_processing(self, processing_target_identifier: Identifier)->bool:
        qualifies = True

        # Qualify the processing_target_identifier as a valid processing type identifier
        if processing_target_identifier.identifier_type != 'ExecutionScope':
            return qualifies
        elif processing_target_identifier.key != 'processing':
            return qualifies

        # Extract processing command and processing environment
        processing_command = None
        processing_environment = None
        processing_target_context: IdentifierContext
        for processing_target_context in processing_target_identifier.identifier_contexts:
            if processing_target_context.context_type == 'Command':
                processing_command = processing_target_context.context_name
            elif processing_target_context.context_type == 'Environment':
                processing_environment = processing_target_context.context_name

        # Extract task processing rules
        candidate_identifier: Identifier
        require_command_to_qualify = False
        require_environment_to_qualify = False
        required_commands = list()
        required_environments = list()
        for candidate_identifier in self.identifiers:
            if candidate_identifier.identifier_type == processing_target_identifier.identifier_type: # ExecutionScope
                if candidate_identifier.key == 'EXCLUDE':
                    candidate_identifier_context: IdentifierContext
                    for candidate_identifier_context in candidate_identifier.identifier_contexts:
                        if candidate_identifier_context.context_type == 'Command':
                            if candidate_identifier_context.context_name == processing_command:
                                qualifies = False
                                self.logger.info('Task "{}" disqualified from processing by explicit exclusion of processing command "{}"'.format(self.task_id, processing_command))
                        elif candidate_identifier_context.context_type == 'Environment':
                            if candidate_identifier_context.context_name == processing_environment:
                                qualifies = False
                                self.logger.info('Task "{}" disqualified from processing by explicit exclusion of processing environment "{}"'.format(self.task_id, processing_environment))
                elif candidate_identifier.key == 'INCLUDE':
                    for candidate_identifier_context in candidate_identifier.identifier_contexts:
                        if candidate_identifier_context.context_type == 'Command':
                            require_command_to_qualify = True
                            required_commands.append(candidate_identifier_context.context_name)
                        elif candidate_identifier_context.context_type == 'Environment':
                            require_environment_to_qualify = True
                            required_environments.append(candidate_identifier_context.context_name)
        if qualifies is True: # Only proceed matching if qualifies is still true - no need to test if it is false
            if require_command_to_qualify is True and len(required_commands) > 0:
                if processing_command not in required_commands:
                    qualifies = False
                    self.logger.info('Task "{}" disqualified from processing because  processing command "{}" was not included in the relevant context'.format(self.task_id, processing_command))
            if require_environment_to_qualify is True and len(required_environments) > 0:
                if processing_environment not in required_environments:
                    qualifies = False
                    self.logger.info('Task "{}" disqualified from processing by environment "{}" not been defined in the relevant context'.format(self.task_id, processing_environment))

        return qualifies

    def match_name_or_label_identifier(self, identifier: Identifier)->bool:
        # Determine if this task can be processed given the processing identifier.
        if identifier.identifier_type == 'ExecutionScope' and identifier.key == 'processing':
            return self.task_qualifies_for_processing(processing_target_identifier=identifier)

        # Only process if input identifier is of a name or label type        
        if identifier.identifier_type not in ('ManifestName', 'Label',):
            return False
        
        # name or label match logic
        task_identifier: Identifier
        for task_identifier in self.identifiers:
            if task_identifier.identifier_type != 'ExecutionScope' and task_identifier.key != 'processing':

                basic_match = False
                if task_identifier.identifier_type == 'ManifestName':
                    if task_identifier.key == identifier.key:
                        basic_match = True
                elif task_identifier.identifier_type == 'Label':
                    if task_identifier.key == identifier.key and task_identifier.val == identifier.val:
                        basic_match = True

                if len(identifier.identifier_contexts) == 0:
                    return basic_match  # No need for further processing - we have at least one match
                else:
                    # If we have a basic match, and the input identifier has some context,  match at least one of the provided contexts as well in order to return true
                    if basic_match is True:
                        task_identifier_context: IdentifierContext
                        for task_identifier_context in task_identifier.identifier_contexts:
                            identifier_context: IdentifierContext
                            for identifier_context in identifier.identifier_contexts:
                                if identifier_context == task_identifier_context:
                                    return True # No need for further processing - we have at least one contextual match as well

        
        return False

    def _register_annotations(self):
        if 'annotations' not in self.metadata:                          # pragma: no cover
            return
        if self.metadata['annotations'] is None:                        # pragma: no cover
            return
        if isinstance(self.metadata['annotations'], dict) is False:     # pragma: no cover
            return
        for annotation_key, annotation_value in self.metadata['annotations'].items():
            self.annotations[annotation_key] = '{}'.format(annotation_value)

    def _dependencies_found_in_metadata(self, meta_data: dict)->list:
        if 'dependencies' not in self.metadata:                         # pragma: no cover
            return list()
        if self.metadata['dependencies'] is None:                       # pragma: no cover
            return list()
        if isinstance(self.metadata['dependencies'], list) is False:    # pragma: no cover
            return list()
        return self.metadata['dependencies']

    def _register_dependencies(self):
        """
              metadata:
                dependencies:
                - identifierType: ManifestName|Label      # Link to a Non-contextual identifier
                  identifiers:
                  - key: STRING
                    value: STRING                         # Optional - required for identifierType "Label"
        """
        for dependency in self._dependencies_found_in_metadata(meta_data=self.metadata):
            if isinstance(dependency, dict) is True:
                if 'identifierType' in dependency and 'identifiers' in dependency:
                    if dependency['identifiers'] is not None and dependency['identifierType'] is not None:
                        if isinstance(dependency['identifiers'], list) and isinstance(dependency['identifierType'], str):
                            dependency_reference_type = dependency['identifierType']
                            dependency_references = dependency['identifiers']
                            for dependency_reference in dependency_references:
                                if 'key' in dependency_reference:
                                    if dependency_reference_type == 'ManifestName':
                                        self.task_dependencies.append(
                                            Identifier(
                                                identifier_type='ManifestName',
                                                key=dependency_reference['key']
                                            )
                                        )
                                    if dependency_reference_type == 'Label':
                                        self.task_dependencies.append(
                                            Identifier(
                                                identifier_type='Label',
                                                key=dependency_reference['key'],
                                                val=dependency_reference['value']
                                            )
                                        )

    def _calculate_task_checksum(self)->str:
        data = dict()
        data['kind'] = self.kind
        data['version'] = self.version
        if len(self.metadata) > 0:
            data['metadata'] = self.metadata
        if len(self.spec) > 0:
            data['spec'] = self.spec
        self.task_as_dict = data
        return hashlib.sha256(json.dumps(data).encode('utf-8')).hexdigest()

    def _determine_task_id(self):
        """
                  identifiers:                    # Non-contextual identifier
                  - type: STRING                  # Example: ManifestName
                    key: STRING                   # Example: my-manifest
                    value: STRING|NULL            # [Optional]                  <-- Not required for type "ManifestName"
                  - type: STRING                  # Example: Label
                    key: STRING                   # Example: my-key
                    value: STRING|NULL            # Example: my-value           <-- Required for type "Label"
        """
        task_id = self._calculate_task_checksum()
        self.task_checksum = copy.deepcopy(task_id)
        
        identifier: Identifier
        for identifier in self.identifiers:
            if len(identifier.identifier_contexts) == 0:            
                if identifier.identifier_type == 'ManifestName':
                    if identifier.key is not None:
                        if isinstance(identifier.key, str) is True:
                            if len(identifier.key) > 0:
                                task_id = copy.deepcopy(identifier.key)
                                self.task_can_be_persisted = True
        if self.task_can_be_persisted is False:
            self.logger.warning(message='Task "{}" is NOT a named task and can therefore NOT be persisted.'.format(task_id))
        return task_id
        
    def __iter__(self):
        for k,v in self.task_as_dict.items():
            yield (k, v)


class TaskProcessor:

    def __init__(self, kind: str, kind_versions: list, supported_commands: list=['apply', 'get', 'delete', 'describe'], logger: LoggerWrapper=LoggerWrapper()):
        self.logger = logger
        self.kind = kind
        self.versions = kind_versions
        self.supported_commands = supported_commands

    def task_pre_processing_check(
        self,
        task: Task,
        command: str,
        context: str='default',
        key_value_store: KeyValueStore=KeyValueStore(),
        call_process_task_if_check_pass: bool=False,
        state_persistence: StatePersistence=StatePersistence()
    )->KeyValueStore:
        """
        Checks if the task can be run.
        """
        task_run_id = 'PROCESSING_TASK:{}:{}:{}'.format(
            task.task_id,
            command,
            context
        )
        if task_run_id not in key_value_store.store:
            key_value_store.save(key=task_run_id, value=1)
        if key_value_store.store[task_run_id] == 1:
            try:
                if call_process_task_if_check_pass is True:
                    key_value_store = self.process_task(task=task, command=command, context=context, key_value_store=key_value_store, state_persistence=state_persistence)
                    key_value_store.store[task_run_id] = 2
            except: # pragma: no cover
                key_value_store.store[task_run_id] = -1
        else:
            self.logger.warning(message='Appears task was already previously validated and/or executed')
        return key_value_store

    def process_task(self, task: Task, command: str, context: str='default', key_value_store: KeyValueStore=KeyValueStore(), state_persistence: StatePersistence=StatePersistence())->KeyValueStore:
        raise Exception('Not implemented')  # pragma: no cover


def hook_function_always_throw_exception(
    hook_name:str,
    task:Task,
    key_value_store:KeyValueStore,
    command:str,
    context:str,
    task_life_cycle_stage:int,
    extra_parameters:dict,
    logger:LoggerWrapper
):
    task_id = 'unknown'
    if task is not None:
        if isinstance(task, Task):
            task_id = task.task_id
    exception_message = 'Hook "{}" forced exception on command "{}" in context "{}" for life stage "{}" in task "{}"'.format(
        hook_name,
        command,
        context,
        task_life_cycle_stage,
        task_id
    )
    if 'ExceptionMessage' in extra_parameters:
        logger.error(exception_message)
        exception_message = extra_parameters['ExceptionMessage']
    raise Exception(exception_message)


def build_command_identifier(command: str, context: str)->Identifier:
    processing_contexts = IdentifierContexts()
    processing_contexts.add_identifier_context(
        identifier_context=IdentifierContext(
            context_type='Environment',
            context_name=context
        )
    )
    processing_contexts.add_identifier_context(
        identifier_context=IdentifierContext(
            context_type='Command',
            context_name=command
        )
    )
    processing_target_identifier = Identifier(
        identifier_type='ExecutionScope',
        key='processing',
        identifier_contexts=processing_contexts
    )
    return processing_target_identifier


class Tasks:

    """
        TASK_PRE_REGISTER                       = 1
        TASK_PRE_REGISTER_ERROR                 = -1
        TASK_REGISTERED                         = 2
        TASK_REGISTERED_ERROR                   = -2
        TASK_PRE_PROCESSING_START               = 3
        TASK_PRE_PROCESSING_START_ERROR         = -3
        TASK_PRE_PROCESSING_COMPLETED           = 4
        TASK_PRE_PROCESSING_COMPLETED_ERROR     = -4
        TASK_PROCESSING_PRE_START               = 5
        TASK_PROCESSING_PRE_START_ERROR         = -5
        TASK_PROCESSING_POST_DONE               = 6
        TASK_PROCESSING_POST_DONE_ERROR         = -6
    """

    def __init__(self, logger: LoggerWrapper=LoggerWrapper(), key_value_store: KeyValueStore=KeyValueStore(), hooks: Hooks=Hooks(), state_persistence: StatePersistence=StatePersistence()):
        self.logger = logger
        self.tasks = dict()
        self.task_processors_executors = dict()
        self.task_processor_register = dict()
        self.key_value_store = key_value_store
        self.hooks = hooks
        self.state_persistence = state_persistence
        self.state_persistence.retrieve_all_state_from_persistence()
        self._register_task_registration_failure_exception_throwing_hook()

    def _register_task_registration_failure_exception_throwing_hook(self):
        required_task_life_cycle_stages = TaskLifecycleStages(init_default_stages=False)
        required_task_life_cycle_stages.register_lifecycle_stage(task_life_cycle_stage=TaskLifecycleStage.TASK_REGISTERED_ERROR)
        if self.hooks.any_hook_exists(command='NOT_APPLICABLE', context='ALL', task_life_cycle_stage=TaskLifecycleStage.TASK_REGISTERED_ERROR) is False:
            self.hooks.register_hook(
                hook=Hook(
                    name='DEFAULT_TASK_REGISTERED_ERROR_HOOK',
                    commands=['NOT_APPLICABLE',],
                    contexts=['ALL',],
                    task_life_cycle_stages=required_task_life_cycle_stages,
                    function_impl=hook_function_always_throw_exception,
                    logger=self.logger
                )
            )

    def add_task(self, task: Task):
        if task.task_id in self.tasks:
            raise Exception('Task with ID "{}" was already added previously. Please use the "metadata.name" attribute to identify separate (but perhaps similar) manifests.')
        self.key_value_store = self.hooks.process_hook(
            command='NOT_APPLICABLE',
            context='ALL',
            task_life_cycle_stage=TaskLifecycleStage.TASK_PRE_REGISTER,
            key_value_store=copy.deepcopy(self.key_value_store),
            task=task,
            task_id=task.task_id
        )
        processor_id = '{}:{}'.format(task.kind, task.version)
        if processor_id not in self.task_processor_register:
            self.key_value_store = self.hooks.process_hook(
                command='NOT_APPLICABLE',
                context='ALL',
                task_life_cycle_stage=TaskLifecycleStage.TASK_REGISTERED_ERROR,
                key_value_store=self.key_value_store,
                task=task,
                task_id='N/A',
                extra_parameters='Task kind "{}" with version "{}" has no processor registered. Ensure all task processors are registered before adding tasks.'.format(task.kind, task.version),
                logger=self.logger
            )
        self.tasks[task.task_id] = task
        self.key_value_store = self.hooks.process_hook(
            command='NOT_APPLICABLE',
            context='ALL',
            task_life_cycle_stage=TaskLifecycleStage.TASK_REGISTERED,
            key_value_store=copy.deepcopy(self.key_value_store),
            task=task,
            task_id=task.task_id
        )

    def register_task_processor(self, processor: TaskProcessor):
        if isinstance(processor.versions, list):
            executor_id = '{}'.format(processor.kind)
            for version in processor.versions:
                executor_id = '{}:{}'.format(executor_id, version)
            self.task_processors_executors[executor_id] = processor
            for version in processor.versions:
                id = '{}:{}'.format(processor.kind, version)
                self.task_processor_register[id] = executor_id

    def find_task_by_name(self, name: str, calling_task_id: str=None)->Task:
        for task_id, candidate_task in self.tasks.items():
            process = True
            if calling_task_id is not None:
                if calling_task_id == task_id:
                    process = False
            if process is True:
                if candidate_task.task_match_name(name=name) is True:
                    return candidate_task
        return None
       
    def get_task_by_task_id(self, task_id: str)->Task:
        if task_id in self.tasks:
            return self.tasks[task_id]
        raise Exception('Task with task_id "{}" NOT FOUND'.format(task_id))

    def find_tasks_matching_identifier_and_return_list_of_task_ids(self, identifier: Identifier)->list:
        tasks_found = list()
        task_id: str
        task: Task
        for task_id, task in self.tasks.items():
            if task.match_name_or_label_identifier(identifier=identifier) is True:
                tasks_found.append(task.task_id)
        return tasks_found

    def _order_tasks(self, ordered_list: list, candidate_task: Task, processing_target_identifier: Identifier)->list:
        new_ordered_list = copy.deepcopy(ordered_list)
        task_dependency_identifier: Identifier
        for task_dependency_identifier in candidate_task.task_dependencies:
            candidate_dependant_tasks_as_list = self.find_tasks_matching_identifier_and_return_list_of_task_ids(identifier=task_dependency_identifier)
            if task_dependency_identifier.identifier_type == 'ManifestName' and len(candidate_dependant_tasks_as_list) == 0:
                raise Exception('Dependant task "{}" required, but NOT FOUND'.format(task_dependency_identifier.key))
            candidate_dependant_task_id: str
            for candidate_dependant_task_id in candidate_dependant_tasks_as_list:
                if candidate_dependant_task_id not in new_ordered_list:
                    dependant_candidate_task = self.get_task_by_task_id(task_id=candidate_dependant_task_id)
                    if dependant_candidate_task.task_qualifies_for_processing(processing_target_identifier=processing_target_identifier) is True:
                        if dependant_candidate_task not in new_ordered_list:
                            new_ordered_list.append(candidate_dependant_task_id)
                    else:
                        raise Exception('Dependant task "{}" has Task "{}" as dependency, but the dependant task is not in scope for processing - cannot proceed. Either remove the task dependency or adjust the execution scope of the dependant task.'.format(candidate_task.task_id, candidate_dependant_task_id))
        if candidate_task.task_id not in new_ordered_list:
            new_ordered_list.append(candidate_task.task_id)
        return new_ordered_list

    def calculate_current_task_order(self, processing_target_identifier: Identifier)->list:
        task_order = list()
        task_id: str
        task: Task
        for task_id, task in self.tasks.items():
            self.logger.debug('calculate_current_task_order(): Considering task "{}"'.format(task.task_id))
            if task.task_qualifies_for_processing(processing_target_identifier=processing_target_identifier) is True:
                if task.task_id not in task_order:
                    task_order = self._order_tasks(ordered_list=task_order, candidate_task=task, processing_target_identifier=processing_target_identifier)
        return task_order

    def process_context(self, command: str, context: str):
        # First, build the processing identifier object
        processing_target_identifier = build_command_identifier(command=command, context=context)

        # Determine the order based on task dependencies
        task_order = self.calculate_current_task_order(processing_target_identifier=processing_target_identifier)
        task_order = list(dict.fromkeys(task_order))    # de-duplicate
        self.logger.debug('task_order={}'.format(task_order))

        # Process tasks in order, with the available task processor registered for this task kind and version
        for task_id in task_order:
            if task_id in self.tasks:
                task = self.tasks[task_id]

                self.key_value_store = self.key_value_store = self.hooks.process_hook(
                    command=command,
                    context=context,
                    task_life_cycle_stage=TaskLifecycleStage.TASK_PRE_PROCESSING_START,
                    key_value_store=self.key_value_store,
                    task=task,
                    task_id=task_id,
                    logger=self.logger
                )

                target_task_processor_id = '{}:{}'.format(task.kind, task.version)
                if target_task_processor_id in self.task_processor_register:
                    target_task_processor_executor_id = self.task_processor_register[target_task_processor_id]
                    if target_task_processor_executor_id in self.task_processors_executors:
                        target_task_processor_executor = self.task_processors_executors[target_task_processor_executor_id]
                        if isinstance(target_task_processor_executor, TaskProcessor):                            
                            self.key_value_store = target_task_processor_executor.task_pre_processing_check(task=task, command=command, context=context, key_value_store=self.key_value_store, call_process_task_if_check_pass=True, state_persistence=self.state_persistence)

                            self.key_value_store = self.key_value_store = self.hooks.process_hook(
                                command=command,
                                context=context,
                                task_life_cycle_stage=TaskLifecycleStage.TASK_PRE_PROCESSING_COMPLETED,
                                key_value_store=self.key_value_store,
                                task=task,
                                task_id=task_id,
                                logger=self.logger
                            )

                            self.key_value_store = self.key_value_store = self.hooks.process_hook(
                                command=command,
                                context=context,
                                task_life_cycle_stage=TaskLifecycleStage.TASK_PROCESSING_PRE_START,
                                key_value_store=self.key_value_store,
                                task=task,
                                task_id=task_id,
                                logger=self.logger
                            )
                            
                            self.state_persistence.persist_all_state()

                            self.key_value_store = self.key_value_store = self.hooks.process_hook(
                                command=command,
                                context=context,
                                task_life_cycle_stage=TaskLifecycleStage.TASK_PROCESSING_POST_DONE,
                                key_value_store=self.key_value_store,
                                task=task,
                                task_id=task_id,
                                logger=self.logger
                            )

