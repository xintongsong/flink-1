################################################################################
#  Licensed to the Apache Software Foundation (ASF) under one
#  or more contributor license agreements.  See the NOTICE file
#  distributed with this work for additional information
#  regarding copyright ownership.  The ASF licenses this file
#  to you under the Apache License, Version 2.0 (the
#  "License"); you may not use this file except in compliance
#  with the License.  You may obtain a copy of the License at
#
#      http://www.apache.org/licenses/LICENSE-2.0
#
#  Unless required by applicable law or agreed to in writing, software
#  distributed under the License is distributed on an "AS IS" BASIS,
#  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
#  See the License for the specific language governing permissions and
# limitations under the License.
################################################################################
import json
import os
import uuid

__all__ = ['DependencyManager']


class DependencyManager(object):
    """
    Container class of dependency-related parameters. It collects all the dependency parameters
    and transmit them to JVM before executing job.
    """

    PYTHON_FILE_PREFIX = "python_file"
    PYTHON_REQUIREMENTS_PREFIX = "python_requirements_list_file"
    PYTHON_REQUIREMENTS_DIR_PREFIX = "python_requirements_dir"
    PYTHON_ARCHIVE_PREFIX = "python_archive"

    PYTHON_FILE_MAP = "PYTHON_FILE_MAP"
    PYTHON_REQUIREMENTS = "PYTHON_REQUIREMENTS"
    PYTHON_REQUIREMENTS_DIR = "PYTHON_REQUIREMENTS_DIR"
    PYTHON_ARCHIVES_MAP = "PYTHON_ARCHIVES_MAP"
    PYTHON_EXEC = "PYTHON_EXEC"

    def __init__(self):
        self._user_file_map = dict()  # type: dict[str, str]
        self._python_file_map = dict()  # type: dict[str, str]
        self._requirements_list_name = None  # type: str
        self._requirements_dir_name = None  # type: str
        self._archives_map = dict()  # type: dict[str, str]
        self._counter_map = dict()  # type: dict[str, int]

    def _generate_file_name(self, prefix):
        if prefix not in self._counter_map:
            self._counter_map[prefix] = 0
        else:
            self._counter_map[prefix] += 1
        return "%s_%d_%s" % (prefix, self._counter_map[prefix], uuid.uuid4())

    def add_python_file(self, file_path):
        name = self._generate_file_name(DependencyManager.PYTHON_FILE_PREFIX)
        self._user_file_map[name] = file_path
        self._python_file_map[name] = os.path.basename(file_path)

    def set_python_requirements(self, requirements_list_file, requirements_cached_dir=None):
        if self._requirements_list_name is None:
            self._requirements_list_name = "%s_%s" % (
                DependencyManager.PYTHON_REQUIREMENTS_PREFIX, uuid.uuid4())
        self._user_file_map[self._requirements_list_name] = requirements_list_file

        if requirements_cached_dir is not None:
            if self._requirements_dir_name is None:
                self._requirements_dir_name = "%s_%s" % (
                    DependencyManager.PYTHON_REQUIREMENTS_DIR_PREFIX, uuid.uuid4())
            self._user_file_map[self._requirements_dir_name] = requirements_cached_dir
        else:
            # remove existing requirements directory configuration
            if self._requirements_dir_name is not None:
                del self._user_file_map[self._requirements_dir_name]
                self._requirements_dir_name = None

    def add_python_archive(self, archive_path, target_dir=None):
        name = self._generate_file_name(DependencyManager.PYTHON_ARCHIVE_PREFIX)
        self._user_file_map[name] = archive_path
        if target_dir is not None:
            self._archives_map[name] = target_dir
        else:
            self._archives_map[name] = os.path.basename(self._user_file_map[name])

    def transmit_parameters_to_jvm(self, parameters):
        """
        Transmit dependency setting to JVM. This method should be called before executing or
        translating the job.

        This method is used internal.

        :param parameters: The Configuration object of TableConfig.
        :type parameters: pyflink.common.Configuration
        """
        job_param = dict()
        if len(self._python_file_map) > 0:
            job_param[DependencyManager.PYTHON_FILE_MAP] = json.dumps(self._python_file_map)
        if self._requirements_list_name is not None:
            job_param[DependencyManager.PYTHON_REQUIREMENTS] = self._requirements_list_name
        if self._requirements_dir_name is not None:
            job_param[DependencyManager.PYTHON_REQUIREMENTS_DIR] = \
                self._requirements_dir_name
        if len(self._archives_map) > 0:
            job_param[DependencyManager.PYTHON_ARCHIVES_MAP] = \
                json.dumps(self._archives_map)
        for key in job_param:
            parameters.set_string(key, job_param[key])

    def register_files_to_jvm(self, j_env):
        """
        Register user files to Java ExecutionEnvironment/StreamExecutionEnvironment.

        This method is used internal.

        :param j_env: The Java ExecutionEnvironment/StreamExecutionEnvironment object.
        :type j_env: JavaObject
        """
        for key in self._user_file_map:
            j_env.registerCachedFile(self._user_file_map[key], key)
