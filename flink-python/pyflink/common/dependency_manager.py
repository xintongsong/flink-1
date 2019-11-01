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
    PYTHON_REQUIREMENTS_FILE_PREFIX = "python_requirements_file"
    PYTHON_REQUIREMENTS_CACHE_PREFIX = "python_requirements_cache"
    PYTHON_ARCHIVE_PREFIX = "python_archive"

    PYTHON_FILE_MAP = "PYTHON_FILE_MAP"
    PYTHON_REQUIREMENTS_FILE = "PYTHON_REQUIREMENTS_FILE"
    PYTHON_REQUIREMENTS_CACHE = "PYTHON_REQUIREMENTS_CACHE"
    PYTHON_ARCHIVES_MAP = "PYTHON_ARCHIVES_MAP"
    PYTHON_EXEC = "PYTHON_EXEC"

    def __init__(self, parameters, j_env):
        self._parameters = parameters
        self._j_env = j_env
        self._python_file_map = dict()  # type: dict[str, str]
        self._archives_map = dict()  # type: dict[str, str]
        self._counter_map = dict()  # type: dict[str, int]

    def _generate_file_key(self, prefix):
        if prefix not in self._counter_map:
            self._counter_map[prefix] = 0
        else:
            self._counter_map[prefix] += 1
        return "%s_%d_%s" % (prefix, self._counter_map[prefix], uuid.uuid4())

    def add_python_file(self, file_path):
        key = self._generate_file_key(DependencyManager.PYTHON_FILE_PREFIX)
        self._python_file_map[key] = os.path.basename(file_path)
        self._parameters.set_string(
            DependencyManager.PYTHON_FILE_MAP, json.dumps(self._python_file_map))
        self.register_file(key, file_path)

    def set_python_requirements(self, requirements_file_path, requirements_cached_dir=None):

        if self._parameters.contains_key(DependencyManager.PYTHON_REQUIREMENTS_FILE):
            self.remove_file(
                self._parameters.get_string(DependencyManager.PYTHON_REQUIREMENTS_FILE, ""))
            self._parameters.remove_config(DependencyManager.PYTHON_REQUIREMENTS_FILE)

        if self._parameters.contains_key(DependencyManager.PYTHON_REQUIREMENTS_CACHE):
            self.remove_file(
                self._parameters.get_string(DependencyManager.PYTHON_REQUIREMENTS_CACHE, ""))
            self._parameters.remove_config(DependencyManager.PYTHON_REQUIREMENTS_CACHE)

        requirements_file_key = "%s_%s" % (
            DependencyManager.PYTHON_REQUIREMENTS_FILE_PREFIX, uuid.uuid4())
        self._parameters.set_string(DependencyManager.PYTHON_REQUIREMENTS_FILE,
                                    requirements_file_key)
        self.register_file(requirements_file_key, requirements_file_path)

        if requirements_cached_dir is not None:
            requirements_cache_key = "%s_%s" % (
                DependencyManager.PYTHON_REQUIREMENTS_CACHE_PREFIX, uuid.uuid4())
            self._parameters.set_string(DependencyManager.PYTHON_REQUIREMENTS_CACHE,
                                        requirements_cache_key)
            self.register_file(requirements_cache_key, requirements_cached_dir)

    def add_python_archive(self, archive_path, target_dir=None):
        key = self._generate_file_key(DependencyManager.PYTHON_ARCHIVE_PREFIX)
        if target_dir is not None:
            self._archives_map[key] = target_dir
        else:
            self._archives_map[key] = os.path.basename(archive_path)
        self._parameters.set_string(DependencyManager.PYTHON_ARCHIVES_MAP,
                                    json.dumps(self._archives_map))
        self.register_file(key, archive_path)

    def register_file(self, key, file_path):
        self._j_env.registerCachedFile(file_path, key)

    def remove_file(self, key):
        cache_files = self._j_env.getCachedFiles()
        for key_file_tuple in cache_files:
            if key == key_file_tuple.f0:
                cache_files.remove(key_file_tuple)
                return
