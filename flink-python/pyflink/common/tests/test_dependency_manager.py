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
import re
import unittest

from pyflink.common import Configuration
from pyflink.common.dependency_manager import DependencyManager
from pyflink.table import TableConfig
from pyflink.testing.test_case_utils import PyFlinkTestCase


def replace_uuid(input_obj):
    if isinstance(input_obj, str):
        return re.sub(r'[a-z0-9]{8}-[a-z0-9]{4}-[a-z0-9]{4}-[a-z0-9]{4}-[a-z0-9]{12}',
                      '{uuid}', input_obj)
    elif isinstance(input_obj, dict):
        input_obj_copy = dict()
        for key in input_obj:
            input_obj_copy[replace_uuid(key)] = replace_uuid(input_obj[key])
        return input_obj_copy


class DependencyManagerTests(PyFlinkTestCase):

    def setUp(self):
        self.dependency_manager = DependencyManager()
        self.j_env = MockedJavaEnv()
        self.config = Configuration()

    def test_add_python_file(self):
        self.dependency_manager.add_python_file("tmp_dir/test_file1.py")
        self.dependency_manager.add_python_file("tmp_dir/test_file2.py")
        self.dependency_manager.transmit_parameters_to_jvm(self.config)
        self.dependency_manager.register_files_to_jvm(self.j_env)

        self.assertEqual(
            {"python_file_0_{uuid}": "test_file1.py", "python_file_1_{uuid}": "test_file2.py"},
            json.loads(replace_uuid(self.config.to_dict()[DependencyManager.PYTHON_FILE_MAP])))
        self.assertEqual(
            {"python_file_0_{uuid}": "tmp_dir/test_file1.py",
             "python_file_1_{uuid}": "tmp_dir/test_file2.py"},
            replace_uuid(self.j_env.result))

    def test_set_python_requirements(self):
        self.dependency_manager.set_python_requirements("tmp_dir/requirements.txt",
                                                        "tmp_dir/cache_dir")
        self.dependency_manager.transmit_parameters_to_jvm(self.config)
        self.dependency_manager.register_files_to_jvm(self.j_env)

        self.assertEqual(
            "python_requirements_list_file_{uuid}",
            replace_uuid(self.config.to_dict()[DependencyManager.PYTHON_REQUIREMENTS]))
        self.assertEqual(
            "python_requirements_dir_{uuid}",
            replace_uuid(self.config.to_dict()[DependencyManager.PYTHON_REQUIREMENTS_DIR]))
        self.assertEqual(
            {"python_requirements_list_file_{uuid}": "tmp_dir/requirements.txt",
             "python_requirements_dir_{uuid}": "tmp_dir/cache_dir"},
            replace_uuid(self.j_env.result))

        # test single parameter and remove old requirements_dir setting
        self.j_env.result = dict()
        self.config = Configuration()
        self.dependency_manager.set_python_requirements("tmp_dir/requirements.txt")
        self.dependency_manager.transmit_parameters_to_jvm(self.config)
        self.dependency_manager.register_files_to_jvm(self.j_env)

        self.assertEqual(
            "python_requirements_list_file_{uuid}",
            replace_uuid(self.config.to_dict()[DependencyManager.PYTHON_REQUIREMENTS]))
        self.assertNotIn(DependencyManager.PYTHON_REQUIREMENTS_DIR, self.config.to_dict())
        self.assertEqual(
            {"python_requirements_list_file_{uuid}": "tmp_dir/requirements.txt"},
            replace_uuid(self.j_env.result))

    def test_add_python_archive(self):
        self.dependency_manager.add_python_archive("tmp_dir/py27.zip")
        self.dependency_manager.add_python_archive("tmp_dir/venv2.zip", "py37")
        self.dependency_manager.transmit_parameters_to_jvm(self.config)
        self.dependency_manager.register_files_to_jvm(self.j_env)

        self.assertEqual(
            {"python_archive_0_{uuid}": "py27.zip", "python_archive_1_{uuid}": "py37"},
            json.loads(replace_uuid(self.config.to_dict()[DependencyManager.PYTHON_ARCHIVES_MAP])))
        self.assertEqual(
            {"python_archive_0_{uuid}": "tmp_dir/py27.zip",
             "python_archive_1_{uuid}": "tmp_dir/venv2.zip"},
            replace_uuid(self.j_env.result))

    def test_set_python_executable(self):
        table_config = TableConfig()
        table_config.set_python_executable("/usr/bin/python3")

        self.assertEqual(
            "/usr/bin/python3",
            table_config.get_configuration().get_string(DependencyManager.PYTHON_EXEC, ""))


class MockedJavaEnv(object):

    def __init__(self):
        self.result = dict()

    def registerCachedFile(self, file_path, name):
        self.result[name] = file_path


if __name__ == "__main__":
    try:
        import xmlrunner

        testRunner = xmlrunner.XMLTestRunner(output='target/test-reports')
    except ImportError:
        testRunner = None
    unittest.main(testRunner=testRunner, verbosity=2)
