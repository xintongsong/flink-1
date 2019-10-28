/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.python;

import org.apache.flink.annotation.Internal;
import org.apache.flink.api.common.cache.DistributedCache;

import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.ObjectMapper;

import java.io.File;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

/**
 * This class is used to parse the information of python dependency and environment management
 * stored in GlobalJobParameters.
 * The parse result will be used to create PythonEnvironmentManager.
 */
@Internal
public class PythonDependencyManager {

	public static final String PYTHON_FILE_MAP = "PYTHON_FILE_MAP";
	public static final String PYTHON_REQUIREMENTS = "PYTHON_REQUIREMENTS";
	public static final String PYTHON_REQUIREMENTS_DIR = "PYTHON_REQUIREMENTS_DIR";
	public static final String PYTHON_ARCHIVES_MAP = "PYTHON_ARCHIVES_MAP";
	public static final String PYTHON_EXEC = "PYTHON_EXEC";

	private Map<String, String> filesInPythonPath;
	private String requirementsFilePath;
	private String requirementsDirPath;
	private String pythonExec;
	private Map<String, String> archives;

	/**
	 * @param filesInPythonPath Local path and origin file name of user python files uploaded by
	 *                          TableEnvironment#add_python_file() or command option "-pyfs".
	 *                          Key is local absolute path and value is origin file name.
	 * @param requirementsFilePath The file path of requirements.txt file specified by
	 *                             TableEnvironment#set_python_requirements() or command option "-pyreq".
	 * @param requirementsDirPath The path of the directory uploaded by TableEnvironment#set_python_requirements()
	 *                            or command option "-pyreq". It is used to support installing python packages offline.
	 *                            If exists it should contain all the packages listed in requirements.txt file.
	 * @param pythonExec The path of python interpreter (e.g. /usr/local/bin/python).
	 *                   It can be specified by TableConfig#set_python_executable() or command option "-pyexec".
	 * @param archives Local path and target directory name of user python archives uploaded by
	 *                 TableEnvironment#add_python_archive() or command option "-pyarch".
	 *                 Key is local absolute path and value is target directory name.
	 */
	public PythonDependencyManager(
		Map<String, String> filesInPythonPath,
		String requirementsFilePath,
		String requirementsDirPath,
		String pythonExec,
		Map<String, String> archives) {
		this.filesInPythonPath = filesInPythonPath;
		this.requirementsFilePath = requirementsFilePath;
		this.requirementsDirPath = requirementsDirPath;
		this.pythonExec = pythonExec;
		this.archives = archives;
	}

	/**
	 * @return Local path and origin file name of user python files uploaded by
	 * 	       TableEnvironment#add_python_file() or command option "-pyfs".
	 * 	       Key is local absolute path and value is origin file name.
	 */
	public Map<String, String> getFilesInPythonPath() {
		return filesInPythonPath;
	}

	/**
	 * @return The file path of requirements.txt file specified by
	 * 	       TableEnvironment#set_python_requirements() or command option "-pyreq".
	 */
	public String getRequirementsFilePath() {
		return requirementsFilePath;
	}

	/**
	 * @return The path of the directory uploaded by TableEnvironment#set_python_requirements()
	 * 	       or command option "-pyreq". It is used to support installing python packages
	 * 	       offline.
	 * 	       If exists it should contain all the packages listed in requirements.txt file.
	 */
	public String getRequirementsDirPath() {
		return requirementsDirPath;
	}

	/**
	 * @return The path of python interpreter (e.g. /usr/local/bin/python).
	 * 	       It can be specified by TableConfig#set_python_executable() or command option "-pyexec".
	 */
	public String getPythonExec() {
		return pythonExec;
	}

	/**
	 * @return Local path and target directory name of user python archives uploaded by
	 * 	       TableEnvironment#add_python_archive() or command option "-pyarch".
	 * 	       Key is local absolute path and value is target directory name.
	 */
	public Map<String, String> getArchives() {
		return archives;
	}

	/**
	 * Creates PythonDependencyManager from GlobalJobParameters and DistributedCache.
	 *
	 * @param dependencyMetaData The parameter map which contains information of python dependency.
	 *                           Usually it is the map of GlobalJobParameters.
	 * @param distributedCache The DistributedCache object of current task.
	 * @return The PythonDependencyManager object that contains whole information of python dependency.
	 */
	public static PythonDependencyManager createDependencyManager(
		Map<String, String> dependencyMetaData,
		DistributedCache distributedCache)
		throws IOException {
		Map<String, String> filesPathToFilesName = new HashMap<>();
		if (dependencyMetaData.get(PYTHON_FILE_MAP) != null) {
			ObjectMapper mapper = new ObjectMapper();
			Map<String, String> filesIdToFilesName =
				mapper.readValue(dependencyMetaData.get(PYTHON_FILE_MAP), HashMap.class);
			for (Map.Entry<String, String> entry: filesIdToFilesName.entrySet()) {
				File pythonFile = distributedCache.getFile(entry.getKey());
				String filePath = pythonFile.getAbsolutePath();
				filesPathToFilesName.put(filePath, entry.getValue());
			}
		}

		String requirementsFilePath = null;
		String requirementsDirPath = null;
		if (dependencyMetaData.get(PYTHON_REQUIREMENTS) != null) {
			requirementsFilePath = distributedCache.getFile(
				dependencyMetaData.get(PYTHON_REQUIREMENTS)).getAbsolutePath();
			if (dependencyMetaData.get(PYTHON_REQUIREMENTS_DIR) != null) {
				requirementsDirPath = distributedCache.getFile(
					dependencyMetaData.get(PYTHON_REQUIREMENTS_DIR)).getAbsolutePath();
			}
		}

		Map<String, String> archives = new HashMap<>();
		if (dependencyMetaData.get(PYTHON_ARCHIVES_MAP) != null) {
			ObjectMapper mapper = new ObjectMapper();
			Map<String, String> archivesMap =
				mapper.readValue(dependencyMetaData.get(PYTHON_ARCHIVES_MAP), HashMap.class);

			for (Map.Entry<String, String> entry: archivesMap.entrySet()) {
				String zipFilePath = distributedCache.getFile(entry.getKey()).getAbsolutePath();
				String targetPath = entry.getValue();
				archives.put(zipFilePath, targetPath);
			}
		}

		String pythonExec = null;
		if (dependencyMetaData.get(PYTHON_EXEC) != null) {
			pythonExec = dependencyMetaData.get(PYTHON_EXEC);
		}

		return new PythonDependencyManager(
			filesPathToFilesName,
			requirementsFilePath,
			requirementsDirPath,
			pythonExec,
			archives);
	}
}
