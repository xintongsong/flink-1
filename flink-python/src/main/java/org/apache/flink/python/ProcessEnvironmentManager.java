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
import org.apache.flink.annotation.VisibleForTesting;
import org.apache.flink.configuration.ConfigConstants;
import org.apache.flink.python.util.UnzipUtil;
import org.apache.flink.util.FileUtils;

import org.apache.beam.model.pipeline.v1.RunnerApi;
import org.apache.beam.runners.core.construction.Environments;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.DataOutputStream;
import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.nio.file.FileSystems;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;

/**
 * The ProcessEnvironmentManager used to prepare the working dir of python UDF worker
 * and create ProcessEnvironment object of Beam Fn API. It will be created if the python
 * function runner is configured to run python UDF in process mode.
 */
@Internal
public class ProcessEnvironmentManager implements PythonEnvironmentManager {

	private static final Logger LOG = LoggerFactory.getLogger(ProcessEnvironmentManager.class);

	static final String PYTHON_REQUIREMENTS = "_PYTHON_REQUIREMENTS";
	static final String PYTHON_REQUIREMENTS_DIR = "_PYTHON_REQUIREMENTS_DIR";
	static final String PYTHON_REQUIREMENTS_TARGET_DIR = "_PYTHON_REQUIREMENTS_TARGET";
	static final String PYTHON_WORKING_DIR = "_PYTHON_WORKING_DIR";

	static final String PYTHON_TMP_DIR_PREFIX = "python_dist_";
	static final String PYTHON_REQUIREMENTS_TARGET_DIR_NAME = "python_requirements_target";
	static final String PYTHON_ARCHIVES_DIR = "python_archives";
	static final String PYTHON_PATH_FILES_DIR = "python_path_files_dir";

	private PythonDependencyManager dependencyManager;
	private String pythonTmpDirectoryBase;
	private String requirementsTargetDirPath;
	private String pythonWorkingDirectory;
	private String pythonPathFilesDirectory;
	private Thread shutdownHook;
	private Map<String, String> systemEnv;

	static boolean testCopy = false;

	private ProcessEnvironmentManager(
		PythonDependencyManager dependencyManager,
		String pythonTmpDirectoryBase,
		String pythonPathFilesDirectory,
		String pythonWorkingDirectory,
		String requirementsTargetDirPath,
		Map<String, String> systemEnv) {
		this.dependencyManager = dependencyManager;
		this.pythonTmpDirectoryBase = pythonTmpDirectoryBase;
		this.pythonPathFilesDirectory = pythonPathFilesDirectory;
		this.pythonWorkingDirectory = pythonWorkingDirectory;
		this.requirementsTargetDirPath = requirementsTargetDirPath;
		this.systemEnv = systemEnv;
	}

	@Override
	public void cleanup() {
		if (shutdownHook != null) {
			shutdownHook.run();
			Runtime.getRuntime().removeShutdownHook(shutdownHook);
			shutdownHook = null;
		}
	}

	@Override
	public RunnerApi.Environment createEnvironment() {
		prepareEnvironment();
		Map<String, String> generatedEnv = generateEnvironmentVariable();
		String flinkHomePath = systemEnv.get(ConfigConstants.ENV_FLINK_HOME_DIR);
		String pythonWorkerCommand =
			flinkHomePath + File.separator + "bin" + File.separator + "pyflink-udf-runner.sh";

		return Environments.createProcessEnvironment(
			"",
			"",
			pythonWorkerCommand,
			generatedEnv);
	}

	/**
	 * Just return a empty RetrievalToken because no files will be transmit via ArtifactService in process mode.
	 *
	 * @return The path of empty RetrievalToken.
	 */
	@Override
	public String createRetrievalToken() throws IOException {
		if (shutdownHook == null) {
			shutdownHook = registerShutdownHook();
		}

		File retrievalToken = new File(pythonTmpDirectoryBase,
			"retrieval_token_" + UUID.randomUUID().toString() + ".json");
		if (!retrievalToken.getParentFile().exists() && !retrievalToken.getParentFile().mkdir()) {
			throw new IOException(
				"Could not create the parent directory of RetrievalToken file: " +
					retrievalToken.getAbsolutePath());
		}
		if (retrievalToken.createNewFile()) {
			final DataOutputStream dos = new DataOutputStream(new FileOutputStream(retrievalToken));
			dos.writeBytes("{\"manifest\": {}}");
			dos.flush();
			dos.close();
			return retrievalToken.getAbsolutePath();
		} else {
			throw new IOException(
				"Could not create the RetrievalToken file: " + retrievalToken.getAbsolutePath());
		}
	}

	/**
	 * Generate the environment variables used to create the launcher process of python UDF worker.
	 *
	 * <p>To avoid unnecessary IO usage, when running in process mode no artifacts will be transmitted
	 * via ArtifactService of Beam. Instead, the path of artifacts will be transmit to the launcher
	 * of python UDF worker via environment variable.
	 *
	 * @return Environment variable map containing paths of python dependencies.
	 */
	@VisibleForTesting
	Map<String, String> generateEnvironmentVariable() {
		Map<String, String> systemEnv = new HashMap<>(this.systemEnv);
		if (dependencyManager.getFilesInPythonPath().size() > 0) {
			List<String> pythonPathFileList = new ArrayList<>();
			for (Map.Entry<String, String> entry : dependencyManager.getFilesInPythonPath().entrySet()) {
				String distributedCacheFileName = new File(entry.getKey()).getName();
				String actualFileName = entry.getValue();
				File pythonFile = new File(entry.getKey());
				String pathForSearching = pythonPathFilesDirectory + File.separator
					+ distributedCacheFileName + File.separator + actualFileName;
				if (pythonFile.isFile()) {
					if (actualFileName.lastIndexOf(".") > 0) {
						String suffix =
							actualFileName.substring(actualFileName.lastIndexOf(".") + 1);
						if (suffix.length() <= 3 && suffix.startsWith("py")) {
							// If the file is single py file, use its parent directory as PYTHONPATH
							pathForSearching = pythonPathFilesDirectory + File.separator
								+ distributedCacheFileName;
						}
					}
				}
				pythonPathFileList.add(pathForSearching);
			}
			String pythonPath = String.join(File.pathSeparator, pythonPathFileList);
			String systemPythonPath = systemEnv.get("PYTHONPATH");
			if (systemPythonPath != null && systemPythonPath.length() > 0) {
				pythonPath = String.join(File.pathSeparator, pythonPath, systemPythonPath);
			}
			if (pythonPath.length() > 0) {
				systemEnv.put("PYTHONPATH", pythonPath);
			}
			LOG.info("PYTHONPATH of python worker: {}", pythonPath);
		}

		if (dependencyManager.getArchives().size() > 0) {
			String path = pythonTmpDirectoryBase + File.separator + PYTHON_ARCHIVES_DIR;
			systemEnv.put(PYTHON_WORKING_DIR, path);
			LOG.info("python working dir of python worker: {}", path);
		}

		if (dependencyManager.getRequirementsFilePath() != null) {
			systemEnv.put(PYTHON_REQUIREMENTS, dependencyManager.getRequirementsFilePath());
			LOG.info("requirments.txt of python worker: {}", dependencyManager.getRequirementsFilePath());

			if (dependencyManager.getRequirementsDirPath() != null) {
				systemEnv.put(PYTHON_REQUIREMENTS_DIR, dependencyManager.getRequirementsDirPath());
				LOG.info("requirments cached dir of python worker: {}", dependencyManager.getRequirementsDirPath());
			}

			systemEnv.put(PYTHON_REQUIREMENTS_TARGET_DIR, requirementsTargetDirPath);
			LOG.info("prefix parameter of python worker: {}", requirementsTargetDirPath);
		}

		if (dependencyManager.getPythonExec() != null) {
			systemEnv.put("python", dependencyManager.getPythonExec());
			LOG.info("user defined python interpreter path: {}", dependencyManager.getPythonExec());
		}
		return systemEnv;
	}

	@VisibleForTesting
	void prepareEnvironment() {
		if (shutdownHook == null) {
			shutdownHook = registerShutdownHook();
		}

		try {
			prepareWorkingDir();
		} catch (IllegalArgumentException | IOException e) {
			cleanup();
			throw new RuntimeException(e);
		}
	}

	/**
	 * Prepare the working directory for running the python UDF worker.
	 *
	 * <p>For the files to append to PYTHONPATH directly, restore their origin file names.
	 * For the files used for pip installations, prepare the target directory to contain the install result.
	 * For the archive files, extract them to their target directory and try to restore their origin permissions.
	 *
	 * @throws IOException
	 * @throws IllegalArgumentException
	 */
	private void prepareWorkingDir() throws IOException, IllegalArgumentException {
		for (Map.Entry<String, String> entry : dependencyManager.getFilesInPythonPath().entrySet()) {
			// The origin file name will be wiped during the transmission in Flink Distributed Cache
			// and replaced by a unreadable character sequence, now restore their origin name.
			// Every python file will be placed at :
			// ${pythonTmpDirectoryBase}/python_path_files_dir/${distributedCacheFileName}/${originFileName}
			String distributedCacheFileName = new File(entry.getKey()).getName();
			String actualFileName = entry.getValue();
			Path target = FileSystems.getDefault().getPath(pythonPathFilesDirectory,
				distributedCacheFileName, actualFileName);
			if (!target.getParent().toFile().mkdirs()) {
				throw new IllegalArgumentException(
					String.format("can not create the directory: %s !", target.getParent().toString()));
			}
			Path src = FileSystems.getDefault().getPath(entry.getKey());
			try {
				if (testCopy) {
					throw new IOException();
				}
				Files.createSymbolicLink(target, src);
			} catch (IOException e) {
				FileUtils.copy(new org.apache.flink.core.fs.Path(src.toUri()),
					new org.apache.flink.core.fs.Path(target.toUri()), false);
			}
		}

		for (Map.Entry<String, String> entry : dependencyManager.getArchives().entrySet()) {
			UnzipUtil.extractZipFileWithPermissions(
				entry.getKey(), pythonWorkingDirectory + File.separator + entry.getValue());
		}

		File targetDir = new File(requirementsTargetDirPath);
		if (!targetDir.exists()) {
			if (!targetDir.mkdirs()) {
				throw new IllegalArgumentException(
					String.format("Creating the requirements target directory: %s failed!",
						requirementsTargetDirPath));
			}
		} else {
			if (!targetDir.isDirectory()) {
				throw new IllegalArgumentException(
					String.format("The requirements target directory path: %s is not a directory!",
						requirementsTargetDirPath));
			}
		}
	}

	private Thread registerShutdownHook() {
		Thread thread = new Thread(() -> FileUtils.deleteDirectoryQuietly(new File(pythonTmpDirectoryBase)));
		Runtime.getRuntime().addShutdownHook(thread);
		return thread;
	}

	@VisibleForTesting
	String getPythonTmpDirectoryBase() {
		return pythonTmpDirectoryBase;
	}

	public static ProcessEnvironmentManager create(
		PythonDependencyManager dependencyManager,
		String tmpDirectoryRoot,
		Map<String, String> environmentVariable) {

		String pythonTmpDirectoryBase = new File(tmpDirectoryRoot).getAbsolutePath() +
			File.separator + PYTHON_TMP_DIR_PREFIX + UUID.randomUUID().toString();
		String pythonWorkingDirectory = pythonTmpDirectoryBase + File.separator + PYTHON_ARCHIVES_DIR;
		String requirementsTargetDirPath = pythonTmpDirectoryBase + File.separator + PYTHON_REQUIREMENTS_TARGET_DIR_NAME;
		String pythonPathFilesDirectory = pythonTmpDirectoryBase + File.separator + PYTHON_PATH_FILES_DIR;

		return new ProcessEnvironmentManager(
			dependencyManager,
			pythonTmpDirectoryBase,
			pythonPathFilesDirectory,
			pythonWorkingDirectory,
			requirementsTargetDirPath,
			environmentVariable);
	}
}
