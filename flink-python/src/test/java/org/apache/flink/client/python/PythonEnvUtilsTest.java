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

package org.apache.flink.client.python;

import org.apache.flink.core.fs.Path;
import org.apache.flink.util.FileUtils;

import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.nio.file.StandardOpenOption;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.UUID;

/**
 * Tests for the {@link PythonEnvUtils}.
 */
public class PythonEnvUtilsTest {
	private String tmpDirPath;

	@Before
	public void prepareTestEnvironment() {
		tmpDirPath = System.getProperty("java.io.tmpdir") +
			File.separator + "pyflink_" + UUID.randomUUID();
		new File(tmpDirPath).mkdirs();
	}

	@Test
	public void testPreparePythonEnvironment() throws IOException {
		// xxx/a.zip, xxx/subdir/b.py, xxx/subdir/c.zip
		File a = new File(tmpDirPath + File.separator + "a.zip");
		a.createNewFile();
		File moduleDir = new File(tmpDirPath + File.separator + "module_dir");
		moduleDir.mkdir();
		File subdir = new File(tmpDirPath + File.separator + "subdir");
		subdir.mkdir();
		File b = new File(tmpDirPath + File.separator + "subdir" + File.separator + "b.py");
		b.createNewFile();
		File c = new File(tmpDirPath + File.separator + "subdir" + File.separator + "c.zip");
		c.createNewFile();

		List<Path> pyFilesList = new ArrayList<>();
		pyFilesList.add(new Path(a.getAbsolutePath()));
		pyFilesList.add(new Path(moduleDir.getAbsolutePath()));
		// test relative path
		String relativePath = Paths.get(new File("").getAbsolutePath())
			.relativize(Paths.get(b.getAbsolutePath())).toString();
		pyFilesList.add(new Path(relativePath));
		// test path with schema
		pyFilesList.add(new Path("file://" + c.getAbsolutePath()));

		PythonEnvUtils.PythonEnvironment env = PythonEnvUtils.preparePythonEnvironment(
			pyFilesList,
			tmpDirPath);
		String base = replaceUUID(env.storageDirectory);
		Set<String> expectedPythonPaths = new HashSet<>();
		expectedPythonPaths.add(String.join(File.separator, base, "{uuid}", "a.zip"));
		expectedPythonPaths.add(String.join(File.separator, base, "{uuid}", "module_dir"));
		expectedPythonPaths.add(String.join(File.separator, base, "{uuid}"));
		expectedPythonPaths.add(String.join(File.separator, base, "{uuid}", "c.zip"));

		Assert.assertEquals(
			expectedPythonPaths,
			new HashSet<>(Arrays.asList(replaceUUID(env.pythonPath).split(File.pathSeparator))));
	}

	@Test
	public void testStartPythonProcess() {
		PythonEnvUtils.PythonEnvironment pythonEnv = new PythonEnvUtils.PythonEnvironment();
		pythonEnv.storageDirectory = tmpDirPath;
		pythonEnv.pythonPath = tmpDirPath;
		List<String> commands = new ArrayList<>();
		String pyPath = String.join(File.separator, tmpDirPath, "verifier.py");
		try {
			File pyFile = new File(pyPath);
			pyFile.createNewFile();
			pyFile.setExecutable(true);
			String pyProgram = "#!/usr/bin/python\n" +
				"# -*- coding: UTF-8 -*-\n" +
				"import os\n" +
				"import sys\n" +
				"\n" +
				"if __name__=='__main__':\n" +
				"\tfilename = sys.argv[1]\n" +
				"\tfo = open(filename, \"w\")\n" +
				"\tfo.write(os.getcwd())\n" +
				"\tfo.close()";
			Files.write(pyFile.toPath(), pyProgram.getBytes(), StandardOpenOption.WRITE);
			String result = String.join(File.separator, tmpDirPath, "python_working_directory.txt");
			commands.add(pyPath);
			commands.add(result);
			Process pythonProcess = PythonEnvUtils.startPythonProcess(pythonEnv, commands);
			int exitCode = pythonProcess.waitFor();
			if (exitCode != 0) {
				throw new RuntimeException("Python process exits with code: " + exitCode);
			}
			String cmdResult = new String(Files.readAllBytes(new File(result.toString()).toPath()));
			// Check if the working directory of python process is the same as java process.
			Assert.assertEquals(cmdResult, System.getProperty("user.dir"));
			pythonProcess.destroyForcibly();
			new File(pyPath).delete();
			new File(result).delete();
		} catch (IOException | InterruptedException e) {
			throw new RuntimeException("test start Python process failed " + e.getMessage());
		}
	}

	@After
	public void cleanEnvironment() {
		FileUtils.deleteDirectoryQuietly(new File(tmpDirPath));
	}

	private static String replaceUUID(String originPath) {
		return originPath.replaceAll(
			"[a-z0-9]{8}-[a-z0-9]{4}-[a-z0-9]{4}-[a-z0-9]{4}-[a-z0-9]{12}",
			"{uuid}");
	}
}
