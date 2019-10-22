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

package org.apache.flink.kubernetes.utils;

import org.apache.flink.configuration.ConfigOption;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.CoreOptions;
import org.apache.flink.kubernetes.configuration.KubernetesConfigOptions;
import org.apache.flink.runtime.clusterframework.BootstrapTools;
import org.apache.flink.runtime.clusterframework.ContaineredTaskManagerParameters;

import org.apache.commons.lang3.StringUtils;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;

/**
 * Common utils for Kubernetes.
 */
public class KubernetesUtils {

	/**
	 * Read file content to string.
	 * @param filePath file path
	 * @return content
	 */
	public static String getContentFromFile(String filePath) {
		File file = new File(filePath);
		if (file.exists()) {
			StringBuilder content = new StringBuilder();
			String line;
			try (BufferedReader reader = new BufferedReader(new InputStreamReader(new FileInputStream(file)))){
				while ((line = reader.readLine()) != null) {
					content.append(line).append(System.lineSeparator());
				}
			} catch (IOException e) {
				throw new RuntimeException("Error read file content.", e);
			}
			return content.toString();
		}
		return null;
	}

	/**
	 * Generates the shell command to start a job manager for kubernetes.
	 * @param flinkConfig The Flink configuration.
	 * @param jobManagerMemoryMb JobManager heap size.
	 * @param configDirectory The configuration directory for the flink-conf.yaml
	 * @param logDirectory The log directory.
	 * @param hasLogback Uses logback?
	 * @param hasLog4j Uses log4j?
	 * @param mainClass The main class to start with.
	 * @return A String containing the job manager startup command.
	 */
	public static String getJobManagerStartCommand(
		Configuration flinkConfig,
		int jobManagerMemoryMb,
		String configDirectory,
		String logDirectory,
		boolean hasLogback,
		boolean hasLog4j,
		String mainClass) {

		final Map<String, String> startCommandValues = new HashMap<>();
		startCommandValues.put("java", "$JAVA_HOME/bin/java");
		startCommandValues.put("classpath", "-classpath " + "$" + Constants.ENV_FLINK_CLASSPATH);

		startCommandValues.put("jvmmem", String.format("-Xmx%dm -Xms%dm", jobManagerMemoryMb, jobManagerMemoryMb));

		startCommandValues.put("jvmopts", getJavaOpts(flinkConfig, CoreOptions.FLINK_JM_JVM_OPTIONS));

		startCommandValues.put("logging",
			getLogging(logDirectory + "/jobmanager.log", configDirectory, hasLogback, hasLog4j));

		startCommandValues.put("class", mainClass);

		startCommandValues.put("redirects",
			"1> " + logDirectory + "/jobmanager.out " +
			"2> " + logDirectory + "/jobmanager.err");

		final String commandTemplate = flinkConfig.getString(KubernetesConfigOptions.CONTAINER_START_COMMAND_TEMPLATE);
		return BootstrapTools.getStartCommand(commandTemplate, startCommandValues);
	}

	/**
	 * Generates the shell command to start a task manager for kubernetes.
	 * @param flinkConfig The Flink configuration.
	 * @param tmParams Parameters for the task manager.
	 * @param configDirectory The configuration directory for the flink-conf.yaml
	 * @param logDirectory The log directory.
	 * @param hasLogback Uses logback?
	 * @param hasLog4j Uses log4j?
	 * @param mainClass The main class to start with.
	 * @return A String containing the task manager startup command.
	 */
	public static String getTaskManagerShellCommand(
		Configuration flinkConfig,
		ContaineredTaskManagerParameters tmParams,
		String configDirectory,
		String logDirectory,
		boolean hasLogback,
		boolean hasLog4j,
		Class<?> mainClass) {

		final Map<String, String> startCommandValues = new HashMap<>();
		startCommandValues.put("java", "$JAVA_HOME/bin/java");
		startCommandValues.put("classpath", "-classpath " + "$" + Constants.ENV_FLINK_CLASSPATH);

		ArrayList<String> params = new ArrayList<>();
		params.add(String.format("-Xms%dm", tmParams.taskManagerHeapSizeMB()));
		params.add(String.format("-Xmx%dm", tmParams.taskManagerHeapSizeMB()));

		if (tmParams.taskManagerDirectMemoryLimitMB() >= 0) {
			params.add(String.format("-XX:MaxDirectMemorySize=%dm",
				tmParams.taskManagerDirectMemoryLimitMB()));
		}

		startCommandValues.put("jvmmem", StringUtils.join(params, ' '));

		startCommandValues.put("jvmopts", getJavaOpts(flinkConfig, CoreOptions.FLINK_TM_JVM_OPTIONS));

		startCommandValues.put("logging",
			getLogging(logDirectory + "/taskmanager.log", configDirectory, hasLogback, hasLog4j));

		startCommandValues.put("class", mainClass.getName());

		startCommandValues.put("redirects",
			"1> " + logDirectory + "/taskmanager.out " +
			"2> " + logDirectory + "/taskmanager.err");

		final String commandTemplate = flinkConfig
			.getString(KubernetesConfigOptions.CONTAINER_START_COMMAND_TEMPLATE);
		return BootstrapTools.getStartCommand(commandTemplate, startCommandValues);
	}

	private static String getJavaOpts(Configuration flinkConfig, ConfigOption<String> configOption) {
		String javaOpts = flinkConfig.getString(CoreOptions.FLINK_JVM_OPTIONS);
		if (flinkConfig.getString(configOption).length() > 0) {
			javaOpts += " " + flinkConfig.getString(configOption);
		}
		return javaOpts;
	}

	private static String getLogging(String logFile, String confDir, boolean hasLogback, boolean hasLog4j) {
		StringBuilder logging = new StringBuilder();
		if (hasLogback || hasLog4j) {
			logging.append("-Dlog.file=").append(logFile);
			if (hasLogback) {
				logging.append(" -Dlogback.configurationFile=file:").append(confDir).append("/logback.xml");
			}
			if (hasLog4j) {
				logging.append(" -Dlog4j.configuration=file:").append(confDir).append("/log4j.properties");
			}
		}
		return logging.toString();
	}
}
