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

package org.apache.flink.kubernetes.taskmanager;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.ConfigurationUtils;
import org.apache.flink.configuration.GlobalConfiguration;
import org.apache.flink.core.fs.FileSystem;
import org.apache.flink.core.plugin.PluginUtils;
import org.apache.flink.kubernetes.KubernetesResourceManager;
import org.apache.flink.runtime.clusterframework.types.ResourceID;
import org.apache.flink.runtime.entrypoint.ClusterConfiguration;
import org.apache.flink.runtime.entrypoint.ClusterConfigurationParserFactory;
import org.apache.flink.runtime.entrypoint.FlinkParseException;
import org.apache.flink.runtime.entrypoint.parser.CommandLineParser;
import org.apache.flink.runtime.taskexecutor.TaskManagerRunner;
import org.apache.flink.runtime.util.EnvironmentInformation;
import org.apache.flink.runtime.util.JvmShutdownSafeguard;
import org.apache.flink.runtime.util.SignalHandler;
import org.apache.flink.util.Preconditions;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * This class is the executable entry point for running a TaskExecutor in a Kubernetes pod.
 */
public class KubernetesTaskExecutorRunner {

	protected static final Logger LOG = LoggerFactory.getLogger(KubernetesTaskExecutorRunner.class);

	public static void main(String[] args) throws Exception {
		EnvironmentInformation.logEnvironmentInfo(LOG, "Kubernetes TaskExecutor runner", args);
		SignalHandler.register(LOG);
		JvmShutdownSafeguard.installAsShutdownHook(LOG);

		// Some config options for task manager are added to args, such as managed memory size.
		final CommandLineParser<ClusterConfiguration> commandLineParser =
			new CommandLineParser<>(new ClusterConfigurationParserFactory());

		ClusterConfiguration clusterConfiguration = null;
		try {
			clusterConfiguration = commandLineParser.parse(args);
		} catch (FlinkParseException e) {
			LOG.error("Could not parse command line arguments {}.", args, e);
			commandLineParser.printHelp(KubernetesTaskExecutorRunner.class.getSimpleName());
			System.exit(1);
		}

		final Configuration dynamicProperties = ConfigurationUtils.createConfiguration(
			clusterConfiguration.getDynamicProperties());
		final Configuration configuration = GlobalConfiguration.loadConfiguration(
			clusterConfiguration.getConfigDir(), dynamicProperties);

		FileSystem.initialize(configuration, PluginUtils.createPluginManagerFromRootFolder(configuration));

		final String resourceID = System.getenv().get(KubernetesResourceManager.ENV_RESOURCE_ID);
		Preconditions.checkArgument(resourceID != null,
			"Resource ID variable %s not set", KubernetesResourceManager.ENV_RESOURCE_ID);

		TaskManagerRunner.runTaskManager(configuration, new ResourceID(resourceID));
	}
}
