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

package org.apache.flink.runtime.util;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.runtime.clusterframework.TaskExecutorResourceSpec;
import org.apache.flink.runtime.clusterframework.TaskExecutorResourceUtils;
import org.apache.flink.runtime.taskexecutor.TaskManagerRunner;

import java.util.Arrays;

import static org.apache.flink.util.Preconditions.checkArgument;

/**
 * Utility class for using java utilities in bash scripts.
 */
public class BashJavaUtils {

	private static final String CMD_GET_TM_RESOURCE_CONFIGS_AND_JVM_PARAMS = "GET_TM_RESOURCE_CONFIGS_AND_JVM_PARAMS";

	public static void main(String[] args) throws Exception {
		checkArgument(args.length > 0, "Command not specified.");

		switch (args[0]) {
			case CMD_GET_TM_RESOURCE_CONFIGS_AND_JVM_PARAMS: getTmResourceConfigsAndJvmParams(args); break;
			default: throw new RuntimeException(String.format("Unknown command: %s", args[0]));
		}
	}

	private static void getTmResourceConfigsAndJvmParams(String[] args) throws Exception {
		Configuration configuration = TaskManagerRunner.loadConfiguration(Arrays.copyOfRange(args, 1, args.length));
		TaskExecutorResourceSpec taskExecutorResourceSpec = TaskExecutorResourceUtils.resourceSpecFromConfig(configuration);
		System.out.println(TaskExecutorResourceUtils.generateDynamicConfigsStr(taskExecutorResourceSpec));
		System.out.println(TaskExecutorResourceUtils.generateJvmParametersStr(taskExecutorResourceSpec));
	}
}
