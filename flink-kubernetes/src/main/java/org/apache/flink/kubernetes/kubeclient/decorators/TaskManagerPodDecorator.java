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

package org.apache.flink.kubernetes.kubeclient.decorators;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.kubernetes.configuration.KubernetesConfigOptions;
import org.apache.flink.kubernetes.kubeclient.TaskManagerPodParameter;
import org.apache.flink.kubernetes.kubeclient.resources.FlinkPod;
import org.apache.flink.kubernetes.utils.Constants;
import org.apache.flink.util.Preconditions;

import io.fabric8.kubernetes.api.model.ConfigMapVolumeSourceBuilder;
import io.fabric8.kubernetes.api.model.Container;
import io.fabric8.kubernetes.api.model.ContainerBuilder;
import io.fabric8.kubernetes.api.model.ContainerPortBuilder;
import io.fabric8.kubernetes.api.model.EnvVar;
import io.fabric8.kubernetes.api.model.KeyToPath;
import io.fabric8.kubernetes.api.model.Pod;
import io.fabric8.kubernetes.api.model.PodSpecBuilder;
import io.fabric8.kubernetes.api.model.Quantity;
import io.fabric8.kubernetes.api.model.ResourceRequirementsBuilder;
import io.fabric8.kubernetes.api.model.Volume;
import io.fabric8.kubernetes.api.model.VolumeMountBuilder;

import java.io.File;
import java.util.Arrays;
import java.util.Map;
import java.util.stream.Collectors;

import static org.apache.flink.configuration.GlobalConfiguration.FLINK_CONF_FILENAME;
import static org.apache.flink.kubernetes.utils.Constants.CONFIG_FILE_LOG4J_NAME;
import static org.apache.flink.kubernetes.utils.Constants.CONFIG_FILE_LOGBACK_NAME;
import static org.apache.flink.kubernetes.utils.Constants.CONFIG_MAP_PREFIX;
import static org.apache.flink.kubernetes.utils.Constants.FLINK_CONF_VOLUME;

/**
 * Task manager specific pod configuration.
 * */
public class TaskManagerPodDecorator extends Decorator<Pod, FlinkPod> {

	private static final String CONTAINER_NAME = "flink-task-manager";

	private final TaskManagerPodParameter parameter;

	public TaskManagerPodDecorator(TaskManagerPodParameter parameters) {
		Preconditions.checkNotNull(parameters);
		this.parameter = parameters;
	}

	@Override
	protected Pod doDecorate(Pod pod, Configuration flinkConfig) {

		String clusterId = flinkConfig.getString(KubernetesConfigOptions.CLUSTER_ID);
		Preconditions.checkNotNull(clusterId, "ClusterId must be specified!");

		Map<String, String> labels = org.apache.flink.kubernetes.kubeclient.decorators.LabelBuilder
			.withExist(pod.getMetadata().getLabels())
			.withTaskManagerComponent()
			.toLabels();

		pod.getMetadata().setLabels(labels);
		pod.getMetadata().setName(this.parameter.getPodName());

		Volume configMapVolume = new Volume();
		configMapVolume.setName(FLINK_CONF_VOLUME);
		configMapVolume.setConfigMap(new ConfigMapVolumeSourceBuilder()
			.withName(CONFIG_MAP_PREFIX + clusterId)
			.withItems(Arrays.asList(
				new KeyToPath(FLINK_CONF_FILENAME, null, FLINK_CONF_FILENAME),
				new KeyToPath(CONFIG_FILE_LOG4J_NAME, null, CONFIG_FILE_LOG4J_NAME),
				new KeyToPath(CONFIG_FILE_LOGBACK_NAME, null, CONFIG_FILE_LOGBACK_NAME)))
			.build());

		pod.setSpec(new PodSpecBuilder()
			.withVolumes(configMapVolume)
			.withContainers(createTaskManagerContainer(flinkConfig))
			.build());
		return pod;
	}

	private Container createTaskManagerContainer(Configuration flinkConfig) {
		Quantity taskManagerCpuQuantity = new Quantity(String.valueOf(parameter.getTaskManagerCpus()));
		Quantity taskManagerMemoryQuantity = new Quantity(String.valueOf(
			parameter.getTaskManagerMemory() + Constants.RESOURCE_UNIT_MB));
		String image = flinkConfig.getString(KubernetesConfigOptions.CONTAINER_IMAGE);
		String pullPolicy = flinkConfig.getString(KubernetesConfigOptions.CONTAINER_IMAGE_PULL_POLICY);
		String flinkConfDirInPod = flinkConfig.getString(KubernetesConfigOptions.FLINK_CONF_DIR);
		return new ContainerBuilder()
			.withName(CONTAINER_NAME)
			.withArgs(this.parameter.getArgs())
			.withImage(image)
			.withImagePullPolicy(pullPolicy)
			.withResources(new ResourceRequirementsBuilder()
				.addToRequests(Constants.RESOURCE_NAME_MEMORY, taskManagerMemoryQuantity)
				.addToRequests(Constants.RESOURCE_NAME_CPU, taskManagerCpuQuantity)
				.addToLimits(Constants.RESOURCE_NAME_MEMORY, taskManagerMemoryQuantity)
				.addToLimits(Constants.RESOURCE_NAME_CPU, taskManagerCpuQuantity)
				.build())
			.withPorts(new ContainerPortBuilder().withContainerPort(Constants.TASK_MANAGER_RPC_PORT).build())
			.withEnv(this.parameter.getEnvironmentVariables()
				.entrySet()
				.stream()
				.map(kv -> new EnvVar(kv.getKey(), kv.getValue(), null))
				.collect(Collectors.toList()))
			.withVolumeMounts(new VolumeMountBuilder()
				.withName(FLINK_CONF_VOLUME)
				.withMountPath(new File(flinkConfDirInPod, FLINK_CONF_FILENAME).getPath())
				.withSubPath(FLINK_CONF_FILENAME)
				.build(), new VolumeMountBuilder()
				.withName(FLINK_CONF_VOLUME)
				.withMountPath(new File(flinkConfDirInPod, CONFIG_FILE_LOG4J_NAME).getPath())
				.withSubPath(CONFIG_FILE_LOG4J_NAME)
				.build(), new VolumeMountBuilder()
				.withName(FLINK_CONF_VOLUME)
				.withMountPath(new File(flinkConfDirInPod, CONFIG_FILE_LOGBACK_NAME).getPath())
				.withSubPath(CONFIG_FILE_LOGBACK_NAME)
				.build())
			.build();
	}
}
