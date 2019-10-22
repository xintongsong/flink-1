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

import org.apache.flink.client.cli.CliFrontend;
import org.apache.flink.client.deployment.ClusterSpecification;
import org.apache.flink.configuration.ConfigOption;
import org.apache.flink.configuration.ConfigOptions;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.JobManagerOptions;
import org.apache.flink.configuration.RestOptions;
import org.apache.flink.kubernetes.configuration.KubernetesConfigOptions;
import org.apache.flink.kubernetes.kubeclient.resources.FlinkDeployment;
import org.apache.flink.kubernetes.utils.Constants;
import org.apache.flink.kubernetes.utils.KubernetesUtils;
import org.apache.flink.util.Preconditions;

import io.fabric8.kubernetes.api.model.ConfigMapVolumeSourceBuilder;
import io.fabric8.kubernetes.api.model.Container;
import io.fabric8.kubernetes.api.model.ContainerBuilder;
import io.fabric8.kubernetes.api.model.ContainerPortBuilder;
import io.fabric8.kubernetes.api.model.KeyToPath;
import io.fabric8.kubernetes.api.model.PodSpec;
import io.fabric8.kubernetes.api.model.PodSpecBuilder;
import io.fabric8.kubernetes.api.model.Quantity;
import io.fabric8.kubernetes.api.model.ResourceRequirementsBuilder;
import io.fabric8.kubernetes.api.model.Volume;
import io.fabric8.kubernetes.api.model.VolumeMountBuilder;
import io.fabric8.kubernetes.api.model.apps.Deployment;
import io.fabric8.kubernetes.api.model.apps.DeploymentSpecBuilder;

import java.io.File;
import java.util.Arrays;
import java.util.Map;

import static org.apache.flink.configuration.GlobalConfiguration.FLINK_CONF_FILENAME;
import static org.apache.flink.kubernetes.utils.Constants.BLOB_SERVER_PORT;
import static org.apache.flink.kubernetes.utils.Constants.CONFIG_FILE_LOG4J_NAME;
import static org.apache.flink.kubernetes.utils.Constants.CONFIG_FILE_LOGBACK_NAME;
import static org.apache.flink.kubernetes.utils.Constants.CONFIG_MAP_PREFIX;
import static org.apache.flink.kubernetes.utils.Constants.FLINK_CONF_VOLUME;

/**
 * Flink master specific deployment configuration.
 * */
public class FlinkMasterDeploymentDecorator extends Decorator<Deployment, FlinkDeployment> {

	private static final String CONTAINER_NAME = "flink-job-manager";

	private final ClusterSpecification clusterSpecification;

	public static final ConfigOption<String> ENTRY_POINT_CLASS = ConfigOptions
		.key("kubernetes.internal.jobmanager.entrypoint.class")
		.noDefaultValue()
		.withDescription("The entrypoint class for jobmanager. It will be set in kubernetesClusterDescriptor.");

	public static final ConfigOption<String> ENTRY_POINT_CLASS_ARGS = ConfigOptions
		.key("kubernetes.internal.jobmanager.entrypoint.class.args")
		.noDefaultValue()
		.withDescription("The args of entrypoint class for jobmanager. It will be set in FlinkKubernetesCustomCli.");

	public FlinkMasterDeploymentDecorator(ClusterSpecification clusterSpecification) {
		this.clusterSpecification = clusterSpecification;
	}

	@Override
	protected Deployment doDecorate(Deployment deployment, Configuration flinkConfig) {
		String clusterId = flinkConfig.getString(KubernetesConfigOptions.CLUSTER_ID);
		Preconditions.checkNotNull(clusterId, "ClusterId must be specified!");

		String mainClass = flinkConfig.getString(ENTRY_POINT_CLASS);
		Preconditions.checkNotNull(mainClass, "Main class must be specified!");

		Map<String, String> labels = LabelBuilder
			.withExist(deployment.getMetadata().getLabels())
			.withJobManagerComponent()
			.toLabels();

		deployment.getMetadata().setLabels(labels);

		Volume configMapVolume = new Volume();
		configMapVolume.setName(FLINK_CONF_VOLUME);
		configMapVolume.setConfigMap(new ConfigMapVolumeSourceBuilder()
			.withName(CONFIG_MAP_PREFIX + clusterId)
			.withItems(Arrays.asList(
				new KeyToPath(FLINK_CONF_FILENAME, null, FLINK_CONF_FILENAME),
				new KeyToPath(CONFIG_FILE_LOG4J_NAME, null, CONFIG_FILE_LOG4J_NAME),
				new KeyToPath(CONFIG_FILE_LOGBACK_NAME, null, CONFIG_FILE_LOGBACK_NAME)))
			.build());

		Container container = createJobManagerContainer(flinkConfig, mainClass);

		String serviceAccount = flinkConfig.getString(KubernetesConfigOptions.JOB_MANAGER_SERVICE_ACCOUNT);
		PodSpec podSpec = new PodSpecBuilder()
			.withServiceAccountName(serviceAccount)
			.withVolumes(configMapVolume)
			.withContainers(container)
			.build();

		deployment.setSpec(new DeploymentSpecBuilder()
			.withReplicas(1)
			.withNewTemplate().withNewMetadata().withLabels(labels).endMetadata()
			.withSpec(podSpec).endTemplate()
			.withNewSelector().addToMatchLabels(labels).endSelector().build());
		return deployment;
	}

	private Container createJobManagerContainer(Configuration flinkConfig, String mainClass) {
		String confDir = CliFrontend.getConfigurationDirectoryFromEnv();
		String flinkConfDirInPod = flinkConfig.getString(KubernetesConfigOptions.FLINK_CONF_DIR);
		boolean hasLogback = new File(confDir, Constants.CONFIG_FILE_LOGBACK_NAME).exists();
		boolean hasLog4j = new File(confDir, Constants.CONFIG_FILE_LOG4J_NAME).exists();
		String logDirInPod = flinkConfig.getString(KubernetesConfigOptions.FLINK_LOG_DIR);
		String mainClassArgs = flinkConfig.getString(ENTRY_POINT_CLASS_ARGS);
		String startCommand = KubernetesUtils.getJobManagerStartCommand(
			flinkConfig,
			clusterSpecification.getMasterMemoryMB(),
			flinkConfDirInPod,
			logDirInPod,
			hasLogback,
			hasLog4j,
			mainClassArgs != null ? mainClass + " " + mainClassArgs : mainClass);

		String image = flinkConfig.getString(KubernetesConfigOptions.CONTAINER_IMAGE);
		String pullPolicy = flinkConfig.getString(
			KubernetesConfigOptions.CONTAINER_IMAGE_PULL_POLICY);

		double cpu = flinkConfig.getDouble(KubernetesConfigOptions.JOB_MANAGER_CPU);
		Quantity jobManagerCpuQuantity = new Quantity(String.valueOf(cpu));
		Quantity jobManagerMemoryQuantity = new Quantity(String.valueOf(
			clusterSpecification.getMasterMemoryMB() + Constants.RESOURCE_UNIT_MB));

		return new ContainerBuilder()
			.withName(CONTAINER_NAME)
			.withCommand(flinkConfig.getString(KubernetesConfigOptions.KUBERNETES_ENTRY_PATH))
			.withArgs(Arrays.asList("/bin/bash", "-c", startCommand))
			.withImage(image)
			.withImagePullPolicy(pullPolicy)
			.withResources(new ResourceRequirementsBuilder()
				.addToRequests(Constants.RESOURCE_NAME_MEMORY, jobManagerMemoryQuantity)
				.addToRequests(Constants.RESOURCE_NAME_CPU, jobManagerCpuQuantity)
				.addToLimits(Constants.RESOURCE_NAME_MEMORY, jobManagerMemoryQuantity)
				.addToLimits(Constants.RESOURCE_NAME_CPU, jobManagerCpuQuantity)
				.build())
			.withPorts(Arrays.asList(
				new ContainerPortBuilder().withContainerPort(flinkConfig.getInteger(RestOptions.PORT)).build(),
				new ContainerPortBuilder().withContainerPort(flinkConfig.getInteger(JobManagerOptions.PORT)).build(),
				new ContainerPortBuilder().withContainerPort(BLOB_SERVER_PORT).build()))
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
