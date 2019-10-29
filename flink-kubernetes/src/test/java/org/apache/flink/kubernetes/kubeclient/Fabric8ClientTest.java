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

package org.apache.flink.kubernetes.kubeclient;

import org.apache.flink.client.deployment.ClusterSpecification;
import org.apache.flink.kubernetes.KubernetesTestBase;
import org.apache.flink.kubernetes.configuration.KubernetesConfigOptions;
import org.apache.flink.kubernetes.utils.Constants;

import io.fabric8.kubernetes.api.model.ConfigMap;
import io.fabric8.kubernetes.api.model.Container;
import io.fabric8.kubernetes.api.model.ContainerPort;
import io.fabric8.kubernetes.api.model.EnvVarBuilder;
import io.fabric8.kubernetes.api.model.Pod;
import io.fabric8.kubernetes.api.model.PodSpec;
import io.fabric8.kubernetes.api.model.Service;
import io.fabric8.kubernetes.api.model.ServicePort;
import io.fabric8.kubernetes.api.model.apps.Deployment;
import io.fabric8.kubernetes.client.KubernetesClient;
import org.hamcrest.Matchers;
import org.junit.Before;
import org.junit.Test;

import java.io.File;
import java.io.IOException;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import static org.apache.flink.configuration.GlobalConfiguration.FLINK_CONF_FILENAME;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThat;

/**
 * Tests for Fabric implementation of {@link FlinkKubeClient}.
 */
public class Fabric8ClientTest extends KubernetesTestBase {

	private FlinkKubeClient flinkKubeClient;

	private KubernetesClient kubeClient;

	@Before
	public void setUp() throws IOException {
		super.setUp();
		flinkKubeClient = getFabric8FlinkKubeClient();
		kubeClient = getKubeClient();
	}

	@Test
	public void testCreateConfigMap() throws Exception {
		flinkKubeClient.createConfigMap();

		List<ConfigMap> configMaps = kubeClient.configMaps().list().getItems();
		assertEquals(1, configMaps.size());

		// Check labels
		ConfigMap configMap = configMaps.get(0);
		assertEquals(Constants.CONFIG_MAP_PREFIX + clusterId, configMap.getMetadata().getName());
		Map<String, String> labels = getCommonLabels();
		assertEquals(labels, configMap.getMetadata().getLabels());

		// Check owner reference
		assertEquals(1, configMap.getMetadata().getOwnerReferences().size());
		assertEquals(mockServiceId, configMap.getMetadata().getOwnerReferences().get(0).getUid());

		// Check data
		assertEquals(1, configMap.getData().size());
		assertThat(configMap.getData().get(FLINK_CONF_FILENAME),
			Matchers.containsString(KubernetesConfigOptions.CLUSTER_ID.key()));
		assertThat(configMap.getData().get(FLINK_CONF_FILENAME),
			Matchers.containsString(KubernetesConfigOptions.CONTAINER_IMAGE.key()));
	}

	@Test
	public void testCreateInternalService() throws Exception {
		flinkKubeClient.createInternalService(clusterId);

		List<Service> services = kubeClient.services().list().getItems();
		assertEquals(1, services.size());

		Service service = services.get(0);
		assertEquals(clusterId, service.getMetadata().getName());
		Map<String, String> labels = getCommonLabels();
		assertEquals(labels, service.getMetadata().getLabels());

		assertEquals(0, service.getMetadata().getOwnerReferences().size());

		assertEquals(KubernetesConfigOptions.ServiceExposedType.ClusterIP.toString(), service.getSpec().getType());

		assertEquals(labels, service.getSpec().getSelector());

		assertThat(service.getSpec().getPorts().stream().map(ServicePort::getPort).collect(Collectors.toList()),
			Matchers.hasItems(8081, 6123, 6124));

		// Internal service will be deleted. Other resources are deleted by gc.
		flinkKubeClient.stopAndCleanupCluster(clusterId);
		assertEquals(0, kubeClient.services().list().getItems().size());
	}

	@Test
	public void testCreateRestService() throws Exception {
		flinkKubeClient.createRestService(clusterId).get();

		List<Service> services = kubeClient.services().list().getItems();
		assertEquals(1, services.size());

		Service service = services.get(0);
		assertEquals(clusterId + Constants.FLINK_REST_SERVICE_SUFFIX, service.getMetadata().getName());
		Map<String, String> labels = getCommonLabels();
		assertEquals(labels, service.getMetadata().getLabels());

		assertEquals(1, service.getMetadata().getOwnerReferences().size());
		assertEquals(mockServiceId, service.getMetadata().getOwnerReferences().get(0).getUid());

		assertEquals(KubernetesConfigOptions.ServiceExposedType.LoadBalancer.toString(), service.getSpec().getType());

		assertEquals(labels, service.getSpec().getSelector());

		assertThat(service.getSpec().getPorts().stream().map(ServicePort::getPort).collect(Collectors.toList()),
			Matchers.hasItems(8081));

		Endpoint endpoint = flinkKubeClient.getRestEndpoints(clusterId);
		assertEquals(mockServiceIp, endpoint.getAddress());
		assertEquals(8081, endpoint.getPort());
	}

	@Test
	public void testCreateFlinkMasterDeployment() {
		ClusterSpecification clusterSpecification = new ClusterSpecification.ClusterSpecificationBuilder()
			.setMasterMemoryMB(1234)
			.createClusterSpecification();

		flinkKubeClient.createFlinkMasterDeployment(clusterSpecification);

		List<Deployment> deployments = kubeClient.apps().deployments().list().getItems();
		assertEquals(1, deployments.size());

		Deployment deployment = deployments.get(0);
		assertEquals(clusterId, deployment.getMetadata().getName());
		Map<String, String> labels = getCommonLabels();
		labels.put(Constants.LABEL_COMPONENT_KEY, Constants.LABEL_COMPONENT_JOB_MANAGER);
		assertEquals(labels, deployment.getMetadata().getLabels());

		assertEquals(1, deployment.getMetadata().getOwnerReferences().size());
		assertEquals(mockServiceId, deployment.getMetadata().getOwnerReferences().get(0).getUid());

		PodSpec jmPodSpec = deployment.getSpec().getTemplate().getSpec();
		assertEquals("default", jmPodSpec.getServiceAccountName());
		assertEquals(1, jmPodSpec.getVolumes().size());
		assertEquals(1, jmPodSpec.getContainers().size());
		Container jmContainer = jmPodSpec.getContainers().get(0);

		assertEquals(clusterSpecification.getMasterMemoryMB() + Constants.RESOURCE_UNIT_MB,
			jmContainer.getResources().getRequests().get(Constants.RESOURCE_NAME_MEMORY).getAmount());
		assertEquals(clusterSpecification.getMasterMemoryMB() + Constants.RESOURCE_UNIT_MB,
			jmContainer.getResources().getLimits().get(Constants.RESOURCE_NAME_MEMORY).getAmount());

		assertThat(jmContainer.getPorts().stream().map(ContainerPort::getContainerPort).collect(Collectors.toList()),
			Matchers.hasItems(8081, 6123, 6124));

		assertEquals(1, jmContainer.getVolumeMounts().size());
		String mountPath = flinkConfig.getString(KubernetesConfigOptions.FLINK_CONF_DIR);
		assertEquals(new File(mountPath, FLINK_CONF_FILENAME).getPath(),
			jmContainer.getVolumeMounts().get(0).getMountPath());
		assertEquals(FLINK_CONF_FILENAME, jmContainer.getVolumeMounts().get(0).getSubPath());
	}

	@Test
	public void testCreateTaskManagerPod() {
		String podName = "taskmanager-1";
		List<String> commands = Arrays.asList("/bin/bash", "-c", "start-command-of-taskmanager");
		int tmMem = 1234;
		double tmCpu = 1.2;
		Map<String, String> env = new HashMap<>();
		env.put("RESOURCE_ID", podName);
		TaskManagerPodParameter parameter = new TaskManagerPodParameter(
			podName,
			commands,
			tmMem,
			tmCpu,
			env);
		flinkKubeClient.createTaskManagerPod(parameter);

		List<Pod> pods = kubeClient.pods().list().getItems();
		assertEquals(1, pods.size());

		Pod tmPod = pods.get(0);
		assertEquals(podName, tmPod.getMetadata().getName());
		Map<String, String> labels = getCommonLabels();
		labels.put(Constants.LABEL_COMPONENT_KEY, Constants.LABEL_COMPONENT_TASK_MANAGER);
		assertEquals(labels, tmPod.getMetadata().getLabels());

		assertEquals(1, tmPod.getMetadata().getOwnerReferences().size());
		assertEquals(mockServiceId, tmPod.getMetadata().getOwnerReferences().get(0).getUid());

		assertEquals(1, tmPod.getSpec().getContainers().size());
		Container tmContainer = tmPod.getSpec().getContainers().get(0);
		assertEquals(containerImage, tmContainer.getImage());
		assertEquals(commands, tmContainer.getArgs());

		assertEquals(tmMem + Constants.RESOURCE_UNIT_MB,
			tmContainer.getResources().getRequests().get(Constants.RESOURCE_NAME_MEMORY).getAmount());
		assertEquals(tmMem + Constants.RESOURCE_UNIT_MB,
			tmContainer.getResources().getLimits().get(Constants.RESOURCE_NAME_MEMORY).getAmount());
		assertEquals(String.valueOf(tmCpu),
			tmContainer.getResources().getRequests().get(Constants.RESOURCE_NAME_CPU).getAmount());
		assertEquals(String.valueOf(tmCpu),
			tmContainer.getResources().getRequests().get(Constants.RESOURCE_NAME_CPU).getAmount());

		assertThat(tmContainer.getEnv(), Matchers.contains(
			new EnvVarBuilder().withName("RESOURCE_ID").withValue(podName).build()));

		assertThat(tmContainer.getPorts().stream().map(ContainerPort::getContainerPort).collect(Collectors.toList()),
			Matchers.hasItems(6122));

		assertEquals(1, tmContainer.getVolumeMounts().size());
		String mountPath = flinkConfig.getString(KubernetesConfigOptions.FLINK_CONF_DIR);
		assertEquals(new File(mountPath, FLINK_CONF_FILENAME).getPath(),
			tmContainer.getVolumeMounts().get(0).getMountPath());
		assertEquals(FLINK_CONF_FILENAME, tmContainer.getVolumeMounts().get(0).getSubPath());

		// Stop the pod
		flinkKubeClient.stopPod(podName);
		assertEquals(0, kubeClient.pods().list().getItems().size());
	}
}
