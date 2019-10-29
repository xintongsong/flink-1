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

package org.apache.flink.kubernetes;

import org.apache.flink.client.deployment.ClusterSpecification;
import org.apache.flink.client.program.ClusterClient;
import org.apache.flink.configuration.BlobServerOptions;
import org.apache.flink.configuration.JobManagerOptions;
import org.apache.flink.configuration.TaskManagerOptions;
import org.apache.flink.kubernetes.kubeclient.FlinkKubeClient;
import org.apache.flink.kubernetes.kubeclient.resources.FlinkService;
import org.apache.flink.kubernetes.utils.Constants;

import io.fabric8.kubernetes.api.model.Container;
import io.fabric8.kubernetes.api.model.ServiceList;
import io.fabric8.kubernetes.client.KubernetesClient;
import org.junit.Test;

import static org.junit.Assert.assertEquals;

/**
 * Tests for the {@link KubernetesClusterDescriptor}.
 */
public class KubernetesClusterDescriptorTest extends KubernetesTestBase {

	@Test
	public void testDeploySessionCluster() throws Exception {

		FlinkKubeClient flinkKubeClient = getFabric8FlinkKubeClient();
		KubernetesClusterDescriptor descriptor = new KubernetesClusterDescriptor(flinkConfig, flinkKubeClient);

		ClusterSpecification clusterSpecification = new ClusterSpecification.ClusterSpecificationBuilder()
			.setMasterMemoryMB(1234)
			.setTaskManagerMemoryMB(1222)
			.setNumberTaskManagers(1)
			.setSlotsPerTaskManager(1)
			.createClusterSpecification();

		ClusterClient<String> clusterClient = descriptor.deploySessionCluster(clusterSpecification);

		assertEquals(clusterId, clusterClient.getClusterId());
		assertEquals(String.format("http://%s:8081", mockServiceIp), clusterClient.getWebInterfaceURL());

		// Check updated flink config options
		assertEquals(String.valueOf(Constants.BLOB_SERVER_PORT), flinkConfig.getString(BlobServerOptions.PORT));
		assertEquals(String.valueOf(Constants.TASK_MANAGER_RPC_PORT), flinkConfig.getString(TaskManagerOptions.RPC_PORT));
		assertEquals(clusterId + "." + nameSpace, flinkConfig.getString(JobManagerOptions.ADDRESS));
		assertEquals(mockServiceId, flinkConfig.getString(FlinkService.SERVICE_ID));

		KubernetesClient kubeClient = server.getClient();
		ServiceList serviceList = kubeClient.services().list();
		assertEquals(2, serviceList.getItems().size());
		assertEquals(clusterId, serviceList.getItems().get(0).getMetadata().getName());

		Container jmContainer = kubeClient.apps().deployments().list().getItems().get(0).getSpec()
			.getTemplate().getSpec().getContainers().get(0);
		assertEquals(clusterSpecification.getMasterMemoryMB() + Constants.RESOURCE_UNIT_MB,
			jmContainer.getResources().getRequests().get(Constants.RESOURCE_NAME_MEMORY).getAmount());
		assertEquals(clusterSpecification.getMasterMemoryMB() + Constants.RESOURCE_UNIT_MB,
			jmContainer.getResources().getLimits().get(Constants.RESOURCE_NAME_MEMORY).getAmount());
	}

	@Test
	public void testKillCluster() throws Exception {

		FlinkKubeClient flinkKubeClient = getFabric8FlinkKubeClient();
		KubernetesClusterDescriptor descriptor = new KubernetesClusterDescriptor(flinkConfig, flinkKubeClient);

		ClusterSpecification clusterSpecification = new ClusterSpecification.ClusterSpecificationBuilder()
			.setMasterMemoryMB(1)
			.setTaskManagerMemoryMB(1)
			.setNumberTaskManagers(1)
			.setSlotsPerTaskManager(1)
			.createClusterSpecification();

		descriptor.deploySessionCluster(clusterSpecification);

		KubernetesClient kubeClient = server.getClient();
		assertEquals(2, kubeClient.services().list().getItems().size());

		descriptor.killCluster(clusterId);
		// Mock kubernetes server do not delete the rest service by gc, so the rest service still exist.
		assertEquals(1, kubeClient.services().list().getItems().size());
		assertEquals(mockServiceId, kubeClient.services().list().getItems().get(0)
			.getMetadata().getOwnerReferences().get(0).getUid());
	}

}
