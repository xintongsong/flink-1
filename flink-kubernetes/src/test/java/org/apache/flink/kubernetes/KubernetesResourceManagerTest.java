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

import org.apache.flink.api.common.JobID;
import org.apache.flink.api.common.time.Time;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.kubernetes.configuration.KubernetesConfigOptions;
import org.apache.flink.kubernetes.kubeclient.FlinkKubeClient;
import org.apache.flink.kubernetes.kubeclient.resources.FlinkPod;
import org.apache.flink.kubernetes.utils.Constants;
import org.apache.flink.runtime.clusterframework.types.AllocationID;
import org.apache.flink.runtime.clusterframework.types.ResourceID;
import org.apache.flink.runtime.clusterframework.types.ResourceProfile;
import org.apache.flink.runtime.clusterframework.types.SlotID;
import org.apache.flink.runtime.entrypoint.ClusterInformation;
import org.apache.flink.runtime.heartbeat.HeartbeatServices;
import org.apache.flink.runtime.highavailability.HighAvailabilityServices;
import org.apache.flink.runtime.instance.HardwareDescription;
import org.apache.flink.runtime.messages.Acknowledge;
import org.apache.flink.runtime.metrics.groups.ResourceManagerMetricGroup;
import org.apache.flink.runtime.metrics.groups.UnregisteredMetricGroups;
import org.apache.flink.runtime.registration.RegistrationResponse;
import org.apache.flink.runtime.resourcemanager.JobLeaderIdService;
import org.apache.flink.runtime.resourcemanager.ResourceManagerGateway;
import org.apache.flink.runtime.resourcemanager.SlotRequest;
import org.apache.flink.runtime.resourcemanager.slotmanager.SlotManager;
import org.apache.flink.runtime.rpc.FatalErrorHandler;
import org.apache.flink.runtime.rpc.RpcService;
import org.apache.flink.runtime.rpc.TestingRpcService;
import org.apache.flink.runtime.taskexecutor.SlotReport;
import org.apache.flink.runtime.taskexecutor.SlotStatus;
import org.apache.flink.runtime.taskexecutor.TaskExecutorGateway;
import org.apache.flink.runtime.taskexecutor.TaskExecutorRegistrationSuccess;
import org.apache.flink.runtime.util.TestingFatalErrorHandler;

import io.fabric8.kubernetes.api.model.Container;
import io.fabric8.kubernetes.api.model.ContainerStateBuilder;
import io.fabric8.kubernetes.api.model.ContainerStatusBuilder;
import io.fabric8.kubernetes.api.model.EnvVarBuilder;
import io.fabric8.kubernetes.api.model.Pod;
import io.fabric8.kubernetes.api.model.PodList;
import io.fabric8.kubernetes.api.model.PodStatusBuilder;
import io.fabric8.kubernetes.client.KubernetesClient;
import org.hamcrest.Matchers;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.util.Collections;
import java.util.Map;
import java.util.concurrent.Callable;
import java.util.concurrent.CompletableFuture;

import static junit.framework.TestCase.assertEquals;
import static org.hamcrest.Matchers.instanceOf;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.mock;

/**
 * Test cases for {@link KubernetesResourceManager}.
 */
public class KubernetesResourceManagerTest extends KubernetesTestBase {

	private static final Time TIMEOUT = Time.seconds(10L);

	private TestingFatalErrorHandler testingFatalErrorHandler;

	private final String taskManagerHost = "host1";

	@Before
	public void setup() {
		testingFatalErrorHandler = new TestingFatalErrorHandler();
	}

	@After
	public void teardown() throws Exception {
		if (testingFatalErrorHandler != null) {
			testingFatalErrorHandler.rethrowError();
		}
	}

	private TestingKubernetesResourceManager createAndStartResourceManager(
		Configuration configuration) throws Exception {

		TestingRpcService rpcService = new TestingRpcService(configuration);
		TestingFatalErrorHandler testingFatalErrorHandler = new TestingFatalErrorHandler();
		org.apache.flink.runtime.resourcemanager.utils.MockResourceManagerRuntimeServices rmServices =
			new org.apache.flink.runtime.resourcemanager.utils.MockResourceManagerRuntimeServices(rpcService, TIMEOUT);

		TestingKubernetesResourceManager kubernetesResourceManager = new TestingKubernetesResourceManager(
			rpcService,
			"kubernetesResourceManager",
			ResourceID.generate(),
			configuration,
			rmServices.highAvailabilityServices,
			rmServices.heartbeatServices,
			rmServices.slotManager,
			rmServices.jobLeaderIdService,
			new ClusterInformation("localhost", 1234),
			testingFatalErrorHandler,
			UnregisteredMetricGroups.createUnregisteredResourceManagerMetricGroup()
		);
		kubernetesResourceManager.start();
		rmServices.grantLeadership();
		return kubernetesResourceManager;
	}

	class TestingKubernetesResourceManager extends KubernetesResourceManager {

		private SlotManager slotManager;

		public TestingKubernetesResourceManager(
			RpcService rpcService,
			String resourceManagerEndpointId,
			ResourceID resourceId,
			Configuration flinkConfig,
			HighAvailabilityServices highAvailabilityServices,
			HeartbeatServices heartbeatServices,
			SlotManager slotManager,
			JobLeaderIdService jobLeaderIdService,
			ClusterInformation clusterInformation,
			FatalErrorHandler fatalErrorHandler,
			ResourceManagerMetricGroup resourceManagerMetricGroup) {
			super(
				rpcService,
				resourceManagerEndpointId,
				resourceId,
				flinkConfig,
				highAvailabilityServices,
				heartbeatServices,
				slotManager,
				jobLeaderIdService,
				clusterInformation,
				fatalErrorHandler,
				resourceManagerMetricGroup
			);
			this.slotManager = slotManager;
		}

		@Override
		protected FlinkKubeClient createFlinkKubeClient() {
			return getFabric8FlinkKubeClient();
		}

		<T> CompletableFuture<T> runInMainThread(Callable<T> callable) {
			return callAsync(callable, TIMEOUT);
		}

		MainThreadExecutor getMainThreadExecutorForTesting() {
			return super.getMainThreadExecutor();
		}

		SlotManager getSlotManager() {
			return this.slotManager;
		}
	}

	@Test
	public void testStartAndStopWorker() throws Exception {

		TestingKubernetesResourceManager resourceManager = this.createAndStartResourceManager(flinkConfig);
		ResourceProfile resourceProfile = new ResourceProfile(1.1, 2);

		// Request slot from SlotManager.
		CompletableFuture<?> registerSlotRequestFuture = resourceManager.runInMainThread(() -> {
			resourceManager.getSlotManager().registerSlotRequest(
				new SlotRequest(new JobID(), new AllocationID(), resourceProfile, taskManagerHost));
			return null;
		});
		registerSlotRequestFuture.get();

		KubernetesClient client = getKubeClient();
		PodList list = client.pods().list();
		assertEquals(1, list.getItems().size());
		Pod pod = list.getItems().get(0);

		Map<String, String> labels = getCommonLabels();
		labels.put(Constants.LABEL_COMPONENT_KEY, Constants.LABEL_COMPONENT_TASK_MANAGER);
		assertEquals(labels, pod.getMetadata().getLabels());

		assertEquals(1, pod.getSpec().getContainers().size());
		Container tmContainer = pod.getSpec().getContainers().get(0);
		assertEquals(containerImage, tmContainer.getImage());

		String podName = clusterId + Constants.NAME_SEPARATOR + "taskmanager" + Constants.NAME_SEPARATOR + 1;
		assertEquals(podName, pod.getMetadata().getName());
		assertEquals(1, pod.getSpec().getContainers().size());

		// Check environments
		assertThat(tmContainer.getEnv(), Matchers.contains(
			new EnvVarBuilder().withName("RESOURCE_ID").withValue(podName).build()));

		// Check task manager main class args.
		assertEquals(3, tmContainer.getArgs().size());
		String confDirOption = "--configDir " + flinkConfig.getString(KubernetesConfigOptions.FLINK_CONF_DIR);
		assertTrue(tmContainer.getArgs().get(2).contains(confDirOption));

		resourceManager.onAdded(Collections.singletonList(new FlinkPod(flinkConfig, pod)));
		ResourceID resourceID = new ResourceID(podName);
		assertThat(resourceManager.getWorkerNodeMap().keySet(), Matchers.contains(resourceID));

		// Remote task executor registers with YarnResourceManager.
		TaskExecutorGateway mockTaskExecutorGateway = mock(TaskExecutorGateway.class);
		((TestingRpcService) resourceManager.getRpcService()).registerGateway(taskManagerHost, mockTaskExecutorGateway);

		final ResourceManagerGateway rmGateway = resourceManager.getSelfGateway(ResourceManagerGateway.class);

		final SlotReport slotReport = new SlotReport(
			new SlotStatus(
				new SlotID(resourceID, 1),
				new ResourceProfile(10, 1, 1, 1, 0, 0, Collections.emptyMap())));

		CompletableFuture<Integer> numberRegisteredSlotsFuture = rmGateway
			.registerTaskExecutor(
				taskManagerHost,
				resourceID,
				1234,
				new HardwareDescription(1, 2L, 3L, 4L),
				Time.seconds(10L))
			.thenCompose(
				(RegistrationResponse response) -> {
					assertThat(response, instanceOf(TaskExecutorRegistrationSuccess.class));
					final TaskExecutorRegistrationSuccess success = (TaskExecutorRegistrationSuccess) response;
					return rmGateway.sendSlotReport(
						resourceID,
						success.getRegistrationId(),
						slotReport,
						Time.seconds(10L));
				})
			.handleAsync(
				(Acknowledge ignored, Throwable throwable) -> resourceManager.getSlotManager().getNumberRegisteredSlots(),
				resourceManager.getMainThreadExecutorForTesting());

		final int numberRegisteredSlots = numberRegisteredSlotsFuture.get();

		Assert.assertEquals(1, numberRegisteredSlots);

		// Unregister all task executors and release all task managers.
		CompletableFuture<?> unregisterAndReleaseFuture = resourceManager.runInMainThread(() -> {
			resourceManager.getSlotManager().unregisterTaskManagersAndReleaseResources();
			return null;
		});
		unregisterAndReleaseFuture.get();
		assertEquals(0, client.pods().list().getItems().size());

		resourceManager.getRpcService().stopService().get();
	}

	@Test
	public void testTaskManagerTerminated() throws Exception {
		TestingKubernetesResourceManager resourceManager = this.createAndStartResourceManager(flinkConfig);
		ResourceProfile resourceProfile = new ResourceProfile(1.1, 2);

		CompletableFuture<?> registerSlotRequestFuture = resourceManager.runInMainThread(() -> {
			resourceManager.getSlotManager().registerSlotRequest(
				new SlotRequest(new JobID(), new AllocationID(), resourceProfile, taskManagerHost));
			return null;
		});
		registerSlotRequestFuture.get();

		KubernetesClient client = getKubeClient();
		assertEquals(1, client.pods().list().getItems().size());
		Pod pod = client.pods().list().getItems().get(0);
		String taskManagerPrefix = clusterId + Constants.NAME_SEPARATOR + "taskmanager" + Constants.NAME_SEPARATOR;
		assertEquals(taskManagerPrefix + 1, pod.getMetadata().getName());

		resourceManager.onAdded(Collections.singletonList(new FlinkPod(flinkConfig, pod)));

		resourceManager.onModified(Collections.singletonList(new FlinkPod(flinkConfig, pod)));
		assertEquals(1, client.pods().list().getItems().size());
		assertEquals(taskManagerPrefix + 1, client.pods().list().getItems().get(0).getMetadata().getName());

		// Terminate the pod.
		pod.setStatus(new PodStatusBuilder()
			.withContainerStatuses(new ContainerStatusBuilder().withState(
				new ContainerStateBuilder().withNewTerminated().endTerminated().build())
				.build())
			.build());

		// A new task manager has been created
		resourceManager.onModified(Collections.singletonList(new FlinkPod(flinkConfig, pod)));
		assertEquals(1, client.pods().list().getItems().size());
		assertEquals(taskManagerPrefix + 2, client.pods().list().getItems().get(0).getMetadata().getName());

		resourceManager.getRpcService().stopService().get();
	}
}
