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

import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.ConfigurationUtils;
import org.apache.flink.configuration.TaskManagerOptions;
import org.apache.flink.kubernetes.configuration.KubernetesConfigOptions;
import org.apache.flink.kubernetes.kubeclient.FlinkKubeClient;
import org.apache.flink.kubernetes.kubeclient.KubeClientFactory;
import org.apache.flink.kubernetes.kubeclient.TaskManagerPodParameter;
import org.apache.flink.kubernetes.kubeclient.resources.FlinkPod;
import org.apache.flink.kubernetes.taskmanager.KubernetesTaskExecutorRunner;
import org.apache.flink.kubernetes.utils.Constants;
import org.apache.flink.kubernetes.utils.KubernetesUtils;
import org.apache.flink.runtime.clusterframework.ApplicationStatus;
import org.apache.flink.runtime.clusterframework.ContaineredTaskManagerParameters;
import org.apache.flink.runtime.clusterframework.types.ResourceID;
import org.apache.flink.runtime.clusterframework.types.ResourceProfile;
import org.apache.flink.runtime.concurrent.FutureUtils;
import org.apache.flink.runtime.entrypoint.ClusterInformation;
import org.apache.flink.runtime.entrypoint.parser.CommandLineOptions;
import org.apache.flink.runtime.heartbeat.HeartbeatServices;
import org.apache.flink.runtime.highavailability.HighAvailabilityServices;
import org.apache.flink.runtime.metrics.groups.ResourceManagerMetricGroup;
import org.apache.flink.runtime.resourcemanager.JobLeaderIdService;
import org.apache.flink.runtime.resourcemanager.ResourceManager;
import org.apache.flink.runtime.resourcemanager.slotmanager.SlotManager;
import org.apache.flink.runtime.rpc.FatalErrorHandler;
import org.apache.flink.runtime.rpc.RpcService;
import org.apache.flink.util.FlinkException;
import org.apache.flink.util.FlinkRuntimeException;
import org.apache.flink.util.Preconditions;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;

import java.io.File;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Kubernetes specific implementation of the {@link ResourceManager}.
 */
public class KubernetesResourceManager extends ResourceManager<KubernetesWorkerNode>
	implements FlinkKubeClient.PodCallbackHandler {

	private static final Logger LOG = LoggerFactory.getLogger(KubernetesResourceManager.class);

	public static final String ENV_RESOURCE_ID = "RESOURCE_ID";

	private static final String TASK_MANAGER_PREFIX = "taskmanager";

	private final ConcurrentMap<ResourceID, KubernetesWorkerNode> workerNodeMap;

	private final int numberOfTaskSlots;

	private final int defaultTaskManagerMemoryMB;

	private final double defaultCpus;

	private final Collection<ResourceProfile> slotsPerWorker;

	private final Configuration flinkConfig;

	private final AtomicLong maxPodId = new AtomicLong(0);

	private final String clusterId;

	private FlinkKubeClient kubeClient;

	/** The number of pods requested, but not yet granted. */
	private int numPendingPodRequests;

	public KubernetesResourceManager(
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
			highAvailabilityServices,
			heartbeatServices,
			slotManager,
			jobLeaderIdService,
			clusterInformation,
			fatalErrorHandler,
			resourceManagerMetricGroup);

		this.flinkConfig = flinkConfig;
		this.clusterId = flinkConfig.getString(KubernetesConfigOptions.CLUSTER_ID);

		this.workerNodeMap = new ConcurrentHashMap<>();

		numPendingPodRequests = 0;

		this.numberOfTaskSlots = flinkConfig.getInteger(TaskManagerOptions.NUM_TASK_SLOTS);
		this.defaultTaskManagerMemoryMB = ConfigurationUtils.getTaskManagerHeapMemory(flinkConfig).getMebiBytes();
		this.defaultCpus = flinkConfig.getDouble(KubernetesConfigOptions.TASK_MANAGER_CPU, numberOfTaskSlots);

		this.slotsPerWorker = createWorkerSlotProfiles(flinkConfig);
	}

	@Override
	protected void initialize() {
		LOG.info("Initializing Kubernetes client.");
		LOG.info("KubernetesResourceManager.initialize clusterId:{}", clusterId);

		this.kubeClient = createFlinkKubeClient();
		this.kubeClient.watchPodsAndDoCallback(getTaskManagerLabels(), this);
	}

	@Override
	public CompletableFuture<Void> onStop() {
		// shut down all components
		Throwable firstException = null;

		if (kubeClient != null) {
			try {
				kubeClient.close();
			} catch (Throwable t) {
				firstException = t;
			}
		}

		final CompletableFuture<Void> terminationFuture = super.onStop();

		if (firstException != null) {
			return FutureUtils.completedExceptionally(new FlinkException(
				"Error while shutting down Kubernetes resource manager", firstException));
		} else {
			return terminationFuture;
		}
	}

	@Override
	protected void internalDeregisterApplication(ApplicationStatus finalStatus, @Nullable String optionalDiagnostics) {
		LOG.info("Stopping kubernetes cluster, id: {0}", clusterId);
		this.kubeClient.stopAndCleanupCluster(this.clusterId);
	}

	@Override
	public Collection<ResourceProfile> startNewWorker(ResourceProfile resourceProfile) {
		LOG.info("Starting new worker with resource profile, {}", resourceProfile.toString());
		if (!slotsPerWorker.iterator().next().isMatching(resourceProfile)) {
			return Collections.emptyList();
		}
		requestKubernetesPod();
		return slotsPerWorker;
	}

	@Override
	protected KubernetesWorkerNode workerStarted(ResourceID resourceID) {
		return workerNodeMap.get(resourceID);
	}

	@Override
	public boolean stopWorker(final KubernetesWorkerNode worker) {
		Preconditions.checkNotNull(this.kubeClient);
		LOG.info("Stopping Worker {}.", worker.getResourceID().toString());
		try {
			this.kubeClient.stopPod(worker.getResourceID().toString());
		} catch (Exception e) {
			this.kubeClient.logException(e);
			return false;
		}
		workerNodeMap.remove(worker.getResourceID());
		return true;
	}

	@Override
	public void onAdded(List<FlinkPod> pods) {
		for (FlinkPod pod : pods) {
			log.info("Received new TaskExecutor pod: {} - Remaining pending pod requests: {}",
				pod.getName(), numPendingPodRequests);

			if (numPendingPodRequests > 0) {
				numPendingPodRequests--;
				KubernetesWorkerNode worker = new KubernetesWorkerNode(new ResourceID(pod.getName()));
				workerNodeMap.put(worker.getResourceID(), worker);
			} else {
				log.info("Returning excess pod {}.", pod.getName());
				kubeClient.stopPod(pod.getName());
			}
		}
	}

	@Override
	public void onModified(List<FlinkPod> pods) {
		pods.forEach(this::removePodIfTerminated);
	}

	@Override
	public void onDeleted(List<FlinkPod> pods) {
		pods.forEach(this::removePodIfTerminated);
	}

	@Override
	public void onError(List<FlinkPod> pods) {
		pods.forEach(this::removePodIfTerminated);
	}

	protected ConcurrentMap<ResourceID, KubernetesWorkerNode> getWorkerNodeMap() {
		return workerNodeMap;
	}

	private void requestKubernetesPod() {
		Preconditions.checkNotNull(this.kubeClient);

		numPendingPodRequests++;

		log.info("Requesting new TaskExecutor pod with <{},{}>. Number pending requests {}.",
			defaultTaskManagerMemoryMB,
			defaultCpus,
			numPendingPodRequests);

		String podName = clusterId + Constants.NAME_SEPARATOR + TASK_MANAGER_PREFIX +
			Constants.NAME_SEPARATOR + maxPodId.addAndGet(1);

		try {
			HashMap<String, String> env = new HashMap<>();
			env.put(ENV_RESOURCE_ID, podName);

			TaskManagerPodParameter parameter = new TaskManagerPodParameter(
				podName,
				getTaskManagerStartCommand(podName),
				defaultTaskManagerMemoryMB,
				defaultCpus,
				env);

			this.kubeClient.createTaskManagerPod(parameter);
		} catch (Exception e) {
			this.kubeClient.logException(e);
			throw new FlinkRuntimeException("Could not start new worker");
		}
	}

	/**
	 * Request new pod if pending pods cannot satisfies pending slot requests.
	 */
	private void requestKubernetesPodIfRequired() {
		int requiredTaskManagerSlots = getNumberRequiredTaskManagerSlots();
		int pendingTaskManagerSlots = numPendingPodRequests * numberOfTaskSlots;

		if (requiredTaskManagerSlots > pendingTaskManagerSlots) {
			requestKubernetesPod();
		}
	}

	private void removePodIfTerminated(FlinkPod pod) {
		if (pod.isTerminated()) {
			kubeClient.stopPod(pod.getName());
			final KubernetesWorkerNode kubernetesWorkerNode = workerNodeMap.remove(new ResourceID(pod.getName()));
			if (kubernetesWorkerNode != null) {
				requestKubernetesPodIfRequired();
			}
		}
	}

	private List<String> getTaskManagerStartCommand(String podName) {
		final ContaineredTaskManagerParameters taskManagerParameters =
			ContaineredTaskManagerParameters.create(flinkConfig, 1333, numberOfTaskSlots);

		log.info("TaskExecutor {} will be started with container size {} MB, JVM heap size {} MB, " +
				"JVM direct memory limit {} MB",
			podName,
			taskManagerParameters.taskManagerTotalMemoryMB(),
			taskManagerParameters.taskManagerHeapSizeMB(),
			taskManagerParameters.taskManagerDirectMemoryLimitMB());

		String confDir = flinkConfig.getString(KubernetesConfigOptions.FLINK_CONF_DIR);
		boolean hasLogback = new File(confDir, Constants.CONFIG_FILE_LOGBACK_NAME).exists();
		boolean hasLog4j = new File(confDir, Constants.CONFIG_FILE_LOG4J_NAME).exists();

		String logDir = flinkConfig.getString(KubernetesConfigOptions.FLINK_LOG_DIR);

		String command = KubernetesUtils.getTaskManagerShellCommand(flinkConfig, taskManagerParameters,
			confDir, logDir, hasLogback, hasLog4j, KubernetesTaskExecutorRunner.class, getTaskManagerMainClassArgs());
		return Arrays.asList("/bin/bash", "-c", command);
	}

	/**
	 * Some configuration options may be overridden or added in flink master pod. So they need to be
	 * taken to task managers.
	 * @return Configuration dynamic properties.
	 */
	private String getTaskManagerMainClassArgs() {
		StringBuilder mainClassArgs = new StringBuilder();
		// Add conf dir option.
		mainClassArgs.append("--").append(CommandLineOptions.CONFIG_DIR_OPTION.getLongOpt()).append(" ")
			.append(flinkConfig.getString(KubernetesConfigOptions.FLINK_CONF_DIR)).append(" ");

		// Add dynamic configuration options.
		mainClassArgs.append("-").append(CommandLineOptions.DYNAMIC_PROPERTY_OPTION.getOpt())
			.append(TaskManagerOptions.LEGACY_MANAGED_MEMORY_SIZE.key()).append("=")
			.append(flinkConfig.getString(TaskManagerOptions.MANAGED_MEMORY_SIZE));

		return mainClassArgs.toString();
	}

	/**
	 * Get task manager label for the current flink cluster. They will be used to watching the pods status.
	 * @return Task manager labels.
	 */
	private Map<String, String> getTaskManagerLabels() {
		Map<String, String> labels = new HashMap<>();
		labels.put(Constants.LABEL_TYPE_KEY, Constants.LABEL_TYPE_NATIVE_TYPE);
		labels.put(Constants.LABEL_APP_KEY, clusterId);
		labels.put(Constants.LABEL_COMPONENT_KEY, Constants.LABEL_COMPONENT_TASK_MANAGER);
		return labels;
	}

	protected FlinkKubeClient createFlinkKubeClient() {
		return KubeClientFactory.fromConfiguration(this.flinkConfig);
	}
}
