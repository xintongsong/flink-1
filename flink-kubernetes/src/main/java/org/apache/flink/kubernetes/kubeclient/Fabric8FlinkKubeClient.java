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
import org.apache.flink.configuration.ConfigOption;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.RestOptions;
import org.apache.flink.kubernetes.configuration.KubernetesConfigOptions;
import org.apache.flink.kubernetes.kubeclient.decorators.ConfigMapDecorator;
import org.apache.flink.kubernetes.kubeclient.decorators.Decorator;
import org.apache.flink.kubernetes.kubeclient.decorators.FlinkMasterDeploymentDecorator;
import org.apache.flink.kubernetes.kubeclient.decorators.InitializerDecorator;
import org.apache.flink.kubernetes.kubeclient.decorators.OwnerReferenceDecorator;
import org.apache.flink.kubernetes.kubeclient.decorators.ServiceDecorator;
import org.apache.flink.kubernetes.kubeclient.decorators.TaskManagerPodDecorator;
import org.apache.flink.kubernetes.kubeclient.resources.ActionWatcher;
import org.apache.flink.kubernetes.kubeclient.resources.FlinkConfigMap;
import org.apache.flink.kubernetes.kubeclient.resources.FlinkDeployment;
import org.apache.flink.kubernetes.kubeclient.resources.FlinkPod;
import org.apache.flink.kubernetes.kubeclient.resources.FlinkService;
import org.apache.flink.kubernetes.utils.Constants;

import io.fabric8.kubernetes.api.model.ConfigMap;
import io.fabric8.kubernetes.api.model.Pod;
import io.fabric8.kubernetes.api.model.Service;
import io.fabric8.kubernetes.api.model.ServicePort;
import io.fabric8.kubernetes.api.model.apps.Deployment;
import io.fabric8.kubernetes.client.KubernetesClient;
import io.fabric8.kubernetes.client.KubernetesClientException;
import io.fabric8.kubernetes.client.Watch;
import io.fabric8.kubernetes.client.Watcher;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;

/**
 * The implementation of {@link FlinkKubeClient}.
 */
public class Fabric8FlinkKubeClient implements FlinkKubeClient {

	private static final Logger LOG = LoggerFactory.getLogger(Fabric8FlinkKubeClient.class);

	private final Configuration flinkConfig;

	private final KubernetesClient internalClient;

	private List<Decorator<ConfigMap, FlinkConfigMap>> configMapDecorators;

	private List<Decorator<Service, FlinkService>> internalServiceDecorators;
	private List<Decorator<Service, FlinkService>> restServiceDecorators;

	private List<Decorator<Deployment, FlinkDeployment>> flinkMasterDeploymentDecorators;

	private List<Decorator<Pod, FlinkPod>> taskManagerPodDecorators;

	@Nonnull
	private final String clusterId;

	@Nonnull
	private final String nameSpace;

	public Fabric8FlinkKubeClient(Configuration flinkConfig, KubernetesClient client) {
		this.internalClient = client;
		this.flinkConfig = flinkConfig;
		this.configMapDecorators = new ArrayList<>();
		this.internalServiceDecorators = new ArrayList<>();
		this.restServiceDecorators = new ArrayList<>();
		this.flinkMasterDeploymentDecorators = new ArrayList<>();
		this.taskManagerPodDecorators = new ArrayList<>();

		this.clusterId = flinkConfig.getString(KubernetesConfigOptions.CLUSTER_ID);
		this.nameSpace = flinkConfig.getString(KubernetesConfigOptions.NAMESPACE);

		initialize();
	}

	private void initialize() {
		this.configMapDecorators.add(new InitializerDecorator<>(Constants.CONFIG_MAP_PREFIX + clusterId));
		this.configMapDecorators.add(new OwnerReferenceDecorator<>());
		this.configMapDecorators.add(new ConfigMapDecorator());

		this.internalServiceDecorators.add(new InitializerDecorator<>(clusterId));
		this.internalServiceDecorators.add(new ServiceDecorator(
			KubernetesConfigOptions.ServiceExposedType.ClusterIP, false));

		this.restServiceDecorators.add(new InitializerDecorator<>(clusterId + Constants.FLINK_REST_SERVICE_SUFFIX));
		String exposedType = flinkConfig.getString(KubernetesConfigOptions.REST_SERVICE_EXPOSED_TYPE);
		this.restServiceDecorators.add(new ServiceDecorator(
			KubernetesConfigOptions.ServiceExposedType.valueOf(exposedType), true));
		this.restServiceDecorators.add(new OwnerReferenceDecorator<>());

		this.flinkMasterDeploymentDecorators.add(new InitializerDecorator<>(clusterId, Constants.APPS_API_VERSION));
		this.flinkMasterDeploymentDecorators.add(new OwnerReferenceDecorator<>(Constants.APPS_API_VERSION));

		this.taskManagerPodDecorators.add(new InitializerDecorator<>());
		this.taskManagerPodDecorators.add(new OwnerReferenceDecorator<>());
	}

	@Override
	public void createConfigMap() {
		FlinkConfigMap configMap = new FlinkConfigMap(this.flinkConfig);

		for (Decorator<ConfigMap, FlinkConfigMap> c : this.configMapDecorators) {
			configMap = c.decorate(configMap);
		}
		LOG.info("Create config map with data size {}", configMap.getInternalResource().getData().size());
		this.internalClient.configMaps().create(configMap.getInternalResource());
	}

	@Override
	public CompletableFuture<FlinkService> createInternalService(String clusterId) {
		return createService(clusterId, this.internalServiceDecorators);
	}

	@Override
	public CompletableFuture<FlinkService> createRestService(String clusterId) {
		return createService(clusterId + Constants.FLINK_REST_SERVICE_SUFFIX, this.restServiceDecorators);
	}

	@Override
	public void createFlinkMasterDeployment(ClusterSpecification clusterSpecification) {
		FlinkDeployment deployment = new FlinkDeployment(this.flinkConfig);

		for (Decorator<Deployment, FlinkDeployment> d : this.flinkMasterDeploymentDecorators) {
			deployment = d.decorate(deployment);
		}

		deployment = new FlinkMasterDeploymentDecorator(clusterSpecification).decorate(deployment);
		LOG.info("Create Flink Master deployment with spec: {}", deployment.getInternalResource().getSpec().toString());

		this.internalClient.apps().deployments().inNamespace(this.nameSpace).create(
			deployment.getInternalResource());
	}

	@Override
	public void createTaskManagerPod(TaskManagerPodParameter parameter) {
		FlinkPod pod = new FlinkPod(this.flinkConfig);

		for (Decorator<Pod, FlinkPod> d : this.taskManagerPodDecorators) {
			pod = d.decorate(pod);
		}

		pod = new TaskManagerPodDecorator(parameter).decorate(pod);
		LOG.info("Create TaskManager pod with spec: {}", pod.getInternalResource().getSpec().toString());

		this.internalClient.pods().inNamespace(this.nameSpace).create(pod.getInternalResource());
	}

	@Override
	public void stopPod(String podName) {
		this.internalClient.pods().withName(podName).delete();
	}

	@Override
	@Nullable
	public Endpoint getRestEndpoints(String clusterId) {
		int restPort = this.flinkConfig.getInteger(RestOptions.PORT);
		String serviceExposedType = flinkConfig.getString(KubernetesConfigOptions.REST_SERVICE_EXPOSED_TYPE);

		// Return the service.namespace directly when use ClusterIP.
		if (serviceExposedType.equals(KubernetesConfigOptions.ServiceExposedType.ClusterIP.toString())) {
			return new Endpoint(clusterId + "." + nameSpace, restPort);
		}

		FlinkService restService = getRestService(clusterId);
		if (restService == null) {
			return null;
		}
		Service service = restService.getInternalResource();

		String address = null;

		if (service.getStatus() != null && (service.getStatus().getLoadBalancer() != null ||
			service.getStatus().getLoadBalancer().getIngress() != null)) {
			if (service.getStatus().getLoadBalancer().getIngress().size() > 0) {
				address = service.getStatus().getLoadBalancer().getIngress().get(0).getIp();
			} else {
				address = this.internalClient.getMasterUrl().getHost();
				restPort = getServiceNodePort(service, RestOptions.PORT);
			}
		} else if (service.getSpec().getExternalIPs() != null && service.getSpec().getExternalIPs().size() > 0) {
			address = service.getSpec().getExternalIPs().get(0);
		}
		return new Endpoint(address, restPort);
	}

	@Override
	public void stopAndCleanupCluster(String clusterId) {
		this.internalClient.services().inNamespace(this.nameSpace).withName(clusterId).cascading(true).delete();
	}

	@Override
	public void handleException(Exception e) {
		LOG.error("Kubernetes Exception: {}", e);
	}

	@Override
	@Nullable
	public FlinkService getInternalService(String clusterId) {
		return getService(clusterId);
	}

	@Override
	@Nullable
	public FlinkService getRestService(String clusterId) {
		return getService(clusterId + Constants.FLINK_REST_SERVICE_SUFFIX);
	}

	@Override
	public void watchPodsAndDoCallback(Map<String, String> labels, PodCallbackHandler callbackHandler) {
		Watcher<Pod> watcher = new Watcher<Pod>() {
			@Override
			public void eventReceived(Action action, Pod pod) {
				switch (action) {
					case ADDED:
						callbackHandler.onAdded(Collections.singletonList(new FlinkPod(flinkConfig, pod)));
						break;
					case MODIFIED:
						callbackHandler.onModified(Collections.singletonList(new FlinkPod(flinkConfig, pod)));
						break;
					case ERROR:
						callbackHandler.onError(Collections.singletonList(new FlinkPod(flinkConfig, pod)));
						break;
					case DELETED:
						callbackHandler.onDeleted(Collections.singletonList(new FlinkPod(flinkConfig, pod)));
						break;
					default:
						LOG.info("Skip handling {} event for pod {}", action, pod.getMetadata().getName());
						break;
				}
			}

			@Override
			public void onClose(KubernetesClientException e) {
				LOG.error("Pods watcher onClose");
				if (e != null) {
					LOG.error(e.getMessage(), e);
				}
			}
		};
		this.internalClient.pods().withLabels(labels).watch(watcher);
	}

	@Override
	public void close() {
		this.internalClient.close();
	}

	// Private util methods

	private CompletableFuture<FlinkService> createService(String serviceName,
			List<Decorator<Service, FlinkService>> serviceDecorators) {
		FlinkService flinkService = new FlinkService(this.flinkConfig);
		for (Decorator<Service, FlinkService> d : serviceDecorators) {
			flinkService = d.decorate(flinkService);
		}
		LOG.info("Create service with spec: {}", flinkService.getInternalResource().getSpec().toString());
		this.internalClient.services().create(flinkService.getInternalResource());

		ActionWatcher<Service> watcher = new ActionWatcher<>(Watcher.Action.ADDED,
			flinkService.getInternalResource());
		Watch watchConnectionManager = this.internalClient.services()
			.inNamespace(this.nameSpace).withName(serviceName).watch(watcher);

		return CompletableFuture.supplyAsync(() -> {
			Service createdService = watcher.await(1, TimeUnit.MINUTES);
			watchConnectionManager.close();

			return new FlinkService(this.flinkConfig, createdService);
		});
	}

	private FlinkService getService(String serviceName) {
		Service service = this.internalClient.services().inNamespace(nameSpace).withName(serviceName).fromServer().get();
		if (service == null) {
			LOG.debug("Service {} does not exist", serviceName);
			return null;
		}

		FlinkService flinkService = new FlinkService(this.flinkConfig);
		flinkService.setInternalResource(service);
		return flinkService;
	}

	/**
	 * To get nodePort of configured ports.
	 */
	private int getServiceNodePort(Service service, ConfigOption<Integer> configPort) {
		int port = this.flinkConfig.getInteger(configPort);
		if (service.getSpec() != null && service.getSpec().getPorts() != null) {
			for (ServicePort p : service.getSpec().getPorts()) {
				if (p.getPort() == port) {
					return p.getNodePort();
				}
			}
		}
		return port;
	}
}
