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

import org.apache.flink.configuration.ConfigConstants;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.kubernetes.configuration.KubernetesConfigOptions;
import org.apache.flink.kubernetes.kubeclient.Fabric8FlinkKubeClient;
import org.apache.flink.kubernetes.kubeclient.FlinkKubeClient;
import org.apache.flink.kubernetes.kubeclient.decorators.Decorator;
import org.apache.flink.kubernetes.kubeclient.decorators.FlinkMasterDeploymentDecorator;
import org.apache.flink.kubernetes.kubeclient.decorators.InitializerDecorator;
import org.apache.flink.kubernetes.kubeclient.decorators.OwnerReferenceDecorator;
import org.apache.flink.kubernetes.kubeclient.decorators.ServiceDecorator;
import org.apache.flink.kubernetes.kubeclient.resources.FlinkService;
import org.apache.flink.kubernetes.utils.Constants;
import org.apache.flink.runtime.clusterframework.BootstrapTools;
import org.apache.flink.test.util.TestBaseUtils;
import org.apache.flink.util.TestLogger;

import io.fabric8.kubernetes.api.model.LoadBalancerIngress;
import io.fabric8.kubernetes.api.model.LoadBalancerStatus;
import io.fabric8.kubernetes.api.model.Service;
import io.fabric8.kubernetes.api.model.ServiceStatusBuilder;
import io.fabric8.kubernetes.api.model.WatchEvent;
import io.fabric8.kubernetes.client.KubernetesClient;
import org.junit.Before;
import org.junit.Rule;
import org.junit.rules.TemporaryFolder;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Base test class for Kubernetes.
 */
public class KubernetesTestBase extends TestLogger {
	@Rule
	public MixedKubernetesServer server = new MixedKubernetesServer(true, true);

	@Rule
	public TemporaryFolder temporaryFolder = new TemporaryFolder();

	private File flinkConfDir;

	protected final String nameSpace = "test";

	protected final Configuration flinkConfig = new Configuration();

	protected final String clusterId = "my-flink-cluster1";

	protected final String containerImage = "flink-k8s-test:latest";

	protected final String mockServiceId = "mock-uuid-of-service";

	protected final String mockServiceHostName = "mock-host-name-of-service";

	protected final String mockServiceIp = "192.168.0.1";

	@Before
	public void setUp() throws IOException {
		flinkConfig.setString(KubernetesConfigOptions.NAMESPACE, nameSpace);
		flinkConfig.setString(KubernetesConfigOptions.CLUSTER_ID, clusterId);
		flinkConfig.setString(KubernetesConfigOptions.CONTAINER_IMAGE, containerImage);
		flinkConfig.setString(FlinkService.SERVICE_ID, mockServiceId);
		flinkConfig.setString(FlinkMasterDeploymentDecorator.ENTRY_POINT_CLASS, "main-class");

		flinkConfDir = temporaryFolder.newFolder().getAbsoluteFile();
		BootstrapTools.writeConfiguration(new Configuration(), new File(flinkConfDir, "flink-conf.yaml"));
		Map<String, String> map = new HashMap<>();
		map.put(ConfigConstants.ENV_FLINK_CONF_DIR, flinkConfDir.toString());
		TestBaseUtils.setEnv(map);

		// Set mock requests.
		mockInternalServiceActionWatch();
		mockRestServiceActionWatcher();
		mockGetRestService();
	}

	protected FlinkKubeClient getFabric8FlinkKubeClient(){
		return getFabric8FlinkKubeClient(this.flinkConfig);
	}

	protected FlinkKubeClient getFabric8FlinkKubeClient(Configuration flinkConfig){
		return new Fabric8FlinkKubeClient(flinkConfig, server.getClient().inNamespace(nameSpace));
	}

	protected KubernetesClient getKubeClient() {
		return server.getClient().inNamespace(nameSpace);
	}

	private void mockRestServiceActionWatcher() {
		String serviceName = clusterId + Constants.FLINK_REST_SERVICE_SUFFIX;

		String path = String.format("/api/v1/namespaces/%s/services?fieldSelector=metadata.name%%3D%s&watch=true",
			nameSpace, serviceName);
		server.expect()
			.withPath(path)
			.andUpgradeToWebSocket()
			.open()
			.waitFor(1000)
			.andEmit(new WatchEvent(getMockRestService(), "ADDED"))
			.done()
			.once();
	}

	private void mockInternalServiceActionWatch() {

		String path = String.format("/api/v1/namespaces/%s/services?fieldSelector=metadata.name%%3D%s&watch=true",
			nameSpace, clusterId);
		server.expect()
			.withPath(path)
			.andUpgradeToWebSocket()
			.open()
			.waitFor(1000)
			.andEmit(new WatchEvent(getMockInternalService(), "ADDED"))
			.done()
			.once();
	}

	private void mockGetRestService() {
		String serviceName = clusterId + Constants.FLINK_REST_SERVICE_SUFFIX;

		String path = String.format("/api/v1/namespaces/%s/services/%s", nameSpace, serviceName);
		server.expect()
			.withPath(path)
			.andReturn(200, getMockRestService())
			.always();
	}

	private Service getMockRestService() {
		List<Decorator<Service, FlinkService>> restServiceDecorators = new ArrayList<>();
		restServiceDecorators.add(new InitializerDecorator<>(clusterId + Constants.FLINK_REST_SERVICE_SUFFIX));
		String exposedType = flinkConfig.getString(KubernetesConfigOptions.REST_SERVICE_EXPOSED_TYPE);
		restServiceDecorators.add(new ServiceDecorator(
			KubernetesConfigOptions.ServiceExposedType.valueOf(exposedType), true));
		restServiceDecorators.add(new OwnerReferenceDecorator<>());

		FlinkService flinkService = new FlinkService(this.flinkConfig);
		for (Decorator<Service, FlinkService> d : restServiceDecorators) {
			flinkService = d.decorate(flinkService);
		}

		Service service = flinkService.getInternalResource();
		service.setStatus(new ServiceStatusBuilder()
			.withLoadBalancer(new LoadBalancerStatus(Collections.singletonList(
			new LoadBalancerIngress(mockServiceHostName, mockServiceIp)))).build());
		return service;
	}

	private Service getMockInternalService() {
		List<Decorator<Service, FlinkService>> internalServiceDecorators = new ArrayList<>();
		internalServiceDecorators.add(new InitializerDecorator<>(clusterId));
		internalServiceDecorators.add(new ServiceDecorator(
			KubernetesConfigOptions.ServiceExposedType.ClusterIP, true));

		FlinkService flinkService = new FlinkService(this.flinkConfig);
		for (Decorator<Service, FlinkService> d : internalServiceDecorators) {
			flinkService = d.decorate(flinkService);
		}
		flinkService.getInternalResource().getMetadata().setUid(mockServiceId);
		return flinkService.getInternalResource();
	}

	protected Map<String, String> getCommonLabels() {
		Map<String, String> labels = new HashMap<>();
		labels.put(Constants.LABEL_TYPE_KEY, Constants.LABEL_TYPE_NATIVE_TYPE);
		labels.put(Constants.LABEL_APP_KEY, clusterId);
		return labels;
	}

}
