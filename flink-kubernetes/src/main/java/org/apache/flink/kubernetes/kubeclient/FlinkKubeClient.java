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
import org.apache.flink.kubernetes.kubeclient.resources.FlinkPod;
import org.apache.flink.kubernetes.kubeclient.resources.FlinkService;

import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;

/**
 * The client to talk with kubernetes.
 */
public interface FlinkKubeClient extends AutoCloseable {

	/**
	 * Create kubernetes config map, include flink-conf.yaml, log4j.properties.
	 */
	void createConfigMap() throws Exception;

	/**
	 * Create kubernetes service for internal use. This will be set to jobmanager rpc address.
	 * It is the owner of all resources. After deletion, all other resource will be deleted by gc.
	 * A CompletableFuture is returned and could be used to wait for service ready.
	 */
	CompletableFuture<FlinkService> createInternalService(String clusterId) throws Exception;

	/**
	 * Create kubernetes service for rest port. This will be used by client to interact with flink cluster.
	 */
	CompletableFuture<FlinkService> createRestService(String clusterId) throws Exception;

	/**
	 * Create flink master deployment with replication of 1.
	 */
	void createFlinkMasterDeployment(ClusterSpecification clusterSpec);

	/**
	 * Create task manager pod.
	 */
	void createTaskManagerPod(TaskManagerPodParameter parameter);

	/**
	 * Stop a specified pod by name.
	 */
	void stopPod(String podName);

	/**
	 * Stop cluster and clean up all resources, include services, auxiliary services and all running pods.
	 */
	void stopAndCleanupCluster(String clusterId);

	/**
	 * Get the kubernetes internal service of the given flink clusterId.
	 */
	FlinkService getInternalService(String clusterId);

	/**
	 * Get the kubernetes rest service of the given flink clusterId.
	 */
	FlinkService getRestService(String clusterId);

	/**
	 * Get the rest endpoints for access outside cluster.
	 */
	Endpoint getRestEndpoints(String clusterId);

	/**
	 * Log exceptions.
	 */
	void handleException(Exception e);

	/**
	 * Watch the pods selected by labels and do the {@link PodCallbackHandler}.
	 */
	void watchPodsAndDoCallback(Map<String, String> labels, PodCallbackHandler callbackHandler);

	/**
	 * Callback handler for kubernetes pods.
	 */
	interface PodCallbackHandler {

		void onAdded(List<FlinkPod> pods);

		void onModified(List<FlinkPod> pods);

		void onDeleted(List<FlinkPod> pods);

		void onError(List<FlinkPod> pods);
	}

}
