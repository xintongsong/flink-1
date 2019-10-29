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

package org.apache.flink.kubernetes.kubeclient.resources;

import org.apache.flink.configuration.ConfigOption;
import org.apache.flink.configuration.ConfigOptions;
import org.apache.flink.configuration.Configuration;

import io.fabric8.kubernetes.api.model.Service;

/**
 * Represent Service resource in kubernetes.
 */
public class FlinkService extends Resource<Service> {

	public static final ConfigOption<String> SERVICE_ID = ConfigOptions
		.key("kubernetes.internal.service.id")
		.noDefaultValue()
		.withDescription("The service id will be set in configuration after created. It will be used for gc.");

	public FlinkService(Configuration flinkConfig) {
		super(flinkConfig);
		this.internalResource = new Service();
	}

	public FlinkService(Configuration flinkConfig, Service service) {
		super(flinkConfig);
		this.internalResource = service;
	}
}
