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
import org.apache.flink.kubernetes.kubeclient.resources.Resource;

/**
 * Abstract decorator for add features to resource such as deployment/pod/service.
 */
public abstract class Decorator<R, T extends Resource<R>> {

	protected Boolean isEnabled(Configuration flinkConfig) {
		return true;
	}

	/**
	 * do decorate the real resource.
	 */
	protected abstract R doDecorate(R resource, Configuration flinkConfig);

	/**
	 * extract real resource from resource, decorate and put it back.
	 */
	public T decorate(T resource) {

		//skip if it was disabled
		if (!this.isEnabled(resource.getFlinkConfig())) {
			return resource;
		}

		R realResource = resource.getInternalResource();
		realResource = doDecorate(realResource, resource.getFlinkConfig());
		resource.setInternalResource(realResource);

		return resource;
	}
}
