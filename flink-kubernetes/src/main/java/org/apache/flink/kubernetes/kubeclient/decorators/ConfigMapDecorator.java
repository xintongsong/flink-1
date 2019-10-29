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
import org.apache.flink.configuration.ConfigConstants;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.kubernetes.kubeclient.resources.FlinkConfigMap;
import org.apache.flink.kubernetes.utils.KubernetesUtils;
import org.apache.flink.util.Preconditions;

import io.fabric8.kubernetes.api.model.ConfigMap;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.util.HashMap;
import java.util.Map;

import static org.apache.flink.configuration.GlobalConfiguration.FLINK_CONF_FILENAME;
import static org.apache.flink.kubernetes.utils.Constants.CONFIG_FILE_LOG4J_NAME;
import static org.apache.flink.kubernetes.utils.Constants.CONFIG_FILE_LOGBACK_NAME;

/**
 * Decorate config map for flink configuration and log files.
 */
public class ConfigMapDecorator extends Decorator<ConfigMap, FlinkConfigMap> {

	private static final Logger LOG = LoggerFactory.getLogger(ConfigMapDecorator.class);

	@Override
	protected ConfigMap doDecorate(ConfigMap resource, Configuration flinkConfig) {

		String confDir = CliFrontend.getConfigurationDirectoryFromEnv();
		Preconditions.checkArgument(confDir != null);

		StringBuilder flinkConfContent = new StringBuilder();
		Map<String, String> flinkConfigMap = new HashMap<>(flinkConfig.toMap());
		// Remove some keys should not be taken to jobmanager and taskmanager.
		flinkConfigMap.remove(ConfigConstants.ENV_FLINK_PLUGINS_DIR);
		flinkConfigMap.forEach((k, v) ->
			flinkConfContent.append(k).append(": ").append(v).append(System.lineSeparator()));
		Map<String, String> configMap = new HashMap<>();
		configMap.put(FLINK_CONF_FILENAME, flinkConfContent.toString());

		String log4jFile = confDir + File.separator + CONFIG_FILE_LOG4J_NAME;
		String log4jContent = KubernetesUtils.getContentFromFile(log4jFile);
		if (log4jContent != null) {
			configMap.put(CONFIG_FILE_LOG4J_NAME, log4jContent);
		} else {
			LOG.info("File {} not exist, will not add to configMap", log4jFile);
		}

		String logbackFile = confDir + File.separator + CONFIG_FILE_LOGBACK_NAME;
		String logbackContent = KubernetesUtils.getContentFromFile(logbackFile);
		if (logbackContent != null) {
			configMap.put(CONFIG_FILE_LOGBACK_NAME, logbackContent);
		} else {
			LOG.info("File {} not exist, will not add to configMap", logbackFile);
		}
		resource.setData(configMap);
		return resource;
	}
}
