/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.core.plugin;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.util.FlinkRuntimeException;
import org.apache.flink.util.Preconditions;

import javax.annotation.Nonnull;

import java.io.IOException;
import java.util.Collection;
import java.util.Collections;

/**
 * Utility functions for the plugin mechanism.
 */
public final class PluginUtils {

	private PluginUtils() {
		throw new AssertionError("Singleton class.");
	}

	public static PluginManager createPluginManagerFromRootFolder(Configuration configuration) {
		return createPluginManagerFromRootFolder(PluginConfig.fromConfiguration(configuration));
	}

	private static PluginManager createPluginManagerFromRootFolder(PluginConfig pluginConfig) {
		if (pluginConfig.getPluginsPath().isPresent()) {
			try {
				Collection<PluginDescriptor> pluginDescriptors =
					new DirectoryBasedPluginFinder(pluginConfig.getPluginsPath().get()).findPlugins();
				return new PluginManager(pluginDescriptors, pluginConfig.getAlwaysParentFirstPatterns());
			} catch (IOException e) {
				throw new FlinkRuntimeException("Exception when trying to initialize plugin system.", e);
			}
		}
		else {
			return new PluginManager(Collections.emptyList(), pluginConfig.getAlwaysParentFirstPatterns());
		}
	}

	/**
	 * Check class isolation of different plugins. Each plugin classes should have only one object in the collection.
	 * @param objectInDifferentPlugins object collection of different plugins.
	 */
	public static void checkClassIsolationInDifferentPlugins(@Nonnull Collection<Object> objectInDifferentPlugins) {
		if (objectInDifferentPlugins.size() < 2) {
			return;
		}
		objectInDifferentPlugins.forEach(
			e -> objectInDifferentPlugins.forEach(another -> checkVisibility(e, another))
		);
	}

	private static void checkVisibility(Object objectInPlugin, Object objectInAnotherPlugin) {
		if (objectInPlugin.getClass().getCanonicalName().equals(objectInAnotherPlugin.getClass().getCanonicalName())) {
			return;
		}

		boolean visible = true;
		try {
			objectInPlugin.getClass().getClassLoader().loadClass(objectInAnotherPlugin.getClass().getCanonicalName());
		} catch (ClassNotFoundException e) {
			visible = false;
		}
		Preconditions.checkState(!visible, "%s should not be visible for class %s in another plugin.",
			objectInAnotherPlugin.getClass().getCanonicalName(),
			objectInPlugin.getClass().getCanonicalName());
	}
}
