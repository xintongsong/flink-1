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

package org.apache.flink.dist;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.TaskManagerOptions;

import org.junit.Test;

import static org.hamcrest.Matchers.is;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.assertTrue;

/**
 * Tests for BashJavaUtils.
 */
public class BashJavaUtilsTest extends JavaBashTestBase {

	@Test
	public void testGetTmResourceConfigsAndJvmParams() throws Exception {
		String[] command = {"src/test/bin/getTmResourceConfigsAndJvmParamsFromBashJavaUtils.sh"};
		String[] results = executeScript(command).split("\n");

		assertNotNull(results);
		assertThat(results.length, is(2));

		verifyDynamicConfigs(results[0]);
		verifyJvmParams(results[1]);
	}

	private void verifyDynamicConfigs(String dynamicConfigsStr) {
		Configuration conf = new Configuration();
		String[] dynamicConfigs = dynamicConfigsStr.split(" ");
		assertTrue(dynamicConfigs.length % 2 == 0);

		for (int i = 0; i < dynamicConfigs.length; ++i) {
			String configStr = dynamicConfigs[i];
			if (i % 2 == 0) {
				assertThat(configStr, is("-D"));
			} else {
				String[] configKV = configStr.split("=");
				assertThat(configKV.length, is(2));
				conf.setString(configKV[0], configKV[1]);
			}
		}

		assertTrue(conf.contains(TaskManagerOptions.FRAMEWORK_HEAP_MEMORY));
		assertTrue(conf.contains(TaskManagerOptions.TASK_HEAP_MEMORY));
		assertTrue(conf.contains(TaskManagerOptions.TASK_OFF_HEAP_MEMORY));
		assertTrue(conf.contains(TaskManagerOptions.SHUFFLE_MEMORY_MAX));
		assertTrue(conf.contains(TaskManagerOptions.SHUFFLE_MEMORY_MIN));
		assertTrue(conf.contains(TaskManagerOptions.MANAGED_MEMORY_SIZE));
		assertTrue(conf.contains(TaskManagerOptions.MANAGED_MEMORY_OFFHEAP_SIZE));
	}

	private void verifyJvmParams(String jvmParamsStr) {
		boolean xmx = false;
		boolean xms = false;
		boolean maxDirect = false;
		boolean metaspace = false;

		for (String paramStr : jvmParamsStr.split(" ")) {
			if (paramStr.startsWith("-Xmx")) {
				xmx = true;
			} else if (paramStr.startsWith("-Xms")) {
				xms = true;
			} else if (paramStr.startsWith("-XX:MaxDirectMemorySize=")) {
				maxDirect = true;
			} else if (paramStr.startsWith("-XX:MaxMetaspaceSize=")) {
				metaspace = true;
			}
		}

		assertTrue(xmx && xms && maxDirect && metaspace);
	}
}
