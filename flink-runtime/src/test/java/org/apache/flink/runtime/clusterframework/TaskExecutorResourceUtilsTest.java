package org.apache.flink.runtime.clusterframework;

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

import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.MemorySize;
import org.apache.flink.configuration.TaskManagerOptions;
import org.apache.flink.util.ConfigurationException;
import org.apache.flink.util.TestLogger;

import org.junit.Test;

import java.util.HashMap;
import java.util.Map;
import java.util.function.Consumer;

import static org.hamcrest.Matchers.anyOf;
import static org.hamcrest.Matchers.greaterThanOrEqualTo;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.lessThanOrEqualTo;
import static org.hamcrest.Matchers.startsWith;
import static org.junit.Assert.assertThat;

/**
 * Tests for {@link TaskExecutorResourceUtils}.
 */
public class TaskExecutorResourceUtilsTest extends TestLogger {

	static final MemorySize TASK_HEAP_SIZE = MemorySize.parse("100m");
	static final MemorySize MANAGED_MEM_SIZE = MemorySize.parse("200m");
	static final MemorySize TOTAL_FLINK_MEM_SIZE = MemorySize.parse("800m");
	static final MemorySize TOTAL_PROCESS_MEM_SIZE = MemorySize.parse("1g");

	static final TaskExecutorResourceSpec TM_RESOURCE_SPEC = new TaskExecutorResourceSpec(
		MemorySize.parse("1m"),
		MemorySize.parse("2m"),
		MemorySize.parse("3m"),
		MemorySize.parse("4m"),
		MemorySize.parse("5m"),
		MemorySize.parse("6m"),
		MemorySize.parse("7m"),
		MemorySize.parse("8m"));

	@Test
	public void testGenerateDynamicConfigurations() {
		String dynamicConfigsStr = TaskExecutorResourceUtils.generateDynamicConfigsStr(TM_RESOURCE_SPEC);
		Map<String, String> configs = new HashMap<>();
		for (String configStr : dynamicConfigsStr.split(" ")) {
			assertThat(configStr, startsWith("-D"));
			String[] configKV = configStr.substring(2).split("=");
			assertThat(configKV.length, is(2));
			configs.put(configKV[0], configKV[1]);
		}

		assertThat(MemorySize.parse(configs.get(TaskManagerOptions.FRAMEWORK_HEAP_MEMORY.key())), is(TM_RESOURCE_SPEC.getFrameworkHeapSize()));
		assertThat(MemorySize.parse(configs.get(TaskManagerOptions.TASK_HEAP_MEMORY.key())), is(TM_RESOURCE_SPEC.getTaskHeapSize()));
		assertThat(MemorySize.parse(configs.get(TaskManagerOptions.TASK_OFF_HEAP_MEMORY.key())), is(TM_RESOURCE_SPEC.getTaskOffHeapSize()));
		assertThat(MemorySize.parse(configs.get(TaskManagerOptions.SHUFFLE_MEMORY_MAX.key())), is(TM_RESOURCE_SPEC.getShuffleMemSize()));
		assertThat(MemorySize.parse(configs.get(TaskManagerOptions.SHUFFLE_MEMORY_MIN.key())), is(TM_RESOURCE_SPEC.getShuffleMemSize()));
		assertThat(MemorySize.parse(configs.get(TaskManagerOptions.MANAGED_MEMORY_SIZE.key())), is(TM_RESOURCE_SPEC.getManagedMemorySize()));
		assertThat(MemorySize.parse(configs.get(TaskManagerOptions.MANAGED_MEMORY_OFFHEAP_SIZE.key())), is(TM_RESOURCE_SPEC.getOffHeapManagedMemorySize()));
	}

	@Test
	public void testGenerateJvmParameters() throws Exception {
		String jvmParamsStr = TaskExecutorResourceUtils.generateJvmParametersStr(TM_RESOURCE_SPEC);
		MemorySize heapSizeMax = null;
		MemorySize heapSizeMin = null;
		MemorySize directSize = null;
		MemorySize metaspaceSize = null;
		for (String paramStr : jvmParamsStr.split(" ")) {
			if (paramStr.startsWith("-Xmx")) {
				heapSizeMax = MemorySize.parse(paramStr.substring("-Xmx".length()));
			} else if (paramStr.startsWith("-Xms")) {
				heapSizeMin = MemorySize.parse(paramStr.substring("-Xms".length()));
			} else if (paramStr.startsWith("-XX:MaxDirectMemorySize=")) {
				directSize = MemorySize.parse(paramStr.substring("-XX:MaxDirectMemorySize=".length()));
			} else if (paramStr.startsWith("-XX:MetaspaceSize=")) {
				metaspaceSize = MemorySize.parse(paramStr.substring("-XX:MetaspaceSize=".length()));
			} else {
				throw new Exception("Unknown JVM parameter: " + paramStr);
			}
		}

		assertThat(heapSizeMax, is(TM_RESOURCE_SPEC.getFrameworkHeapSize().add(TM_RESOURCE_SPEC.getTaskHeapSize()).add(TM_RESOURCE_SPEC.getOnHeapManagedMemorySize())));
		assertThat(heapSizeMin, is(heapSizeMax));
		assertThat(directSize, is(TM_RESOURCE_SPEC.getTaskOffHeapSize().add(TM_RESOURCE_SPEC.getShuffleMemSize())));
		assertThat(metaspaceSize, is(TM_RESOURCE_SPEC.getJvmMetaspaceSize()));
	}

	@Test
	public void testConfigFrameworkHeapMemory() throws Exception {
		final MemorySize frameworkHeapSize = MemorySize.parse("100m");

		Configuration conf = new Configuration();
		conf.setString(TaskManagerOptions.FRAMEWORK_HEAP_MEMORY, frameworkHeapSize.getMebiBytes() + "m");

		validateInAllConfigurations(conf, taskExecutorResourceSpec -> assertThat(taskExecutorResourceSpec.getFrameworkHeapSize(), is(frameworkHeapSize)));
	}

	@Test
	public void testConfigTaskHeapMemory() throws Exception {
		final MemorySize taskHeapSize = MemorySize.parse("50m");

		Configuration conf = new Configuration();
		conf.setString(TaskManagerOptions.TASK_HEAP_MEMORY, taskHeapSize.getMebiBytes() + "m");

		// validate only in configuration with explicit task heap and managed memory size,
		// in other cases, heap memory is derived from total flink memory, thus the configured value is not used by design
		validateInConfigWithExplicitTaskHeapAndManagedMem(conf, taskExecutorResourceSpec -> assertThat(taskExecutorResourceSpec.getTaskHeapSize(), is(taskHeapSize)));
	}

	@Test
	public void testConfigTaskOffheapMemory() throws Exception {
		final MemorySize taskOffHeapSize = MemorySize.parse("50m");

		Configuration conf = new Configuration();
		conf.setString(TaskManagerOptions.TASK_OFF_HEAP_MEMORY, taskOffHeapSize.getMebiBytes() + "m");

		validateInAllConfigurations(conf, taskExecutorResourceSpec -> assertThat(taskExecutorResourceSpec.getTaskOffHeapSize(), is(taskOffHeapSize)));
	}

	@Test
	public void testConfigShuffleMemorySizeAbsolute() throws Exception {
		final MemorySize shuffleSize = MemorySize.parse("100m");

		Configuration conf = new Configuration();
		conf.setString(TaskManagerOptions.SHUFFLE_MEMORY_MAX, shuffleSize.getMebiBytes() + "m");
		conf.setString(TaskManagerOptions.SHUFFLE_MEMORY_MIN, shuffleSize.getMebiBytes() + "m");

		validateInAllConfigurations(conf, taskExecutorResourceSpec -> assertThat(taskExecutorResourceSpec.getShuffleMemSize(), is(shuffleSize)));
	}

	@Test
	public void testConfigShuffleMemorySizeFraction() throws Exception {
		final MemorySize shuffleMin = MemorySize.parse("50m");
		final MemorySize shuffleMax = MemorySize.parse("200m");
		final float fraction = 0.2f;

		Configuration conf = new Configuration();
		conf.setString(TaskManagerOptions.SHUFFLE_MEMORY_MAX, shuffleMax.getMebiBytes() + "m");
		conf.setString(TaskManagerOptions.SHUFFLE_MEMORY_MIN, shuffleMin.getMebiBytes() + "m");
		conf.setFloat(TaskManagerOptions.SHUFFLE_MEMORY_FRACTION, fraction);

		validateInAllConfigurations(conf, taskExecutorResourceSpec -> {
			assertThat(taskExecutorResourceSpec.getShuffleMemSize().getBytes(), greaterThanOrEqualTo(shuffleMin.getBytes()));
			assertThat(taskExecutorResourceSpec.getShuffleMemSize().getBytes(), lessThanOrEqualTo(shuffleMax.getBytes()));
			assertThat(taskExecutorResourceSpec.getShuffleMemSize(), anyOf(
				is(shuffleMin),
				is(shuffleMax),
				is(taskExecutorResourceSpec.getTotalFlinkMemorySize().multiply(fraction))
			));
		});
	}

	@Test
	public void testConfigManagedMemorySizeAbsolute() throws Exception {
		final MemorySize managedMemSize = MemorySize.parse("100m");

		Configuration conf = new Configuration();
		conf.setString(TaskManagerOptions.MANAGED_MEMORY_SIZE, managedMemSize.getMebiBytes() + "m");

		validateInAllConfigurations(conf, taskExecutorResourceSpec -> assertThat(taskExecutorResourceSpec.getManagedMemorySize(), is(managedMemSize)));
	}

	@Test
	public void testConfigManagedMemorySizeFraction() throws Exception {
		final float fraction = 0.5f;

		Configuration conf = new Configuration();
		conf.setFloat(TaskManagerOptions.MANAGED_MEMORY_FRACTION, fraction);

		// validate in configurations where managed memory is not explicitly configured
		validateInConfigWithExplicitTotalFlinkMem(conf, taskExecutorResourceSpec ->
			assertThat(taskExecutorResourceSpec.getManagedMemorySize(), is(taskExecutorResourceSpec.getTotalFlinkMemorySize().multiply(fraction))));
		validateInConfigWithExplicitTotalProcessMem(conf, taskExecutorResourceSpec ->
			assertThat(taskExecutorResourceSpec.getManagedMemorySize(), is(taskExecutorResourceSpec.getTotalFlinkMemorySize().multiply(fraction))));
	}

	@Test
	public void testConfigOffHeapManagedMemorySize() throws Exception {
		final float fraction = 0.5f;

		Configuration conf = new Configuration();
		conf.setFloat(TaskManagerOptions.MANAGED_MEMORY_OFFHEAP_FRACTION, fraction);

		validateInAllConfigurations(conf, taskExecutorResourceSpec -> {
			assertThat(taskExecutorResourceSpec.getOffHeapManagedMemorySize(), is(taskExecutorResourceSpec.getManagedMemorySize().multiply(fraction)));
			assertThat(taskExecutorResourceSpec.getOnHeapManagedMemorySize(), is(taskExecutorResourceSpec.getManagedMemorySize().subtract(taskExecutorResourceSpec.getOffHeapManagedMemorySize())));
		});
	}

	@Test
	public void testConfigJvmMetaspaceSize() throws Exception {
		final MemorySize jvmMetaspaceSize = MemorySize.parse("50m");

		Configuration conf = new Configuration();
		conf.setString(TaskManagerOptions.JVM_METASPACE, jvmMetaspaceSize.getMebiBytes() + "m");

		validateInAllConfigurations(conf, taskExecutorResourceSpec -> assertThat(taskExecutorResourceSpec.getJvmMetaspaceSize(), is(jvmMetaspaceSize)));
	}

	@Test
	public void testConfigJvmOverheadSizeAbsolute() throws Exception {
		final MemorySize jvmOverheadSize = MemorySize.parse("50m");

		Configuration conf = new Configuration();
		conf.setString(TaskManagerOptions.JVM_OVERHEAD_MAX, jvmOverheadSize.getMebiBytes() + "m");
		conf.setString(TaskManagerOptions.JVM_OVERHEAD_MIN, jvmOverheadSize.getMebiBytes() + "m");

		validateInAllConfigurations(conf, taskExecutorResourceSpec -> assertThat(taskExecutorResourceSpec.getJvmOverheadSize(), is(jvmOverheadSize)));
	}

	@Test
	public void testConfigJvmOverheadSizeFraction() throws Exception {
		final MemorySize minSize = MemorySize.parse("50m");
		final MemorySize maxSize = MemorySize.parse("200m");
		final float fraction = 0.2f;

		Configuration conf = new Configuration();
		conf.setString(TaskManagerOptions.JVM_OVERHEAD_MAX, maxSize.getMebiBytes() + "m");
		conf.setString(TaskManagerOptions.JVM_OVERHEAD_MIN, minSize.getMebiBytes() + "m");
		conf.setFloat(TaskManagerOptions.JVM_OVERHEAD_FRACTION, fraction);

		validateInAllConfigurations(conf, taskExecutorResourceSpec -> {
			assertThat(taskExecutorResourceSpec.getJvmOverheadSize().getBytes(), greaterThanOrEqualTo(minSize.getBytes()));
			assertThat(taskExecutorResourceSpec.getJvmOverheadSize().getBytes(), lessThanOrEqualTo(maxSize.getBytes()));
			assertThat(taskExecutorResourceSpec.getJvmOverheadSize(), anyOf(
				is(minSize),
				is(maxSize),
				is(taskExecutorResourceSpec.getTotalProcessMemorySize().multiply(fraction))
			));
		});
	}

	@Test
	public void testConfigTotalFlinkMemorySize() throws Exception {
		final MemorySize totalFlinkMemorySize = MemorySize.parse("1g");

		Configuration conf = new Configuration();
		conf.setString(TaskManagerOptions.TOTAL_FLINK_MEMORY, totalFlinkMemorySize.getMebiBytes() + "m");

		TaskExecutorResourceSpec taskExecutorResourceSpec = TaskExecutorResourceUtils.resourceSpecFromConfig(conf);
		assertThat(taskExecutorResourceSpec.getTotalFlinkMemorySize(), is(totalFlinkMemorySize));
	}

	@Test
	public void testConfigTotalProcessMemorySize() throws Exception {
		final MemorySize totalProcessMemorySize = MemorySize.parse("1g");

		Configuration conf = new Configuration();
		conf.setString(TaskManagerOptions.TOTAL_PROCESS_MEMORY, totalProcessMemorySize.getMebiBytes() + "m");

		TaskExecutorResourceSpec taskExecutorResourceSpec = TaskExecutorResourceUtils.resourceSpecFromConfig(conf);
		assertThat(taskExecutorResourceSpec.getTotalProcessMemorySize(), is(totalProcessMemorySize));
	}

	private void validateInAllConfigurations(final Configuration customConfig, Consumer<TaskExecutorResourceSpec> validateFunc)
		throws ConfigurationException {
		validateInConfigWithExplicitTaskHeapAndManagedMem(customConfig, validateFunc);
		validateInConfigWithExplicitTotalFlinkMem(customConfig, validateFunc);
		validateInConfigWithExplicitTotalProcessMem(customConfig, validateFunc);
	}

	private void validateInConfigWithExplicitTaskHeapAndManagedMem(
		final Configuration customConfig, Consumer<TaskExecutorResourceSpec> validateFunc) throws ConfigurationException{
		log.info("Validating in configuration with explicit task heap and managed memory size.");
		final Configuration config = configWithExplicitTaskHeapAndManageMem();
		config.addAll(customConfig);
		validateFunc.accept(TaskExecutorResourceUtils.resourceSpecFromConfig(config));
	}

	private void validateInConfigWithExplicitTotalFlinkMem(
		final Configuration customConfig, Consumer<TaskExecutorResourceSpec> validateFunc) throws ConfigurationException{
		log.info("Validating in configuration with explicit total flink memory size.");
		final Configuration config = configWithExplicitTotalFlinkMem();
		config.addAll(customConfig);
		validateFunc.accept(TaskExecutorResourceUtils.resourceSpecFromConfig(config));
	}

	private void validateInConfigWithExplicitTotalProcessMem(
		final Configuration customConfig, Consumer<TaskExecutorResourceSpec> validateFunc) throws ConfigurationException{
		log.info("Validating in configuration with explicit total process memory size.");
		final Configuration config = configWithExplicitTotalProcessMem();
		config.addAll(customConfig);
		validateFunc.accept(TaskExecutorResourceUtils.resourceSpecFromConfig(config));
	}

	private static Configuration configWithExplicitTaskHeapAndManageMem() {
		final Configuration conf = new Configuration();
		conf.setString(TaskManagerOptions.TASK_HEAP_MEMORY, TASK_HEAP_SIZE.getMebiBytes() + "m");
		conf.setString(TaskManagerOptions.MANAGED_MEMORY_SIZE, MANAGED_MEM_SIZE.getMebiBytes() + "m");
		return conf;
	}

	private static Configuration configWithExplicitTotalFlinkMem() {
		final Configuration conf = new Configuration();
		conf.setString(TaskManagerOptions.TOTAL_FLINK_MEMORY, TOTAL_FLINK_MEM_SIZE.getMebiBytes() + "m");
		return conf;
	}

	private static Configuration configWithExplicitTotalProcessMem() {
		final Configuration conf = new Configuration();
		conf.setString(TaskManagerOptions.TOTAL_PROCESS_MEMORY, TOTAL_PROCESS_MEM_SIZE.getMebiBytes() + "m");
		return conf;
	}
}
