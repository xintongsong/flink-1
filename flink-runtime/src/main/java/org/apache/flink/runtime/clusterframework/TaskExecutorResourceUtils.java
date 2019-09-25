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

package org.apache.flink.runtime.clusterframework;

import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple6;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.MemorySize;
import org.apache.flink.configuration.TaskManagerOptions;
import org.apache.flink.util.ConfigurationException;

import java.util.HashMap;
import java.util.Map;

import static org.apache.flink.util.Preconditions.checkArgument;

/**
 * Utility class for TaskExecutor memory configurations.
 */
public class TaskExecutorResourceUtils {

	private TaskExecutorResourceUtils() {}

	// ------------------------------------------------------------------------
	//  Generating JVM Parameters
	// ------------------------------------------------------------------------

	public static String generateJvmParametersStr(final TaskExecutorResourceSpec taskExecutorResourceSpec) {
		final MemorySize jvmHeapSize = taskExecutorResourceSpec.getFrameworkHeapSize()
			.add(taskExecutorResourceSpec.getTaskHeapSize())
			.add(taskExecutorResourceSpec.getOnHeapManagedMemorySize());
		final MemorySize jvmDirectSize = taskExecutorResourceSpec.getTaskOffHeapSize()
			.add(taskExecutorResourceSpec.getShuffleMemSize());
		final MemorySize jvmMetaspaceSize = taskExecutorResourceSpec.getJvmMetaspaceSize();

		return "-Xmx" + jvmHeapSize.getBytes()
			+ " -Xms" + jvmHeapSize.getBytes()
			+ " -XX:MaxDirectMemorySize=" + jvmDirectSize.getBytes()
			+ " -XX:MetaspaceSize=" + jvmMetaspaceSize.getBytes();
	}

	// ------------------------------------------------------------------------
	//  Generating Dynamic Config Options
	// ------------------------------------------------------------------------

	public static String generateDynamicConfigsStr(final TaskExecutorResourceSpec taskExecutorResourceSpec) {
		final Map<String, String> configs = new HashMap<>();
		configs.put(TaskManagerOptions.FRAMEWORK_HEAP_MEMORY.key(), taskExecutorResourceSpec.getFrameworkHeapSize().getBytes() + "b");
		configs.put(TaskManagerOptions.TASK_HEAP_MEMORY.key(), taskExecutorResourceSpec.getTaskHeapSize().getBytes() + "b");
		configs.put(TaskManagerOptions.TASK_OFF_HEAP_MEMORY.key(), taskExecutorResourceSpec.getTaskOffHeapSize().getBytes() + "b");
		configs.put(TaskManagerOptions.SHUFFLE_MEMORY_MIN.key(), taskExecutorResourceSpec.getShuffleMemSize().getBytes() + "b");
		configs.put(TaskManagerOptions.SHUFFLE_MEMORY_MAX.key(), taskExecutorResourceSpec.getShuffleMemSize().getBytes() + "b");
		configs.put(TaskManagerOptions.MANAGED_MEMORY_SIZE.key(), taskExecutorResourceSpec.getManagedMemorySize().getBytes() + "b");
		configs.put(TaskManagerOptions.MANAGED_MEMORY_OFFHEAP_SIZE.key(), taskExecutorResourceSpec.getOffHeapManagedMemorySize().getBytes() + "b");
		return assembleDynamicConfigsStr(configs);
	}

	private static String assembleDynamicConfigsStr(final Map<String, String> configs) {
		final StringBuffer sb = new StringBuffer();
		for (Map.Entry<String, String> entry : configs.entrySet()) {
			sb.append("-D").append(entry.getKey()).append("=").append(entry.getValue()).append(" ");
		}
		return sb.toString();
	}

	// ------------------------------------------------------------------------
	//  Memory Configuration Calculations
	// ------------------------------------------------------------------------

	public static TaskExecutorResourceSpec resourceSpecFromConfig(final Configuration config) throws ConfigurationException {
		if (isTaskHeapMemorySizeExplicitlyConfigured(config) && isManagedMemorySizeExplicitlyConfigured(config)) {
			// both task heap memory and managed memory is configured, use these to derive total flink memory
			return deriveResourceSpecWithExplicitTaskAndManagedMemory(config);
		} else if (isTotalFlinkMemorySizeExplicitlyConfigured(config)) {
			// total flink memory is configured, but not task heap and managed memory, derive from total flink memory
			return deriveResourceSpecWithTotalFlinkMemory(config);
		} else if (isTotalProcessMemorySizeExplicitlyConfigured(config)) {
			return deriveResourceSpecWithTotalProcessMemory(config);
		} else {
			throw new ConfigurationException("Either Task Heap Memory size and Managed Memory size, or Total Flink"
				+ " Memory size, or Total Process Memory size need to be configured explicitly.");
		}
	}

	private static TaskExecutorResourceSpec deriveResourceSpecWithExplicitTaskAndManagedMemory(final Configuration config) {
		// derive total flink internal memory sizes from explicitly configure task heap memory size and managed memory size

		final MemorySize frameworkHeapMemorySize = getFrameworkHeapMemorySize(config);
		final MemorySize taskHeapMemorySize = getTaskHeapMemorySize(config);
		final MemorySize taskOffHeapMemorySize = getTaskOffHeapMemorySize(config);

		final MemorySize managedMemorySize = getManagedMemorySize(config);
		final Tuple2<MemorySize, MemorySize> managedMemorySizeTuple2 = deriveOnHeapAndOffHeapManagedMemorySizeFromManagedMemorySize(config, managedMemorySize);
		final MemorySize onHeapManagedMemorySize = managedMemorySizeTuple2.f0;
		final MemorySize offHeapManagedMemorySize = managedMemorySizeTuple2.f1;

		final MemorySize shuffleMemorySize = deriveShuffleMemoryWithInverseFraction(config,
			frameworkHeapMemorySize.add(taskHeapMemorySize).add(taskOffHeapMemorySize).add(managedMemorySize));

		// derive total flink external memory sizes from derived total flink memory size

		final MemorySize totalFlinkMemorySize = frameworkHeapMemorySize
			.add(taskHeapMemorySize)
			.add(taskOffHeapMemorySize)
			.add(shuffleMemorySize)
			.add(managedMemorySize);

		final Tuple2<MemorySize, MemorySize> totalFlinkExternalMemorySizeTuple2 = deriveTotalFlinkExternalMemorySizes(config, totalFlinkMemorySize);

		return new TaskExecutorResourceSpec(
			frameworkHeapMemorySize,
			taskHeapMemorySize,
			taskOffHeapMemorySize,
			shuffleMemorySize,
			onHeapManagedMemorySize,
			offHeapManagedMemorySize,
			totalFlinkExternalMemorySizeTuple2.f0, // jvmMetaspaceSize
			totalFlinkExternalMemorySizeTuple2.f1); // jvmOverheadSize
	}

	private static TaskExecutorResourceSpec deriveResourceSpecWithTotalFlinkMemory(final Configuration config) {
		// derive total flink internal memory sizes from explicitly configured total flink memory size

		final MemorySize totalFlinkMemorySize = getTotalFlinkMemorySize(config);
		final Tuple6<MemorySize, MemorySize, MemorySize, MemorySize, MemorySize, MemorySize> totalFLinkInternalMemorySizeTuple6 =
			deriveTotalFlinkInternalMemorySizes(config, totalFlinkMemorySize);

		// derive total flink external memory sizes from explicitly configured total flink memory size
		final Tuple2<MemorySize, MemorySize> totalFlinkExternalMemorySizeTuple2 =
			deriveTotalFlinkExternalMemorySizes(config, totalFlinkMemorySize);

		return new TaskExecutorResourceSpec(
			totalFLinkInternalMemorySizeTuple6.f0, // frameworkHeapMemorySize
			totalFLinkInternalMemorySizeTuple6.f1, // taskHeapMemorySize
			totalFLinkInternalMemorySizeTuple6.f2, // taskOffHeapMemorySize
			totalFLinkInternalMemorySizeTuple6.f3, // shuffleMemorySize
			totalFLinkInternalMemorySizeTuple6.f4, // onHeapManagedMemorySize
			totalFLinkInternalMemorySizeTuple6.f5, // offHeapManagedMemorySize
			totalFlinkExternalMemorySizeTuple2.f0, // jvmMetaspaceSize
			totalFlinkExternalMemorySizeTuple2.f1); // jvmOverheadSize
	}

	private static TaskExecutorResourceSpec deriveResourceSpecWithTotalProcessMemory(final Configuration config) {
		// derive total flink external memory sizes from explicitly configured total process memory size

		final MemorySize totalProcessMemorySize = getTotalProcessMemorySize(config);
		final MemorySize jvmMetaspaceSize = getJvmMetaspaceSize(config);
		final MemorySize jvmOverheadSize = deriveJvmOverheadWithFraction(config, totalProcessMemorySize);

		final MemorySize totalFlinkExternalMemorySize = jvmMetaspaceSize.add(jvmOverheadSize);
		checkArgument(totalFlinkExternalMemorySize.getBytes() < totalProcessMemorySize.getBytes(),
			"Sum of configured JVM Metaspace (" + jvmMetaspaceSize.toString()
				+ ") and JVM Overhead (" + jvmOverheadSize.toString()
				+ ") exceed configured Total Process memory (" + totalProcessMemorySize.toString() + ").");
		final MemorySize totalFlinkMemorySize = totalProcessMemorySize.subtract(totalFlinkExternalMemorySize);

		// derive total flink internal memory sizes from derived total flink memory size
		final Tuple6<MemorySize, MemorySize, MemorySize, MemorySize, MemorySize, MemorySize> totalFLinkInternalMemorySizeTuple6 =
			deriveTotalFlinkInternalMemorySizes(config, totalFlinkMemorySize);

		return new TaskExecutorResourceSpec(
			totalFLinkInternalMemorySizeTuple6.f0, // frameworkHeapMemorySize
			totalFLinkInternalMemorySizeTuple6.f1, // taskHeapMemorySize
			totalFLinkInternalMemorySizeTuple6.f2, // taskOffHeapMemorySize
			totalFLinkInternalMemorySizeTuple6.f3, // shuffleMemorySize
			totalFLinkInternalMemorySizeTuple6.f4, // onHeapManagedMemorySize
			totalFLinkInternalMemorySizeTuple6.f5, // offHeapManagedMemorySize
			jvmMetaspaceSize,
			jvmOverheadSize);
	}

	private static Tuple2<MemorySize, MemorySize> deriveTotalFlinkExternalMemorySizes(
		final Configuration config, final MemorySize totalFlinkMemorySize) {
		final MemorySize jvmMetaspaceSize = getJvmMetaspaceSize(config);
		final MemorySize jvmOverheadSize = deriveJvmOverheadWithInverseFraction(config,
			totalFlinkMemorySize.add(jvmMetaspaceSize));
		return new Tuple2<>(jvmMetaspaceSize, jvmOverheadSize);
	}

	private static Tuple6<MemorySize, MemorySize, MemorySize, MemorySize, MemorySize, MemorySize> deriveTotalFlinkInternalMemorySizes(
		final Configuration config, final MemorySize totalFlinkMemorySize) {
		final MemorySize frameworkHeapMemorySize = getFrameworkHeapMemorySize(config);
		final MemorySize taskOffHeapMemorySize = getTaskOffHeapMemorySize(config);

		final MemorySize shuffleMemorySize = deriveShuffleMemoryWithFraction(config, totalFlinkMemorySize);
		final MemorySize managedMemorySize = deriveManagedMemoryAbsoluteOrWithFraction(config, totalFlinkMemorySize);

		final MemorySize totalFlinkExceptTaskHeapMemorySize =
			frameworkHeapMemorySize.add(taskOffHeapMemorySize).add(shuffleMemorySize).add(managedMemorySize);
		checkArgument(totalFlinkExceptTaskHeapMemorySize.getBytes() < totalFlinkMemorySize.getBytes(),
			"Sum of configured Framework Heap Memory (" + frameworkHeapMemorySize.toString()
				+ "), Task Off-Heap Memory (" + taskOffHeapMemorySize.toString()
				+ "), Shuffle Memory (" + shuffleMemorySize.toString()
				+ ") and Managed Memory (" + managedMemorySize.toString()
				+ ") exceed configured Total Flink Memory (" + totalFlinkMemorySize.toString() + ").");
		final MemorySize taskHeapMemorySize = totalFlinkMemorySize.subtract(totalFlinkExceptTaskHeapMemorySize);

		final Tuple2<MemorySize, MemorySize> tuple2 = deriveOnHeapAndOffHeapManagedMemorySizeFromManagedMemorySize(config, managedMemorySize);
		final MemorySize onHeapManagedMemorySize = tuple2.f0;
		final MemorySize offHeapManagedMemorySize = tuple2.f1;

		return new Tuple6<>(
			frameworkHeapMemorySize,
			taskHeapMemorySize,
			taskOffHeapMemorySize,
			shuffleMemorySize,
			onHeapManagedMemorySize,
			offHeapManagedMemorySize);
	}

	private static Tuple2<MemorySize, MemorySize> deriveOnHeapAndOffHeapManagedMemorySizeFromManagedMemorySize(
		final Configuration config, final MemorySize managedMemorySize) {

		if (isManagedMemoryOffHeapSizeExplicitlyConfigured(config)) {
			// on-heap and off-heap managed memory size is already derived, use the values in configs directly
			final MemorySize offheapSize = getManagedMemoryOffHeapSize(config);
			final MemorySize onheapSize = managedMemorySize.subtract(offheapSize);
			return new Tuple2<>(onheapSize, offheapSize);
		}

		double offheapFraction;
		if (isManagedMemoryOffHeapFractionExplicitlyConfigured(config)) {
			offheapFraction = getManagedMemoryOffHeapFraction(config);
		} else {
			final boolean legacyManagedMemoryOffHeap = config.getBoolean(TaskManagerOptions.MEMORY_OFF_HEAP);
			offheapFraction = legacyManagedMemoryOffHeap ? 1.0 : 0.0;
		}

		final MemorySize offheapSize = managedMemorySize.multiply(offheapFraction);
		final MemorySize onheapSize = managedMemorySize.subtract(offheapSize);
		return new Tuple2<>(onheapSize, offheapSize);
	}

	private static MemorySize deriveManagedMemoryAbsoluteOrWithFraction(final Configuration config, final MemorySize base) {
		if (isManagedMemorySizeExplicitlyConfigured(config)) {
			return getManagedMemorySize(config);
		} else {
			return deriveWithFraction(base, getManagedMemoryRangeFraction(config));
		}
	}

	private static MemorySize deriveShuffleMemoryWithFraction(final Configuration config, final MemorySize base) {
		return deriveWithFraction(base, getShuffleMemoryRangeFraction(config));
	}

	private static MemorySize deriveShuffleMemoryWithInverseFraction(final Configuration config, final MemorySize base) {
		return deriveWithInverseFraction(base, getShuffleMemoryRangeFraction(config));
	}

	private static MemorySize deriveJvmOverheadWithFraction(final Configuration config, final MemorySize base) {
		return deriveWithFraction(base, getJvmOverheadRangeFraction(config));
	}

	private static MemorySize deriveJvmOverheadWithInverseFraction(final Configuration config, final MemorySize base) {
		return deriveWithInverseFraction(base, getJvmOverheadRangeFraction(config));
	}

	private static MemorySize deriveWithFraction(final MemorySize base, final RangeFraction rangeFraction) {
		final long relative = (long) (rangeFraction.fraction * base.getBytes());
		return new MemorySize(Math.max(rangeFraction.minSize.getBytes(), Math.min(rangeFraction.maxSize.getBytes(), relative)));
	}

	private static MemorySize deriveWithInverseFraction(final MemorySize base, final RangeFraction rangeFraction) {
		checkArgument(rangeFraction.fraction < 1);
		final long relative = (long) (rangeFraction.fraction / (1 - rangeFraction.fraction) * base.getBytes());
		return new MemorySize(Math.max(rangeFraction.minSize.getBytes(), Math.min(rangeFraction.maxSize.getBytes(), relative)));
	}

	private static MemorySize getFrameworkHeapMemorySize(final Configuration config) {
		return MemorySize.parse(config.getString(TaskManagerOptions.FRAMEWORK_HEAP_MEMORY));
	}

	private static MemorySize getTaskHeapMemorySize(final Configuration config) {
		checkArgument(isTaskHeapMemorySizeExplicitlyConfigured(config));
		return MemorySize.parse(config.getString(TaskManagerOptions.TASK_HEAP_MEMORY));
	}

	private static MemorySize getTaskOffHeapMemorySize(final Configuration config) {
		return MemorySize.parse(config.getString(TaskManagerOptions.TASK_OFF_HEAP_MEMORY));
	}

	private static MemorySize getManagedMemorySize(final Configuration config) {
		checkArgument(isManagedMemorySizeExplicitlyConfigured(config));
		return MemorySize.parse(config.getString(TaskManagerOptions.MANAGED_MEMORY_SIZE));
	}

	private static RangeFraction getManagedMemoryRangeFraction(final Configuration config) {
		final MemorySize minSize = new MemorySize(0);
		final MemorySize maxSize = new MemorySize(Long.MAX_VALUE);
		final double fraction = config.getFloat(TaskManagerOptions.MANAGED_MEMORY_FRACTION);
		return new RangeFraction(minSize, maxSize, fraction);
	}

	private static double getManagedMemoryOffHeapFraction(final Configuration config) {
		final double offheapFraction = config.getFloat(TaskManagerOptions.MANAGED_MEMORY_OFFHEAP_FRACTION);
		checkArgument(offheapFraction >= 0);
		return offheapFraction;
	}

	private static MemorySize getManagedMemoryOffHeapSize(final Configuration config) {
		checkArgument(isManagedMemoryOffHeapSizeExplicitlyConfigured(config));
		return MemorySize.parse(config.getString(TaskManagerOptions.MANAGED_MEMORY_OFFHEAP_SIZE));
	}

	private static RangeFraction getShuffleMemoryRangeFraction(final Configuration config) {
		final MemorySize minSize = MemorySize.parse(config.getString(TaskManagerOptions.SHUFFLE_MEMORY_MIN));
		final MemorySize maxSize = MemorySize.parse(config.getString(TaskManagerOptions.SHUFFLE_MEMORY_MAX));
		final double fraction = config.getFloat(TaskManagerOptions.SHUFFLE_MEMORY_FRACTION);
		return new RangeFraction(minSize, maxSize, fraction);
	}

	private static MemorySize getJvmMetaspaceSize(final Configuration config) {
		return MemorySize.parse(config.getString(TaskManagerOptions.JVM_METASPACE));
	}

	private static RangeFraction getJvmOverheadRangeFraction(final Configuration config) {
		final MemorySize minSize = MemorySize.parse(config.getString(TaskManagerOptions.JVM_OVERHEAD_MIN));
		final MemorySize maxSize = MemorySize.parse(config.getString(TaskManagerOptions.JVM_OVERHEAD_MAX));
		final double fraction = config.getFloat(TaskManagerOptions.JVM_OVERHEAD_FRACTION);
		return new RangeFraction(minSize, maxSize, fraction);
	}

	private static MemorySize getTotalFlinkMemorySize(final Configuration config) {
		checkArgument(isTotalFlinkMemorySizeExplicitlyConfigured(config));
		if (config.contains(TaskManagerOptions.TOTAL_FLINK_MEMORY)) {
			return MemorySize.parse(config.getString(TaskManagerOptions.TOTAL_FLINK_MEMORY));
		} else {
			final long legacyHeapMemoryMB = config.getInteger(TaskManagerOptions.TASK_MANAGER_HEAP_MEMORY_MB);
			return new MemorySize(legacyHeapMemoryMB << 20); // megabytes to bytes
		}
	}

	private static MemorySize getTotalProcessMemorySize(final Configuration config) {
		checkArgument(isTotalProcessMemorySizeExplicitlyConfigured(config));
		return MemorySize.parse(config.getString(TaskManagerOptions.TOTAL_PROCESS_MEMORY));
	}

	private static boolean isTaskHeapMemorySizeExplicitlyConfigured(final Configuration config) {
		return config.contains(TaskManagerOptions.TASK_HEAP_MEMORY);
	}

	private static boolean isManagedMemorySizeExplicitlyConfigured(final Configuration config) {
		return config.contains(TaskManagerOptions.MANAGED_MEMORY_SIZE);
	}

	private static boolean isManagedMemoryOffHeapFractionExplicitlyConfigured(final Configuration config) {
		return config.getFloat(TaskManagerOptions.MANAGED_MEMORY_OFFHEAP_FRACTION) >= 0;
	}

	private static boolean isManagedMemoryOffHeapSizeExplicitlyConfigured(final Configuration config) {
		return config.contains(TaskManagerOptions.MANAGED_MEMORY_OFFHEAP_SIZE);
	}

	private static boolean isTotalFlinkMemorySizeExplicitlyConfigured(final Configuration config) {
		// backward compatible with the deprecated config option TASK_MANAGER_HEAP_MEMORY_MB only when it's explicitly
		// configured by the user
		return config.contains(TaskManagerOptions.TOTAL_FLINK_MEMORY)
			|| config.contains(TaskManagerOptions.TASK_MANAGER_HEAP_MEMORY_MB);
	}

	private static boolean isTotalProcessMemorySizeExplicitlyConfigured(final Configuration config) {
		return config.contains(TaskManagerOptions.TOTAL_PROCESS_MEMORY);
	}

	private static class RangeFraction {
		final MemorySize minSize;
		final MemorySize maxSize;
		final double fraction;

		RangeFraction(final MemorySize minSize, final MemorySize maxSize, final double fraction) {
			this.minSize = minSize;
			this.maxSize = maxSize;
			this.fraction = fraction;
			checkArgument(minSize.getBytes() <= maxSize.getBytes());
			checkArgument(fraction >= 0 && fraction <= 1);
		}
	}
}
