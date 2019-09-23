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

import org.apache.flink.configuration.MemorySize;

/**
 * Describe the specifics of different resource dimensions of the TaskExecutor.
 */
public class TaskExecutorResourceSpec {

	private MemorySize frameworkHeapSize;

	private MemorySize taskHeapSize;

	private MemorySize taskOffHeapSize;

	private MemorySize shuffleMemSize;

	private MemorySize onHeapManagedMemorySize;

	private MemorySize offHeapManagedMemorySize;

	private MemorySize jvmMetaspaceSize;

	private MemorySize jvmOverheadSize;

	public TaskExecutorResourceSpec(
		MemorySize frameworkHeapSize,
		MemorySize taskHeapSize,
		MemorySize taskOffHeapSize,
		MemorySize shuffleMemSize,
		MemorySize onHeapManagedMemorySize,
		MemorySize offHeapManagedMemorySize,
		MemorySize jvmMetaspaceSize,
		MemorySize jvmOverheadSize) {

		this.frameworkHeapSize = frameworkHeapSize;
		this.taskHeapSize = taskHeapSize;
		this.taskOffHeapSize = taskOffHeapSize;
		this.shuffleMemSize = shuffleMemSize;
		this.onHeapManagedMemorySize = onHeapManagedMemorySize;
		this.offHeapManagedMemorySize = offHeapManagedMemorySize;
		this.jvmMetaspaceSize = jvmMetaspaceSize;
		this.jvmOverheadSize = jvmOverheadSize;
	}

	public MemorySize getFrameworkHeapSize() {
		return frameworkHeapSize;
	}

	public MemorySize getTaskHeapSize() {
		return taskHeapSize;
	}

	public MemorySize getTaskOffHeapSize() {
		return taskOffHeapSize;
	}

	public MemorySize getShuffleMemSize() {
		return shuffleMemSize;
	}

	public MemorySize getOnHeapManagedMemorySize() {
		return onHeapManagedMemorySize;
	}

	public MemorySize getOffHeapManagedMemorySize() {
		return offHeapManagedMemorySize;
	}

	public MemorySize getManagedMemorySize() {
		return onHeapManagedMemorySize.add(offHeapManagedMemorySize);
	}

	public MemorySize getJvmMetaspaceSize() {
		return jvmMetaspaceSize;
	}

	public MemorySize getJvmOverheadSize() {
		return jvmOverheadSize;
	}

	public MemorySize getTotalFlinkMemorySize() {
		return frameworkHeapSize.add(taskHeapSize).add(taskOffHeapSize).add(shuffleMemSize).add(getManagedMemorySize());
	}

	public MemorySize getTotalProcessMemorySize() {
		return getTotalFlinkMemorySize().add(jvmMetaspaceSize).add(jvmOverheadSize);
	}
}
