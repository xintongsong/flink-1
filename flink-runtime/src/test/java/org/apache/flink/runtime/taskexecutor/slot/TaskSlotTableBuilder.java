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

package org.apache.flink.runtime.taskexecutor.slot;

import org.apache.flink.api.common.time.Time;
import org.apache.flink.runtime.clusterframework.types.AllocationID;
import org.apache.flink.runtime.clusterframework.types.ResourceProfile;
import org.apache.flink.runtime.memory.MemoryManager;
import org.apache.flink.runtime.taskexecutor.TaskManagerServices;
import org.apache.flink.runtime.testingUtils.TestingUtils;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

/** Builder for {@link TaskSlotTable}. */
public class TaskSlotTableBuilder {
	private static final long DEFAULT_SLOT_TIMEOUT = 10000L;

	private static final ResourceProfile DEFAULT_RESOURCE_PROFILE =
		TaskManagerServices.computeSlotResourceProfile(1, 10 * MemoryManager.MIN_PAGE_SIZE);

	private List<ResourceProfile> taskSlots;
	private TimerService<AllocationID> timerService;

	private TaskSlotTableBuilder setTaskSlots(List<ResourceProfile> taskSlots) {
		this.taskSlots = new ArrayList<>(taskSlots);
		return this;
	}

	public TaskSlotTableBuilder setTimerService(TimerService<AllocationID> timerService) {
		this.timerService = timerService;
		return this;
	}

	public TaskSlotTableBuilder withTimerServiceTimeout(Time timeout) {
		this.timerService = new TimerService<>(TestingUtils.defaultExecutor(), timeout.toMilliseconds());
		return this;
	}

	public TaskSlotTable build() {
		timerService = timerService == null ? createDefaultTimerService() : timerService;
		return new TaskSlotTable(taskSlots, timerService);
	}

	private static TimerService<AllocationID> createDefaultTimerService() {
		return new TimerService<>(TestingUtils.defaultExecutor(), DEFAULT_SLOT_TIMEOUT);
	}

	public static TaskSlotTableBuilder newBuilder() {
		return newBuilderWithDefaultSlots(1);
	}

	public static TaskSlotTableBuilder newBuilderWithDefaultSlots(int numberOfDefaultSlots) {
		return new TaskSlotTableBuilder().setTaskSlots(createDefaultSlotProfiles(numberOfDefaultSlots));
	}

	public static List<ResourceProfile> createDefaultSlotProfiles(int numberOfDefaultSlots) {
		return Collections.nCopies(numberOfDefaultSlots, DEFAULT_RESOURCE_PROFILE);
	}
}
