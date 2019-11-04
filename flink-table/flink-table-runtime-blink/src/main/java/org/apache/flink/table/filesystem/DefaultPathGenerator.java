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

package org.apache.flink.table.filesystem;

import org.apache.flink.core.fs.Path;

import static org.apache.flink.util.Preconditions.checkArgument;

/**
 * Default Path generator.
 */
public class DefaultPathGenerator implements PathGenerator {

	private final int taskNumber;
	private final long checkpointId;
	private final Path taskTmpDir;

	private transient int nameCounter = 0;

	public DefaultPathGenerator(Path temporaryPath, int taskNumber, long checkpointId) {
		checkArgument(checkpointId != -1, "checkpoint id start with 0.");
		this.taskNumber = taskNumber;
		this.checkpointId = checkpointId;
		this.taskTmpDir = new Path(new Path(temporaryPath, "cp-" + checkpointId), "task-" + taskNumber);
	}

	@Override
	public Path getTaskTemporaryPath() {
		return taskTmpDir;
	}

	@Override
	public Path generate(String... directories) throws Exception {
		Path parentPath = taskTmpDir;
		for (String dir : directories) {
			parentPath = new Path(parentPath, dir);
		}
		return new Path(parentPath, newFileName());
	}

	private String newFileName() {
		return String.format("cp-%d-task-%d-file-%d", checkpointId, taskNumber, nameCounter++);
	}
}
