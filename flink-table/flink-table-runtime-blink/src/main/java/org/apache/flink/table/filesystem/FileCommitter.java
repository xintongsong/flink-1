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

import org.apache.flink.annotation.Internal;
import org.apache.flink.core.fs.Path;

import java.io.Serializable;

import static org.apache.flink.util.Preconditions.checkArgument;

/**
 * FileCommitter describes the commit of task output for file system sink.
 *
 * <p>See {@link DefaultFileCommitter}.
 */
@Internal
public abstract class FileCommitter implements Serializable {

	private static final long serialVersionUID = 1L;

	protected final Path temporaryPath;
	protected final boolean overwrite;

	public FileCommitter(Path temporaryPath, boolean overwrite) {
		this.temporaryPath = temporaryPath;
		this.overwrite = overwrite;
	}

	/**
	 * Delete path, it is a recursive deletion.
	 */
	protected abstract void deletePath(Path taskTmpPath) throws Exception;

	/**
	 * For committing job's output after successful batch job completion or one checkpoint finish
	 * for streaming job. Should move all files to final output paths. And should commit all
	 * checkpoint ids that less than current checkpoint id.
	 */
	public abstract void commit(long checkpointId) throws Exception;

	/**
	 * Create a new path generator from task and checkpoint id.
	 * And clean the temporary directory for task.
	 */
	final PathGenerator newGeneratorAndCleanDirector(
			int taskNumber, long checkpointId) throws Exception {
		PathGenerator pathGenerator = new PathGenerator(taskNumber, checkpointId);
		deletePath(pathGenerator.taskTmpDir);
		return pathGenerator;
	}

	/**
	 * Path generator to generate new path to write and prepare task temporary directory.
	 */
	public final class PathGenerator {

		private final int taskNumber;
		private final long checkpointId;
		private final Path taskTmpDir;

		private transient int nameCounter = 0;

		private PathGenerator(int taskNumber, long checkpointId) {
			checkArgument(checkpointId != -1, "checkpoint id start with 0.");
			this.taskNumber = taskNumber;
			this.checkpointId = checkpointId;
			this.taskTmpDir = new Path(new Path(temporaryPath, "cp-" + checkpointId), "task-" + taskNumber);
		}

		/**
		 * Generate a new path with directories.
		 */
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
}
