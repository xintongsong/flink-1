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

package org.apache.flink.table.sink.filesystem;

import org.apache.flink.annotation.Internal;
import org.apache.flink.core.fs.Path;

import java.io.Serializable;

import static org.apache.flink.util.Preconditions.checkArgument;

/**
 * FileCommitter describes the commit of task output for file system sink.
 *
 * <p>See {@link FileSystemFileCommitter}.
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
	public abstract void deletePath(Path taskTmpPath) throws Exception;

	/**
	 * For committing job's output after successful batch job completion or one checkpoint finish
	 * for streaming job. Should move all files to final output paths. And should commit all
	 * checkpoint ids that less than current checkpoint id.
	 */
	public abstract void commit(long checkpointId) throws Exception;

	/**
	 * Get path generator for task.
	 */
	final PathGenerator pathGenerator(int taskNumber) {
		return new PathGenerator(taskNumber);
	}

	/**
	 * Path generator to generate new path to write and prepare task temporary directory.
	 */
	public final class PathGenerator {

		private final int taskNumber;

		private transient long checkpointId = -1;
		private transient Path taskTmpDir;
		private transient int nameCounter = 0;

		private PathGenerator(int taskNumber) {
			this.taskNumber = taskNumber;
		}

		/**
		 * Start a transaction, remember the checkpoint id and delete task temporary directory to write.
		 */
		public void startTransaction(long checkpointId) throws Exception {
			checkArgument(checkpointId != -1);
			this.checkpointId = checkpointId;
			this.taskTmpDir = new Path(new Path(temporaryPath, "cp-" + checkpointId), "task-" + taskNumber);
			deletePath(taskTmpDir);
		}

		/**
		 * Generate a new path without partition.
		 */
		public Path generate() throws Exception {
			Path path = new Path(taskTmpDir, newFileName());
			deletePath(path);
			return path;
		}

		/**
		 * Generate a new path with partition path.
		 */
		public Path generate(String partition) throws Exception {
			Path path = new Path(new Path(taskTmpDir, partition), newFileName());
			deletePath(path);
			return path;
		}

		private String newFileName() {
			checkArgument(checkpointId != -1);
			return String.format("cp-%d-task-%d-file-%d", checkpointId, taskNumber, nameCounter++);
		}
	}
}
