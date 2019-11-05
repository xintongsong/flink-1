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
import org.apache.flink.core.fs.FileStatus;
import org.apache.flink.core.fs.FileSystem;
import org.apache.flink.core.fs.Path;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import static org.apache.flink.util.Preconditions.checkArgument;

/**
 * Manage temporary files for writing files. Use special rules to organize directories
 * for temporary files.
 *
 * <p>Temporary file directory contains the following directory parts:
 *  1.temporary base path directory.
 *  2.checkpoint id directory.
 *  3.task id directory.
 *  4.directories to specify partitioning.
 *  5.data files.
 *  eg: /tmp/cp-1/task-0/p0=1/p1=2/fileName.
 */
@Internal
public class TempFileManager {

	private static final String CHECKPOINT_DIR_PREFIX = "cp-";
	private static final String TASK_DIR_PREFIX = "task-";

	private final int taskNumber;
	private final long checkpointId;
	private final Path taskTmpDir;

	private transient int nameCounter = 0;

	public TempFileManager(Path temporaryPath, int taskNumber, long checkpointId) {
		checkArgument(checkpointId != -1, "checkpoint id start with 0.");
		this.taskNumber = taskNumber;
		this.checkpointId = checkpointId;
		this.taskTmpDir = new Path(
				new Path(temporaryPath, checkpointName(checkpointId)),
				TASK_DIR_PREFIX + taskNumber);
	}

	public Path getTaskTemporaryPath() {
		return taskTmpDir;
	}

	/**
	 * Generate a new path with directories.
	 */
	public Path generateTempFile(String... directories) throws Exception {
		Path parentPath = taskTmpDir;
		for (String dir : directories) {
			parentPath = new Path(parentPath, dir);
		}
		return new Path(parentPath, newFileName());
	}

	private String newFileName() {
		return String.format(
				checkpointName(checkpointId) + "-" + taskName(taskNumber) + "-file-%d",
				nameCounter++);
	}

	private static boolean isTaskDir(String fileName) {
		return fileName.startsWith(TASK_DIR_PREFIX);
	}

	private static boolean isCheckpointDir(String fileName) {
		return fileName.startsWith(CHECKPOINT_DIR_PREFIX);
	}

	private static long getCheckpointId(String fileName) {
		return Long.parseLong(fileName.substring(3, fileName.length()));
	}

	private static String checkpointName(long checkpointId) {
		return CHECKPOINT_DIR_PREFIX + checkpointId;
	}

	private static String taskName(int task) {
		return TASK_DIR_PREFIX + task;
	}

	/**
	 * Delete checkpoint path.
	 */
	public static void deleteCheckpoint(
			FileSystem fs, Path basePath, long checkpointId) throws IOException {
		fs.delete(new Path(basePath, checkpointName(checkpointId)), true);
	}

	/**
	 * Returns checkpoints whose keys are less than or equal to {@code toCpId}
	 * in temporary base path.
	 */
	public static long[] headCheckpoints(FileSystem fs, Path basePath, long toCpId) throws IOException {
		List<Long> cps = new ArrayList<>();

		for (FileStatus taskStatus : fs.listStatus(basePath)) {
			String name = taskStatus.getPath().getName();
			if (isCheckpointDir(name)) {
				long currentCp = getCheckpointId(name);
				// commit paths that less than current checkpoint id.
				if (currentCp <= toCpId) {
					cps.add(currentCp);
				}
			}
		}
		return cps.stream().mapToLong(v -> v).toArray();
	}

	/**
	 * Returns task temporary paths in this checkpoint.
	 */
	public static List<Path> listTaskTemporaryPaths(
			FileSystem fs, Path basePath, long checkpointId) throws Exception {
		List<Path> taskTmpPaths = new ArrayList<>();

		for (FileStatus taskStatus : fs.listStatus(new Path(basePath, checkpointName(checkpointId)))) {
			if (isTaskDir(taskStatus.getPath().getName())) {
				taskTmpPaths.add(taskStatus.getPath());
			}
		}
		return taskTmpPaths;
	}
}
