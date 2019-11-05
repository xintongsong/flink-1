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
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.core.fs.FileStatus;
import org.apache.flink.core.fs.FileSystem;
import org.apache.flink.core.fs.Path;
import org.apache.flink.table.filesystem.MetaStoreFactory.MetaStore;
import org.apache.flink.util.Preconditions;

import java.io.IOException;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

import static org.apache.flink.table.filesystem.FileSystemUtils.generatePartName;
import static org.apache.flink.table.filesystem.FileSystemUtils.generateSpecFromName;
import static org.apache.flink.util.Preconditions.checkArgument;

/**
 * File system file committer implementation. It move all files to output path from temporary path.
 *
 * <p>In a checkpoint:
 *  1.Every task will invoke {@link #createGeneratorAndCleanDir} to initialization, it returns
 *  a path generator to generate path for task writing. And clean the temporary path of task.
 *  2.After writing done for this checkpoint, need invoke {@link #commitJob(long)}, will move the
 *  temporary files to real output path.
 *
 * <p>Temporary file directory contains the following directory parts:
 *  1.temporary base path directory.
 *  2.checkpoint id directory.
 *  3.task id directory.
 *  4.directories to specify partitioning.
 *  5.data files.
 *  eg: /tmp/cp-1/task-0/p0=1/p1=2/fileName.
 *
 * <p>Batch is a special case of Streaming, which has only one checkpoint.
 */
@Internal
public class FileCommitter implements Serializable {

	private static final long serialVersionUID = 1L;

	private static final String CHECKPOINT_DIR_PREFIX = "cp-";
	private static final String TASK_DIR_PREFIX = "task-";

	private final FileSystemFactory factory;
	private final MetaStoreFactory metaStoreFactory;
	private final boolean overwrite;
	private final Path tmpPath;
	private final Map<String, String> staticPartitions;
	private final int partitionColumnSize;

	public FileCommitter(
			FileSystemFactory factory,
			MetaStoreFactory metaStoreFactory,
			boolean overwrite,
			Path tmpPath,
			Map<String, String> staticPartitions,
			int partitionColumnSize) {
		this.factory = factory;
		this.metaStoreFactory = metaStoreFactory;
		this.overwrite = overwrite;
		this.tmpPath = tmpPath;
		this.staticPartitions = staticPartitions;
		this.partitionColumnSize = partitionColumnSize;
	}

	/**
	 * For committing job's output after successful batch job completion or one checkpoint finish
	 * for streaming job. Should move all files to final output paths. And should commit all
	 * checkpoint ids that less than current checkpoint id.
	 */
	public void commitJob(long checkpointId) throws Exception {
		FileSystem fs = factory.create(tmpPath.toUri());
		Set<Path> overwritten = new HashSet<>();

		try (MetaStore metaStore = metaStoreFactory.createMetaStore()) {
			for (FileStatus taskStatus : fs.listStatus(tmpPath)) {
				String name = taskStatus.getPath().getName();
				if (isCheckpointDir(name)) {
					// commit paths that less than current checkpoint id.
					if (getCheckpointId(name) <= checkpointId) {
						for (FileStatus status : fs.listStatus(taskStatus.getPath())) {
							if (isTaskDir(status.getPath().getName())) {
								commitTaskOutputFiles(fs, metaStore, status.getPath(), overwritten);
							}
						}
						// clean checkpoint path
						fs.delete(taskStatus.getPath(), true);
					}
				}
			}
		}
	}

	/**
	 * Commit a task temporary path.
	 */
	private void commitTaskOutputFiles(
			FileSystem fs,
			MetaStore metaStore,
			Path path,
			Set<Path> overwritten) throws Exception {
		try {
			boolean isPartitioned = partitionColumnSize > 0;
			boolean isDynamicPartition = isPartitioned && partitionColumnSize != staticPartitions.size();

			if (isPartitioned) {
				if (isDynamicPartition) {
					List<Tuple2<LinkedHashMap<String, String>, Path>> partitionSpecs =
							searchPartSpecAndPaths(fs, path, partitionColumnSize);
					for (Tuple2<LinkedHashMap<String, String>, Path> spec : partitionSpecs) {
						loadPartition(fs, metaStore, spec.f1, spec.f0, overwritten);
					}
				} else {
					LinkedHashMap<String, String> partSpec = new LinkedHashMap<>();
					for (String partCol : staticPartitions.keySet()) {
						partSpec.put(partCol, staticPartitions.get(partCol));
					}
					loadPartition(fs, metaStore, path, partSpec, overwritten);
				}
			} else {
				moveFiles(fs, path, metaStore.getTablePath(), overwritten);
			}
		} finally {
			fs.delete(path, true);
		}
	}

	/**
	 * Load a single partition.
	 */
	private void loadPartition(
			FileSystem fs,
			MetaStore metaStore,
			Path srcDir,
			LinkedHashMap<String, String> partSpec,
			Set<Path> overwritten) throws Exception {
		Optional<Path> pathFromMeta = metaStore.getFirstPathFromPartSpec(partSpec);
		Path path = pathFromMeta.orElseGet(() -> new Path(
				metaStore.getTablePath(), generatePartName(partSpec)));
		moveFiles(fs, srcDir, path, overwritten);
		if (!pathFromMeta.isPresent()) {
			metaStore.addPartition(partSpec, path);
		}
	}

	/**
	 * Moves files from srcDir to destDir. Delete files in destDir first when overwrite.
	 */
	private void moveFiles(FileSystem fs, Path srcDir, Path destDir, Set<Path> overwritten) throws Exception {
		if (!srcDir.equals(destDir)) {
			// TODO: src and dest may be on different FS
			Preconditions.checkState(
					fs.exists(destDir) || fs.mkdirs(destDir),
					"Failed to create dest path " + destDir);
			if (overwrite && !overwritten.contains(destDir)) {
				// delete existing files for overwrite
				FileStatus[] existingFiles = listStatusWithoutHidden(fs, destDir);
				if (existingFiles != null) {
					for (FileStatus existingFile : existingFiles) {
						// TODO: We need move to trash when auto-purge is false.
						fs.delete(existingFile.getPath(), true);
					}
				}
				overwritten.add(destDir);
			}
			FileStatus[] srcFiles = listStatusWithoutHidden(fs, srcDir);
			for (FileStatus srcFile : srcFiles) {
				Path srcPath = srcFile.getPath();
				Path destPath = new Path(destDir, srcPath.getName());
				int count = 1;
				while (!fs.rename(srcPath, destPath)) {
					String name = srcPath.getName() + "_copy_" + count;
					destPath = new Path(destDir, name);
					count++;
				}
			}
		}
	}

	private FileStatus[] listStatusWithoutHidden(FileSystem fs, Path dir) throws IOException {
		return Arrays.stream(fs.listStatus(dir)).filter(fileStatus -> {
			String name = fileStatus.getPath().getName();
			return !name.startsWith("_") && !name.startsWith(".");
		}).toArray(FileStatus[]::new);
	}

	/**
	 * Create a new path generator from task and checkpoint id.
	 * And clean the temporary directory for task.
	 */
	public PathGenerator createGeneratorAndCleanDir(
			int taskNumber, long checkpointId) throws Exception {
		PathGenerator pathGenerator = new PathGenerator(
				tmpPath, taskNumber, checkpointId);
		Path path = pathGenerator.getTaskTemporaryPath();
		factory.create(path.toUri()).delete(path, true);
		return pathGenerator;
	}

	/**
	 * Search all partitions in this path.
	 *
	 * @param path search path.
	 * @param partitionNumber partition number, it will affect path structure.
	 * @return all partition specs to its path.
	 */
	public static List<Tuple2<LinkedHashMap<String, String>, Path>> searchPartSpecAndPaths(
			FileSystem fs, Path path, int partitionNumber) throws IOException {
		FileStatus[] generatedParts = getFileStatusRecurse(path, partitionNumber, fs);
		List<Tuple2<LinkedHashMap<String, String>, Path>> ret = new ArrayList<>();
		for (FileStatus part : generatedParts) {
			ret.add(new Tuple2<>(generateSpecFromName(part.getPath()), part.getPath()));
		}
		return ret;
	}

	private static FileStatus[] getFileStatusRecurse(Path path, int expectLevel, FileSystem fs) throws IOException {
		ArrayList<FileStatus> result = new ArrayList<>();

		try {
			FileStatus fileStatus = fs.getFileStatus(path);
			listStatusRecursively(fs, fileStatus, 0, expectLevel, result);
		} catch (IOException ignore) {
			return new FileStatus[0];
		}

		return result.toArray(new FileStatus[result.size()]);
	}

	private static void listStatusRecursively(
			FileSystem fs,
			FileStatus fileStatus,
			int level,
			int expectLevel,
			List<FileStatus> results) throws IOException {
		if (expectLevel == level) {
			results.add(fileStatus);
			return;
		}

		if (fileStatus.isDir()) {
			for (FileStatus stat : fs.listStatus(fileStatus.getPath())) {
				listStatusRecursively(fs, stat, level + 1, expectLevel, results);
			}
		}
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

	/**
	 * Path generator for task temporary path.
	 */
	public static class PathGenerator {

		private final int taskNumber;
		private final long checkpointId;
		private final Path taskTmpDir;

		private transient int nameCounter = 0;

		private PathGenerator(Path temporaryPath, int taskNumber, long checkpointId) {
			checkArgument(checkpointId != -1, "checkpoint id start with 0.");
			this.taskNumber = taskNumber;
			this.checkpointId = checkpointId;
			this.taskTmpDir = new Path(
					new Path(temporaryPath, CHECKPOINT_DIR_PREFIX + checkpointId),
					TASK_DIR_PREFIX + taskNumber);
		}

		private Path getTaskTemporaryPath() {
			return taskTmpDir;
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
			return String.format(
					CHECKPOINT_DIR_PREFIX + "%d-" + TASK_DIR_PREFIX + "%d-file-%d",
					checkpointId, taskNumber, nameCounter++);
		}
	}
}
