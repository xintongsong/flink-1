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
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.core.fs.FileStatus;
import org.apache.flink.core.fs.FileSystem;
import org.apache.flink.core.fs.Path;
import org.apache.flink.table.api.TableException;
import org.apache.flink.util.Preconditions;

import java.io.IOException;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Default file system {@link FileCommitter} implementation. It just move all files to
 * output path from temporary path.
 *
 * <p>NOTE: Here, we have no way to update catalog.
 */
@Internal
public class FileSystemFileCommitter extends FileCommitter {

	private static final Pattern PARTITION_NAME_PATTERN = Pattern.compile("([^/]+)=([^/]+)");

	private final Path outputPath;
	private final Map<String, String> staticPartitions;
	private final String[] partitionColumns;

	public FileSystemFileCommitter(
			boolean overwrite,
			Path tmpPath,
			Path outputPath,
			Map<String, String> staticPartitions,
			String[] partitionColumns) {
		super(tmpPath, overwrite);
		this.outputPath = outputPath;
		this.staticPartitions = staticPartitions;
		this.partitionColumns = partitionColumns;
	}

	@Override
	public void deletePath(Path taskTmpDir) throws Exception {
		FileSystem.get(taskTmpDir.toUri()).delete(taskTmpDir, true);
	}

	@Override
	public void commit(long checkpointId) throws Exception {
		FileSystem fs = FileSystem.get(temporaryPath.toUri());
		for (FileStatus taskStatus : fs.listStatus(temporaryPath)) {
			String name = taskStatus.getPath().getName();
			if (name.startsWith("cp-")) {
				long cpId = Long.parseLong(name.substring(3, name.length()));

				// commit paths that less than current checkpoint id.
				if (cpId <= checkpointId) {
					for (FileStatus status : fs.listStatus(taskStatus.getPath())) {
						if (status.getPath().getName().startsWith("task-")) {
							commitTaskOutputFiles(status.getPath());
						}
					}

					// clean checkpoint path
					deletePath(taskStatus.getPath());
				}
			}
		}
	}

	/**
	 * Commit a task temporary path.
	 */
	private void commitTaskOutputFiles(Path path) throws Exception {
		try {
			boolean isPartitioned = partitionColumns.length > 0;
			boolean isDynamicPartition = isPartitioned && partitionColumns.length != staticPartitions.size();

			if (isPartitioned) {
				if (isDynamicPartition) {
					List<Tuple2<LinkedHashMap<String, String>, Path>> partitionSpecs =
							searchPartSpecAndPaths(path, partitionColumns.length);
					for (Tuple2<LinkedHashMap<String, String>, Path> spec : partitionSpecs) {
						loadPartition(spec.f1, spec.f0);
					}
				} else {
					LinkedHashMap<String, String> partSpec = new LinkedHashMap<>();
					for (String partCol : staticPartitions.keySet()) {
						partSpec.put(partCol, staticPartitions.get(partCol));
					}
					loadPartition(path, partSpec);
				}
			} else {
				moveFiles(path, outputPath);
			}
		} finally {
			deletePath(path);
		}
	}

	/**
	 * Load a single partition.
	 */
	private void loadPartition(Path srcDir, LinkedHashMap<String, String> partSpec) throws Exception {
		moveFiles(srcDir, new Path(outputPath, makePartitionName(partSpec)));
	}

	/**
	 * Moves all files under srcDir into destDir. Delete destDir all files first when overwrite.
	 */
	private void moveFiles(Path srcDir, Path destDir) throws Exception {
		if (!srcDir.equals(destDir)) {
			FileSystem fs = destDir.getFileSystem();
			Preconditions.checkState(fs.exists(destDir) || fs.mkdirs(destDir), "Failed to create dest path " + destDir);
			if (overwrite) {
				// delete existing files for overwrite
				FileStatus[] existingFiles = fs.listStatus(destDir);
				if (existingFiles != null) {
					for (FileStatus existingFile : existingFiles) {
						deletePath(existingFile.getPath());
					}
				}
			}
			FileStatus[] srcFiles = fs.listStatus(srcDir);
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

	/**
	 * Search all partitions in this path.
	 *
	 * @param path search path.
	 * @param partitionNumber partition number, it will affect path structure.
	 * @return all partition specs to its path.
	 */
	public static List<Tuple2<LinkedHashMap<String, String>, Path>> searchPartSpecAndPaths(
			Path path, int partitionNumber) throws IOException {
		FileStatus[] generatedParts = getFileStatusRecurse(path, partitionNumber, path.getFileSystem());
		List<Tuple2<LinkedHashMap<String, String>, Path>> ret = new ArrayList<>();
		for (FileStatus part : generatedParts) {
			ret.add(new Tuple2<>(makeSpecFromName(part.getPath()), part.getPath()));
		}
		return ret;
	}

	private static FileStatus[] getFileStatusRecurse(Path path, int expectLevel, FileSystem fs) throws IOException {
		ArrayList<FileStatus> result = new ArrayList<>();

		try {
			FileStatus fileStatus = fs.getFileStatus(path);
			listStatusRecursively(fs, fileStatus, 0, expectLevel, result);
		} catch (IOException var5) {
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

	/**
	 * Make partition spec from path.
	 *
	 * <p>See {@link #makePartitionName(LinkedHashMap)}.
	 *
	 * @param currPath partition file path.
	 * @return Sequential partition specs.
	 */
	public static LinkedHashMap<String, String> makeSpecFromName(Path currPath) {
		LinkedHashMap<String, String> fullPartSpec = new LinkedHashMap<>();
		List<String[]> kvs = new ArrayList<>();
		do {
			String component = currPath.getName();
			Matcher m = PARTITION_NAME_PATTERN.matcher(component);
			if (m.matches()) {
				String k = m.group(1);
				String v = m.group(2);
				String[] kv = new String[2];
				kv[0] = k;
				kv[1] = v;
				kvs.add(kv);
			}
			currPath = currPath.getParent();
		} while (currPath != null && !currPath.getName().isEmpty());

		// reverse the list since we checked the part from leaf dir to table's base dir
		for (int i = kvs.size(); i > 0; i--) {
			fullPartSpec.put(kvs.get(i - 1)[0], kvs.get(i - 1)[1]);
		}

		return fullPartSpec;
	}

	/**
	 * Make partition path from partition spec.
	 *
	 * <p>See {@link #makeSpecFromName(Path)}.
	 */
	public static String makePartitionName(LinkedHashMap<String, String> partitionSpec) {
		StringBuilder suffixBuf = new StringBuilder();
		int i = 0;
		for (Map.Entry<String, String> e : partitionSpec.entrySet()) {
			if (e.getValue() == null || e.getValue().length() == 0) {
				throw new TableException("Partition spec is incorrect. " + partitionSpec);
			}
			if (i > 0) {
				suffixBuf.append(Path.SEPARATOR);
			}
			suffixBuf.append(e.getKey());
			suffixBuf.append('=');
			suffixBuf.append(e.getValue());
			i++;
		}
		suffixBuf.append(Path.SEPARATOR);
		return suffixBuf.toString();
	}
}

