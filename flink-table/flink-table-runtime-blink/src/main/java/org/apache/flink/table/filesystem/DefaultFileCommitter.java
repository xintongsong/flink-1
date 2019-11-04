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
import org.apache.flink.table.api.TableException;
import org.apache.flink.util.Preconditions;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Default file system {@link FileCommitter} implementation. It just move all files to
 * output path from temporary path.
 *
 * <p>NOTE: Here, we have no way to update catalog.
 */
@Internal
public class DefaultFileCommitter implements FileCommitter {

	private static final long serialVersionUID = 1L;

	private static final Pattern PARTITION_NAME_PATTERN = Pattern.compile("([^/]+)=([^/]+)");

	private final boolean overwrite;
	private final Path tmpPath;
	private final Path outputPath;
	private final Map<String, String> staticPartitions;
	private final int partitionColumnSize;

	public DefaultFileCommitter(
			boolean overwrite,
			Path tmpPath,
			Path outputPath,
			Map<String, String> staticPartitions,
			int partitionColumnSize) {
		this.overwrite = overwrite;
		this.tmpPath = tmpPath;
		this.outputPath = outputPath;
		this.staticPartitions = staticPartitions;
		this.partitionColumnSize = partitionColumnSize;
	}

	@Override
	public void deletePath(Path taskTmpDir) throws Exception {
		FileSystem.get(taskTmpDir.toUri()).delete(taskTmpDir, true);
	}

	@Override
	public void commit(long checkpointId) throws Exception {
		FileSystem fs = FileSystem.get(tmpPath.toUri());
		Set<Path> overwritten = new HashSet<>();
		for (FileStatus taskStatus : fs.listStatus(tmpPath)) {
			String name = taskStatus.getPath().getName();
			if (name.startsWith("cp-")) {
				long cpId = Long.parseLong(name.substring(3, name.length()));

				// commit paths that less than current checkpoint id.
				if (cpId <= checkpointId) {
					for (FileStatus status : fs.listStatus(taskStatus.getPath())) {
						if (status.getPath().getName().startsWith("task-")) {
							commitTaskOutputFiles(status.getPath(), overwritten);
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
	private void commitTaskOutputFiles(Path path, Set<Path> overwritten) throws Exception {
		try {
			boolean isPartitioned = partitionColumnSize > 0;
			boolean isDynamicPartition = isPartitioned && partitionColumnSize != staticPartitions.size();

			if (isPartitioned) {
				if (isDynamicPartition) {
					List<Tuple2<LinkedHashMap<String, String>, Path>> partitionSpecs =
							searchPartSpecAndPaths(path, partitionColumnSize);
					for (Tuple2<LinkedHashMap<String, String>, Path> spec : partitionSpecs) {
						loadPartition(spec.f1, spec.f0, overwritten);
					}
				} else {
					LinkedHashMap<String, String> partSpec = new LinkedHashMap<>();
					for (String partCol : staticPartitions.keySet()) {
						partSpec.put(partCol, staticPartitions.get(partCol));
					}
					loadPartition(path, partSpec, overwritten);
				}
			} else {
				moveFiles(path, outputPath, overwritten);
			}
		} finally {
			deletePath(path);
		}
	}

	/**
	 * Load a single partition.
	 */
	private void loadPartition(Path srcDir, LinkedHashMap<String, String> partSpec, Set<Path> overwritten) throws Exception {
		String partName = makePartitionName(partSpec);
		if (partName.equals("")) {
			throw new RuntimeException("partName is illegal: " + partName);
		}
		moveFiles(srcDir, new Path(outputPath, partName), overwritten);
	}

	/**
	 * Moves files from srcDir to destDir. Delete files in destDir first when overwrite.
	 */
	private void moveFiles(Path srcDir, Path destDir, Set<Path> overwritten) throws Exception {
		if (!srcDir.equals(destDir)) {
			FileSystem fs = destDir.getFileSystem();
			Preconditions.checkState(fs.exists(destDir) || fs.mkdirs(destDir), "Failed to create dest path " + destDir);
			if (overwrite && !overwritten.contains(destDir)) {
				// delete existing files for overwrite
				FileStatus[] existingFiles = fs.listStatus(destDir);
				if (existingFiles != null) {
					for (FileStatus existingFile : existingFiles) {
						deletePath(existingFile.getPath());
					}
				}
				overwritten.add(destDir);
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

	@Override
	public PathGenerator newGeneratorAndCleanDirector(
			int taskNumber, long checkpointId) throws Exception {
		DefaultPathGenerator pathGenerator = new DefaultPathGenerator(
				tmpPath, taskNumber, checkpointId);
		deletePath(pathGenerator.getTaskTemporaryPath());
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

	/**
	 * Default {@link PartitionPathMaker} to be consistent with {@link DefaultFileCommitter}.
	 */
	public static class DefaultPartitionPathMaker implements PartitionPathMaker {

		@Override
		public String makePartitionPath(
				LinkedHashMap<String, String> partitionValues) throws Exception {
			return makePartitionName(partitionValues);
		}
	}
}

