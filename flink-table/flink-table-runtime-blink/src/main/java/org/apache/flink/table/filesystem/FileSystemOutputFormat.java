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
import org.apache.flink.api.common.io.FinalizeOnMaster;
import org.apache.flink.api.common.io.OutputFormat;
import org.apache.flink.api.java.typeutils.TypeExtractor;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.core.fs.FileSystem;
import org.apache.flink.core.fs.Path;
import org.apache.flink.table.api.TableException;
import org.apache.flink.types.Row;

import java.io.IOException;
import java.io.Serializable;
import java.util.HashMap;
import java.util.Map;

import static org.apache.flink.util.Preconditions.checkNotNull;

/**
 * File system {@link OutputFormat} for batch job. It commit in {@link #finalizeGlobal(int)}.
 *
 * @param <T> The type of the consumed records.
 */
@Internal
public class FileSystemOutputFormat<T> implements OutputFormat<T>, FinalizeOnMaster, Serializable {

	private static final long serialVersionUID = 1L;

	private static final long CHECKPOINT_ID = 0;

	private final PartitionComputer<T> computer;
	private final PartitionWriterFactory<T> partitionWriterFactory;
	private final OutputFormatFactory<T> formatFactory;
	private final FileCommitter committer;

	private transient PartitionWriter<T> writer;
	private transient Configuration parameters;

	FileSystemOutputFormat(
			OutputFormatFactory<T> formatFactory,
			PartitionComputer<T> computer,
			PartitionWriterFactory<T> partitionWriterFactory,
			FileCommitter committer) {
		this.computer = computer;
		this.partitionWriterFactory = partitionWriterFactory;
		this.formatFactory = formatFactory;
		this.committer = committer;
	}

	@Override
	public void finalizeGlobal(int parallelism) throws IOException {
		try {
			committer.commitJob(CHECKPOINT_ID);
		} catch (Exception e) {
			throw new TableException("Exception in finalizeGlobal", e);
		}
	}

	@Override
	public void configure(Configuration parameters) {
		this.parameters = parameters;
	}

	@Override
	public void open(int taskNumber, int numTasks) throws IOException {
		try {
			FileCommitter.PathGenerator pathGenerator = committer.createGeneratorAndCleanDir(
					taskNumber, CHECKPOINT_ID);
			PartitionWriter.Context<T> context = new PartitionWriter.Context<>(
					parameters, formatFactory);
			writer = partitionWriterFactory.create(context, pathGenerator, computer);
		} catch (Exception e) {
			throw new TableException("Exception in open", e);
		}
	}

	@Override
	public void writeRecord(T record) throws IOException {
		try {
			writer.write(record);
		} catch (Exception e) {
			throw new TableException("Exception in writeRecord", e);
		}
	}

	@Override
	public void close() throws IOException {
		try {
			writer.close();
		} catch (Exception e) {
			throw new TableException("Exception in close", e);
		}
	}

	/**
	 * Builder to build {@link FileSystemOutputFormat}.
	 */
	public static class Builder<T> {

		private String[] columnNames;
		private String[] partitionColumns;
		private OutputFormatFactory<T> formatFactory;
		private MetaStoreFactory metaStoreFactory;
		private Path tmpPath;

		private String defaultPartName = "_DEFAULT_PARTITION_";
		private Map<String, String> staticPartitions = new HashMap<>();
		private boolean dynamicGrouped = false;
		private boolean overwrite = false;
		private FileSystemFactory fileSystemFactory = (FileSystemFactory) FileSystem::get;
		private PartitionComputer<T> computer;

		public Builder<T> setDefaultPartName(String defaultPartName) {
			this.defaultPartName = defaultPartName;
			return this;
		}

		public Builder<T> setColumnNames(String[] columnNames) {
			this.columnNames = columnNames;
			return this;
		}

		public Builder<T> setPartitionColumns(String[] partitionColumns) {
			this.partitionColumns = partitionColumns;
			return this;
		}

		public Builder<T> setStaticPartitions(Map<String, String> staticPartitions) {
			this.staticPartitions = staticPartitions;
			return this;
		}

		public Builder<T> setDynamicGrouped(boolean dynamicGrouped) {
			this.dynamicGrouped = dynamicGrouped;
			return this;
		}

		public Builder<T> setFormatFactory(OutputFormatFactory<T> formatFactory) {
			this.formatFactory = formatFactory;
			return this;
		}

		public Builder<T> setFileSystemFactory(FileSystemFactory fileSystemFactory) {
			this.fileSystemFactory = fileSystemFactory;
			return this;
		}

		public Builder<T> setMetaStoreFactory(MetaStoreFactory metaStoreFactory) {
			this.metaStoreFactory = metaStoreFactory;
			return this;
		}

		public Builder<T> setOverwrite(boolean overwrite) {
			this.overwrite = overwrite;
			return this;
		}

		public Builder<T> setTmpPath(Path tmpPath) {
			this.tmpPath = tmpPath;
			return this;
		}

		public Builder<T> setPartitionComputer(PartitionComputer<T> computer) {
			this.computer = computer;
			return this;
		}

		public FileSystemOutputFormat<T> build() {
			checkNotNull(columnNames, "columnNames should not be null");
			checkNotNull(partitionColumns, "partitionColumns should not be null");
			checkNotNull(formatFactory, "formatFactory should not be null");
			checkNotNull(metaStoreFactory, "metaStoreFactory should not be null");
			checkNotNull(tmpPath, "tmpPath should not be null");

			Class conversionClass = TypeExtractor.<T>createTypeInfo(
					formatFactory, OutputFormatFactory.class, formatFactory.getClass(), 0).getTypeClass();
			PartitionComputer<T> computer = this.computer;
			if (computer == null) {
				if (conversionClass == Row.class) {
					//noinspection unchecked
					computer = (PartitionComputer<T>) new RowPartitionComputer(
							defaultPartName, columnNames, partitionColumns);
				} else {
					throw new TableException("Need specify PartitionComputer or use Row.");
				}
			}

			FileCommitter committer = new FileCommitter(
					fileSystemFactory,
					metaStoreFactory,
					overwrite,
					tmpPath,
					staticPartitions,
					partitionColumns.length);

			return new FileSystemOutputFormat<>(
					formatFactory,
					computer,
					PartitionWriterFactory.get(
							partitionColumns.length - staticPartitions.size() > 0,
							dynamicGrouped),
					committer);
		}
	}
}
