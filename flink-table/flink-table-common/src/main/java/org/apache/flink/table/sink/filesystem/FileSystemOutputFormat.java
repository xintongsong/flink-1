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
import org.apache.flink.api.common.io.FinalizeOnMaster;
import org.apache.flink.api.common.io.OutputFormat;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.table.api.TableException;

import java.io.IOException;
import java.io.Serializable;

/**
 * File system {@link OutputFormat} for batch job. It commit in {@link #finalizeGlobal(int)}.
 *
 * @param <T> The type of the consumed records.
 */
@Internal
public class FileSystemOutputFormat<T> implements OutputFormat<T>, FinalizeOnMaster, Serializable {

	private static final long CHECKPOINT_ID = 0;

	private final PartitionComputer<T> computer;
	private final PartitionWriterFactory<T> partitionWriterFactory;
	private final OutputFormatFactory<T> outputFormatFactory;
	private final FileCommitter committer;

	private transient PartitionWriter<T> writer;
	private transient Configuration parameters;

	public FileSystemOutputFormat(
			PartitionComputer<T> computer,
			PartitionWriterFactory<T> partitionWriterFactory,
			OutputFormatFactory<T> outputFormatFactory,
			FileCommitter committer) {
		this.computer = computer;
		this.partitionWriterFactory = partitionWriterFactory;
		this.outputFormatFactory = outputFormatFactory;
		this.committer = committer;
	}

	@Override
	public void finalizeGlobal(int parallelism) throws IOException {
		try {
			committer.commit(CHECKPOINT_ID);
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
			FileCommitter.PathGenerator pathGenerator = committer.pathGenerator(taskNumber);
			writer = partitionWriterFactory.create(outputFormatFactory);
			writer.open(new ContextImpl<>(parameters, taskNumber, numTasks, pathGenerator, computer));
			pathGenerator.startTransaction(CHECKPOINT_ID);
			writer.startTransaction();
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
			writer.endTransaction();
		} catch (Exception e) {
			throw new TableException("Exception in close", e);
		}
	}
}
