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
import org.apache.flink.configuration.Configuration;
import org.apache.flink.core.fs.Path;
import org.apache.flink.table.sink.filesystem.PartitionWriter.Context;

/**
 * Default {@link Context} implementation.
 *
 * @param <T> The type of the consumed records.
 */
@Internal
public class ContextImpl<T> implements Context<T> {

	private final Configuration conf;
	private final int taskNumber;
	private final int numTask;
	private final FileCommitter.PathGenerator generator;
	private final PartitionComputer<T> computer;

	public ContextImpl(
			Configuration conf,
			int taskNumber,
			int numTask,
			FileCommitter.PathGenerator generator,
			PartitionComputer<T> computer) {
		this.conf = conf;
		this.taskNumber = taskNumber;
		this.numTask = numTask;
		this.generator = generator;
		this.computer = computer;
	}

	@Override
	public Configuration configuration() {
		return conf;
	}

	@Override
	public int taskNumber() {
		return taskNumber;
	}

	@Override
	public int numTask() {
		return numTask;
	}

	@Override
	public Path generatePath() {
		return generator.generate();
	}

	@Override
	public Path generatePath(String partition) {
		return generator.generate(partition);
	}

	@Override
	public String computePartition(T in) throws Exception {
		return computer.computePartition(in);
	}

	@Override
	public T projectColumnsToWrite(T in) throws Exception {
		return computer.projectColumnsToWrite(in);
	}
}
