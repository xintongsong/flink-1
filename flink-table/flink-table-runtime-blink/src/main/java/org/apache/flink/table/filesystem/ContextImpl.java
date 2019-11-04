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
import org.apache.flink.api.common.io.OutputFormat;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.core.fs.Path;
import org.apache.flink.table.filesystem.PartitionWriter.Context;

import java.io.IOException;

/**
 * Default {@link Context} implementation.
 *
 * @param <T> The type of the consumed records.
 */
@Internal
public class ContextImpl<T> implements Context<T> {

	private final Configuration conf;
	private final PartitionComputer<T> computer;
	private final OutputFormatFactory<T> factory;

	private FileCommitter.PathGenerator generator;

	public ContextImpl(
			Configuration conf,
			PartitionComputer<T> computer,
			OutputFormatFactory<T> factory) {
		this.conf = conf;
		this.computer = computer;
		this.factory = factory;
	}

	public void setPathGenerator(FileCommitter.PathGenerator generator) {
		this.generator = generator;
	}

	@Override
	public OutputFormat<T> createNewOutputFormat(Path path) throws IOException {
		OutputFormat<T> format = factory.createOutputFormat(path);
		format.configure(conf);
		// Here we just think of it as a single file format, so there can only be a single task.
		format.open(0, 1);
		return format;
	}

	@Override
	public Path generatePath(String... directories) throws Exception {
		return generator.generate(directories);
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
