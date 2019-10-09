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
import org.apache.flink.api.common.io.OutputFormat;

/**
 * {@link PartitionWriter} for grouped dynamic partition inserting. It will create a new format
 * when partition changed.
 *
 * @param <T> The type of the consumed records.
 */
@Internal
public class GroupedPartitionWriter<T> implements PartitionWriter<T> {

	private final OutputFormatFactory<T> factory;

	private OutputFormat<T> currentFormat;
	private String currentPartition;
	private Context<T> context;

	public GroupedPartitionWriter(OutputFormatFactory<T> factory) {
		this.factory = factory;
	}

	@Override
	public void open(Context<T> context) throws Exception {
		this.context = context;
	}

	@Override
	public void startTransaction() throws Exception {
		this.currentFormat = null;
	}

	@Override
	public void write(T in) throws Exception {
		String partition = context.computePartition(in);
		if (currentPartition == null || !partition.equals(currentPartition)) {
			if (currentPartition != null) {
				currentFormat.close();
			}

			currentFormat = factory.createOutputFormat(context.generatePath(partition));
			currentFormat.configure(context.configuration());
			currentFormat.open(context.taskNumber(), context.numTask());
			currentPartition = partition;
		}
		currentFormat.writeRecord(context.projectColumnsToWrite(in));
	}

	@Override
	public void endTransaction() throws Exception {
		if (currentFormat != null) {
			currentFormat.close();
		}
	}
}
