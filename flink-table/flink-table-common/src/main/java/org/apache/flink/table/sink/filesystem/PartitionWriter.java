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

/**
 * Partition writer to write records with partition.
 *
 * <p>See {@link NonPartitionWriter}.
 * See {@link DynamicPartitionWriter}.
 * See {@link GroupedPartitionWriter}.
 *
 * @param <T> The type of the consumed records.
 */
@Internal
public interface PartitionWriter<T> {

	/**
	 * Open writer with {@link Context}.
	 */
	void open(Context<T> context) throws Exception;

	/**
	 * Start a transaction.
	 * In batch mode, there is only one transaction.
	 * In Streaming mode, every checkpoint should be one transaction.
	 */
	void startTransaction() throws Exception;

	/**
	 * Write a record.
	 */
	void write(T in) throws Exception;

	/**
	 * End a transaction.
	 */
	void endTransaction() throws Exception;

	/**
	 * Context for partition writer, provide some information and generation utils.
	 */
	interface Context<T> {

		/**
		 * The configuration containing the parameters attached to the contract.
		 */
		Configuration configuration();

		/**
		 * Gets the number of this parallel subtask. The numbering starts from 0 and goes up to parallelism-1.
		 */
		int taskNumber();

		/**
		 * Gets the parallelism with which the parallel task runs.
		 */
		int numTask();

		/**
		 * Generate a new path without partition.
		 *
		 * <p>See {@link NonPartitionWriter}.
		 */
		Path generatePath();

		/**
		 * Generate a new path with partition path.
		 *
		 * <p>See {@link DynamicPartitionWriter} and {@link GroupedPartitionWriter}.
		 */
		Path generatePath(String partition);

		/**
		 * Compute partition path from record.
		 */
		String computePartition(T in) throws Exception;

		/**
		 * Project non-partition columns for output writer.
		 */
		T projectColumnsToWrite(T in) throws Exception;
	}
}
