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

import org.apache.flink.api.common.io.OutputFormat;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.core.fs.Path;
import org.apache.flink.table.filesystem.PartitionWriter.Context;
import org.apache.flink.types.Row;

import org.junit.Assert;
import org.junit.Test;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Test for {@link PartitionWriter}s.
 */
public class PartitionWriterTest {

	private Map<String, List<Row>> records = new HashMap<>();

	private OutputFormatFactory<Row> factory = (OutputFormatFactory<Row>) path ->
			new OutputFormat<Row>() {
		@Override
		public void configure(Configuration parameters) {
		}

		@Override
		public void open(int taskNumber, int numTasks) throws IOException {
		}

		@Override
		public void writeRecord(Row record) throws IOException {
			records.compute(path.toString(), (path1, rows) -> {
				rows = rows == null ? new ArrayList<>() : rows;
				rows.add(record);
				return rows;
			});
		}

		@Override
		public void close() throws IOException {
		}
	};

	private String basePath = "cp1";

	private Context<Row> context = new Context<Row>() {

		@Override
		public OutputFormat<Row> createNewOutputFormat(Path path) throws IOException {
			return factory.createOutputFormat(path);
		}

		@Override
		public Path generatePath(String... directories) throws Exception {
			Path parentPath = new Path(basePath);
			for (String dir : directories) {
				parentPath = new Path(parentPath, dir);
			}
			return parentPath;
		}

		@Override
		public String computePartition(Row in) throws Exception {
			return (String) in.getField(0);
		}

		@Override
		public Row projectColumnsToWrite(Row in) throws Exception {
			return in;
		}
	};

	@Test
	public void testNonPartitionWriter() throws Exception {
		NonPartitionWriter<Row> writer = new NonPartitionWriter<>();
		writer.open(context);

		writer.write(Row.of("p1", 1));
		writer.write(Row.of("p1", 2));
		writer.write(Row.of("p2", 2));
		writer.close();
		Assert.assertEquals("{cp1=[p1,1, p1,2, p2,2]}", records.toString());

		basePath = "cp2";
		writer = new NonPartitionWriter<>();
		writer.open(context);
		writer.write(Row.of("p3", 3));
		writer.write(Row.of("p5", 5));
		writer.write(Row.of("p2", 2));
		writer.close();
		Assert.assertEquals("{cp2=[p3,3, p5,5, p2,2], cp1=[p1,1, p1,2, p2,2]}", records.toString());
	}

	@Test
	public void testGroupedPartitionWriter() throws Exception {
		GroupedPartitionWriter<Row> writer = new GroupedPartitionWriter<>();
		writer.open(context);

		writer.write(Row.of("p1", 1));
		writer.write(Row.of("p1", 2));
		writer.write(Row.of("p2", 2));
		writer.close();
		Assert.assertEquals("{cp1/p2=[p2,2], cp1/p1=[p1,1, p1,2]}", records.toString());

		basePath = "cp2";
		writer = new GroupedPartitionWriter<>();
		writer.open(context);
		writer.write(Row.of("p3", 3));
		writer.write(Row.of("p4", 5));
		writer.write(Row.of("p5", 2));
		writer.close();
		Assert.assertEquals(
				"{cp2/p3=[p3,3], cp1/p2=[p2,2], cp1/p1=[p1,1, p1,2], cp2/p5=[p5,2], cp2/p4=[p4,5]}",
				records.toString());
	}

	@Test
	public void testDynamicPartitionWriter() throws Exception {
		DynamicPartitionWriter<Row> writer = new DynamicPartitionWriter<>();
		writer.open(context);

		writer.write(Row.of("p1", 1));
		writer.write(Row.of("p2", 2));
		writer.write(Row.of("p1", 2));
		writer.close();
		Assert.assertEquals("{cp1/p2=[p2,2], cp1/p1=[p1,1, p1,2]}", records.toString());

		basePath = "cp2";
		writer = new DynamicPartitionWriter<>();
		writer.open(context);
		writer.write(Row.of("p4", 5));
		writer.write(Row.of("p3", 3));
		writer.write(Row.of("p5", 2));
		writer.close();
		Assert.assertEquals(
				"{cp2/p3=[p3,3], cp1/p2=[p2,2], cp1/p1=[p1,1, p1,2], cp2/p5=[p5,2], cp2/p4=[p4,5]}",
				records.toString());
	}
}
