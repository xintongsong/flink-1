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
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/**
 * Test for {@link PartitionWriter}s.
 */
public class PartitionWriterTest {

	@ClassRule
	public static final TemporaryFolder TEMP_FOLDER = new TemporaryFolder();

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
			Path parent = path.getParent();
			String key = parent.getName().startsWith("task-") ?
					parent.getParent().getName() :
					parent.getParent().getParent().getName() + Path.SEPARATOR + parent.getName();

			records.compute(key, (path1, rows) -> {
				rows = rows == null ? new ArrayList<>() : rows;
				rows.add(record);
				return rows;
			});
		}

		@Override
		public void close() throws IOException {
		}
	};

	private final String basePath = TEMP_FOLDER.newFolder().getPath();

	private final Context<Row> context = path -> factory.createOutputFormat(path);

	private final DefaultFileCommitter committer = new DefaultFileCommitter(
			true, new Path(basePath), null, new HashMap<>(), 0);

	private PathGenerator pathGenerator = committer.createGeneratorAndCleanDir(0, 1);

	private PartitionComputer<Row> computer = new PartitionComputer<Row>() {

		@Override
		public LinkedHashMap<String, String> makePartitionValues(Row in) throws Exception {
			LinkedHashMap<String, String> ret = new LinkedHashMap<>(1);
			ret.put("", in.getField(0).toString());
			return ret;
		}

		@Override
		public Row projectColumnsToWrite(Row in) throws Exception {
			return in;
		}
	};

	private PartitionPathMaker maker = (PartitionPathMaker) v -> v.values().iterator().next();

	public PartitionWriterTest() throws Exception {
	}

	@Test
	public void testNonPartitionWriter() throws Exception {
		NonPartitionWriter<Row> writer = new NonPartitionWriter<>(context, pathGenerator, computer);

		writer.write(Row.of("p1", 1));
		writer.write(Row.of("p1", 2));
		writer.write(Row.of("p2", 2));
		writer.close();
		Assert.assertEquals("{cp-1=[p1,1, p1,2, p2,2]}", records.toString());

		pathGenerator = committer.createGeneratorAndCleanDir(0, 2);
		writer = new NonPartitionWriter<>(context, pathGenerator, computer);
		writer.write(Row.of("p3", 3));
		writer.write(Row.of("p5", 5));
		writer.write(Row.of("p2", 2));
		writer.close();
		Assert.assertEquals("{cp-2=[p3,3, p5,5, p2,2], cp-1=[p1,1, p1,2, p2,2]}", records.toString());
	}

	@Test
	public void testGroupedPartitionWriter() throws Exception {
		GroupedPartitionWriter<Row> writer = new GroupedPartitionWriter<>(
				context, pathGenerator, computer, maker);

		writer.write(Row.of("p1", 1));
		writer.write(Row.of("p1", 2));
		writer.write(Row.of("p2", 2));
		writer.close();
		Assert.assertEquals("{cp-1/p1=[p1,1, p1,2], cp-1/p2=[p2,2]}", records.toString());

		pathGenerator = committer.createGeneratorAndCleanDir(0, 2);
		writer = new GroupedPartitionWriter<>(context, pathGenerator, computer, maker);
		writer.write(Row.of("p3", 3));
		writer.write(Row.of("p4", 5));
		writer.write(Row.of("p5", 2));
		writer.close();
		Assert.assertEquals(
				"{cp-1/p1=[p1,1, p1,2], cp-2/p4=[p4,5], cp-1/p2=[p2,2], cp-2/p3=[p3,3], cp-2/p5=[p5,2]}",
				records.toString());
	}

	@Test
	public void testDynamicPartitionWriter() throws Exception {
		DynamicPartitionWriter<Row> writer = new DynamicPartitionWriter<>(
				context, pathGenerator, computer, maker);

		writer.write(Row.of("p1", 1));
		writer.write(Row.of("p2", 2));
		writer.write(Row.of("p1", 2));
		writer.close();
		Assert.assertEquals("{cp-1/p1=[p1,1, p1,2], cp-1/p2=[p2,2]}", records.toString());

		pathGenerator = committer.createGeneratorAndCleanDir(0, 2);
		writer = new DynamicPartitionWriter<>(context, pathGenerator, computer, maker);
		writer.write(Row.of("p4", 5));
		writer.write(Row.of("p3", 3));
		writer.write(Row.of("p5", 2));
		writer.close();
		Assert.assertEquals(
				"{cp-1/p1=[p1,1, p1,2], cp-2/p4=[p4,5], cp-1/p2=[p2,2], cp-2/p3=[p3,3], cp-2/p5=[p5,2]}",
				records.toString());
	}
}
