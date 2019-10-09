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

import org.apache.flink.api.common.io.OutputFormat;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.core.fs.FileSystem;
import org.apache.flink.core.fs.Path;
import org.apache.flink.streaming.api.functions.sink.OutputFormatSinkFunction;
import org.apache.flink.streaming.api.operators.StreamSink;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;
import org.apache.flink.streaming.util.OneInputStreamOperatorTestHarness;
import org.apache.flink.types.Row;

import org.apache.commons.io.FileUtils;
import org.junit.Before;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import java.io.File;
import java.io.IOException;
import java.io.OutputStreamWriter;
import java.io.Writer;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;
import java.util.TreeMap;
import java.util.concurrent.atomic.AtomicReference;

import static org.junit.Assert.assertEquals;

/**
 * Test for {@link FileSystemOutputFormat}.
 */
public class FileSystemOutputFormatTest {

	@ClassRule
	public static final TemporaryFolder TEMP_FOLDER = new TemporaryFolder();

	private File tmpFile;
	private File outputFile;

	private static Map<File, String> getFileContentByPath(File directory) throws IOException {
		Map<File, String> contents = new HashMap<>(4);

		if (!directory.exists() || !directory.isDirectory()) {
			return contents;
		}

		final Collection<File> filesInBucket = FileUtils.listFiles(directory, null, true);
		for (File file : filesInBucket) {
			contents.put(file, FileUtils.readFileToString(file));
		}
		return contents;
	}

	@Before
	public void before() throws IOException {
		tmpFile = TEMP_FOLDER.newFolder();
		outputFile = TEMP_FOLDER.newFolder();
	}

	@Test
	public void testClosingWithoutInput() throws Exception {
		try (OneInputStreamOperatorTestHarness<Row, Object> testHarness = createSink(
				false, false, false, false, new HashMap<>(), new AtomicReference<>())) {
			testHarness.setup();
			testHarness.open();
		}
	}

	@Test
	public void testNonPartition() throws Exception {
		AtomicReference<FileSystemOutputFormat<Row>> ref = new AtomicReference<>();
		try (OneInputStreamOperatorTestHarness<Row, Object> testHarness = createSink(
				false, false, false, false, new HashMap<>(), ref)) {
			writeUnorderedRecords(testHarness);
			assertEquals(1, getFileContentByPath(new File(tmpFile, "cp-0")).size());
		}

		ref.get().finalizeGlobal(1);
		Map<File, String> content = getFileContentByPath(outputFile);
		assertEquals(1, content.size());
		assertEquals(
				"a1,1,p1\n" + "a2,2,p1\n" + "a2,2,p2\n" + "a3,3,p1\n",
				content.values().iterator().next());
	}

	private void writeUnorderedRecords(
			OneInputStreamOperatorTestHarness<Row, Object> testHarness) throws Exception {
		testHarness.setup();
		testHarness.open();

		testHarness.processElement(new StreamRecord<>(Row.of("a1", 1, "p1"), 1L));
		testHarness.processElement(new StreamRecord<>(Row.of("a2", 2, "p1"), 1L));
		testHarness.processElement(new StreamRecord<>(Row.of("a2", 2, "p2"), 1L));
		testHarness.processElement(new StreamRecord<>(Row.of("a3", 3, "p1"), 1L));
	}

	@Test
	public void testOverrideNonPartition() throws Exception {
		testNonPartition();

		AtomicReference<FileSystemOutputFormat<Row>> ref = new AtomicReference<>();
		try (OneInputStreamOperatorTestHarness<Row, Object> testHarness = createSink(
				true, false, false, false, new HashMap<>(), ref)) {
			writeUnorderedRecords(testHarness);
			assertEquals(1, getFileContentByPath(new File(tmpFile, "cp-0")).size());
		}

		ref.get().finalizeGlobal(1);
		Map<File, String> content = getFileContentByPath(outputFile);
		assertEquals(1, content.size());
		assertEquals(
				"a1,1,p1\n" + "a2,2,p1\n" + "a2,2,p2\n" + "a3,3,p1\n",
				content.values().iterator().next());
	}

	@Test
	public void testStaticPartition() throws Exception {
		AtomicReference<FileSystemOutputFormat<Row>> ref = new AtomicReference<>();
		Map<String, String> staticParts = new HashMap<>();
		staticParts.put("c", "p1");
		try (OneInputStreamOperatorTestHarness<Row, Object> testHarness = createSink(
				false, true, false, false, staticParts, ref)) {
			testHarness.setup();
			testHarness.open();

			testHarness.processElement(new StreamRecord<>(Row.of("a1", 1), 1L));
			testHarness.processElement(new StreamRecord<>(Row.of("a2", 2), 1L));
			testHarness.processElement(new StreamRecord<>(Row.of("a2", 2), 1L));
			testHarness.processElement(new StreamRecord<>(Row.of("a3", 3), 1L));
			assertEquals(1, getFileContentByPath(new File(tmpFile, "cp-0")).size());
		}

		ref.get().finalizeGlobal(1);
		Map<File, String> content = getFileContentByPath(outputFile);
		assertEquals(1, content.size());
		assertEquals("c=p1", content.keySet().iterator().next().getParentFile().getName());
		assertEquals(
				"a1,1\n" + "a2,2\n" + "a2,2\n" + "a3,3\n",
				content.values().iterator().next());
	}

	@Test
	public void testDynamicPartition() throws Exception {
		AtomicReference<FileSystemOutputFormat<Row>> ref = new AtomicReference<>();
		try (OneInputStreamOperatorTestHarness<Row, Object> testHarness = createSink(
				false, true, true, false, new HashMap<>(), ref)) {
			writeUnorderedRecords(testHarness);
			assertEquals(2, getFileContentByPath(new File(tmpFile, "cp-0")).size());
		}

		ref.get().finalizeGlobal(1);
		Map<File, String> content = getFileContentByPath(outputFile);
		Map<String, String> sortedContent = new TreeMap<>();
		content.forEach((file, s) -> sortedContent.put(file.getParentFile().getName(), s));

		assertEquals(2, sortedContent.size());
		assertEquals("a1,1\n" + "a2,2\n" + "a3,3\n", sortedContent.get("c=p1"));
		assertEquals("a2,2\n", sortedContent.get("c=p2"));
	}

	@Test
	public void testGroupedDynamicPartition() throws Exception {
		AtomicReference<FileSystemOutputFormat<Row>> ref = new AtomicReference<>();
		try (OneInputStreamOperatorTestHarness<Row, Object> testHarness = createSink(
				false, true, true, true, new HashMap<>(), ref)) {
			testHarness.setup();
			testHarness.open();

			testHarness.processElement(new StreamRecord<>(Row.of("a1", 1, "p1"), 1L));
			testHarness.processElement(new StreamRecord<>(Row.of("a2", 2, "p1"), 1L));
			testHarness.processElement(new StreamRecord<>(Row.of("a3", 3, "p1"), 1L));
			testHarness.processElement(new StreamRecord<>(Row.of("a2", 2, "p2"), 1L));
			assertEquals(2, getFileContentByPath(new File(tmpFile, "cp-0")).size());
		}

		ref.get().finalizeGlobal(1);
		Map<File, String> content = getFileContentByPath(outputFile);
		Map<String, String> sortedContent = new TreeMap<>();
		content.forEach((file, s) -> sortedContent.put(file.getParentFile().getName(), s));

		assertEquals(2, sortedContent.size());
		assertEquals("a1,1\n" + "a2,2\n" + "a3,3\n", sortedContent.get("c=p1"));
		assertEquals("a2,2\n", sortedContent.get("c=p2"));
	}

	private OneInputStreamOperatorTestHarness<Row, Object> createSink(
			boolean override,
			boolean partition,
			boolean dynamicPartition,
			boolean grouped,
			Map<String, String> staticPartitions,
			AtomicReference<FileSystemOutputFormat<Row>> sinkRef) throws Exception {
		String[] columnNames = new String[]{"a", "b", "c"};
		String[] partitionColumns = partition ? new String[]{"c"} : new String[0];
		RowPartitionComputer computer = new RowPartitionComputer(columnNames, partitionColumns, "default");

		OutputFormatFactory<Row> factory = (OutputFormatFactory<Row>) path -> new OutputFormat<Row>() {

			private transient Writer writer;

			@Override
			public void configure(Configuration parameters) {
			}

			@Override
			public void open(int taskNumber, int numTasks) throws IOException {
				writer = new OutputStreamWriter(
						path.getFileSystem().create(path, FileSystem.WriteMode.OVERWRITE));
			}

			@Override
			public void writeRecord(Row record) throws IOException {
				writer.append(record.toString()).append("\n");
			}

			@Override
			public void close() throws IOException {
				writer.close();
			}
		};

		FileSystemFileCommitter committer = new FileSystemFileCommitter(
				override,
				new Path(tmpFile.getPath()),
				new Path(outputFile.getPath()),
				staticPartitions,
				partitionColumns);

		FileSystemOutputFormat<Row> sink = new FileSystemOutputFormat<>(
				computer,
				PartitionWriterFactory.get(dynamicPartition, grouped),
				factory,
				committer);

		sinkRef.set(sink);

		return new OneInputStreamOperatorTestHarness<>(
				new StreamSink<>(new OutputFormatSinkFunction<>(sink)),
				1, 1, 0);
	}
}
