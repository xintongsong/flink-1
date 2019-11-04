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

package org.apache.flink.orc;

import org.apache.flink.core.fs.FileInputSplit;
import org.apache.flink.core.fs.Path;
import org.apache.flink.table.dataformat.BaseRow;
import org.apache.flink.util.InstantiationUtil;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.ql.io.sarg.PredicateLeaf;
import org.apache.orc.TypeDescription;
import org.junit.After;
import org.junit.Test;

import java.io.IOException;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;

/**
 * Unit tests for {@link OrcColumnarRowInputFormat}.
 */
public class OrcColumnarRowInputFormatTest {

	private OrcColumnarRowInputFormat rowOrcInputFormat;

	@After
	public void tearDown() throws IOException {
		if (rowOrcInputFormat != null) {
			rowOrcInputFormat.close();
			rowOrcInputFormat.closeInputFormat();
		}
		rowOrcInputFormat = null;
	}

	private static final String TEST_FILE_FLAT = "test-data-flat.orc";
	private static final TypeDescription TEST_SCHEMA_FLAT = TypeDescription.fromString(
			"struct<_col0:int,_col1:string,_col2:string,_col3:string,_col4:int,_col5:string,_col6:int,_col7:int,_col8:int>");

	@Test
	public void testSerialization() throws Exception {
		rowOrcInputFormat = new OrcColumnarRowInputFormat(
				new Path(getPath(TEST_FILE_FLAT)), TEST_SCHEMA_FLAT, new Configuration());

		rowOrcInputFormat.selectFields(0, 4, 1);
		rowOrcInputFormat.addPredicate(
			new OrcColumnarRowInputFormat.Equals("_col1", PredicateLeaf.Type.STRING, "M"));

		byte[] bytes = InstantiationUtil.serializeObject(rowOrcInputFormat);
		OrcColumnarRowInputFormat copy = InstantiationUtil.deserializeObject(bytes, getClass().getClassLoader());

		FileInputSplit[] splits = copy.createInputSplits(1);
		copy.openInputFormat();
		copy.open(splits[0]);
		assertFalse(copy.reachedEnd());
		BaseRow row = copy.nextRecord(null);

		assertNotNull(row);
		assertEquals(3, row.getArity());
		// check first row
		assertEquals(1, row.getInt(0));
		assertEquals(500, row.getInt(1));
		assertEquals("M", row.getString(2).toString());
	}

	@Test
	public void testReadFileInSplits() throws IOException {

		rowOrcInputFormat = new OrcColumnarRowInputFormat(new Path(getPath(TEST_FILE_FLAT)), TEST_SCHEMA_FLAT, new Configuration());
		rowOrcInputFormat.selectFields(0, 1);

		FileInputSplit[] splits = rowOrcInputFormat.createInputSplits(4);
		assertEquals(4, splits.length);
		rowOrcInputFormat.openInputFormat();

		long cnt = 0;
		// read all splits
		for (FileInputSplit split : splits) {

			// open split
			rowOrcInputFormat.open(split);
			// read and count all rows
			while (!rowOrcInputFormat.reachedEnd()) {
				assertNotNull(rowOrcInputFormat.nextRecord(null));
				cnt++;
			}
		}
		// check that all rows have been read
		assertEquals(1920800, cnt);
	}

	@Test
	public void testReadFileWithFilter() throws IOException {

		rowOrcInputFormat = new OrcColumnarRowInputFormat(new Path(getPath(TEST_FILE_FLAT)), TEST_SCHEMA_FLAT, new Configuration());
		rowOrcInputFormat.selectFields(0, 1);

		// read head and tail of file
		rowOrcInputFormat.addPredicate(
			new OrcColumnarRowInputFormat.Or(
				new OrcColumnarRowInputFormat.LessThan("_col0", PredicateLeaf.Type.LONG, 10L),
				new OrcColumnarRowInputFormat.Not(
					new OrcColumnarRowInputFormat.LessThanEquals("_col0", PredicateLeaf.Type.LONG, 1920000L))
			));
		rowOrcInputFormat.addPredicate(
			new OrcColumnarRowInputFormat.Equals("_col1", PredicateLeaf.Type.STRING, "M"));

		FileInputSplit[] splits = rowOrcInputFormat.createInputSplits(1);
		assertEquals(1, splits.length);
		rowOrcInputFormat.openInputFormat();

		// open split
		rowOrcInputFormat.open(splits[0]);

		// read and count all rows
		long cnt = 0;
		while (!rowOrcInputFormat.reachedEnd()) {
			assertNotNull(rowOrcInputFormat.nextRecord(null));
			cnt++;
		}
		// check that only the first and last stripes of the file have been read.
		// Each stripe has 5000 rows, except the last which has 800 rows.
		assertEquals(5800, cnt);
	}

	@Test
	public void testReadFileWithEvolvedSchema() throws IOException {

		rowOrcInputFormat = new OrcColumnarRowInputFormat(
				new Path(getPath(TEST_FILE_FLAT)),
				TypeDescription.fromString("struct<_col0:int,_col1:string,_col4:string,_col3:string>"),
			new Configuration());
		rowOrcInputFormat.selectFields(3, 0, 2);

		rowOrcInputFormat.addPredicate(
			new OrcColumnarRowInputFormat.LessThan("_col0", PredicateLeaf.Type.LONG, 10L));

		FileInputSplit[] splits = rowOrcInputFormat.createInputSplits(1);
		assertEquals(1, splits.length);
		rowOrcInputFormat.openInputFormat();

		// open split
		rowOrcInputFormat.open(splits[0]);

		// read and validate first row
		assertFalse(rowOrcInputFormat.reachedEnd());
		BaseRow row = rowOrcInputFormat.nextRecord(null);
		assertNotNull(row);
		assertEquals(3, row.getArity());
		assertEquals("Primary", row.getString(0).toString());
		assertEquals(1, row.getInt(1));
		assertEquals("M", row.getString(2).toString());

		// read and count remaining rows
		long cnt = 1;
		while (!rowOrcInputFormat.reachedEnd()) {
			assertNotNull(rowOrcInputFormat.nextRecord(null));
			cnt++;
		}
		// check that only the first and last stripes of the file have been read.
		// Each stripe has 5000 rows, except the last which has 800 rows.
		assertEquals(5000, cnt);
	}

	private String getPath(String fileName) {
		return getClass().getClassLoader().getResource(fileName).getPath();
	}
}
