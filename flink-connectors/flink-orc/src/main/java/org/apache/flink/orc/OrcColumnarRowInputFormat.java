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
import org.apache.flink.orc.vector.AbstractOrcColumnVector;
import org.apache.flink.table.dataformat.BaseRow;
import org.apache.flink.table.dataformat.ColumnarRow;
import org.apache.flink.table.dataformat.vector.VectorizedColumnBatch;

import org.apache.hadoop.conf.Configuration;
import org.apache.orc.TypeDescription;

import java.io.IOException;
import java.util.Arrays;

/**
 * InputFormat to read ORC files into {@link BaseRow}.
 */
public class OrcColumnarRowInputFormat extends OrcInputFormat<BaseRow> {

	// the number of rows read in a batch
	private static final int DEFAULT_BATCH_SIZE = 2048;

	// the vector of rows that is read in a batch
	private transient VectorizedColumnBatch columnarBatch;

	private transient ColumnarRow row;

	/**
	 * Creates an OrcRowInputFormat.
	 *
	 * @param path The path to read ORC files from.
	 * @param orcSchema The schema of the ORC files as ORC TypeDescription.
	 * @param orcConfig The configuration to read the ORC files with.
	 */
	public OrcColumnarRowInputFormat(Path path, TypeDescription orcSchema, Configuration orcConfig) {
		this(path, orcSchema, orcConfig, DEFAULT_BATCH_SIZE);
	}

	/**
	 * Creates an OrcRowInputFormat.
	 *
	 * @param path The path to read ORC files from.
	 * @param orcSchema The schema of the ORC files as ORC TypeDescription.
	 * @param orcConfig The configuration to read the ORC files with.
	 * @param batchSize The number of Row objects to read in a batch.
	 */
	public OrcColumnarRowInputFormat(Path path, TypeDescription orcSchema, Configuration orcConfig, int batchSize) {
		super(path, orcSchema, orcConfig, batchSize);
	}

	@Override
	public void open(FileInputSplit fileSplit) throws IOException {
		super.open(fileSplit);
		// create and initialize the row batch
		AbstractOrcColumnVector[] vectors = Arrays.stream(selectedFields)
				.mapToObj(i -> rowBatch.cols[i])
				.map(AbstractOrcColumnVector::createVector)
				.toArray(AbstractOrcColumnVector[]::new);
		this.columnarBatch = new VectorizedColumnBatch(vectors);
		this.row = new ColumnarRow(columnarBatch);
	}

	@Override
	public void closeInputFormat() throws IOException {
		this.columnarBatch = null;
		super.closeInputFormat();
	}

	@Override
	public int fillRows() {
		int size = rowBatch.size;
		columnarBatch.setNumRows(size);
		return size;
	}

	@Override
	public BaseRow nextRecord(BaseRow reuse) throws IOException {
		// return the next row
		row.setRowId(this.nextRow++);
		return row;
	}
}
