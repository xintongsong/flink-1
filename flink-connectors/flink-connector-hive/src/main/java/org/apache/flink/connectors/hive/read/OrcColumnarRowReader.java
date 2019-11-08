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

package org.apache.flink.connectors.hive.read;

import org.apache.flink.core.fs.Path;
import org.apache.flink.orc.OrcReader;
import org.apache.flink.orc.vector.AbstractOrcColumnVector;
import org.apache.flink.table.dataformat.BaseRow;
import org.apache.flink.table.dataformat.ColumnarRow;
import org.apache.flink.table.dataformat.vector.ColumnVector;
import org.apache.flink.table.dataformat.vector.VectorizedColumnBatch;
import org.apache.flink.table.dataformat.vector.heap.AbstractHeapVector;
import org.apache.flink.table.types.DataType;

import org.apache.hadoop.conf.Configuration;
import org.apache.orc.TypeDescription;

import java.io.IOException;
import java.util.List;
import java.util.Map;

/**
 * {@link OrcReader} to read ORC files into {@link BaseRow}.
 */
public class OrcColumnarRowReader extends OrcReader<BaseRow> {

	// the vector of rows that is read in a batch
	private final VectorizedColumnBatch columnarBatch;

	private final ColumnarRow row;

	public OrcColumnarRowReader(
			Configuration conf,
			TypeDescription schema,
			int[] selectedFields,
			String[] fieldNames,
			DataType[] fieldTypes,
			List<String> partitionKeys,
			Map<String, Object> partitionSpec,
			List<Predicate> conjunctPredicates,
			int batchSize,
			Path path,
			long splitStart,
			long splitEnd) throws IOException {
		super(conf, schema, selectedFields, conjunctPredicates, batchSize, path, splitStart, splitEnd);
		// create and initialize the row batch
		ColumnVector[] vectors = new ColumnVector[selectedFields.length];
		for (int i = 0; i < vectors.length; i++) {
			String name = fieldNames[i];
			if (partitionKeys.contains(name)) {
				DataType type = fieldTypes[i];
				AbstractHeapVector heapVector = AbstractHeapVector.createHeapColumn(
						type.getLogicalType(), batchSize);
				AbstractHeapVector.fill(
						heapVector,
						type.getLogicalType(),
						partitionSpec.get(name));
				vectors[i] = heapVector;
			} else {
				vectors[i] = AbstractOrcColumnVector.createVector(rowBatch.cols[i]);
			}
		}
		this.columnarBatch = new VectorizedColumnBatch(vectors);
		this.row = new ColumnarRow(columnarBatch);
	}

	@Override
	protected int fillRows() {
		int size = rowBatch.size;
		columnarBatch.setNumRows(size);
		return size;
	}

	@Override
	public BaseRow nextRecord(BaseRow reuse) {
		// return the next row
		row.setRowId(this.nextRow++);
		return row;
	}
}
