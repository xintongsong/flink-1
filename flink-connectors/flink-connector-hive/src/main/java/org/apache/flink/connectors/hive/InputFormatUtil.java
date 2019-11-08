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

package org.apache.flink.connectors.hive;

import org.apache.flink.api.common.io.InputFormat;
import org.apache.flink.core.fs.Path;
import org.apache.flink.orc.OrcColumnarRowInputFormat;
import org.apache.flink.table.api.TableSchema;
import org.apache.flink.table.catalog.CatalogTable;
import org.apache.flink.table.dataformat.BaseRow;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.metastore.api.FieldSchema;
import org.apache.hadoop.hive.metastore.api.StorageDescriptor;
import org.apache.hadoop.hive.ql.io.orc.OrcInputFormat;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfoUtils;
import org.apache.hadoop.mapred.JobConf;
import org.apache.orc.TypeDescription;

import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

/**
 * Util to create hive input format.
 */
class InputFormatUtil {

	private static boolean useOrcVectorizedRead(
			CatalogTable catalogTable,
			List<HiveTablePartition> partitions,
			int[] projectedFields) {
		TableSchema schema = catalogTable.getSchema();
		boolean support = Arrays.stream(schema.getFieldDataTypes()).allMatch(t -> {
			switch (t.getLogicalType().getTypeRoot()) {
				case CHAR:
				case VARCHAR:
				case BOOLEAN:
				case BINARY:
				case VARBINARY:
				case DECIMAL:
				case TINYINT:
				case SMALLINT:
				case INTEGER:
				case BIGINT:
				case FLOAT:
				case DOUBLE:
				case DATE:
				case TIME_WITHOUT_TIME_ZONE:
					return true;
				default:
					return false;
			}
		});

		boolean hasPartField = projectedFields == null ?
				!catalogTable.getPartitionKeys().isEmpty() :
				Arrays.stream(projectedFields).anyMatch(i ->
						catalogTable.getPartitionKeys().contains(
								catalogTable.getSchema().getFieldNames()[i]));
		if (hasPartField) {
			support = false;
		}

		if (partitions.isEmpty()) {
			support = false;
		}

		String inputFormat = null;
		Map<String, String> parameters = null;
		for (HiveTablePartition partition : partitions) {
			StorageDescriptor sd = partition.getStorageDescriptor();
			boolean isOrc = sd.getSerdeInfo().getSerializationLib().toLowerCase().contains("orc");
			if (!isOrc) {
				support = false;
			}

			if (inputFormat != null && !inputFormat.equals(sd.getInputFormat())) {
				support = false;
			} else {
				inputFormat = sd.getInputFormat();
			}

			if (parameters != null && !parameters.equals(sd.getSerdeInfo().getParameters())) {
				support = false;
			} else {
				parameters = sd.getSerdeInfo().getParameters();
			}
		}

		return support;
	}

	static InputFormat<BaseRow, ?> createInputFormat(
			JobConf jobConf,
			CatalogTable catalogTable,
			List<HiveTablePartition> partitions,
			int[] projectedFields) {
		if (!useOrcVectorizedRead(catalogTable, partitions, projectedFields)) {
			return new HiveTableInputFormat(jobConf, catalogTable, partitions, projectedFields);
		}

		StorageDescriptor sd = partitions.get(0).getStorageDescriptor();
		List<FieldSchema> cols = sd.getCols();
		List<TypeDescription> typeDescriptions = cols.stream()
				.map(FieldSchema::getType)
				.map(TypeInfoUtils::getTypeInfoFromTypeString)
				.map(OrcInputFormat::convertTypeInfo)
				.collect(Collectors.toList());
		TypeDescription typeDescription = TypeDescription.createStruct();
		for (int i = 0; i < typeDescriptions.size(); i++) {
			typeDescription.addField(cols.get(i).getName(), typeDescriptions.get(i));
		}

		Configuration conf = new Configuration(jobConf);
		sd.getSerdeInfo().getParameters().forEach(conf::set);

		OrcColumnarRowInputFormat format = new OrcColumnarRowInputFormat(null, typeDescription, conf);
		format.setFilePaths(partitions.stream().map(p ->
				new Path(p.getStorageDescriptor().getLocation())).toArray(Path[]::new));
		if (projectedFields != null) {
			format.selectFields(projectedFields);
		}
		return format;
	}
}
