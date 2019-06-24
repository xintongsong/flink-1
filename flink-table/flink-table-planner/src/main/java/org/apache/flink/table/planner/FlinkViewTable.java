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

package org.apache.flink.table.planner;

import org.apache.flink.table.api.SqlDialect;
import org.apache.flink.table.api.TableConfig;
import org.apache.flink.table.api.ValidationException;

import org.apache.calcite.plan.RelOptTable;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.type.RelProtoDataType;
import org.apache.calcite.schema.impl.ViewTable;

import java.util.List;

import static org.apache.flink.util.Preconditions.checkNotNull;

/**
 * Thin extension to {@link ViewTable} that performs {@link SqlDialect} check
 * before expanding the view. It throws exception if the view's dialect is different
 * from configured in the {@link TableConfig}.
 */
public class FlinkViewTable extends ViewTable {
	private final SqlDialect viewDialect;

	public FlinkViewTable(
			SqlDialect viewDialect,
			RelProtoDataType rowType,
			String viewSql,
			List<String> schemaPath,
			List<String> viewPath) {
		super(null, rowType, viewSql, schemaPath, viewPath);
		this.viewDialect = checkNotNull(viewDialect);
	}

	@Override
	public RelNode toRel(RelOptTable.ToRelContext context, RelOptTable relOptTable) {
		TableConfig tableConfig = context.getCluster().getPlanner().getContext().unwrap(TableConfig.class);
		SqlDialect sqlDialect = tableConfig.getSqlDialect();
		if (!viewDialect.equals(sqlDialect)) {
			throw new ValidationException(String.format(
				"Can not query a view with %s conformance, with sql parser set to %s conformance",
				viewDialect,
				sqlDialect));
		}

		return super.toRel(context, relOptTable);
	}
}
