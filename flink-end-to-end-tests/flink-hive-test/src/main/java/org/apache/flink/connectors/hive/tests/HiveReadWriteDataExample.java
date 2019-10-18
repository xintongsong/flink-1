/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.connectors.hive.tests;

import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.SqlDialect;
import org.apache.flink.table.api.TableEnvironment;
import org.apache.flink.table.api.config.ExecutionConfigOptions;
import org.apache.flink.table.catalog.hive.HiveCatalog;

/**
 * End to end test for HiveConnector.
 */
public class HiveReadWriteDataExample {
	public static void main(String[] args) throws Exception {
		EnvironmentSettings settings = EnvironmentSettings.newInstance().useBlinkPlanner().inBatchMode().build();
		TableEnvironment tableEnv = TableEnvironment.create(settings);
		tableEnv.getConfig().getConfiguration().setInteger(ExecutionConfigOptions.TABLE_EXEC_RESOURCE_DEFAULT_PARALLELISM.key(), 1);
		tableEnv.getConfig().setSqlDialect(SqlDialect.HIVE);

		String catalogName = "hive";
		String defaultDatabase = "default";
		String hiveConfDir = "/usr/local/hive/conf";
		String version = "2.3.4";
		HiveCatalog hiveCatalog = new HiveCatalog(catalogName, defaultDatabase, hiveConfDir, version);

		tableEnv.registerCatalog("hive", hiveCatalog);
		tableEnv.sqlUpdate("insert into hive.`default`.dest_non_partition_table " +
						"select * from hive.`default`.non_partition_table");
		tableEnv.execute("HiveReadWriteDataExample");
	}
}
