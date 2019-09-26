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

package org.apache.flink.yarn;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.source.RichSourceFunction;

import java.util.List;

/**
 * Parallel data source that produces no data and check the distributed cache accessibility i.e., finishes immediately.
 */
public class CacheBasedDataSource extends RichSourceFunction {
	private List<String> cacheFiles;

	public CacheBasedDataSource(List<String> cacheFiles) {
		this.cacheFiles = cacheFiles;
	}

	@Override
	public void open(Configuration config) throws Exception {
		for (String filePath : cacheFiles) {
			getRuntimeContext().getDistributedCache().getFile(filePath);
		}
	}

	@Override
	public void run(SourceContext ctx) throws Exception {}

	@Override
	public void cancel() {}
}
