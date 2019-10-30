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

package org.apache.flink.table.runtime.typeutils.serializers.python;

import org.apache.flink.api.common.typeutils.SerializerTestBase;
import org.apache.flink.api.common.typeutils.TypeSerializer;

import java.sql.Timestamp;

/**
 * Test for {@link TimestampSerializer}.
 */
public class TimestampSerializerTest extends SerializerTestBase<Timestamp> {
	@Override
	protected TypeSerializer<Timestamp> createSerializer() {
		return TimestampSerializer.INSTANCE;
	}

	@Override
	protected int getLength() {
		return 8;
	}

	@Override
	protected Class<Timestamp> getTypeClass() {
		return Timestamp.class;
	}

	@Override
	protected Timestamp[] getTestData() {
		return new Timestamp[]{Timestamp.valueOf("2019-09-10 18:00:00")};
	}
}
