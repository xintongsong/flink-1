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

import org.apache.flink.annotation.Internal;
import org.apache.flink.api.common.typeutils.SimpleTypeSerializerSnapshot;
import org.apache.flink.api.common.typeutils.TypeSerializerSnapshot;
import org.apache.flink.api.common.typeutils.base.TypeSerializerSingleton;
import org.apache.flink.core.memory.DataInputView;
import org.apache.flink.core.memory.DataOutputView;

import java.io.IOException;
import java.sql.Timestamp;
import java.util.TimeZone;

/**
 * Takes long instead of a long and a int as the serialized value. It not only reduces the length of
 * the serialized value, but also makes the serialized value consistent between
 * the legacy planner and the blink planner.
 */
@Internal
public class TimestampSerializer extends TypeSerializerSingleton<Timestamp> {

	private static final long serialVersionUID = 1L;

	private static final TimeZone LOCAL_TZ = TimeZone.getDefault();

	public static final TimestampSerializer INSTANCE = new TimestampSerializer();

	@Override
	public boolean isImmutableType() {
		return false;
	}

	@Override
	public Timestamp createInstance() {
		return new Timestamp(0L);
	}

	@Override
	public Timestamp copy(Timestamp from) {
		if (from == null) {
			return null;
		}
		return new Timestamp(from.getTime());
	}

	@Override
	public Timestamp copy(Timestamp from, Timestamp reuse) {
		if (from == null) {
			return null;
		}
		reuse.setTime(from.getTime());
		return reuse;
	}

	@Override
	public int getLength() {
		return 8;
	}

	@Override
	public void serialize(Timestamp record, DataOutputView target) throws IOException {
		if (record == null) {
			throw new IllegalArgumentException("The Timestamp record must not be null.");
		}
		target.writeLong(timestampToInternal(record));
	}

	@Override
	public Timestamp deserialize(DataInputView source) throws IOException {
		return internalToTimestamp(source.readLong());
	}

	private long timestampToInternal(Timestamp ts) {
		long time = ts.getTime();
		return time + LOCAL_TZ.getOffset(time);
	}

	public Timestamp internalToTimestamp(long v) {
		return new Timestamp(v - LOCAL_TZ.getOffset(v));
	}

	@Override
	public Timestamp deserialize(Timestamp reuse, DataInputView source) throws IOException {
		return deserialize(source);
	}

	@Override
	public void copy(DataInputView source, DataOutputView target) throws IOException {
		serialize(deserialize(source), target);
	}

	@Override
	public TypeSerializerSnapshot<Timestamp> snapshotConfiguration() {
		return new TimestampSerializerSnapshot();
	}

	/**
	 * Serializer configuration snapshot for compatibility and format evolution.
	 */
	@SuppressWarnings("WeakerAccess")
	public static final class TimestampSerializerSnapshot extends SimpleTypeSerializerSnapshot<Timestamp> {

		public TimestampSerializerSnapshot() {
			super(() -> INSTANCE);
		}
	}
}
