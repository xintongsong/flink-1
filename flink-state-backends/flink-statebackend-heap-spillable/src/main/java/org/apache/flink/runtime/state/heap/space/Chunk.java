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

package org.apache.flink.runtime.state.heap.space;

import java.nio.ByteBuffer;

/**
 * A chunk is a logically contiguous space backed by {@link ByteBuffer}. It maybe comprised
 * of multiple physical spaces. For example, for a chunk with 1GB space, it maybe a file, or
 * contains multiple 4MB segments on heap.
 */
public interface Chunk {

	/**
	 * Allocate a space with the given size from the chunk.
	 *
	 * @param size size of the space to allocate.
	 * @return offset of space in the chunk, or -1 that indicates
	 * there is no enough space.
	 */
	int allocate(int size);

	/**
	 * Release the space with the offset in the chunk.
	 *
	 * @param offset offset of the space in the chunk.
	 */
	void free(int offset);

	/**
	 * Returns the id of this chunk.
	 *
	 * @return id of this chunk.
	 */
	int getChunkId();

	/**
	 * Return the capacity of this chunk.
	 */
	int getChunkCapacity();

	/**
	 * Returns the backed {@link ByteBuffer} for the space with the offset.
	 *
	 * @param offsetInChunk offset of space in the chunk.
	 * @return byte buffer backed the space.
	 */
	ByteBuffer getByteBuffer(int offsetInChunk);

	/**
	 * Returns the offset of the space in the backed {@link ByteBuffer}.
	 *
	 * @param offsetInChunk offset of space in the chunk.
	 * @return offset of space in the byte buffer..
	 */
	int getOffsetInByteBuffer(int offsetInChunk);
}
