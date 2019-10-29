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

package org.apache.flink.runtime.io;

import org.apache.flink.annotation.Internal;

import java.util.concurrent.CompletableFuture;

/**
 * Interface defining couple of essential methods for listening on data availability using
 * {@link CompletableFuture}. For usage check out for example {@link PullingAsyncDataInput}.
 */
@Internal
public interface AvailabilityProvider {
	/**
	 * Constant that allows to avoid volatile checks {@link CompletableFuture#isDone()}. Check
	 * {@link #isAvailable()} and {@link #isVolatileAvailable()}for more explanation.
	 */
	CompletableFuture<?> AVAILABLE = CompletableFuture.completedFuture(null);

	/**
	 * Checks whether this instance is available via constant {@link #AVAILABLE} to avoid volatile access.
	 *
	 * @return true if this instance is available for further processing.
	 */
	default boolean isAvailable() {
		return getAvailableFuture() == AVAILABLE;
	}

	/**
	 * In order to avoid volatile access in {@link CompletableFuture#isDone()}, we check the condition
	 * of <code>future == AVAILABLE</code> firstly, so it would get performance benefits when hot looping.
	 *
	 * @return true if this instance is available for further processing.
	 */
	default boolean isVolatileAvailable() {
		CompletableFuture<?> future = getAvailableFuture();
		return future == AVAILABLE || future.isDone();
	}

	/**
	 * @return a future that is completed if the respective provider is available.
	 */
	CompletableFuture<?> getAvailableFuture();

	/**
	 * A availability implementation for providing the helpful functions of resetting the
	 * available/unavailable states.
	 */
	final class AvailabilityHelper implements AvailabilityProvider {

		private CompletableFuture<?> availableFuture = new CompletableFuture<>();

		/**
		 * Judges to reset the current available state as unavailable.
		 */
		public void resetUnavailable() {
			if (isVolatileAvailable()) {
				availableFuture = new CompletableFuture<>();
			}
		}

		/**
		 * Resets the constant completed {@link #AVAILABLE} as the current state.
		 */
		public void resetAvailable() {
			availableFuture = AVAILABLE;
		}

		/**
		 *  Returns the previously not completed future and resets the constant completed
		 *  {@link #AVAILABLE} as the current state.
		 */
		public CompletableFuture<?> getUnavailableToResetAvailable() {
			CompletableFuture<?> toNotify = availableFuture;
			availableFuture = AVAILABLE;
			return toNotify;
		}

		@Override
		public CompletableFuture<?> getAvailableFuture() {
			return availableFuture;
		}
	}
}
