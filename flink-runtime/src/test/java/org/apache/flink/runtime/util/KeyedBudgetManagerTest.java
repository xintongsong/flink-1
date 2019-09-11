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

package org.apache.flink.runtime.util;

import org.apache.flink.types.Either;
import org.apache.flink.util.Preconditions;

import org.junit.Test;

import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.stream.LongStream;

import static org.hamcrest.CoreMatchers.is;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.fail;

/**
 * Test suite for {@link KeyedBudgetManager}.
 */
@SuppressWarnings("MagicNumber")
public class KeyedBudgetManagerTest {
	private static final String[] TEST_KEYS = {"k1", "k2", "k3", "k4"};
	private static final long[] TEST_BUDGETS = {15, 17, 22, 11};

	@Test
	public void testSuccessfulAcquisitionForKey() {
		KeyedBudgetManager<String> keyedBudgetManager = createSimpleKeyedBudget();

		long acquired = keyedBudgetManager.acquireBudgetForKey("k1", 10L);

		assertThat(acquired, is(10L));
		checkOneKeyBudgetChange(keyedBudgetManager, "k1", 5L);
	}

	@Test
	public void testFailedAcquisitionForKey() {
		KeyedBudgetManager<String> keyedBudgetManager = createSimpleKeyedBudget();

		long maxPossibleBudgetToAcquire = keyedBudgetManager.acquireBudgetForKey("k1", 20L);

		assertThat(maxPossibleBudgetToAcquire, is(15L));
		checkNoKeyBudgetChange(keyedBudgetManager);
	}

	@Test
	public void testSuccessfulReleaseForKey() {
		KeyedBudgetManager<String> keyedBudgetManager = createSimpleKeyedBudget();

		keyedBudgetManager.acquireBudgetForKey("k1", 10L);
		keyedBudgetManager.releaseBudgetForKey("k1", 5L);

		checkOneKeyBudgetChange(keyedBudgetManager, "k1", 10L);
	}

	@Test
	public void testFailedReleaseForKey() {
		KeyedBudgetManager<String> keyedBudgetManager = createSimpleKeyedBudget();

		keyedBudgetManager.acquireBudgetForKey("k1", 10L);
		try {
			keyedBudgetManager.releaseBudgetForKey("k1", 15L);
			fail("IllegalStateException is expected to fail over-sized release");
		} catch (IllegalStateException e) {
			// expected
		}

		checkOneKeyBudgetChange(keyedBudgetManager, "k1", 5L);
	}

	@Test
	public void testSuccessfulAcquisitionForKeys() {
		KeyedBudgetManager<String> keyedBudgetManager = createSimpleKeyedBudget();

		Either<Map<String, Long>, Long> acquired =
			keyedBudgetManager.acquirePagedBudgetForKeys(Arrays.asList("k2", "k3"), 4, 5);

		assertThat(acquired.isLeft(), is(true));
		assertThat(acquired.left().values().stream().mapToLong(b -> b).sum(), is(4L));

		assertThat(keyedBudgetManager.availableBudgetForKey("k1"), is(15L));
		assertThat(keyedBudgetManager.availableBudgetForKeys(Arrays.asList("k2", "k3")), is(19L));
		assertThat(keyedBudgetManager.totalAvailableBudget(), is(45L));
	}

	@Test
	public void testFailedAcquisitionForKeys() {
		KeyedBudgetManager<String> keyedBudgetManager = createSimpleKeyedBudget();

		Either<Map<String, Long>, Long> acquired =
			keyedBudgetManager.acquirePagedBudgetForKeys(Arrays.asList("k2", "k3"), 6, 6);

		assertThat(acquired.isRight(), is(true));
		assertThat(acquired.right(), is(5L));
		checkNoKeyBudgetChange(keyedBudgetManager);
	}

	@Test
	public void testSuccessfulReleaseForKeys() {
		KeyedBudgetManager<String> keyedBudgetManager = createSimpleKeyedBudget();

		keyedBudgetManager.acquirePagedBudgetForKeys(Arrays.asList("k2", "k3"), 4, 8);
		keyedBudgetManager.releaseBudgetForKeys(createdBudgetMap(new String[] {"k2", "k3"}, new long[] {7, 10}));

		assertThat(keyedBudgetManager.availableBudgetForKeys(Arrays.asList("k2", "k3")), is(24L));
		assertThat(keyedBudgetManager.availableBudgetForKeys(Arrays.asList("k1", "k4")), is(26L));
		assertThat(keyedBudgetManager.totalAvailableBudget(), is(50L));
	}

	@Test
	public void testSuccessfulReleaseForKeysWithMixedRequests() {
		KeyedBudgetManager<String> keyedBudgetManager = createSimpleKeyedBudget();

		keyedBudgetManager.acquirePagedBudgetForKeys(Arrays.asList("k2", "k3"), 4, 8);
		keyedBudgetManager.acquirePagedBudgetForKeys(Arrays.asList("k1", "k4"), 6, 3);
		keyedBudgetManager.releaseBudgetForKeys(createdBudgetMap(new String[] {"k2", "k3"}, new long[] {7, 10}));

		assertThat(keyedBudgetManager.availableBudgetForKeys(Arrays.asList("k2", "k3")), is(24L));
		assertThat(keyedBudgetManager.availableBudgetForKeys(Arrays.asList("k1", "k4")), is(8L));
		assertThat(keyedBudgetManager.totalAvailableBudget(), is(32L));
	}

	private static void checkNoKeyBudgetChange(KeyedBudgetManager<String> keyedBudgetManager) {
		checkKeysBudgetChange(keyedBudgetManager, Collections.emptyMap());
	}

	private static void checkOneKeyBudgetChange(
			KeyedBudgetManager<String> keyedBudgetManager,
			@SuppressWarnings("SameParameterValue") String key,
			long budget) {
		checkKeysBudgetChange(keyedBudgetManager, Collections.singletonMap(key, budget));
	}

	private static void checkKeysBudgetChange(
			KeyedBudgetManager<String> keyedBudgetManager,
			Map<String, Long> changedBudgetPerKey) {
		long totalExpectedBudget = 0L;
		for (int i = 0; i < TEST_KEYS.length; i++) {
			long expectedBudget = changedBudgetPerKey.containsKey(TEST_KEYS[i]) ?
				changedBudgetPerKey.get(TEST_KEYS[i]) : TEST_BUDGETS[i];
			assertThat(keyedBudgetManager.availableBudgetForKey(TEST_KEYS[i]), is(expectedBudget));
			totalExpectedBudget += expectedBudget;
		}
		assertThat(keyedBudgetManager.maxTotalBudget(), is(LongStream.of(TEST_BUDGETS).sum()));
		assertThat(keyedBudgetManager.totalAvailableBudget(), is(totalExpectedBudget));
	}

	private static KeyedBudgetManager<String> createSimpleKeyedBudget() {
		return new KeyedBudgetManager<>(createdBudgetMap(TEST_KEYS, TEST_BUDGETS), 1L);
	}

	private static Map<String, Long> createdBudgetMap(String[] keys, long[] budgets) {
		Preconditions.checkArgument(keys.length == budgets.length);
		Map<String, Long> keydBudgets = new HashMap<>();
		for (int i = 0; i < keys.length; i++) {
			keydBudgets.put(keys[i], budgets[i]);
		}
		return keydBudgets;
	}
}
