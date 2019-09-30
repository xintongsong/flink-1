/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.streaming.runtime.operators;

import org.apache.flink.api.common.ExecutionConfig;
import org.apache.flink.api.common.typeinfo.BasicTypeInfo;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.runtime.execution.CancelTaskException;
import org.apache.flink.runtime.execution.Environment;
import org.apache.flink.runtime.jobgraph.OperatorID;
import org.apache.flink.runtime.operators.testutils.DummyEnvironment;
import org.apache.flink.runtime.state.memory.MemoryStateBackend;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.functions.source.RichSourceFunction;
import org.apache.flink.streaming.api.functions.source.SourceFunction;
import org.apache.flink.streaming.api.graph.StreamConfig;
import org.apache.flink.streaming.api.operators.Output;
import org.apache.flink.streaming.api.operators.StreamSource;
import org.apache.flink.streaming.api.operators.StreamSourceContexts;
import org.apache.flink.streaming.api.watermark.Watermark;
import org.apache.flink.streaming.runtime.streamrecord.StreamElement;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;
import org.apache.flink.streaming.runtime.streamstatus.StreamStatus;
import org.apache.flink.streaming.runtime.streamstatus.StreamStatusMaintainer;
import org.apache.flink.streaming.runtime.tasks.SourceStreamTask;
import org.apache.flink.streaming.runtime.tasks.StreamTaskTestHarness;
import org.apache.flink.streaming.runtime.tasks.TestProcessingTimeService;
import org.apache.flink.streaming.runtime.tasks.TimerService;
import org.apache.flink.streaming.util.CollectorOutput;
import org.apache.flink.streaming.util.MockStreamTask;
import org.apache.flink.streaming.util.MockStreamTaskBuilder;
import org.apache.flink.streaming.util.TestHarnessUtil;
import org.apache.flink.util.ExceptionUtils;

import org.junit.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ConcurrentLinkedQueue;

import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/**
 * Tests for {@link StreamSource} operators.
 */
@SuppressWarnings("serial")
public class StreamSourceOperatorWatermarksTest {

	@Test
	public void testEmitMaxWatermarkForFiniteSource() throws Exception {
		StreamSource<String, ?> sourceOperator = new StreamSource<>(new FiniteSourceWithOutputingRecordInClose());

		StreamTaskTestHarness<String> testHarness = setupSourceStreamTask(
			sourceOperator, BasicTypeInfo.STRING_TYPE_INFO, false);

		testHarness.invoke();
		testHarness.waitForTaskCompletion();

		ConcurrentLinkedQueue<Object> expectedOutput = new ConcurrentLinkedQueue<>();
		expectedOutput.add(new StreamRecord<>("Hello"));
		expectedOutput.add(Watermark.MAX_WATERMARK);

		TestHarnessUtil.assertOutputEquals("Output was not correct.",
			expectedOutput,
			testHarness.getOutput());
	}

	@Test
	public void testNoMaxWatermarkOnImmediateCancel() throws Exception {

		StreamSource<String, ?> sourceOperator = new StreamSource<>(new InfiniteSource<>());

		StreamTaskTestHarness<String> testHarness = setupSourceStreamTask(
			sourceOperator, BasicTypeInfo.STRING_TYPE_INFO, true);

		testHarness.invoke();
		try {
			testHarness.waitForTaskCompletion();
		} catch (Throwable t) {
			assertTrue(ExceptionUtils.findThrowable(t, CancelTaskException.class).isPresent());
		}
		assertTrue(testHarness.getOutput().isEmpty());
	}

	@Test
	public void testNoMaxWatermarkOnAsyncCancel() throws Exception {

		StreamSource<String, ?> sourceOperator = new StreamSource<>(new InfiniteSource<>());

		StreamTaskTestHarness<String> testHarness = setupSourceStreamTask(
			sourceOperator, BasicTypeInfo.STRING_TYPE_INFO, false);

		testHarness.invoke();
		testHarness.waitForTaskRunning();
		Thread.sleep(200);
		testHarness.getTask().cancel(); // cancel task
		try {
			testHarness.waitForTaskCompletion();
		} catch (Throwable t) {
			assertTrue(ExceptionUtils.findThrowable(t, CancelTaskException.class).isPresent());
		}
		assertTrue(testHarness.getOutput().isEmpty());
	}

	@Test
	public void testAutomaticWatermarkContext() throws Exception {

		// regular stream source operator
		final StreamSource<String, InfiniteSource<String>> operator =
			new StreamSource<>(new InfiniteSource<>());

		long watermarkInterval = 10;
		TestProcessingTimeService processingTimeService = new TestProcessingTimeService();
		processingTimeService.setCurrentTime(0);

		setupSourceOperator(operator, TimeCharacteristic.IngestionTime, watermarkInterval, processingTimeService);

		final List<StreamElement> output = new ArrayList<>();

		StreamSourceContexts.getSourceContext(
			TimeCharacteristic.IngestionTime,
			processingTimeService,
			operator.getContainingTask().getCheckpointLock(),
			operator.getContainingTask().getStreamStatusMaintainer(),
			new CollectorOutput<String>(output),
			operator.getExecutionConfig().getAutoWatermarkInterval(),
			-1);

		// periodically emit the watermarks
		// even though we start from 1 the watermark are still
		// going to be aligned with the watermark interval.

		for (long i = 1; i < 100; i += watermarkInterval)  {
			processingTimeService.setCurrentTime(i);
		}

		assertTrue(output.size() == 9);

		long nextWatermark = 0;
		for (StreamElement el : output) {
			nextWatermark += watermarkInterval;
			Watermark wm = (Watermark) el;
			assertTrue(wm.getTimestamp() == nextWatermark);
		}
	}

	// ------------------------------------------------------------------------

	@SuppressWarnings("unchecked")
	private static <T> void setupSourceOperator(
			StreamSource<T, ?> operator,
			TimeCharacteristic timeChar,
			long watermarkInterval,
			final TimerService timeProvider) throws Exception {

		ExecutionConfig executionConfig = new ExecutionConfig();
		executionConfig.setAutoWatermarkInterval(watermarkInterval);

		StreamConfig cfg = new StreamConfig(new Configuration());
		cfg.setStateBackend(new MemoryStateBackend());

		cfg.setTimeCharacteristic(timeChar);
		cfg.setOperatorID(new OperatorID());

		Environment env = new DummyEnvironment("MockTwoInputTask", 1, 0);

		StreamStatusMaintainer streamStatusMaintainer = mock(StreamStatusMaintainer.class);
		when(streamStatusMaintainer.getStreamStatus()).thenReturn(StreamStatus.ACTIVE);

		MockStreamTask mockTask = new MockStreamTaskBuilder(env)
			.setConfig(cfg)
			.setExecutionConfig(executionConfig)
			.setStreamStatusMaintainer(streamStatusMaintainer)
			.setTimerService(timeProvider)
			.build();

		operator.setup(mockTask, cfg, (Output<StreamRecord<T>>) mock(Output.class));
	}

	private static <T> StreamTaskTestHarness<T> setupSourceStreamTask(
		StreamSource<T, ?> sourceOperator,
		TypeInformation<T> outputType,
		final boolean cancelImmediatelyAfterCreation) {

		final StreamTaskTestHarness<T> testHarness = new StreamTaskTestHarness<>(
			(env) -> {
				SourceStreamTask<T, ?, ?> sourceTask = new SourceStreamTask<>(env);
				if (cancelImmediatelyAfterCreation) {
					try {
						sourceTask.cancel();
					} catch (Exception e) {
						throw new RuntimeException(e);
					}
				}
				return sourceTask;
			},
			outputType);
		testHarness.setupOutputForSingletonOperatorChain();

		StreamConfig streamConfig = testHarness.getStreamConfig();
		streamConfig.setStreamOperator(sourceOperator);
		streamConfig.setOperatorID(new OperatorID());
		streamConfig.setTimeCharacteristic(TimeCharacteristic.EventTime);

		return testHarness;
	}

	// ------------------------------------------------------------------------

	private static final class FiniteSourceWithOutputingRecordInClose extends RichSourceFunction<String> {

		private transient volatile boolean canceled = false;

		private transient SourceContext<String> context;

		@Override
		public void run(SourceContext<String> ctx) {
			context = ctx;
		}

		@Override
		public void close() {
			if (!canceled) {
				context.collect("Hello");
			}
		}

		@Override
		public void cancel() {
			canceled = true;
		}
	}

	private static final class InfiniteSource<T> implements SourceFunction<T> {

		private volatile boolean running = true;

		@Override
		public void run(SourceContext<T> ctx) throws Exception {
			while (running) {
				Thread.sleep(20);
			}
		}

		@Override
		public void cancel() {
			running = false;
		}
	}
}
