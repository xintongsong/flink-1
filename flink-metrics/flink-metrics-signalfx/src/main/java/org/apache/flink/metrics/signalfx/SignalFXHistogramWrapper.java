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

package org.apache.flink.metrics.signalfx;

import org.apache.flink.metrics.Histogram;
import org.apache.flink.metrics.HistogramStatistics;

/**
 * Wrapper to use a SignalFX {@link com.signalfx.codahale.metrics.ResettingHistogram} as a Flink {@link Histogram}.
 */
public class SignalFXHistogramWrapper implements Histogram {

	private final com.signalfx.codahale.metrics.ResettingHistogram signalfxHistogram;

	public SignalFXHistogramWrapper(com.signalfx.codahale.metrics.ResettingHistogram signalfxHistogram) {
		this.signalfxHistogram = signalfxHistogram;
	}

	public com.signalfx.codahale.metrics.ResettingHistogram getSignalFXHistogram() {
		return signalfxHistogram;
	}

	@Override
	public void update(long value) {
		signalfxHistogram.update(value);
	}

	@Override
	public long getCount() {
		return signalfxHistogram.getCount();
	}

	@Override
	public HistogramStatistics getStatistics() {
		return new SignalFXHistogramStatistics(signalfxHistogram.getSnapshot());
	}
}
