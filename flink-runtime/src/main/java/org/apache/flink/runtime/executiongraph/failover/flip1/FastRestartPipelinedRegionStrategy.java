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

package org.apache.flink.runtime.executiongraph.failover.flip1;

import org.apache.flink.runtime.jobgraph.IntermediateResultPartitionID;
import org.apache.flink.runtime.scheduler.strategy.ExecutionVertexID;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.IdentityHashMap;
import java.util.Map;
import java.util.Queue;
import java.util.Set;

/**
 * This failover strategy makes the same restart decision as {@link RestartPipelinedRegionStrategy}.
 * It has better failover handling performance at the cost of slower region building and more memory
 * used for region boundary cache. See FLINK-13056.
 */
public class FastRestartPipelinedRegionStrategy extends RestartPipelinedRegionStrategy {

	/** Maps a failover region to its input result partitions. */
	private final IdentityHashMap<FailoverRegion, Collection<IntermediateResultPartitionID>> regionInputs;

	/** Maps a failover region to its consumer regions. */
	private final IdentityHashMap<FailoverRegion, Collection<FailoverRegion>> regionConsumers;

	/** Maps result partition id to its producer failover region. Only for inter-region consumed partitions.*/
	private final Map<IntermediateResultPartitionID, FailoverRegion> partitionProducer;

	/**
	 * Creates a new failover strategy to restart pipelined regions that works on the given topology.
	 *
	 * @param topology containing info about all the vertices and edges
	 * @param resultPartitionAvailabilityChecker helps to query result partition availability
	 */
	public FastRestartPipelinedRegionStrategy(
			final FailoverTopology topology,
			final ResultPartitionAvailabilityChecker resultPartitionAvailabilityChecker) {

		super(topology, resultPartitionAvailabilityChecker);

		this.regionInputs = new IdentityHashMap<>();
		this.regionConsumers = new IdentityHashMap<>();
		this.partitionProducer = new HashMap<>();
		buildRegionInputsAndOutputs();
	}

	private void buildRegionInputsAndOutputs() {
		for (FailoverRegion region : regions) {
			final IdentityHashMap<FailoverRegion, Object> consumers = new IdentityHashMap<>();
			final Set<IntermediateResultPartitionID> inputs = new HashSet<>();
			final Set<ExecutionVertexID> consumerVertices = new HashSet<>();
			final Set<FailoverVertex> regionVertices = region.getAllExecutionVertices();
			regionVertices.forEach(v -> {
				for (FailoverEdge inEdge : v.getInputEdges()) {
					if (!regionVertices.contains(inEdge.getSourceVertex())) {
						inputs.add(inEdge.getResultPartitionID());
					}
				}
				for (FailoverEdge outEdge : v.getOutputEdges()) {
					if (!regionVertices.contains(outEdge.getTargetVertex())) {
						this.partitionProducer.put(outEdge.getResultPartitionID(), region);
						consumerVertices.add(outEdge.getTargetVertex().getExecutionVertexID());
					}
				}
			});
			this.regionInputs.put(region, new ArrayList<>(inputs));
			consumerVertices.forEach(id -> consumers.put(vertexToRegionMap.get(id), null));
			this.regionConsumers.put(region, new ArrayList<>(consumers.keySet()));
		}
	}

	@Override
	protected void calculateProducerRegionsToVisit(
		final FailoverRegion currentRegion,
		final Queue<FailoverRegion> regionsToVisit,
		final Set<FailoverRegion> visitedRegions) {

		for (IntermediateResultPartitionID intermediateResultPartitionID : regionInputs.get(currentRegion)) {
			if (!resultPartitionAvailabilityChecker.isAvailable(intermediateResultPartitionID)) {
				final FailoverRegion producerRegion = partitionProducer.get(intermediateResultPartitionID);
				if (!visitedRegions.contains(producerRegion)) {
					visitedRegions.add(producerRegion);
					regionsToVisit.add(producerRegion);
				}
			}
		}
	}

	@Override
	protected void calculateConsumerRegionsToVisit(
		final FailoverRegion currentRegion,
		final Queue<FailoverRegion> regionsToVisit,
		final Set<FailoverRegion> visitedRegions) {

		for (FailoverRegion consumerRegion : regionConsumers.get(currentRegion)) {
			if (!visitedRegions.contains(consumerRegion)) {
				visitedRegions.add(consumerRegion);
				regionsToVisit.add(consumerRegion);
			}
		}
	}
}
