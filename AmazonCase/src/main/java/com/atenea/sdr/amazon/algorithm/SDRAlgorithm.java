package com.atenea.sdr.amazon.algorithm;

import java.util.Arrays;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
import org.apache.commons.configuration.Configuration;
import org.apache.tinkerpop.gremlin.process.computer.GraphComputer;
import org.apache.tinkerpop.gremlin.process.computer.Memory;
import org.apache.tinkerpop.gremlin.process.computer.MemoryComputeKey;
import org.apache.tinkerpop.gremlin.process.computer.MessageCombiner;
import org.apache.tinkerpop.gremlin.process.computer.MessageScope;
import org.apache.tinkerpop.gremlin.process.computer.Messenger;
import org.apache.tinkerpop.gremlin.process.computer.VertexComputeKey;
import org.apache.tinkerpop.gremlin.process.computer.VertexProgram;
import org.apache.tinkerpop.gremlin.process.computer.ranking.pagerank.PageRankMessageCombiner;
import org.apache.tinkerpop.gremlin.process.computer.util.AbstractVertexProgramBuilder;
import org.apache.tinkerpop.gremlin.process.traversal.Operator;
import org.apache.tinkerpop.gremlin.process.traversal.Step;
import org.apache.tinkerpop.gremlin.process.traversal.Traversal;
import org.apache.tinkerpop.gremlin.process.traversal.Traversal.Admin;
import org.apache.tinkerpop.gremlin.process.traversal.TraversalSource;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.DefaultGraphTraversal;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversal;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.__;
import org.apache.tinkerpop.gremlin.process.traversal.step.TraversalParent;
import org.apache.tinkerpop.gremlin.process.traversal.step.filter.AndStep;
import org.apache.tinkerpop.gremlin.process.traversal.step.filter.HasStep;
import org.apache.tinkerpop.gremlin.process.traversal.step.filter.NotStep;
import org.apache.tinkerpop.gremlin.process.traversal.step.map.VertexStep;
import org.apache.tinkerpop.gremlin.process.traversal.util.PureTraversal;
import org.apache.tinkerpop.gremlin.process.traversal.util.ScriptTraversal;
import org.apache.tinkerpop.gremlin.structure.Edge;
import org.apache.tinkerpop.gremlin.structure.Graph;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.apache.tinkerpop.gremlin.structure.VertexProperty;
import org.apache.tinkerpop.gremlin.structure.util.StringFactory;
import org.apache.tinkerpop.gremlin.util.iterator.IteratorUtils;

import com.atenea.sdr.amazon.tools.ProcessingDirection;

@SuppressWarnings("rawtypes")
public class SDRAlgorithm implements VertexProgram<List<Double>> {

	public static final String PAGE_RANK = "gremlin.pageRankVertexProgram.pageRank";
	private static final String EDGE_COUNT = "gremlin.pageRankVertexProgram.edgeCount";
	private static final String PROPERTY = "gremlin.pageRankVertexProgram.property";
	private static final String MAX_ITERATIONS = "gremlin.pageRankVertexProgram.maxIterations";
	private static final String EDGE_TRAVERSAL = "gremlin.pageRankVertexProgram.edgeTraversal";
	private static final String QUERY_TRAVERSAL = "gremlin.pageRankVertexProgram.queryTraversal";
	private static final String INITIAL_RANK_TRAVERSAL = "gremlin.pageRankVertexProgram.initialRankTraversal";
	private static final String TERMINATION_FLAG = "gremlin.pageRankVertexProgram.convergenceError";
	private static final String PARENT_INDEX = "gremlin.pageRankVertexProgram.parentIndex";
	private static final String CHILD_INDEX = "gremlin.pageRankVertexProgram.childIndex";
	private static final String LIST = "gremlin.pageRankVertexProgram.auxListConnective";
	private static final String VERTEX_ITERATION = "gremlin.pageRankVertexProgram.globalIteration";
	private MessageScope.Local<Double> incidentMessageScope = MessageScope.Local.of(__::outE);
	private MessageScope.Local<Double> outMessageScope = MessageScope.Local.of(__::inE);
	private MessageScope.Local<Double> countMessageScopeOut = MessageScope.Local
			.of(new MessageScope.Local.ReverseTraversalSupplier(this.incidentMessageScope));
	private MessageScope.Local<Double> countMessageScopeIn = MessageScope.Local
			.of(new MessageScope.Local.ReverseTraversalSupplier(this.outMessageScope));
	private PureTraversal<Vertex, Edge> edgeTraversal = null;
	private PureTraversal<Vertex, ? extends Number> initialRankTraversal = null;

	private int maxIterations = 0;
	private String property = PAGE_RANK;
	private PureTraversal<Vertex, Vertex> query = null;
	private List<Step> steps = null;
	private ConcurrentHashMap<String, Double> auxListConnective = new ConcurrentHashMap<>();
	private Set<VertexComputeKey> vertexComputeKeys;
	private Set<MemoryComputeKey> memoryComputeKeys;
	private int parentIndex = -1;
	private int childIndex = -1;

	private SDRAlgorithm() {

	}

	public void loadState(final Graph graph, final Configuration configuration) {

		if (configuration.containsKey(INITIAL_RANK_TRAVERSAL))
			this.setInitialRankTraversal(PureTraversal.loadState(configuration, INITIAL_RANK_TRAVERSAL, graph));
		if (configuration.containsKey(EDGE_TRAVERSAL)) {
			this.edgeTraversal = PureTraversal.loadState(configuration, EDGE_TRAVERSAL, graph);
			this.incidentMessageScope = MessageScope.Local.of(() -> this.edgeTraversal.get().clone());
			this.countMessageScopeOut = MessageScope.Local
					.of(new MessageScope.Local.ReverseTraversalSupplier(this.incidentMessageScope));
			this.outMessageScope = MessageScope.Local.of(() -> this.edgeTraversal.get().clone());
			this.countMessageScopeIn = MessageScope.Local
					.of(new MessageScope.Local.ReverseTraversalSupplier(this.outMessageScope));
		}

		if (configuration.containsKey(QUERY_TRAVERSAL)) {
			this.query = PureTraversal.loadState(configuration, QUERY_TRAVERSAL, graph);
			steps = query.getPure().getSteps();

		}

		this.parentIndex = configuration.getInt(PARENT_INDEX, this.parentIndex);
		this.childIndex = configuration.getInt(CHILD_INDEX, this.childIndex);
		this.maxIterations = configuration.getInt(MAX_ITERATIONS, 20);// steps.size() - 1);
		this.property = configuration.getString(PROPERTY, PAGE_RANK);
		this.vertexComputeKeys = new HashSet<>(Arrays.asList(VertexComputeKey.of(this.property, false),
				VertexComputeKey.of(PARENT_INDEX, false), VertexComputeKey.of(CHILD_INDEX, false),
				VertexComputeKey.of(VERTEX_ITERATION, false), VertexComputeKey.of(EDGE_COUNT, true)));
		this.memoryComputeKeys = new HashSet<>(
				Arrays.asList(MemoryComputeKey.of(TERMINATION_FLAG, Operator.sum, false, true)));
	}

	@Override
	public void storeState(final Configuration configuration) {
		VertexProgram.super.storeState(configuration);
		configuration.setProperty(PROPERTY, this.property);
		configuration.setProperty(MAX_ITERATIONS, this.maxIterations);
		configuration.setProperty(LIST, this.auxListConnective);
		configuration.setProperty(PARENT_INDEX, this.parentIndex);
		configuration.setProperty(CHILD_INDEX, this.childIndex);

		if (null != this.query)
			this.query.storeState(configuration, QUERY_TRAVERSAL);
		if (null != this.edgeTraversal)
			this.edgeTraversal.storeState(configuration, EDGE_TRAVERSAL);
		if (null != this.getInitialRankTraversal())
			this.getInitialRankTraversal().storeState(configuration, INITIAL_RANK_TRAVERSAL);
	}

	@Override
	public GraphComputer.ResultGraph getPreferredResultGraph() {
		return GraphComputer.ResultGraph.NEW;
	}

	@Override
	public GraphComputer.Persist getPreferredPersist() {
		return GraphComputer.Persist.VERTEX_PROPERTIES;
	}

	@Override
	public Set<VertexComputeKey> getVertexComputeKeys() {
		return this.vertexComputeKeys;
	}

	@SuppressWarnings("unchecked")
	@Override
	public Optional<MessageCombiner<List<Double>>> getMessageCombiner() {
		return (Optional) PageRankMessageCombiner.instance();
	}

	@Override
	public Set<MemoryComputeKey> getMemoryComputeKeys() {
		return this.memoryComputeKeys;
	}

	@Override
	public Set<MessageScope> getMessageScopes(final Memory memory) {
		final Set<MessageScope> set = new HashSet<>();
		set.add(memory.isInitialIteration() ? this.countMessageScopeOut : this.incidentMessageScope);
		set.add(memory.isInitialIteration() ? this.countMessageScopeIn : this.outMessageScope);
		return set;
	}

	@Override
	public SDRAlgorithm clone() {
		try {
			final SDRAlgorithm clone = (SDRAlgorithm) super.clone();

			if (null != this.getInitialRankTraversal())
				clone.setInitialRankTraversal(this.getInitialRankTraversal().clone());
			return clone;
		} catch (final CloneNotSupportedException e) {
			throw new IllegalStateException(e.getMessage(), e);
		}
	}

	@Override
	public void setup(final Memory memory) {
		memory.set(TERMINATION_FLAG, 1.0d);
	}

	@SuppressWarnings({ "resource", "unchecked", "unused" })
	public Double algorithm(List<Step> steps, Vertex vertex, Messenger<List<Double>> messenger, Integer iteration,
			Memory memory) {
		double currentWeight = 0.0d;

		double previousWeight = 0.0d;

		if (vertex.<Integer>property(CHILD_INDEX).isPresent()) {
			previousWeight = auxListConnective.get("A-" + vertex.id());
		} else {
			previousWeight = vertex.<Double>property(this.property).orElse(0.0d);
		}

		boolean guardCondition = true; // guardCondition is true by default
		Step step = null;
		ProcessingDirection process = new ProcessingDirection();
		Long countEdges = 0L;

		if (iteration == 0) {

			step = steps.get(steps.size() - 1);

			// HasStep
			if (step instanceof HasStep) {

				guardCondition = ProcessingDirection.processHasStep((HasStep) step, vertex);
				if (guardCondition) {
					Step prevStep = step.getPreviousStep();
					if (prevStep instanceof VertexStep) {

						VertexStep dir = (VertexStep) prevStep;

						for (String s : dir.getEdgeLabels()) {
							Iterator<Edge> edges = vertex.edges(dir.getDirection().opposite(), s);
							while (edges.hasNext()) {

								Edge e = edges.next();
								countEdges++;

							}
							if (countEdges == 0) {
								guardCondition = false;
							}
						}
					}
				}
			}
			// VertexStep
			else if (step instanceof VertexStep) {

				VertexStep dir = (VertexStep) step;

				for (String s : dir.getEdgeLabels()) {
					Iterator<Edge> edges = vertex.edges(dir.getDirection().opposite(), s);
					while (edges.hasNext()) {

						Edge e = edges.next();
						countEdges++;

					}
					if (countEdges == 0) {
						guardCondition = false;
					}
				}

			} // And, Or, Where, Not
			else if (step instanceof TraversalParent) {

				List<Admin<Object, Object>> parentList = ((TraversalParent) step).getLocalChildren();

				int childIndex = vertex.<Integer>property(CHILD_INDEX).orElse(0);
				int parentIndex = vertex.<Integer>property(PARENT_INDEX).orElse(0);

				if (childIndex == 0) {
					vertex.<Integer>property(CHILD_INDEX, 0);
				}

				if (parentIndex == 0) {
					vertex.<Integer>property(PARENT_INDEX, 0);
				}

				if (parentIndex < parentList.size()) {

					DefaultGraphTraversal childrenTraversal = (DefaultGraphTraversal) parentList.get(parentIndex);

					List<Step> childrenSteps = childrenTraversal.getSteps();
					auxListConnective.computeIfAbsent("A-" + vertex.id(), s -> 0d);
					auxListConnective.computeIfAbsent("B-" + vertex.id(), s -> 0d);
					auxListConnective.computeIfAbsent("R-" + vertex.id(), s -> 0d);

					Double weightAuxTraversal = algorithm(childrenSteps, vertex, messenger, childIndex, memory);

					if (childIndex >= childrenSteps.size() + 1) {

						if (step instanceof NotStep) {
							Double weight = 0.0d;
							if (weightAuxTraversal > 0) {
								weight = 0.0d;
							} else {
								weight = 1.0d;
							}
							Double weightAux = weight;
							auxListConnective.compute("A-" + vertex.id(), (key, val) -> weightAux);

						} else if (step instanceof AndStep && parentIndex > 0) {

							Double weightAux = weightAuxTraversal * auxListConnective.get("B-" + vertex.id());
							auxListConnective.compute("A-" + vertex.id(), (key, val) -> val + weightAux);
						} else {

							auxListConnective.compute("A-" + vertex.id(), (key, val) -> weightAuxTraversal);
						}

						vertex.<Integer>property(CHILD_INDEX, 0);
						vertex.<Integer>property(PARENT_INDEX, parentIndex + 1);

						Double weight = auxListConnective.get("A-" + vertex.id());

						auxListConnective.compute("B-" + vertex.id(), (key, val) -> val + weight);

						auxListConnective.compute("A-" + vertex.id(), (key, val) -> 0.0);

					} else {

						auxListConnective.compute("A-" + vertex.id(), (key, val) -> weightAuxTraversal);

						childIndex = vertex.<Integer>property(CHILD_INDEX).orElse(0);
						vertex.<Integer>property(CHILD_INDEX, childIndex + 1);

					}
					guardCondition = false;

				} else {

					step = steps.get(steps.size() - 1);

					Double weight = auxListConnective.get("B-" + vertex.id());

					auxListConnective.compute("R-" + vertex.id(), (key, val) -> weight + val);

					currentWeight = auxListConnective.get("R-" + vertex.id());

					vertex.property(VertexProperty.Cardinality.single, this.property, currentWeight);

					guardCondition = false;

					vertex.<Integer>property(CHILD_INDEX).remove();
					vertex.<Integer>property(PARENT_INDEX).remove();

				}
			}

		} else {
			if (iteration <= steps.size()) {
				step = steps.get(steps.size() - iteration);
			}

			if (iteration == 1) {

				if (step != null) {

					if (step instanceof VertexStep) {
						if (previousWeight > 0) {
							ProcessingDirection.sendBackward(step, messenger);
						}
					} else if (step instanceof HasStep || step instanceof TraversalParent) {
						step = steps.get(steps.size() - 2);
						if (step instanceof VertexStep) {
							if (!vertex.<Integer>property(CHILD_INDEX).isPresent()) {
								Integer it = iteration++;
								vertex.property(VERTEX_ITERATION, iteration);
							} else {
								Integer it = iteration++;
								vertex.property(CHILD_INDEX, iteration);
							}
							if (previousWeight > 0) {
								ProcessingDirection.sendBackward(step, messenger);
							}
						}
					}

				}
			} else {
				Long countMessages = IteratorUtils.reduce(messenger.receiveMessages(), 0.0d, (a, b) -> a + b.get(1))
						.longValue();
				if (countMessages > 0) {

					if (step instanceof VertexStep) {

						VertexStep dir = (VertexStep) step;

						for (String s : dir.getEdgeLabels()) {
							Iterator<Edge> edges = vertex.edges(dir.getDirection().opposite(), s);
							while (edges.hasNext()) {
								Edge e = edges.next();
								countEdges++;
							}
							if (countEdges == 0) {
								guardCondition = false;
							} else {
								countEdges = countEdges + countMessages;
							}
						}
						ProcessingDirection.sendBackward(step, messenger);

					} else if (step instanceof HasStep) {

						guardCondition = ProcessingDirection.processHasStep((HasStep) step, vertex);

						if (!vertex.<Integer>property(CHILD_INDEX).isPresent()) {
							Integer it = iteration++;
							vertex.property(VERTEX_ITERATION, iteration);
						} else {
							Integer it = iteration++;
							vertex.property(CHILD_INDEX, iteration);
						}

						if (guardCondition) {
							Step prevStep = step.getPreviousStep();
							if (prevStep instanceof VertexStep) {

								VertexStep dir = (VertexStep) prevStep;

								for (String s : dir.getEdgeLabels()) {
									Iterator<Edge> edges = vertex.edges(dir.getDirection().opposite(), s);
									while (edges.hasNext()) {

										Edge e = edges.next();
										countEdges++;

									}
									if (countEdges == 0) {
										guardCondition = false;
									} else {
										countEdges = countEdges + countMessages;
									}
								}
								ProcessingDirection.sendBackward(step, messenger);
							} else {
								countEdges = countMessages;
							}

						}

					} else {
						countEdges = countMessages;
					}

				}

			}
		}

		if (guardCondition) {
			currentWeight = previousWeight + countEdges;
			if (!vertex.<Integer>property(CHILD_INDEX).isPresent()) {
				vertex.property(VertexProperty.Cardinality.single, this.property, currentWeight);
			}

		}

		if (!vertex.<Integer>property(CHILD_INDEX).isPresent()) {
			Integer it = iteration++;
			vertex.property(VERTEX_ITERATION, iteration);
		}
		return currentWeight;

	}

	// Execute method
	@SuppressWarnings("unused")
	public void execute(final Vertex vertex, Messenger<List<Double>> messenger, final Memory memory) {

		double currentWeight = 0.0d;

		Integer iteration = vertex.<Integer>property(VERTEX_ITERATION).orElse(0);

		// call algorithm
		currentWeight = algorithm(steps, vertex, messenger, iteration, memory);

		// Terminate if iteration is higher than steps size
		if (steps.size() <= iteration) {
			memory.add(TERMINATION_FLAG, -1); 
		}

	}

	@Override
	public boolean terminate(final Memory memory) {
		boolean terminate = memory.<Double>get(TERMINATION_FLAG) < -1 
				|| memory.getIteration() >= this.maxIterations;
		memory.set(TERMINATION_FLAG, 0.0d);
		return terminate;
	}

	@Override
	public String toString() {
		return StringFactory.vertexProgramString(this, "iterations=" + this.maxIterations);
	}

	public static Builder build() {
		return new Builder();
	}

	public final static class Builder extends AbstractVertexProgramBuilder<Builder> {

		private Builder() {
			super(SDRAlgorithm.class);
		}

		public Builder iterations(final int iterations) {
			this.configuration.setProperty(MAX_ITERATIONS, iterations);
			return this;
		}

		public Builder weightList(final List<Double> weightList) {
			this.configuration.setProperty(LIST, weightList);
			return this;
		}

		public Builder property(final String key) {
			this.configuration.setProperty(PROPERTY, key);
			return this;
		}

		public Builder edges(final Traversal.Admin<Vertex, Edge> edgeTraversal) {
			PureTraversal.storeState(this.configuration, EDGE_TRAVERSAL, edgeTraversal);
			return this;
		}

		public Builder query(final GraphTraversal.Admin<Vertex, Vertex> query) {
			PureTraversal.storeState(this.configuration, QUERY_TRAVERSAL, query);
			return this;
		}

		public Builder initialRank(final Traversal.Admin<Vertex, ? extends Number> initialRankTraversal) {
			PureTraversal.storeState(this.configuration, INITIAL_RANK_TRAVERSAL, initialRankTraversal);
			return this;
		}

		/**
		 * @deprecated 
		 */
		@Deprecated
		public Builder traversal(final TraversalSource traversalSource, final String scriptEngine,
				final String traversalScript, final Object... bindings) {
			return this.edges(new ScriptTraversal<>(traversalSource, scriptEngine, traversalScript, bindings));
		}

		/**
		 * @deprecated 
		 */
		@Deprecated
		public Builder traversal(final Traversal.Admin<Vertex, Edge> traversal) {
			return this.edges(traversal);
		}
	}

	@Override
	public Features getFeatures() {
		return new Features() {
			@Override
			public boolean requiresLocalMessageScopes() {
				return true;
			}

			@Override
			public boolean requiresVertexPropertyAddition() {
				return true;
			}
		};
	}

	public PureTraversal<Vertex, ? extends Number> getInitialRankTraversal() {
		return initialRankTraversal;
	}

	public void setInitialRankTraversal(PureTraversal<Vertex, ? extends Number> initialRankTraversal) {
		this.initialRankTraversal = initialRankTraversal;
	}
}