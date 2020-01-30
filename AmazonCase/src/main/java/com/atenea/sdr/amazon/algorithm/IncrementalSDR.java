package com.atenea.sdr.amazon.algorithm;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.tinkerpop.gremlin.process.traversal.P;
import org.apache.tinkerpop.gremlin.process.traversal.Step;
import org.apache.tinkerpop.gremlin.process.traversal.Traversal;
import org.apache.tinkerpop.gremlin.process.traversal.Traversal.Admin;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.DefaultGraphTraversal;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversal;
import org.apache.tinkerpop.gremlin.process.traversal.step.TraversalParent;
import org.apache.tinkerpop.gremlin.process.traversal.step.filter.NotStep;
import org.apache.tinkerpop.gremlin.process.traversal.step.map.VertexStep;
import org.apache.tinkerpop.gremlin.process.traversal.step.util.EmptyStep;
import org.apache.tinkerpop.gremlin.process.traversal.util.PureTraversal;
import org.apache.tinkerpop.gremlin.structure.Direction;
import org.apache.tinkerpop.gremlin.structure.Graph;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.apache.tinkerpop.gremlin.tinkergraph.structure.TinkerGraph;

import com.atenea.sdr.amazon.tools.TraversalEdge;

public class IncrementalSDR {

	@SuppressWarnings({ "rawtypes", "unchecked", "resource" })
	public static List<Map<String, Object>> updateWeight(Set<Vertex> e, Graph graph, GraphTraversal query) {
		
		
		//Obtain steps
		PureTraversal pT = new PureTraversal(query.asAdmin());
		List<Step> steps = pT.getPure().getSteps();
		
		
		Boolean flagNot = false;


		List<Traversal> listSubgraphTraversal = new ArrayList<Traversal>();

		for (int i = 0; i <= steps.size(); i++) {

			Step s = null;

			List<TraversalEdge> next = new ArrayList<TraversalEdge>();
			List<TraversalEdge> previous = new ArrayList<TraversalEdge>();

			if (i != steps.size()) {

				s = steps.get(i);
			} else { 
				s = steps.get(i - 1);

				if (s instanceof VertexStep) {
					VertexStep edge = (VertexStep) s;

					previous.add(new TraversalEdge(edge.getDirection().opposite(), edge.getEdgeLabels()));

				}

			}

			if (s instanceof TraversalParent) {

				if (s instanceof NotStep) {
					flagNot = true;
				}

				TraversalParent parent = (TraversalParent) s;

				List<Admin<Object, Object>> parentList = parent.getLocalChildren();

				for (int u = 0; u < parentList.size(); u++) {
					DefaultGraphTraversal childrenTraversal = (DefaultGraphTraversal) parentList.get(u);

					List<Step> childrenSteps = childrenTraversal.getSteps();

					for (int x = 0; x <= childrenSteps.size(); x++) {

						List<TraversalEdge> nextChildren = new ArrayList<TraversalEdge>();
						List<TraversalEdge> previousChildren = new ArrayList<TraversalEdge>();

						Step childStep = null;

						if (x != childrenSteps.size()) {

							childStep = childrenSteps.get(x);
						} else { 
							childStep = childrenSteps.get(x - 1);

							if (s instanceof VertexStep) {
								VertexStep edge = (VertexStep) childStep;

								previousChildren
										.add(new TraversalEdge(edge.getDirection().opposite(), edge.getEdgeLabels()));

							}

						}

						if (childStep instanceof VertexStep || childStep.getNextStep() instanceof EmptyStep) {

							if (childStep instanceof VertexStep) {

								VertexStep edge = (VertexStep) childStep;

								nextChildren.add(new TraversalEdge(edge.getDirection(), edge.getEdgeLabels()));

							}

							Step nextStep = childStep;
							do {
								nextStep = nextStep.getNextStep();
								if (nextStep instanceof VertexStep) {
									nextChildren.add(new TraversalEdge(((VertexStep) nextStep).getDirection(),
											((VertexStep) nextStep).getEdgeLabels()));
								}
							} while (!(nextStep instanceof EmptyStep));

							Step previousStep = childStep;
							do {
								previousStep = previousStep.getPreviousStep();
								if (previousStep instanceof VertexStep) {
									previousChildren.add(
											new TraversalEdge(((VertexStep) previousStep).getDirection().opposite(),
													((VertexStep) previousStep).getEdgeLabels()));
								}
							} while (!(previousStep instanceof EmptyStep));

							if (!nextChildren.isEmpty()) {

								GraphTraversal subgraphTraversal = graph.traversal().V(e);

								for (TraversalEdge te : nextChildren) {

									if (te.getDirection() == Direction.OUT) {

										subgraphTraversal = subgraphTraversal.outE(te.getLabels()).subgraph("subgraph")
												.inV();
									} else if (te.getDirection() == Direction.IN) {

										subgraphTraversal = subgraphTraversal.inE(te.getLabels()).subgraph("subgraph")
												.outV();
									} else {

										subgraphTraversal = subgraphTraversal.bothE(te.getLabels()).subgraph("subgraph")
												.bothV();
									}

								}

								listSubgraphTraversal.add(subgraphTraversal);

							}

							if (!previousChildren.isEmpty()) {

								GraphTraversal subgraphTraversal = graph.traversal().V(e);

								for (TraversalEdge te : previousChildren) {

									if (te.getDirection() == Direction.OUT) {

										subgraphTraversal = subgraphTraversal.outE(te.getLabels()).subgraph("subgraph")
												.inV();
									} else if (te.getDirection() == Direction.IN) {

										subgraphTraversal = subgraphTraversal.inE(te.getLabels()).subgraph("subgraph")
												.outV();
									} else {

										subgraphTraversal = subgraphTraversal.bothE(te.getLabels()).subgraph("subgraph")
												.bothV();
									}

								}

								listSubgraphTraversal.add(subgraphTraversal);

							}

						}

					}

				}
			}

			else if (s instanceof VertexStep || s.getNextStep() instanceof EmptyStep) {

				if (s instanceof VertexStep) {
					VertexStep edge = (VertexStep) s;

					next.add(new TraversalEdge(edge.getDirection(), edge.getEdgeLabels()));

				}

				Step nextStep = s;
				do {
					nextStep = nextStep.getNextStep();
					if (nextStep instanceof VertexStep) {
						next.add(new TraversalEdge(((VertexStep) nextStep).getDirection(),
								((VertexStep) nextStep).getEdgeLabels()));
					} else if (nextStep instanceof TraversalParent) {

						Step nextStepChildren = null;
						List<TraversalEdge> nextChildren = new ArrayList<TraversalEdge>();

						TraversalParent parent = (TraversalParent) nextStep;

						List<Admin<Object, Object>> parentList = parent.getLocalChildren();

						for (int u = 0; u < parentList.size(); u++) {

							nextChildren.addAll(next);
							DefaultGraphTraversal childrenTraversal = (DefaultGraphTraversal) parentList.get(u);

							List<Step> childrenSteps = childrenTraversal.getSteps();

							for (int x = 0; x < childrenSteps.size(); x++) {

								nextStepChildren = childrenSteps.get(x);
								if (nextStepChildren instanceof VertexStep) {
									nextChildren.add(new TraversalEdge(((VertexStep) nextStepChildren).getDirection(),
											((VertexStep) nextStepChildren).getEdgeLabels()));
								}
							}

							if (!nextChildren.isEmpty()) {

								GraphTraversal subgraphTraversal = graph.traversal().V(e);

								for (TraversalEdge te : nextChildren) {

									if (te.getDirection() == Direction.OUT) {

										subgraphTraversal = subgraphTraversal.outE(te.getLabels()).subgraph("subgraph")
												.inV();
									} else if (te.getDirection() == Direction.IN) {

										subgraphTraversal = subgraphTraversal.inE(te.getLabels()).subgraph("subgraph")
												.outV();
									} else {

										subgraphTraversal = subgraphTraversal.bothE(te.getLabels()).subgraph("subgraph")
												.bothV();
									}

								}

								listSubgraphTraversal.add(subgraphTraversal);

							}

						}

					}

					if (!next.isEmpty() && nextStep instanceof EmptyStep) {

						GraphTraversal subgraphTraversal = graph.traversal().V(e);

						for (TraversalEdge te : next) {

							if (te.getDirection() == Direction.OUT) {

								subgraphTraversal = subgraphTraversal.outE(te.getLabels()).subgraph("subgraph").inV();
							} else if (te.getDirection() == Direction.IN) {

								subgraphTraversal = subgraphTraversal.inE(te.getLabels()).subgraph("subgraph").outV();
							} else {

								subgraphTraversal = subgraphTraversal.bothE(te.getLabels()).subgraph("subgraph")
										.bothV();
							}

						}

						listSubgraphTraversal.add(subgraphTraversal);

					}

				} while (!(nextStep instanceof EmptyStep));

				Step previousStep = s;
				do {
					previousStep = previousStep.getPreviousStep();
					if (previousStep instanceof VertexStep) {
						previous.add(new TraversalEdge(((VertexStep) previousStep).getDirection().opposite(),
								((VertexStep) previousStep).getEdgeLabels()));
					} else if (previousStep instanceof TraversalParent) {

						Step previousStepChildren = null;
						List<TraversalEdge> previousChildren = new ArrayList<TraversalEdge>();

						TraversalParent parent = (TraversalParent) previousStep;

						List<Admin<Object, Object>> parentList = parent.getLocalChildren();

						for (int u = 0; u < parentList.size(); u++) {

							previousChildren.addAll(previous);
							DefaultGraphTraversal childrenTraversal = (DefaultGraphTraversal) parentList.get(u);

							List<Step> childrenSteps = childrenTraversal.getSteps();
							for (int x = 0; x < childrenSteps.size(); x++) {

								previousStepChildren = childrenSteps.get(x);

								if (previousStepChildren instanceof VertexStep) {
									previousChildren
											.add(new TraversalEdge(((VertexStep) previousStepChildren).getDirection(),
													((VertexStep) previousStepChildren).getEdgeLabels()));
								}
							}

							if (!previousChildren.isEmpty()) {

								GraphTraversal subgraphTraversal = graph.traversal().V(e);

								for (TraversalEdge te : previousChildren) {

									if (te.getDirection() == Direction.OUT) {

										subgraphTraversal = subgraphTraversal.outE(te.getLabels()).subgraph("subgraph")
												.inV();
									} else if (te.getDirection() == Direction.IN) {

										subgraphTraversal = subgraphTraversal.inE(te.getLabels()).subgraph("subgraph")
												.outV();
									} else {

										subgraphTraversal = subgraphTraversal.bothE(te.getLabels()).subgraph("subgraph")
												.bothV();
									}

								}

								listSubgraphTraversal.add(subgraphTraversal);

							}

						}

					}

					if (!previous.isEmpty() && previousStep instanceof EmptyStep) {

						GraphTraversal subgraphTraversal = graph.traversal().V(e);

						for (TraversalEdge te : previous) {

							if (te.getDirection() == Direction.OUT) {

								subgraphTraversal = subgraphTraversal.outE(te.getLabels()).subgraph("subgraph").inV();
							} else if (te.getDirection() == Direction.IN) {

								subgraphTraversal = subgraphTraversal.inE(te.getLabels()).subgraph("subgraph").outV();
							} else {

								subgraphTraversal = subgraphTraversal.bothE(te.getLabels()).subgraph("subgraph")
										.bothV();
							}

						}

						listSubgraphTraversal.add(subgraphTraversal);

					}
				} while (!(previousStep instanceof EmptyStep));

			}
		}

		Traversal[] traversalsSubgraph = listSubgraphTraversal.toArray(new Traversal[0]);
		TinkerGraph subgraph = (TinkerGraph) graph.traversal().V(e).union(traversalsSubgraph).cap("subgraph").next();
		GraphTraversal subgraphWeight = null;
		if (flagNot) {
			subgraphWeight = subgraph.traversal().withComputer().V()
					.program(SDRAlgorithm.build().query(query.asAdmin()).property("weight").create(null));
		} else {

			subgraphWeight = subgraph.traversal().withComputer().V()
					.program(SDRAlgorithm.build().query(query.asAdmin()).property("weight").create(null))
					.has("weight", P.gt(0.0d));
		}

		List<Map<String, Object>> listWeight = subgraphWeight.valueMap(true, "weight").toList();

		return listWeight;
	}


}
