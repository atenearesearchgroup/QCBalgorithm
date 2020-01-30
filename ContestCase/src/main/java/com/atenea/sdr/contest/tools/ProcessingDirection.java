package com.atenea.sdr.contest.tools;

import java.util.Arrays;
import java.util.List;

import org.apache.tinkerpop.gremlin.process.computer.MessageScope;
import org.apache.tinkerpop.gremlin.process.computer.Messenger;
import org.apache.tinkerpop.gremlin.process.traversal.Step;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.__;
import org.apache.tinkerpop.gremlin.process.traversal.step.filter.HasStep;
import org.apache.tinkerpop.gremlin.process.traversal.step.map.VertexStep;
import org.apache.tinkerpop.gremlin.process.traversal.step.util.HasContainer;
import org.apache.tinkerpop.gremlin.structure.Vertex;

public class ProcessingDirection {

	@SuppressWarnings("rawtypes")
	public static void sendForward(Step step, Messenger<List<Double>> messenger) {
		MessageScope.Local<List<Double>> scope = null;
		if (((VertexStep) step).getDirection().toString().equals("OUT")) {
			String link = ((VertexStep) step).getEdgeLabels()[0];

			scope = MessageScope.Local.of(() -> __.outE(link));

		} else

		{
			String link = ((VertexStep) step).getEdgeLabels()[0];
			scope = MessageScope.Local.of(() -> __.inE(link));

		}
		List<Double> list = Arrays.asList(1.0d, 0.0d);
		messenger.sendMessage(scope, list);

	}

	@SuppressWarnings("rawtypes")
	public static void sendBackward(Step step, Messenger<List<Double>> messenger) {

		MessageScope.Local<List<Double>> scope = null;
		VertexStep previousStep = (VertexStep) step;
		if (previousStep.getDirection().toString().equals("OUT")) {

			String link = previousStep.getEdgeLabels()[0];

			scope = MessageScope.Local.of(() -> __.inE(link));

		} else {
			String link = previousStep.getEdgeLabels()[0];

			scope = MessageScope.Local.of(() -> __.outE(link));

		}
		List<Double> list = Arrays.asList(0.0d, 1.0d);

		messenger.sendMessage(scope, list);

	}

	// PROCESS HASSTEP

	public static boolean processHasStep(HasStep<?> step, Vertex vertex) {
		Boolean flag = true;
		List<HasContainer> stepContainer = step.getHasContainers();
		for (HasContainer s : stepContainer) {

			if (s.getKey().equalsIgnoreCase("~label")) {
				if (!vertex.label().equals(s.getValue())) {
					flag = false;
				}
			} else {

				if (!vertex.keys().contains(s.getKey()) || !s.test(vertex)) {
					flag = false;
				}
			}
		}

		return flag;
	}

}
