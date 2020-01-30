package com.atenea.sdr.youtube.queries;

import java.io.IOException;
import java.util.List;
import java.util.logging.FileHandler;
import java.util.logging.Logger;

import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.__;
import org.apache.tinkerpop.gremlin.structure.Graph;

public class PresentSoon {

	private final static Logger LOGGER = Logger.getLogger(PresentSoon.class.getName());

	public void run(Graph graph, FileHandler fh, Boolean subgraph) throws IOException {

		LOGGER.addHandler(fh);

		// Run query

		for (int i = 0; i < 6; i++) {

			Long start = System.currentTimeMillis(); // init time

			List<Object> result = graph.traversal().V().as("video").out("composed").and(__.out("contains")
					.has("timestamp",3000).has("presence", 1),__.out("contains")
					.has("timestamp", 2000).has("presence", 1),__.out("contains")
					.has("timestamp", 1000).has("presence", 1)).select("video").dedup().toList();

			Long end = System.currentTimeMillis(); // end time

			if (subgraph) {
				LOGGER.info(result.size() + " results for subgraph in " + (end - start) + " milliseconds");
			} else {
				LOGGER.info(result.size() + " results for graph in " + (end - start) + " milliseconds");
			}
		}

	}

}
