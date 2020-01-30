package com.atenea.sdr.contest.queries;

import java.io.IOException;
import java.util.List;
import java.util.logging.FileHandler;
import java.util.logging.Logger;

import org.apache.tinkerpop.gremlin.process.traversal.P;
import org.apache.tinkerpop.gremlin.structure.Graph;

public class RecentPart {

	private final static Logger LOGGER = Logger.getLogger(RecentPart.class.getName());

	public void run(Graph graph, FileHandler fh, Boolean subgraph) throws IOException {

		LOGGER.addHandler(fh);

		// Run query

		for (int i = 0; i < 6; i++) {

			Long start = System.currentTimeMillis(); // init time

			List<Object> result = graph.traversal().V().as("participant").in("askedTo").hasLabel("Question")
					.has("date", P.inside(1467331200000L, 1472688000000L)).select("participant").toList();

			Long end = System.currentTimeMillis(); // end time

			if (subgraph) {

				LOGGER.info(result.size() + " results for subgraph in " + (end - start) + " milliseconds");
			} else {

				LOGGER.info(result.size() + " results for graph in " + (end - start) + " milliseconds");
			}
		}

	}
}
