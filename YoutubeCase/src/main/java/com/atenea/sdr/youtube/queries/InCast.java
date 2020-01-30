package com.atenea.sdr.youtube.queries;

import static org.apache.tinkerpop.gremlin.structure.Column.keys;
import static org.apache.tinkerpop.gremlin.structure.Column.values;

import java.io.IOException;
import java.util.Collection;
import java.util.List;
import java.util.logging.FileHandler;
import java.util.logging.Logger;

import org.apache.tinkerpop.gremlin.process.traversal.P;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.__;
import org.apache.tinkerpop.gremlin.structure.Graph;

public class InCast {
	private final static Logger LOGGER = Logger.getLogger(InCast.class.getName());

	public void run(Graph graph, FileHandler fh, Boolean subgraph) throws IOException {

		LOGGER.addHandler(fh);

		// Run query

		for (int i = 0; i < 6; i++) {

			Long start = System.currentTimeMillis(); // init time

			List<Collection<Object>> result = graph.traversal().V().as("video").out("composed").where(__.out("contains").has("presence",1)).select("video").groupCount().unfold().where(__.select(values).is(P.gt(10))).select(keys).toList();
			
			Long end = System.currentTimeMillis(); // end time

			if (subgraph) {
				LOGGER.info(result.size() + " results for subgraph in " + (end - start) + " milliseconds");
			} else {
				LOGGER.info(result.size() + " results for graph in " + (end - start) + " milliseconds");
			}
		}

	}
}
