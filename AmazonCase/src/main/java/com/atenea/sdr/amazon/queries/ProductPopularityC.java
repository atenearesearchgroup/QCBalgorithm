package com.atenea.sdr.amazon.queries;

import java.io.IOException;
import java.util.List;
import java.util.logging.FileHandler;
import java.util.logging.Logger;

import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.__;
import org.apache.tinkerpop.gremlin.structure.Graph;
import org.apache.tinkerpop.gremlin.structure.Vertex;

public class ProductPopularityC {
	private final static Logger LOGGER = Logger.getLogger(ProductPopularityC.class.getName());

	public void run(Graph graph, FileHandler fh, Boolean subgraph) throws IOException {

		LOGGER.addHandler(fh);

		// Run query

		for (int i = 0; i < 6; i++) {

			Long start = System.currentTimeMillis(); // init time

			List<Vertex> result = graph.traversal().V()
					.where(__.out("orders").out("contains").has("idProduct", "product 10")).dedup().toList();
			
			Long end = System.currentTimeMillis(); // end time

			if (subgraph) {
				LOGGER.info(result.size() + " results for subgraph in " + (end - start) + " milliseconds");
			} else {
				LOGGER.info(result.size() + " results for graph in " + (end - start) + " milliseconds");
			}
		}

	}
}
