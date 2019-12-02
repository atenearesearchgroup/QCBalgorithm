package com.atenea.qcb.amazon.queries;

import static org.apache.tinkerpop.gremlin.structure.Column.values;

import java.io.IOException;
import java.util.List;
import java.util.logging.FileHandler;
import java.util.logging.Logger;

import org.apache.tinkerpop.gremlin.process.traversal.P;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.__;
import org.apache.tinkerpop.gremlin.structure.Graph;

public class PreferenceCustomer {
	private final static Logger LOGGER = Logger.getLogger(PreferenceCustomer.class.getName());

	public void run(Graph graph, FileHandler fh, Boolean subgraph) throws IOException {

		LOGGER.addHandler(fh);

		// Run query

		for (int i = 0; i < 6; i++) {

			Long start = System.currentTimeMillis(); // init time

			List<Object> result = graph.traversal().V().has("idProduct", "product 10").in("contains").in("orders")
					.groupCount().unfold().where(__.select(values).is(P.gte(3))).toList();
			
			Long end = System.currentTimeMillis(); // end time

			if (subgraph) {
				LOGGER.info(result.size() + " results for subgraph in " + (end - start) + " milliseconds");
			} else {
				LOGGER.info(result.size() + " results for graph in " + (end - start) + " milliseconds");
			}
		}

	}
}
