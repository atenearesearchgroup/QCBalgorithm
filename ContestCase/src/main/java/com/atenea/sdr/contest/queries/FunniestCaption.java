package com.atenea.sdr.contest.queries;

import static org.apache.tinkerpop.gremlin.structure.Column.keys;
import static org.apache.tinkerpop.gremlin.structure.Column.values;

import java.io.IOException;
import java.util.Collection;
import java.util.List;
import java.util.logging.FileHandler;
import java.util.logging.Logger;

import org.apache.tinkerpop.gremlin.process.traversal.Order;
import org.apache.tinkerpop.gremlin.structure.Graph;

public class FunniestCaption {

	private final static Logger LOGGER = Logger.getLogger(FunniestCaption.class.getName());

	public void run(Graph graph, FileHandler fh, Boolean subgraph) throws IOException {

		LOGGER.addHandler(fh);

		// Run query

		for (int i = 0; i < 6; i++) {

			Long start = System.currentTimeMillis(); // init time

			List<Collection<Object>> result = graph.traversal().V().as("caption").in("contains").out("askedTo")
					.out("answers").has("rate", 3).select("caption").groupCount().unfold().order()
					.by(values, Order.desc).select(keys).limit(1).toList();
			Long end = System.currentTimeMillis(); // end time

			if (subgraph) {

				LOGGER.info(result + " results for subgraph in " + (end - start) + " milliseconds");
			}

			else {
				LOGGER.info(result + " results for graph in " + (end - start) + " milliseconds");
			}

		}
	}

}
