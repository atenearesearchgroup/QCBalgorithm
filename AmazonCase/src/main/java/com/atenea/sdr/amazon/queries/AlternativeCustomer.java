package com.atenea.sdr.amazon.queries;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.logging.FileHandler;
import java.util.logging.Logger;

import org.apache.tinkerpop.gremlin.process.traversal.P;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.__;
import org.apache.tinkerpop.gremlin.structure.Graph;

public class AlternativeCustomer {
	
	private final static Logger LOGGER = Logger.getLogger(AlternativeCustomer.class.getName());

	public void run(Graph graph, FileHandler fh, Boolean subgraph) throws IOException {

		LOGGER.addHandler(fh);
		
		List<String> idProducts = new ArrayList<String>();
		for (int i = 1; i <= 30; i++) {
			idProducts.add("product " + i);
		}

		// Run query

		for (int i = 0; i < 6; i++) {

			Long start = System.currentTimeMillis(); // init time

			List<Object> result = graph.traversal().V().hasLabel("Customer").as("user")
					.not(__.out("orders").out("contains").has("idProduct", P.within(idProducts))).select("user").dedup().toList();
			
			Long end = System.currentTimeMillis(); // end time

			if (subgraph) {
				LOGGER.info(result.size() + " results for subgraph in " + (end - start) + " milliseconds");
			} else {
				LOGGER.info(result.size() + " results for graph in " + (end - start) + " milliseconds");
			}
		}

	}

}
