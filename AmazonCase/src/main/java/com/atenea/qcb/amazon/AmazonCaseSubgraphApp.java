package com.atenea.qcb.amazon;

import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.logging.FileHandler;
import java.util.logging.Logger;
import java.util.logging.SimpleFormatter;

import org.apache.tinkerpop.gremlin.process.traversal.P;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversal;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.__;
import org.apache.tinkerpop.gremlin.structure.Graph;
import org.apache.tinkerpop.gremlin.structure.io.IoCore;
import org.apache.tinkerpop.gremlin.tinkergraph.structure.TinkerGraph;

import com.atenea.qcb.amazon.tools.ObtainSubgraph;

public class AmazonCaseSubgraphApp {

	private final static Logger LOGGER = Logger.getLogger(AmazonCaseSubgraphApp.class.getName());
	static int threads = 8;
	static ExecutorService executor = Executors.newFixedThreadPool(threads);

	public static <E> void main(String[] args) throws Exception {

		FileHandler fh;

		try (InputStream input = new FileInputStream("src/main/resources/config.properties")) {

			// This block configure the logger with handler and formatter

			Properties prop = new Properties();

			// load a properties file
			prop.load(input);

			// get the property value and print it out
			String fileName = prop.getProperty("fileName");

			String fileWeight = prop.getProperty("nameWeights");

			String query = prop.getProperty("query");

			fh = new FileHandler("MyAmazonLogFile" + fileWeight + ".log", true);
			LOGGER.addHandler(fh);
			SimpleFormatter formatter = new SimpleFormatter();
			fh.setFormatter(formatter);

			String name = "";
			GraphTraversal<?, ?> queryTraversal = null;
			
			// Open graph
			Graph graph = TinkerGraph.open();
			graph.io(IoCore.graphml()).readGraph(fileName);

			switch (query) {
			case "1":
				LOGGER.info("Query ProductPopularity");
				name = "ProductPopularity" + fileWeight;
				queryTraversal = graph.traversal().V().out("orders").out("contains").has("idProduct", "product 10");  
				break;

			case "2":
				LOGGER.info("Query ProductPopularityWhere");
				name = "ProductPopularityWhere" + fileWeight;
				queryTraversal = graph.traversal().V().where(__.out("orders").out("contains").has("idProduct", "product 10"));
				break;
			case "3":
				LOGGER.info("Query AlternativeCustomer");
				List<String> idProducts = new ArrayList<String>();
				for (int i = 1; i <= 30; i++) {
					idProducts.add("product " + i);
				}
				name = "AlternativeCustomer" + fileWeight;
				queryTraversal = graph.traversal().V().hasLabel("Customer")
						.not(__.out("orders").out("contains").has("idProduct", P.within(idProducts)));
				break;

			case "4":
				LOGGER.info("Query PackagePopularity");
				name = "PackagePopularity" + fileWeight;
				queryTraversal = graph.traversal().V()
						.and(__.out("orders").out("contains").has("idProduct", "product 10"),
								__.out("orders").out("contains").has("idProduct", "product 20"))
						;
				break;

			case "5":
				LOGGER.info("Query SimilarProductsPopularity");
				name = "SimilarProductsPopularity" + fileWeight;
				queryTraversal = graph.traversal().V().or(
						__.out("orders").out("contains").has("idProduct", "product 20")	,
						__.out("orders").out("contains").has("idProduct", "product 10"));
				break;

			case "6":
				LOGGER.info("Query PreferenceCustomer");
				name = "PreferenceCustomer" + fileWeight;
				queryTraversal = graph.traversal().V().has("idProduct", "product 10").in("contains").in("orders");
				break;

			case "7":
				LOGGER.info("Query PreferenceCustomerSimilarProducts");
				name = "PreferenceCustomerSimilarProducts" + fileWeight;
				queryTraversal = graph.traversal().V().has("idProduct", P.within("product 10","product 20")).in("contains").in("orders");
				break;


			}

			ObtainSubgraph newSubgraph = new ObtainSubgraph();

			newSubgraph.run(graph, fh, queryTraversal, name);

		} catch (IOException ex) {
			LOGGER.info("File not found");
		}

	}

}
