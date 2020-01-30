package com.atenea.sdr.amazon;

import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.Properties;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.logging.FileHandler;
import java.util.logging.Logger;
import java.util.logging.SimpleFormatter;

import org.apache.tinkerpop.gremlin.structure.Graph;
import org.apache.tinkerpop.gremlin.structure.io.IoCore;
import org.apache.tinkerpop.gremlin.tinkergraph.structure.TinkerGraph;

import com.atenea.sdr.amazon.queries.AlternativeCustomer;
import com.atenea.sdr.amazon.queries.PackagePopularity;
import com.atenea.sdr.amazon.queries.PrefCustomer;
import com.atenea.sdr.amazon.queries.PrefCustomerSimProducts;
import com.atenea.sdr.amazon.queries.ProductPopularity;
import com.atenea.sdr.amazon.queries.ProductPopularityC;
import com.atenea.sdr.amazon.queries.SimProductsPopularity;

public class AmazonCaseApp {

	private final static Logger LOGGER = Logger.getLogger(AmazonCaseApp.class.getName());
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
			
			

			fh = new FileHandler("MyAmazonLogFile" + fileWeight + ".log",true);
			LOGGER.addHandler(fh);
			SimpleFormatter formatter = new SimpleFormatter();
			fh.setFormatter(formatter);

			// Open graph
			Graph graph = TinkerGraph.open();
			graph.io(IoCore.graphml()).readGraph(fileName); //UsersByProduct31K.graphml
			
			
			Boolean flagSubgraph = !fileName.startsWith("AmazonData");
			
			switch (query) {

			case "1":

				LOGGER.info("Query ProductPopularity");
				ProductPopularity query1a = new ProductPopularity();
				query1a.run(graph, fh, flagSubgraph);
				break;

			case "2":
				LOGGER.info("Query ProductPopularityC");
				ProductPopularityC query1b = new ProductPopularityC();
				query1b.run(graph, fh, flagSubgraph);
				break;

			case "3":

				LOGGER.info("Query AlternativeCustomer");
				AlternativeCustomer query1c = new AlternativeCustomer();
				query1c.run(graph, fh, flagSubgraph);
				break;

			case "4":

				LOGGER.info("Query PackagePopularity");
				PackagePopularity query1d = new PackagePopularity();
				query1d.run(graph, fh, flagSubgraph);
				break;

			case "5":

				LOGGER.info("Query SimProductsPopularity");
				SimProductsPopularity query1e = new SimProductsPopularity();
				query1e.run(graph, fh, flagSubgraph);

				break;

			case "6":

				LOGGER.info("Query PrefCustomer");
				PrefCustomer query1f = new PrefCustomer();
				query1f.run(graph, fh, flagSubgraph);
				break;
				
			case "7":

				LOGGER.info("Query PrefCustomerSimProducts");
				PrefCustomerSimProducts query1g = new PrefCustomerSimProducts();
				query1g.run(graph, fh, flagSubgraph);
				break;
			}
			
		} catch (IOException ex) {
			LOGGER.info("File not found");
		}

	}

}
