package com.atenea.sdr.contest;

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

import com.atenea.sdr.contest.queries.Abandon;
import com.atenea.sdr.contest.queries.ContestPart;
import com.atenea.sdr.contest.queries.FunniestCaption;
import com.atenea.sdr.contest.queries.FunniestCaptionU;
import com.atenea.sdr.contest.queries.RecentPart;
import com.atenea.sdr.contest.queries.UnchosenCap;

public class ContestCaseApp {

	private final static Logger LOGGER = Logger.getLogger(ContestCaseApp.class.getName());
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

			fh = new FileHandler("MyContestLogFile" + fileWeight + ".log", true);
			LOGGER.addHandler(fh);
			SimpleFormatter formatter = new SimpleFormatter();
			fh.setFormatter(formatter);

			// Open graph
			Graph graph = TinkerGraph.open();
			graph.io(IoCore.graphml()).readGraph(fileName);
			System.out.println(graph);

			Boolean flagSubgraph = !fileName.startsWith("ContestGraph");

			switch (query) {

			case "1":

				LOGGER.info("Query RecentPart");
				RecentPart query1d = new RecentPart();
				query1d.run(graph, fh, flagSubgraph);
				break;

			case "2":

				LOGGER.info("Query ContestPart");
				ContestPart query1a = new ContestPart();
				query1a.run(graph, fh, flagSubgraph);
				break;

			case "3":

				LOGGER.info("Query UnchosenCap");
				UnchosenCap query1c = new UnchosenCap();
				query1c.run(graph, fh, flagSubgraph);
				break;

			case "4":

				LOGGER.info("Query FunniestCaption");
				FunniestCaption query1e = new FunniestCaption();
				query1e.run(graph, fh, flagSubgraph);

				break;

			case "5":

				LOGGER.info("Query Abandon");
				Abandon query1b = new Abandon();
				query1b.run(graph, fh, flagSubgraph);
				break;

			case "6":

				LOGGER.info("Query FunniestCaptionU");
				FunniestCaptionU query1f = new FunniestCaptionU();
				query1f.run(graph, fh, flagSubgraph);
				break;
			}

		} catch (IOException ex) {
			LOGGER.info("File not found");
		}

	}

}
