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

import org.apache.tinkerpop.gremlin.process.traversal.P;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversal;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.__;
import org.apache.tinkerpop.gremlin.structure.Graph;
import org.apache.tinkerpop.gremlin.structure.io.IoCore;
import org.apache.tinkerpop.gremlin.tinkergraph.structure.TinkerGraph;

import com.atenea.sdr.contest.tools.ObtainSubgraph;

public class ContestCaseSubgraphApp {

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

			String name = "";
			GraphTraversal<?, ?> queryTraversal = null;

			switch (query) {
			case "1":

				LOGGER.info("Query RecentPart");
				name = "RecentPart" + fileWeight;
				queryTraversal = graph.traversal().V().in("askedTo").hasLabel("Question").has("date",
						P.inside(1467331200000L, 1472688000000L));
				break;

			case "2":

				LOGGER.info("Query ContestPart");
				name = "ContestPart" + fileWeight;
				queryTraversal = graph.traversal().V().where(__.in("askedTo").in("formulates").has("idContest", 508));
				break;

			case "3":
				LOGGER.info("Query UnchosenCap");
				name = "UnchosenCap" + fileWeight;
				queryTraversal = graph.traversal().V().and(__.out("contains").has("idCaption", 61),
						__.in("answers").out("chooses").has("idCaption", P.neq(61)));
				break;

			case "4":
				LOGGER.info("Query FunniestCaption");
				name = "FunniestCaption" + fileWeight;
				queryTraversal = graph.traversal().V().in("contains").out("askedTo").out("answers").has("rate", 3);
				break;

			case "5":

				LOGGER.info("Query Abandon");
				name = "Abandon" + fileWeight;
				queryTraversal = graph.traversal().V().has("rate", P.gt(0)).in("answers");
				break;

			case "6":
				LOGGER.info("Query FunniestCaptionU");
				name = "FunniestCaptionU" + fileWeight;
				queryTraversal = graph.traversal().V().in("contains").and(
						__.in("generates").has("label", P.within("RandomSampling", "RoundRobin")),
						__.out("askedTo").out("answers").has("rate", 3));
				break;
			}

			ObtainSubgraph newSubgraph = new ObtainSubgraph();

			newSubgraph.run(graph, fh, queryTraversal, name);

		} catch (IOException ex) {
			LOGGER.info("File not found");
		}
	}
}
