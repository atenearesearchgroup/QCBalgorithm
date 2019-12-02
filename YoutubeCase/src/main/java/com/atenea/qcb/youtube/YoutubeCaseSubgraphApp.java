package com.atenea.qcb.youtube;

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

import com.atenea.qcb.youtube.tools.ObtainSubgraph;

public class YoutubeCaseSubgraphApp {
	private final static Logger LOGGER = Logger.getLogger(YoutubeCaseApp.class.getName());
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

			fh = new FileHandler("MyLogFile" + fileWeight + ".log", true);
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

				LOGGER.info("Query GetAnimalVideos");
				name = "GetAnimalVideos" + fileWeight;
				queryTraversal = graph.traversal().V().where(__.out("contains").has("className",
						P.within("cat", "dog", "bird", "zebra", "cow", "bear", "horse", "giraffe", "elephant")));
				break;

			case "2":

				LOGGER.info("Query PresenceAtBeginning");
				name = "PresenceAtBeginning" + fileWeight;
				queryTraversal = graph.traversal().V().out("composed").and(
						__.out("contains").has("timestamp", 3000).has("presence", 1),
						__.out("contains").has("timestamp", 2000).has("presence", 1),
						__.out("contains").has("timestamp", 1000).has("presence", 1));
				break;

			case "3":

				LOGGER.info("Query AnimalHumanInteraction");
				name = "AnimalHumanInteraction" + fileWeight;
				queryTraversal = graph.traversal().V().and(__.out("contains").has("className", "person"),
						__.out("contains").has("className", P.within("cat", "dog", "bird", "zebra", "cow", "bear",
								"horse", "giraffe", "elephant")));
				break;

			case "4":
				LOGGER.info("Query SegmentAbsence");
				name = "SegmentAbsence" + fileWeight;
				queryTraversal = graph.traversal().V().hasLabel("Segment").not(__.out("contains").has("presence", 1));
				break;

			case "5":

				LOGGER.info("Query DomesticAnimalPicture");
				name = "DomesticAnimalPicture" + fileWeight;
				queryTraversal = graph.traversal().V().has("presence", 1).or(
						__.in("contains").out("tracks").has("className", "dog"),
						__.in("contains").out("tracks").has("className", "cat"));
				break;

			case "6":

				LOGGER.info("Query CrowdedDetectionVideo");
				name = "CrowdedDetectionVideo" + fileWeight;
				queryTraversal = graph.traversal().V().out("composed").where(__.out("contains").has("presence", 1));

				break;

			}

			ObtainSubgraph newSubgraph = new ObtainSubgraph();

			newSubgraph.run(graph, fh, queryTraversal, name);

		} catch (IOException ex) {
			LOGGER.info("File not found");
		}
	}

}
