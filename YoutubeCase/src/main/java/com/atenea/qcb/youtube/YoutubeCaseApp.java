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

import org.apache.tinkerpop.gremlin.structure.Graph;
import org.apache.tinkerpop.gremlin.structure.io.IoCore;
import org.apache.tinkerpop.gremlin.tinkergraph.structure.TinkerGraph;

import com.atenea.qcb.youtube.queries.AnimalHumanInteraction;
import com.atenea.qcb.youtube.queries.CrowdedDetectionVideo;
import com.atenea.qcb.youtube.queries.DomesticAnimalPicture;
import com.atenea.qcb.youtube.queries.GetAnimalVideos;
import com.atenea.qcb.youtube.queries.PresenceAtBeginning;
import com.atenea.qcb.youtube.queries.SegmentAbsence;

/**
 * Hello world!
 *
 */
public class YoutubeCaseApp {
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
			System.out.println(graph);
			Boolean flagSubgraph = !fileName.startsWith("Youtube");

			switch (query) {

			case "1":

				LOGGER.info("Query GetAnimalVideos");
				GetAnimalVideos query1d = new GetAnimalVideos();
				query1d.run(graph, fh, flagSubgraph);
				break;

			case "2":

				LOGGER.info("Query SegmentAbsence");
				SegmentAbsence query1f = new SegmentAbsence();
				query1f.run(graph, fh, flagSubgraph);
				break;

			case "3":

				LOGGER.info("Query AnimalHumanInteraction");
				AnimalHumanInteraction query1a = new AnimalHumanInteraction();
				query1a.run(graph, fh, flagSubgraph);
				break;

			case "4":

				LOGGER.info("Query PresenceAtBeginning");
				PresenceAtBeginning query1e = new PresenceAtBeginning();
				query1e.run(graph, fh, flagSubgraph);

				break;

			case "5":

				LOGGER.info("Query DomesticAnimalPicture");
				DomesticAnimalPicture query1c = new DomesticAnimalPicture();
				query1c.run(graph, fh, flagSubgraph);
				break;

			case "6":

				LOGGER.info("Query CrowdedDetectionVideo");
				CrowdedDetectionVideo query1b = new CrowdedDetectionVideo();
				query1b.run(graph, fh, flagSubgraph);
				break;

			}

		} catch (IOException ex) {
			LOGGER.info("File not found");
		}

	}
}
