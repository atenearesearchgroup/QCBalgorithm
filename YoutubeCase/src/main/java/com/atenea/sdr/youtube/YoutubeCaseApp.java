package com.atenea.sdr.youtube;

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

import com.atenea.sdr.youtube.queries.AnimalPerson;
import com.atenea.sdr.youtube.queries.GetAnimalVideos;
import com.atenea.sdr.youtube.queries.InCast;
import com.atenea.sdr.youtube.queries.NotPresent;
import com.atenea.sdr.youtube.queries.Pets;
import com.atenea.sdr.youtube.queries.PresentSoon;

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

				LOGGER.info("Query NotPresent");
				NotPresent query1f = new NotPresent();
				query1f.run(graph, fh, flagSubgraph);
				break;

			case "3":

				LOGGER.info("Query AnimalPerson");
				AnimalPerson query1a = new AnimalPerson();
				query1a.run(graph, fh, flagSubgraph);
				break;

			case "4":

				LOGGER.info("Query PresentSoon");
				PresentSoon query1e = new PresentSoon();
				query1e.run(graph, fh, flagSubgraph);

				break;

			case "5":

				LOGGER.info("Query Pets");
				Pets query1c = new Pets();
				query1c.run(graph, fh, flagSubgraph);
				break;

			case "6":

				LOGGER.info("Query InCast");
				InCast query1b = new InCast();
				query1b.run(graph, fh, flagSubgraph);
				break;

			}

		} catch (IOException ex) {
			LOGGER.info("File not found");
		}

	}
}
