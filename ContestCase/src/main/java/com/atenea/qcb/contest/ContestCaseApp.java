package com.atenea.qcb.contest;

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

import com.atenea.qcb.contest.queries.ChoiceNotChosen;
import com.atenea.qcb.contest.queries.ContestParticipation;
import com.atenea.qcb.contest.queries.FledParticipant;
import com.atenea.qcb.contest.queries.FunniestCaption;
import com.atenea.qcb.contest.queries.FunniestCaptionUnbiased;
import com.atenea.qcb.contest.queries.ParticipantRate;

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

				LOGGER.info("Query ParticipantRate");
				ParticipantRate query1d = new ParticipantRate();
				query1d.run(graph, fh, flagSubgraph);
				break;

			case "2":

				LOGGER.info("Query ContestParticipation");
				ContestParticipation query1a = new ContestParticipation();
				query1a.run(graph, fh, flagSubgraph);
				break;

			case "3":

				LOGGER.info("Query ChoiceNotChosen");
				ChoiceNotChosen query1c = new ChoiceNotChosen();
				query1c.run(graph, fh, flagSubgraph);
				break;

			case "4":

				LOGGER.info("Query FunniestCaption");
				FunniestCaption query1e = new FunniestCaption();
				query1e.run(graph, fh, flagSubgraph);

				break;

			case "5":

				LOGGER.info("Query FledParticipant");
				FledParticipant query1b = new FledParticipant();
				query1b.run(graph, fh, flagSubgraph);
				break;

			case "6":

				LOGGER.info("Query FunniestCaptionUnbiased");
				FunniestCaptionUnbiased query1f = new FunniestCaptionUnbiased();
				query1f.run(graph, fh, flagSubgraph);
				break;
			}

		} catch (IOException ex) {
			LOGGER.info("File not found");
		}

	}

}
