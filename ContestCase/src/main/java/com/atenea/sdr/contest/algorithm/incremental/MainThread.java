package com.atenea.sdr.contest.algorithm.incremental;

import static org.apache.tinkerpop.gremlin.structure.Column.keys;
import static org.apache.tinkerpop.gremlin.structure.Column.values;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileReader;
import java.io.IOException;
import java.io.InputStream;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.HashSet;
import java.util.List;
import java.util.Properties;
import java.util.Set;
import java.util.logging.FileHandler;
import java.util.logging.Logger;
import java.util.stream.Collectors;

import org.apache.tinkerpop.gremlin.process.traversal.Order;
import org.apache.tinkerpop.gremlin.process.traversal.P;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversal;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.__;
import org.apache.tinkerpop.gremlin.structure.Graph;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.apache.tinkerpop.gremlin.structure.io.IoCore;
import org.apache.tinkerpop.gremlin.tinkergraph.structure.TinkerGraph;

@SuppressWarnings("rawtypes")
public class MainThread implements Runnable {

	private final static Logger LOGGER = Logger.getLogger(MainThread.class.getName());
	// Parameters
	private Set<Vertex> vertices = new HashSet<Vertex>();
	private Graph graph = null;
	private Graph subgraph = null;

	private GraphTraversal queryTraversal = null;
	private Boolean incremental = null;

	Long records = 0L;
	FileHandler fh;

	public MainThread(FileHandler fh) {
		this.fh = fh;
	}

	@SuppressWarnings("unused")
	public void startComputing() throws IOException, InterruptedException, NumberFormatException, ParseException {

		try (InputStream input = new FileInputStream("src/main/resources/config.properties")) {

			Properties prop = new Properties();

			// load a properties file
			prop.load(input);

			// get the property value and print it out
			String fileName = prop.getProperty("fileName");
			Long records = Long.parseLong(prop.getProperty("records"));
			Long recordsQuery = Long.parseLong(prop.getProperty("recordsQuery"));
			String fileWeight = prop.getProperty("nameWeights");
			String query = prop.getProperty("query");
			setIncremental(Boolean.parseBoolean(prop.getProperty("incremental")));

			setGraph(fileName);
			setRecords(records);
			setQueryTraversal(query);

			// handler
//			setFh(new FileHandler("MyLogFileIncremental" + fileWeight + ".log", true));
			LOGGER.addHandler(fh);
			SimpleDateFormat formatter = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.ssssss");

			// File with new orders
			File fileContest = new File("src/main/resources/513-responses.csv");
			BufferedReader br = new BufferedReader(new FileReader(fileContest));
			String idContest = "";
			HashSet idsWithWeight = new HashSet();

			while (getSubgraph() == null) {
			}

			LOGGER.info(subgraph.toString());

			runQuery(subgraph, query);

			String line = br.readLine();
			Vertex contest = graph.traversal().addV("Contest").property("idContest", 513L).next();
			// Add nodes and update weights
			for (int x = 0; x < records; x++) {
				line = br.readLine();
				String[] values = line.split(",");
				Set<Vertex> vertices = new HashSet<Vertex>();

				if (values.length > 9) {

					String valAux = values[5];
					for (int i = 6; i < values.length - 3; i++) {
						valAux = valAux.concat(",").concat(values[i]);
					}
					values[5] = valAux;
					values[6] = values[values.length - 3];
					values[7] = values[values.length - 2];
					values[8] = values[values.length - 1];

				}

				Vertex participant;
				if (graph.traversal().V().has("idParticipant", values[3]).hasNext() == false) {
					participant = graph.traversal().addV("Participant").property("idParticipant", values[3])
							.property("uidParticipant", values[3]).next();
					vertices.add(participant);

				} else {
					participant = graph.traversal().V().has("idParticipant", values[3]).next();
				}

				Vertex Caption;
				if (graph.traversal().V().has("idCaption", Long.parseLong(values[6])).hasNext() == false) {
					Caption = graph.traversal().addV("Caption").property("idCaption", Long.parseLong(values[6]))
							.property("content", values[5]).next();
					vertices.add(Caption);
				} else {
					Caption = graph.traversal().V().has("idCaption", Long.parseLong(values[6])).next();
				}

				Vertex Question = graph.traversal().addV("Question")
						.property("date", formatter.parse(values[8]).getTime()).next();

				Vertex Answer = graph.traversal().addV("Answer")
						.property("date",
								formatter.parse(values[8]).getTime() + (long) (Double.parseDouble(values[4]) * 1000))
						.property("rate", (int) Double.parseDouble(values[7])).next();

				contest.addEdge("formulated", Question);

				Question.addEdge("askedTo", participant);

				participant.addEdge("answers", Answer);

				Answer.addEdge("answers", Question);

				Vertex Algorithm = graph.traversal().V().has("label", values[1]).next();

				Algorithm.addEdge("generates", Question);

				Question.addEdge("contains", Caption);

				vertices.add(Answer);
				vertices.add(Question);

				if ((x + 1) % recordsQuery == 0) {

					System.out.println(subgraph);
					runQuery(subgraph, query);

				}
			}

			br.close();
		}

	}

	public void runQuery(Graph graphRun, String query) {
		List result = null;
		Long start = System.currentTimeMillis(); // init time
		switch (query) {
		case "1":
			result = graphRun.traversal().V().as("participant").in("askedTo").hasLabel("Question")
					.has("date", P.inside(1467331200000L, 1472688000000L)).select("participant").toList();
			break;
		case "2":

			result = graphRun.traversal().V().where(__.in("askedTo").in("formulates").has("idContest", 508)).dedup()
					.toList();
			break;

		case "3":

			result = graphRun.traversal().V().and(__.out("contains").has("idCaption", 61),
					__.in("answers").out("chooses").has("idCaption", P.neq(61))).toList();
			break;
		case "4":

			result = graphRun.traversal().V().as("caption").in("contains").out("askedTo").out("answers").has("rate", 3)
					.select("caption").groupCount().unfold().order().by(values, Order.desc).select(keys).limit(1)
					.toList();
			break;

		case "5":
			result = graphRun.traversal().V().has("rate", P.gt(0)).in("answers").groupCount().unfold()
					.where(__.select(values).is(P.eq(1))).select(keys).dedup().toList();
			break;
		case "6":
			result = graphRun.traversal().V().as("caption").in("contains")// .coin(0.95)
					.and(__.in("generates").has("label", P.within("RandomSampling", "RoundRobin")),
							__.out("askedTo").out("answers").has("rate", 3))
					.select("caption").groupCount().unfold().order().by(values, Order.desc).select(keys).limit(1)
					.toList();
			break;
		}

		Long end = System.currentTimeMillis(); // end time

		if (query.equals("4") || query.equals("6")) {
			LOGGER.info(result + " results for subgraph in " + (end - start) + " milliseconds. Timestamp end: " + end);
		} else {

			LOGGER.info(result.size() + " results for subgraph in " + (end - start) + " milliseconds. Timestamp end: "
					+ end);
		}

	}

	public synchronized Boolean getIncremental() {
		return incremental;
	}

	public synchronized void setIncremental(Boolean incremental) {
		this.incremental = incremental;
	}

	public synchronized FileHandler getFh() {
		return fh;
	}

	public synchronized void setFh(FileHandler fh) {
		this.fh = fh;
	}

	public synchronized GraphTraversal getQueryTraversal() {
		return queryTraversal;
	}

	public synchronized void setQueryTraversal(String query) {
		switch (query) {
		case "1":
			LOGGER.info("RecentPart");
			queryTraversal = graph.traversal().V().in("askedTo").hasLabel("Question").has("date",
					P.inside(1467331200000L, 1472688000000L));
			break;

		case "2":

			LOGGER.info("ContestPart");
			queryTraversal = graph.traversal().V().where(__.in("askedTo").in("formulates").has("idContest", 508));
			break;

		case "3":
			LOGGER.info("UnchosenCap");
			queryTraversal = graph.traversal().V().and(__.out("contains").has("idCaption", 61),
					__.in("answers").out("chooses").has("idCaption", P.neq(61)));
			break;
		case "4":

			LOGGER.info("FunniestCaption");
			queryTraversal = graph.traversal().V().in("contains").out("askedTo").out("answers").has("rate", 3);
			break;

		case "5":

			LOGGER.info("Abandon");
			queryTraversal = graph.traversal().V().has("rate", P.gt(0)).in("answers");
			break;

		case "6":
			LOGGER.info("FunniestCaptionU");
			queryTraversal = graph.traversal().V().in("contains").and(
					__.in("generates").has("label", P.within("RandomSampling", "RoundRobin")),
					__.out("askedTo").out("answers").has("rate", 3));
			break;
		}
	}

	public synchronized Graph getSubgraph() {
		return subgraph;
	}

	public synchronized void setSubgraph(Graph subgraph) {
		this.subgraph = subgraph;
	}

	public synchronized Graph getGraph() {
		return graph;
	}

	public synchronized void setGraph(String fileName) throws IOException {
		graph = TinkerGraph.open();
		graph.io(IoCore.graphml()).readGraph(fileName);
	}

	public synchronized Set<Vertex> accessVertices() {
		Set<Vertex> copyVertices = vertices.stream().collect(Collectors.toSet());
		return copyVertices;
	}

	public synchronized void modifyVertices(Set<Vertex> newVertices) {
		vertices.addAll(newVertices);
	}

	public synchronized Long getRecords() {
		return records;
	}

	public synchronized void setRecords(Long records) {
		this.records = records;
	}

	// This method is being called by IncrementalAlgThread class
	public synchronized void callBack(String results) {
		System.out.println(results);
	}

	public synchronized void clearVertices(Set<Vertex> oldVertices) {

		vertices.removeAll(oldVertices);
	}

	@Override
	public void run() {
		try {
			startComputing();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (InterruptedException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (NumberFormatException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (ParseException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}
}