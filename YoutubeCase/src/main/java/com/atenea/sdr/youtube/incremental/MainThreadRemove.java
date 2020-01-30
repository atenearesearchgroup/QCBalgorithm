package com.atenea.sdr.youtube.incremental;

import static org.apache.tinkerpop.gremlin.structure.Column.keys;
import static org.apache.tinkerpop.gremlin.structure.Column.values;

import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.Date;
import java.util.HashSet;
import java.util.List;
import java.util.Properties;
import java.util.Set;
import java.util.logging.FileHandler;
import java.util.logging.Logger;
import java.util.stream.Collectors;

import org.apache.tinkerpop.gremlin.process.traversal.P;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversal;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.__;
import org.apache.tinkerpop.gremlin.structure.Graph;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.apache.tinkerpop.gremlin.structure.io.IoCore;
import org.apache.tinkerpop.gremlin.tinkergraph.structure.TinkerGraph;
@SuppressWarnings("rawtypes")
public class MainThreadRemove implements Runnable {

	private final static Logger LOGGER = Logger.getLogger(MainThreadRemove.class.getName());
	// Parameters
	private Set<Vertex> vertices = new HashSet<Vertex>();
	private Graph graph = null;
	private Graph subgraph = null;
	private GraphTraversal queryTraversal = null;
	private Boolean incremental = null;

	Long records = 0L;
	FileHandler fh;

	public MainThreadRemove(FileHandler fh) {
		this.fh = fh;
	}

	@SuppressWarnings("unused")
	public void startComputing() throws IOException, InterruptedException {

		try (InputStream input = new FileInputStream("src/main/resources/config.properties")) {

			Properties prop = new Properties();

			// load a properties file
			prop.load(input);

			// get the property value and print it out
			String fileName = prop.getProperty("fileName");
			String fileWeight = prop.getProperty("nameWeights");
			Long recordsQuery = Long.parseLong(prop.getProperty("recordsQuery"));
			String query = prop.getProperty("query");

			setRecords(Long.parseLong(prop.getProperty("records")));
			setIncremental(Boolean.parseBoolean(prop.getProperty("incremental")));
			setGraph(fileName);
			setQueryTraversal(query);

			// handler
			LOGGER.addHandler(fh);

			// Segment list
			List<Vertex> segmentList = graph.traversal().V().hasLabel("Segment").toList();

			while (getSubgraph() == null) {
			}

			LOGGER.info(subgraph.toString());

			runQuery(subgraph, query);

			for (int x = 0; x < records; x++) {

				long start = new Date().getTime();
				while (new Date().getTime() - start < 1000L) { // wait a second to remove the next object
				}

				Set<Vertex> verticesToUpdate = new HashSet<Vertex>();

				verticesToUpdate.addAll(graph.traversal().V(segmentList.get(x).id()).both().toList());
				

				getGraph().traversal().V(segmentList.get(x).id()).property("delete","true").iterate();
				

				modifyVertices(verticesToUpdate);

				if ((x + 1) % recordsQuery == 0) {

					System.out.println(getSubgraph());
					runQuery(getSubgraph(), query);

				}
			}

		}
	}

	public void runQuery(Graph graphRun, String query) {
		List result = null;
		Long start = System.currentTimeMillis(); // init time
		switch (query) {
		case "1":
			result = graphRun.traversal().V()
					.where(__.out("contains").has("className",
							P.within("cat", "dog", "bird", "zebra", "cow", "bear", "horse", "giraffe", "elephant")))
					.toList();
			break;
		case "2":

			result = graphRun.traversal().V().hasLabel("Segment").not(__.out("contains").has("presence", 1)).toList();
			break;

		case "3":

			result = graphRun.traversal().V()
					.and(__.out("contains").has("className", "person"), __.out("contains").has("className",
							P.within("cat", "dog", "bird", "zebra", "cow", "bear", "horse", "giraffe", "elephant")))
					.toList();
			break;

		case "4":

			result = graphRun.traversal().V().as("video").out("composed")
					.and(__.out("contains").has("timestamp", 3000).has("presence", 1),
							__.out("contains").has("timestamp", 2000).has("presence", 1),
							__.out("contains").has("timestamp", 1000).has("presence", 1))
					.select("video").dedup().toList();
			break;

		case "5":

			result = graphRun.traversal().V().has("presence", 1)
					.or(__.in("contains").out("tracks").has("className", "dog"),
							__.in("contains").out("tracks").has("className", "cat"))
					.toList();
			break;

		case "6":

			result = graphRun.traversal().V().as("video").out("composed").where(__.out("contains").has("presence", 1))
					.select("video").groupCount().unfold().where(__.select(values).is(P.gt(10))).select(keys).toList();
			break;
		}

		Long end = System.currentTimeMillis(); // end time

		LOGGER.info(
				result.size() + " results for subgraph in " + (end - start) + " milliseconds. Timestamp end: " + end);

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
			LOGGER.info("GetAnimalVideos");
			queryTraversal = graph.traversal().V().where(__.out("contains").has("className",
					P.within("cat", "dog", "bird", "zebra", "cow", "bear", "horse", "giraffe", "elephant")));
			break;

		case "2":

			LOGGER.info("NotPresent");
			queryTraversal = graph.traversal().V().hasLabel("Segment").not(__.out("contains").has("presence", 1));
			break;
		case "3":

			LOGGER.info("AnimalPerson");
			queryTraversal = graph.traversal().V().and(__.out("contains").has("className", "person"),
					__.out("contains").has("className",
							P.within("cat", "dog", "bird", "zebra", "cow", "bear", "horse", "giraffe", "elephant")));
			break;

		case "4":

			LOGGER.info("PresentSoon");
			queryTraversal = graph.traversal().V().out("composed").and(
					__.out("contains").has("timestamp", 3000).has("presence", 1),
					__.out("contains").has("timestamp", 2000).has("presence", 1),
					__.out("contains").has("timestamp", 1000).has("presence", 1));
			break;

		case "5":

			LOGGER.info("Pets");
			queryTraversal = graph.traversal().V().has("presence", 1).or(
					__.in("contains").out("tracks").has("className", "dog"),
					__.in("contains").out("tracks").has("className", "cat"));
			break;

		case "6":

			LOGGER.info("InCast");
			queryTraversal = graph.traversal().V().out("composed").where(__.out("contains").has("presence", 1));
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
			e.printStackTrace();
		} catch (InterruptedException e) {
			e.printStackTrace();
		}
	}
}