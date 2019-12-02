package com.atenea.qcb.youtube.tools;

import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.logging.FileHandler;
import java.util.logging.Logger;

import org.apache.tinkerpop.gremlin.process.traversal.P;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversal;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.__;
import org.apache.tinkerpop.gremlin.structure.Graph;
import org.apache.tinkerpop.gremlin.structure.T;
import org.apache.tinkerpop.gremlin.structure.io.IoCore;
import org.apache.tinkerpop.gremlin.tinkergraph.structure.TinkerGraph;

import com.atenea.qcb.youtube.algorithm.QCBAlgorithm;

public class ObtainSubgraph {
	
	private final static Logger LOGGER = Logger.getLogger(ObtainSubgraph.class.getName());

	@SuppressWarnings({ "unlikely-arg-type", "unchecked","rawtypes" })
	public void run(Graph graph, FileHandler fh, GraphTraversal query, String name) throws IOException {


		try (InputStream input = new FileInputStream("src/main/resources/config.properties")) {

			Properties prop = new Properties();

			// load a properties file
			prop.load(input);

			// get the property value and print it out
			LOGGER.addHandler(fh);
		}


		Long start1 = System.currentTimeMillis(); // init time

		
		GraphTraversal graphWeights = graph.traversal().withComputer().V()
				.program(QCBAlgorithm.build().query(query.asAdmin()).property("weight").create(null))
				.has("weight", P.gt(0.0d));

		List<Object> idsWithWeight = new ArrayList<Object>();


		List<Map<String, Object>> ids1 = (List<Map<String, Object>>) graphWeights.valueMap(true, "weight").toList();
		for (Map<String, Object> map : ids1) {
			idsWithWeight.add(map.get(T.id));
		}
		ids1 = null;

		graphWeights = null;
		Long end1 = System.currentTimeMillis(); // end time

		LOGGER.info(end1 - start1 + " milliseconds to compute weights");

		Long startSubGraph = System.currentTimeMillis(); // init time

		// Obtain subgraph
		graph = (TinkerGraph) graph.traversal().V(idsWithWeight).outE().subgraph("subGraph").inV()
				.where(__.values("weight").is(P.gte(1))).cap("subGraph").next(); // 1\
		graph.traversal().V().hasId(P.without(idsWithWeight)).drop().iterate();

		Long endSubGraph = System.currentTimeMillis(); // init time

		LOGGER.info(graph + " subgraph");

		LOGGER.info(" Subgraph in " + (endSubGraph - startSubGraph) + " milliseconds");
		
		graph.io(IoCore.graphml()).writeGraph(name + ".graphml");
		
	}

}
