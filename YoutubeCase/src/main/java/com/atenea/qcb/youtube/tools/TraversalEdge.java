package com.atenea.qcb.youtube.tools;

import org.apache.tinkerpop.gremlin.structure.Direction;

public class TraversalEdge {
	
	Direction direction;
	String[] labels;
	public Direction getDirection() {
		return direction;
	}
	public void setDirection(Direction direction) {
		this.direction = direction;
	}
	public String[] getLabels() {
		return labels;
	}
	public void setLabels(String[] labels) {
		this.labels = labels;
	}
	public TraversalEdge(Direction direction, String[] labels) {
		super();
		this.direction = direction;
		this.labels = labels;
	}
	
	@Override
	public String toString() {
		return "Direction: " + direction + " Labels: " + labels;
	}

}
