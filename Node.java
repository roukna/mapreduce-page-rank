package ufl.cloudcomp;

import org.apache.commons.lang.StringUtils;

import java.io.IOException;
import java.util.Arrays;

public class Node {

	private double pageRank = 0.25;
	private String[] adjacencyList;

	public double getPageRank() {
		return pageRank;
	}

	public Node setPageRank(double pageRank) {
		this.pageRank = pageRank;
		return this;
	}

	public String[] getAdjacencyList() {
		return adjacencyList;
	}

	public Node setAdjacencyList(String[] adjacencyList) {
		this.adjacencyList = adjacencyList;
		return this;
	}

	public boolean checkIsNode() {
		return adjacencyList != null;
	}

	public String displayNode() {
	
		String sNode = new String();
		sNode = sNode + pageRank;

		if (getAdjacencyList() != null) {
			sNode = sNode + '\t' + StringUtils.join(getAdjacencyList(), '\t');
		}
		return sNode;
	}

	
	public static Node afterMR(String value) throws IOException {

		String[] adjN = StringUtils.splitPreserveAllTokens(value, '\t');

		Node node = new Node().setPageRank(Double.valueOf(adjN[0]));
		if (adjN.length > 1) {
			node.setAdjacencyList(Arrays.copyOfRange(adjN, 1, adjN.length));
		}
		return node;
	}
}
