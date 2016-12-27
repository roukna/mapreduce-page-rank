package ufl.cloudcomp;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class PageRankMapper extends Mapper<Text, Text, Text, Text> {
	
	@Override
	protected void map(Text key, Text value, Context context)
			throws IOException, InterruptedException {

		context.write(key, value);

		Node node = Node.afterMR(value.toString());
		
		String[] adjList = node.getAdjacencyList();
		
		if ( adjList != null && adjList.length > 0) {
			
			double initialPageRank = node.getPageRank();
			//Distribute Page Rank to outgoing nodes
			
			double distPageRank = initialPageRank / (double) adjList.length; 
			
			for (int i = 0; i < adjList.length; i++) {

				String adjN = adjList[i];

				Node adjacentNode = new Node().setPageRank(distPageRank);
				
				context.write(new Text(adjN), new Text(adjacentNode.displayNode())); //emit(nodeid, pagerank) 
			}
		}
	}
}
