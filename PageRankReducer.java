package ufl.cloudcomp;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

public class PageRankReducer extends Reducer<Text, Text, Text, Text> {

	public static final double CSF = 1000.0; // convergence scaling factor
	public static final double DF = 0.85; // damp factor
	public static String NO_OF_NODES = "pagerank.numnodes";
	private int totalNoOfNodes;

	public static enum Counter {
		CDELTA
	}

	@Override
	protected void setup(Context context) throws IOException,
			InterruptedException {
		
		Configuration conf = context.getConfiguration();
		totalNoOfNodes = conf.getInt(NO_OF_NODES, 0);
	}

	private Text outValue = new Text();

	public void reduce(Text key, Iterable<Text> values, Context context)
			throws IOException, InterruptedException {

		double sum = 0;
		double dampingFactor = ((1.0 - DF) / (double) totalNoOfNodes);
		double prev_page_rank = 0;
		
		Node origNode = new Node();
		
		for (Text textValue : values) {

			Node node = Node.afterMR(textValue.toString());
			
			if (node.checkIsNode()) {
				origNode = node;
				prev_page_rank = origNode.getPageRank();
			} else {
				sum += node.getPageRank();
			}
		}

		double newPageRank = dampingFactor + (DF * sum); 
		

		double delta = prev_page_rank - newPageRank;

		origNode.setPageRank(newPageRank);

		outValue.set(origNode.displayNode());
		context.write(key, outValue); 

		int scaledDelta = Math.abs((int) (delta * CSF));

		context.getCounter(Counter.CDELTA).increment(scaledDelta); 
	}
}
