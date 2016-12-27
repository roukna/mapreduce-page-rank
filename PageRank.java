package ufl.cloudcomp;

import org.apache.commons.io.*;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.*;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.*;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.*;
import java.util.*;

public class PageRank {

	static TreeMap<String, String> pageMap = new TreeMap<String, String>();

	public static void main(String[] args) throws Exception {

		String inputPath = args[0];
		String outputPath = args[1];

		iterativeMapReduce(inputPath, outputPath);
	}

	public static void iterativeMapReduce(String inputPath, String outputDir) throws Exception {
		
		int noOfEdges = 0;
		int max_out_deg = 0;
		int min_out_deg = 0;
		int avg_out_deg = 0;
		int noOfNodes = 0;

		int iteration = 1;
		double desiredConvergence = 0.000001; // specify the desired convergence
		
		Configuration conf = new Configuration();
		
		Path inFile = new Path(inputPath);
		Path outDir = new Path(outputDir);
		Path jobOPath = null;
		
		noOfNodes = noOfNodes(inFile);
		
		if (outDir.getFileSystem(conf).exists(outDir))
			outDir.getFileSystem(conf).delete(outDir, true);
		outDir.getFileSystem(conf).mkdirs(outDir);
		

		Path initialFile = new Path(outDir, "initialIP.txt");
		
		double initialPageRank = 1.0 / (double) noOfNodes;
            
			FileSystem fs = inFile.getFileSystem(conf);
			OutputStream outS = fs.create(initialFile);
			
			initialPageRank = 1.0 / (double) noOfNodes; // initialize page rank to 1 by no. of nodes for all nodes
			
    		LineIterator lineIter = IOUtils.lineIterator(fs.open(inFile), "UTF8"); // open the input file
    		
    		while (lineIter.hasNext()) {
    			
    			String line = lineIter.nextLine();
    			
    			String[] lParts = line.toString().split(" ");

    			Node node = new Node().setPageRank(initialPageRank)
    					.setAdjacencyList(
    							Arrays.copyOfRange(lParts, 1, lParts.length));
    			
    			IOUtils.write(lParts[0] + '\t' + node.displayNode() + '\n', outS);
    			
    			noOfEdges += lParts.length;
    			
    			if(lParts.length > max_out_deg)
    				max_out_deg = lParts.length;
    			if (lParts.length < min_out_deg)
    				min_out_deg = lParts.length;
    		}
    		
    		avg_out_deg = noOfEdges/noOfNodes;
    		
    		outS.close();
		
		avg_out_deg = noOfEdges/noOfNodes;
		
		System.out.println("Graph Statistics:");
		System.out.println("****************************************");
		System.out.println("Total Number of Nodes:" + noOfNodes);
		System.out.println("Total Number of Edges:" + noOfEdges);
		System.out.println("Maximum out degree:" + max_out_deg);
		System.out.println("Minimum out degree:" + min_out_deg);
		System.out.println("Average out degree:" + avg_out_deg);
		System.out.println("****************************************");
		
		OutputStream outputS = fs.create(new Path(outDir, "graph_statistics.txt"));
		
		OutputStream outputSP = fs.create(new Path(outDir, "performance_summary.txt"));
		
		IOUtils.write("Graph Statistics:" +'\n', outputS);
		IOUtils.write("No. of nodes: "+ noOfNodes + '\n', outputS);
		IOUtils.write("No. of edges: "+ noOfEdges + '\n', outputS);
		IOUtils.write("Maximum out degree: "+ max_out_deg + '\n', outputS);
		IOUtils.write("Minimum out degree: "+ min_out_deg + '\n', outputS);
		IOUtils.write("Average out degree: "+ avg_out_deg + '\n', outputS);
		
		long totStartTime = System.currentTimeMillis();
		
		IOUtils.write("Iteration "+ '\t' + "Time taken" + '\n', outputSP);

		
		// iterative map reduce until desired convergence is achieved
		while (true) {
			
			long startTime = System.currentTimeMillis();
			
			jobOPath = new Path(outDir, String.valueOf(iteration)); // set the output path of the reduceer
			conf.setInt(PageRankReducer.NO_OF_NODES, noOfNodes); // set the no in the graph in configuration to be accessed from reducer
			
			Job job = Job.getInstance(conf);
			job.setJarByClass(PageRank.class);
			job.setMapperClass(PageRankMapper.class);
			job.setReducerClass(PageRankReducer.class);

			job.setInputFormatClass(KeyValueTextInputFormat.class);

			job.setMapOutputKeyClass(Text.class);
			job.setMapOutputValueClass(Text.class);

			FileInputFormat.setInputPaths(job, initialFile);
			FileOutputFormat.setOutputPath(job, jobOPath);

			if (!job.waitForCompletion(true)) {
				throw new Exception("Job failed");
			}

			long endTime   = System.currentTimeMillis();
			
			IOUtils.write("Iteration "+ iteration + '\t' + (endTime-startTime) + "ms" + '\n', outputSP);
			
			long sumOfConvergence = job.getCounters()
					.findCounter(PageRankReducer.Counter.CDELTA).getValue();

			double convergence = ((double) sumOfConvergence / PageRankReducer.CSF)
					/ (double) noOfNodes;


			if (convergence < desiredConvergence) {
				break;
			}

			initialFile = jobOPath;
			iteration++;
		}
		
		long totEndTime   = System.currentTimeMillis();
		
		IOUtils.write("Total no. of iterations required: "+ iteration + '\n', outputSP);
		IOUtils.write("Total Time taken: "+ '\t' + (totEndTime-totStartTime) + "ms" + '\n', outputSP);
	
		outputSP.close();
		outputS.close();

		topTenPageRanks(jobOPath, outDir);

	}

	public static void topTenPageRanks(Path outputPath, Path out) throws IOException {
		
		Configuration conf = new Configuration();
		
		FileSystem fileSum = outputPath.getFileSystem(conf);
		ContentSummary contSum = fileSum.getContentSummary(outputPath);
		
		long fileCount = contSum.getFileCount() - 1;
		
		for (int i = 0; i < fileCount; i++) {
			Path file = new Path(outputPath, "part-r-0000" + Integer.toString(i));
			
            LineIterator lineIter = IOUtils.lineIterator(fileSum.open(file), "UTF8");
            
			while (lineIter.hasNext()) {
				
				String line = lineIter.nextLine();
				String[] sParts = line.toString().trim().split("\t");
				
				if (sParts.length == 1) {
					pageMap.put(Integer.toString(0), sParts[0]); // if the node is a dangling node give it a page rank of zero
				} else {
					pageMap.put(sParts[1], sParts[0]); // put the value of page rank and node id in the map.
				}
			}
		}

		Path outP = new Path(out, "topNPageRanks.txt");
		OutputStream outS = fileSum.create(outP);
		// write the top ten nodes with highest page ranks to a new file
		for (int i = 10; i > 0; i--) {
			
			Map.Entry<String, String> entry = pageMap.pollLastEntry();
			IOUtils.write(entry.getValue() + ',' + entry.getKey() + '\n', outS);
		}
		outS.close();

	}

	public static int noOfNodes(Path file) throws IOException {
		Configuration conf = new Configuration();
		FileSystem fs = file.getFileSystem(conf);

		return IOUtils.readLines(fs.open(file), "UTF8").size();

	}
}