package nl.hu.hadoop.pagerank;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

class Reduce extends Reducer<Text, Text, Text, Text> {
    public static final double CONVERGENCE_SCALING_FACTOR = 1000.0;
    public static final double DAMPING_FACTOR = 0.85;
    public static String CONF_NUM_NODES_GRAPH = "pagerank.numnodes";
    private int numberOfNodesInGraph;

    public static enum Counter {
        CONV_DELTAS
    }

    @Override
    protected void setup(Context context) throws IOException, InterruptedException {
        numberOfNodesInGraph = context.getConfiguration().getInt(CONF_NUM_NODES_GRAPH, 0);
    }

    private Text outValue = new Text();

    public void reduce(Text key, Iterable<Text> values,Context context) throws IOException, InterruptedException {
        double summedPageRanks = 0;
        Node originalNode = new Node();
        for (Text textValue : values) {
            Node node = Node.fromMR(textValue.toString());

            if (node.containsAdjacentNodes()) {
                // the original node
                originalNode = node;
            } else {
                summedPageRanks += node.getPageRank();
            }
        }

        double dampingFactor = ((1.0 - DAMPING_FACTOR) / (double) numberOfNodesInGraph);

//        double newPageRank = dampingFactor + (DAMPING_FACTOR * summedPageRanks);
        double newPageRank = summedPageRanks;

        double delta = originalNode.getPageRank() - newPageRank;

        originalNode.setPageRank(newPageRank);

        outValue.set(originalNode.toString());

        context.write(key, outValue);
        int scaledDelta = Math.abs((int) (delta * CONVERGENCE_SCALING_FACTOR));
        context.getCounter(Counter.CONV_DELTAS).increment(scaledDelta);
    }
}
