package nl.hu.hadoop.pagerank;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

//class Mapper
//method Map(nid n,node N)
//  p ← N.PageRank/|N.AdjacencyList|
//  Emit(nid n,N) . Pass along graph structure
//  for all nodeid m ∈ N.AdjacencyList do
//    Emit(nid m,p) . Pass PageRank mass to neighbors
class Map extends Mapper<Text, Text, Text, Text> {
    private Text outKey = new Text();
    private Text outValue  = new Text();

    protected void map(Text key, Text value, Context context) throws IOException, InterruptedException {
        if (key.toString().startsWith("#")) {
            return;
        }
        // Write the node and it's value + subnodes
        context.write(key, value);
        Node node = Node.fromMR(value.toString());
        if (node.getAdjacentNodeNames() != null && node.getAdjacentNodeNames().length > 0) {
            double outboundPageRank = node.getPageRank() / (double) node.getAdjacentNodeNames().length;
            // Write all the node's subnodes and their new pagerank
            for (String neighbor : node.getAdjacentNodeNames()) {
                outKey.set(neighbor);
                Node adjacentNode = new Node().setPageRank(outboundPageRank);
                outValue.set(adjacentNode.toString());
                context.write(outKey, outValue);
            }
        }
    }
}