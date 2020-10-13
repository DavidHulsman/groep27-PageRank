package nl.hu.hadoop.pagerank;

import org.apache.commons.io.IOUtils;
import org.apache.commons.io.LineIterator;
import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.KeyValueTextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;
import java.io.OutputStream;
import java.util.HashMap;
import java.util.HashSet;

public class PageRank {
    public static void main(String... args) throws Exception {
        iterate("./p2p-Gnutella31.txt", "./output");
    }

    public static void iterate(String input, String output) throws Exception {
        Configuration conf = new Configuration();

        Path outputPath = new Path(output);
        outputPath.getFileSystem(conf).delete(outputPath, true);
        outputPath.getFileSystem(conf).mkdirs(outputPath);

        Path inputPath = new Path(outputPath, "input.txt");

        int numNodes = createInputFile(new Path(input), inputPath);

        int iter = 1;
        double desiredConvergence = 0.01;

        while (true) {
            Path jobOutputPath = new Path(outputPath, String.valueOf(iter));

            System.out.println("======================================");
            System.out.println("=  Iteration:    " + iter);
            System.out.println("=  Input path:   " + inputPath);
            System.out.println("=  Output path:  " + jobOutputPath);
            System.out.println("======================================");

            if (calcPageRank(inputPath, jobOutputPath, numNodes) < desiredConvergence) {
                System.out.println("Convergence is below " + desiredConvergence + ", we're done");
                break;
            }
            inputPath = jobOutputPath;
            iter++;
        }
    }

    public static int createInputFile(Path file, Path targetFile)
            throws IOException {
        Configuration conf = new Configuration();

        FileSystem fs = file.getFileSystem(conf);
        int numNodes = getNumNodes(file);
        double initialPageRank = 1.0 / (double) numNodes;

        OutputStream os = fs.create(targetFile);
        LineIterator iter = IOUtils.lineIterator(fs.open(file), "UTF8");
        HashMap<String, Node> nodes = new HashMap<>();

        while (iter.hasNext()) {
            String line = iter.nextLine();
            if (line.startsWith("#"))
                continue;
            String[] parts = StringUtils.split(line);
            Node node = nodes.get(parts[0]);
            if (node == null) {
                node = new Node()
                        .setPageRank(initialPageRank)
                        .addAdjacentNodeNames(parts[1]);
                nodes.put(parts[0], node);
            } else {
                node.addAdjacentNodeNames(parts[1]);
            }
        }
        for (java.util.Map.Entry<String, Node> entry : nodes.entrySet()) {
            IOUtils.write(entry.getKey() + '\t' + entry.getValue().toString() + '\n', os);
        }
        os.close();
        return numNodes;
    }

    public static int getNumNodes(Path file) throws IOException {
        Configuration conf = new Configuration();

        FileSystem fs = file.getFileSystem(conf);
        LineIterator iter = IOUtils.lineIterator(fs.open(file), "UTF8");

        HashSet<Integer> nodeIds = new HashSet<>();
        while (iter.hasNext()) {
            String line = iter.nextLine();
            if (line.startsWith("#"))
                continue;
            String[] parts = StringUtils.split(line);
            nodeIds.add(Integer.parseInt(parts[0]));
            nodeIds.add(Integer.parseInt(parts[1]));
        }
        return nodeIds.size();
    }

    public static double calcPageRank(Path inputPath, Path outputPath, int numNodes)
            throws Exception {
        Configuration conf = new Configuration();

        conf.setInt(Reduce.CONF_NUM_NODES_GRAPH, numNodes);

        Job job = Job.getInstance(conf, "PageRankJob");
        job.setJarByClass(PageRank.class);
        job.setMapperClass(Map.class);
        job.setReducerClass(Reduce.class);

        job.setInputFormatClass(KeyValueTextInputFormat.class);

        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Text.class);

        FileInputFormat.setInputPaths(job, inputPath);
        FileOutputFormat.setOutputPath(job, outputPath);

        if (!job.waitForCompletion(true)) {
            throw new Exception("Job failed");
        }

        long summedConvergence = job.getCounters().findCounter(
                Reduce.Counter.CONV_DELTAS).getValue();
        double convergence =
                ((double) summedConvergence /
                        Reduce.CONVERGENCE_SCALING_FACTOR) /
                        (double) numNodes;

        System.out.println("======================================");
        System.out.println("=  Num nodes:           " + numNodes);
        System.out.println("=  Summed convergence:  " + summedConvergence);
        System.out.println("=  Convergence:         " + convergence);
        System.out.println("======================================");

        return convergence;
    }
}

