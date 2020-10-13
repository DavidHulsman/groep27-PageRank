package nl.hu.hadoop.pagerank;


import org.apache.commons.lang3.StringUtils;

import java.io.IOException;
import java.util.*;

import static java.util.Arrays.asList;

public class Node {
    public static final char fieldSeparator = '\t';
    private double pageRank = 0.2;
    private Set<String> adjacentNodeNames = new HashSet<>();

    public static Node fromMR(String value) throws IOException {
        List<String> parts = Arrays.asList(StringUtils.splitPreserveAllTokens(
                value, fieldSeparator));
        if (parts.size() < 1) {
            throw new IOException(
                    "Expected 1 or more parts but received " + parts.size());
        }
        Node node = new Node()
                .setPageRank(Double.valueOf(parts.get(0)));
        if (parts.size() > 1) {
            for (String neighbor : parts.subList(1, parts.size())) {
                if (!neighbor.equals("")) {
                    node.addAdjacentNodeNames(neighbor);
                }
            }
        }
        return node;
    }

    public double getPageRank() {
        return pageRank;
    }

    public Node setPageRank(double pageRank) {
        this.pageRank = pageRank;
        return this;
    }

    public String[] getAdjacentNodeNames() {
        return adjacentNodeNames.toArray(new String[adjacentNodeNames.size()]);
    }

    public boolean containsAdjacentNodes() {
        return adjacentNodeNames.size() > 0;
    }

    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder();
        sb.append(pageRank);

        if (getAdjacentNodeNames() != null) {
            sb.append(fieldSeparator)
                    .append(StringUtils
                            .join(getAdjacentNodeNames(), fieldSeparator));
        }
        return sb.toString();
    }

    public Node addAdjacentNodeNames(String part) {
        adjacentNodeNames.add(part);
        return this;
    }
}
