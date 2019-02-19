package experiment;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;
import java.util.TreeSet;
import commons.Config;
import commons.Entity;
import commons.GraphUtil;
import commons.ReadWriteUtil;
import commons.Util;

public class AddEdge {

  public static Config config = new Config();

  public static void main(String[] args) throws IOException {
    // TODO Auto-generated method stub
    ClassLoader classLoader = new AddEdge().getClass().getClassLoader();
    File file = new File(classLoader.getResource("").getFile());
    String homeDir = file.getAbsolutePath();
    String dataset = "Yelp";
    String dataDir = homeDir + "/data/" + dataset;
    String graphPath = dataDir + "/" + config.getGraphFileName();
    String entityPath = dataDir + "/" + config.getEntityFileName();
    String labelListPath = dataDir + "/" + config.getLabelListFileName();
    String outputPath = "/Users/zhouyang/Google_Drive/Projects/GeoReachHop/add_edge/edges.txt";
    generateEdges(graphPath, entityPath, labelListPath, 1000, outputPath);

  }


  public static void generateEdges(String graphPath, String entityPath, String labelListPath,
      int targetCount, String outputPath) throws IOException {
    Util.println("Read graph from " + graphPath);
    List<TreeSet<Integer>> graph =
        GraphUtil.convertListGraphToTreeSetGraph(GraphUtil.ReadGraph(graphPath));
    Util.println("Read entity from " + entityPath);
    ArrayList<Entity> entities = GraphUtil.ReadEntity(entityPath);
    Util.println("Read labels from " + labelListPath);
    ArrayList<Integer> labelList = ReadWriteUtil.readIntegerArray(labelListPath);
    Util.println("Generate edges output to " + outputPath);
    generateEdges(graph, entities, labelList, targetCount, outputPath);
  }

  /**
   * Generate random edges between vertexes.
   *
   * @param graph
   * @param entities
   * @param labelList
   * @param targetCount
   * @param outputPath
   * @throws IOException
   */
  public static void generateEdges(List<TreeSet<Integer>> graph, List<Entity> entities,
      List<Integer> labelList, int targetCount, String outputPath) throws IOException {
    FileWriter writer = new FileWriter(outputPath);
    Random random = new Random();
    int size = graph.size();
    int count = 0;
    while (count < targetCount) {
      int id1 = random.nextInt(size);
      while (true) {
        int id2 = random.nextInt(size);
        if (id1 != id2) {
          if (!graph.get(id1).contains(id2)) {
            graph.get(id1).add(id2);
            graph.get(id2).add(id1);
            writer.write(String.format("%d,%d", id1, id2));
          }
        }
      }
    }
    writer.close();
  }

}
