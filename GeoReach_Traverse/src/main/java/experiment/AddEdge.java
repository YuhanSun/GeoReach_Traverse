package experiment;

import java.io.FileWriter;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;
import java.util.Set;
import java.util.TreeSet;
import commons.Entity;
import commons.GraphUtil;
import commons.ReadWriteUtil;

public class AddEdge {

  public static void main(String[] args) {
    // TODO Auto-generated method stub

  }


  public static void generateEdges(String graphPath, String entityPath, String labelListPath,
      int targetCount, String outputPath) {
    List<TreeSet<Integer>> graph =
        GraphUtil.convertListGraphToTreeSetGraph(GraphUtil.ReadGraph(graphPath));
    ArrayList<Entity> entities = GraphUtil.ReadEntity(entityPath);
    ArrayList<Integer> labelList = ReadWriteUtil.readIntegerArray(labelListPath);
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
  public static void generateEdges(List<Set<Integer>> graph, List<Entity> entities,
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
