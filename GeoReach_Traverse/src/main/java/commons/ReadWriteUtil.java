package commons;

import java.io.File;
import java.io.FileWriter;
import java.util.ArrayList;
import java.util.List;

public class ReadWriteUtil {
  /**
   * Write a graph to a file.
   * 
   * @param graph
   * @param graphPath
   * @throws Exception
   */
  public static void writeGraphArrayList(ArrayList<ArrayList<Integer>> graph, String graphPath)
      throws Exception {
    FileWriter writer = new FileWriter(new File(graphPath));
    writer.write(String.format("%d\n", graph.size())); // Write node count in the graph.
    for (int i = 0; i < graph.size(); i++) {
      ArrayList<Integer> neighborList = graph.get(i);
      writer.write(String.format("%d,%d", i, neighborList.size()));
      for (int neighbor : neighborList)
        writer.write(String.format(",%d", neighbor));
      writer.write("\n");
    }
    writer.close();
  }

  public static void writeEntityToFile(ArrayList<Entity> entities, String entityPath) {
    FileWriter writer = null;
    try {
      writer = new FileWriter(new File(entityPath));
      writer.write(entities.size() + "\n");
      for (Entity entity : entities) {
        writer.write(entity.id + ",");
        if (entity.IsSpatial)
          writer.write(
              String.format("1,%s,%s\n", String.valueOf(entity.lon), String.valueOf(entity.lat)));
        else
          writer.write("0\n");
      }
      writer.close();
    } catch (Exception e) {
      // TODO: handle exception
      e.printStackTrace();
      System.exit(-1);
    }
  }

  public static <T> void WriteArray(String filename, List<T> arrayList) {
    FileWriter fileWriter = null;
    try {
      fileWriter = new FileWriter(new File(filename));
      for (T line : arrayList)
        fileWriter.write(line.toString() + "\n");
      fileWriter.close();
    } catch (Exception e) {
      e.printStackTrace();
    }
  }

}
