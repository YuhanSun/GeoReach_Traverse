package commons;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.TreeSet;

public class GraphUtil {

  public static int getEdgeCount(List<Collection<Integer>> graph) {
    int count = 0;
    for (Collection<Integer> neighbors : graph) {
      count += neighbors.size();
    }
    return count;
  }

  /**
   * Read graph from a file.
   * 
   * @param graph_path
   * @return
   */
  public static ArrayList<ArrayList<Integer>> ReadGraph(String graph_path) {
    ArrayList<ArrayList<Integer>> graph = null;
    BufferedReader reader = null;
    String str = "";
    try {
      reader = new BufferedReader(new FileReader(new File(graph_path)));
      str = reader.readLine();
      int nodeCount = Integer.parseInt(str);
      graph = new ArrayList<ArrayList<Integer>>(nodeCount);
      int index = -1;
      while ((str = reader.readLine()) != null) {
        index++;
        String[] l_str = str.split(",");
        int id = Integer.parseInt(l_str[0]);
        if (id != index)
          throw new Exception(
              String.format("this line has id %d, but the index should be %d!", id, index));
        int neighbor_count = Integer.parseInt(l_str[1]);
        ArrayList<Integer> line = new ArrayList<Integer>(neighbor_count);
        if (neighbor_count == 0) {
          graph.add(line);
          continue;
        }
        int i = 2;
        while (i < l_str.length) {
          line.add(Integer.parseInt(l_str[i]));
          ++i;
        }
        graph.add(line);
      }
      reader.close();
      if (nodeCount != index + 1)
        throw new Exception(String.format(
            "first line shows node count is %d, but only has %d lines!", nodeCount, index + 1));
    } catch (Exception e) {
      Util.println(str);
      if (reader != null)
        try {
          reader.close();
        } catch (IOException e1) {
          // TODO Auto-generated catch block
          e1.printStackTrace();
        }
      e.printStackTrace();
      System.exit(-1);
    }
    return graph;
  }

  /**
   * Convert from a list graph to treeset graph.
   *
   * @param listGraph
   * @return
   */
  public static List<Collection<Integer>> convertListGraphToTreeSetGraph(
      ArrayList<ArrayList<Integer>> listGraph) {
    List<Collection<Integer>> graph = new ArrayList<>(listGraph.size());
    for (List<Integer> neighbors : listGraph) {
      graph.add(new TreeSet<>(neighbors));
    }
    return graph;
  }

  /**
   * Read entities
   * 
   * @param entity_path
   * @return
   */
  public static ArrayList<Entity> ReadEntity(String entity_path) {
    ArrayList<Entity> entities = null;
    BufferedReader reader = null;
    String str = null;
    int id = 0;
    try {
      if (!Util.pathExist(entity_path))
        throw new Exception(entity_path + " does not exist");

      reader = new BufferedReader(new FileReader(new File(entity_path)));
      str = reader.readLine();
      int node_count = Integer.parseInt(str);
      entities = new ArrayList<Entity>(node_count);
      while ((str = reader.readLine()) != null) {
        Entity entity;
        String[] str_l = str.split(",");
        int flag = Integer.parseInt(str_l[1]);
        if (flag == 0) {
          entity = new Entity(id);
          entities.add(entity);
        } else {
          entity = new Entity(id, Double.parseDouble(str_l[2]), Double.parseDouble(str_l[3]));
          entities.add(entity);
        }
        ++id;
      }
      reader.close();
    } catch (Exception e) {
      Util.println(String.format("error happens in entity id %d", id));
      e.printStackTrace();
      System.exit(-1);
    }
    return entities;
  }

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

}
