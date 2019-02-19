package commons;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;

public class GraphUtil {

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

}
