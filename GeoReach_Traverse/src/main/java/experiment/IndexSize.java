package experiment;

import java.util.ArrayList;
import commons.Util;

public class IndexSize {

  public static void main(String[] args) {
    // String graphPath = args[0];
    // Util.Print(getGraphDBSize(graphPath));
    // Util.Print(getGraphDBSize("D:\\Ubuntu_shared\\GeoMinHop\\data\\Yelp\\graph.txt", true));
    // Util.Print(getGraphDBSize("D:\\Ubuntu_shared\\GeoMinHop\\data\\Gowalla\\graph.txt", true));
    // Util.Print(getGraphDBSize("D:\\Ubuntu_shared\\GeoMinHop\\data\\foursquare\\graph.txt",
    // true));
    Util.Print(getGraphDBSize("D:\\Ubuntu_shared\\Real_Data\\Patents\\new_graph.txt", false));
  }


  /**
   * Return the graph size as bytes. Each node is 16 bytes and each edge is 34 bytes.
   * 
   * @param graphPath
   * @return
   */
  public static int getGraphDBSize(String graphPath, boolean bidirectional) {
    ArrayList<ArrayList<Integer>> graph = Util.ReadGraph(graphPath);
    int nodeCount = graph.size();
    int edgeCount = 0;
    for (ArrayList<Integer> neighbors : graph) {
      edgeCount += neighbors.size();
    }
    if (bidirectional) {
      return nodeCount * 15 + edgeCount * 34 / 2;
    } else
      return nodeCount * 15 + edgeCount * 34;
  }

}
