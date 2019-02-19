package experiment;

import java.util.ArrayList;
import java.util.Iterator;
import org.roaringbitmap.RoaringBitmap;
import commons.Entity;
import commons.GraphUtil;
import commons.Util;
import commons.VertexGeoReachList;
import construction.IndexConstruct;

public class IndexSize {

  public static void main(String[] args) {
    // String graphPath = args[0];
    // Util.Print(getGraphDBSize(graphPath));
    // Util.Print(getGraphDBSize("D:\\Ubuntu_shared\\GeoMinHop\\data\\Yelp\\graph.txt", true));
    // Util.Print(getGraphDBSize("D:\\Ubuntu_shared\\GeoMinHop\\data\\Gowalla\\graph.txt", true));
    // Util.Print(getGraphDBSize("D:\\Ubuntu_shared\\GeoMinHop\\data\\foursquare\\graph.txt",
    // true));
    // Util.Print(getGraphDBSize("D:\\Ubuntu_shared\\Real_Data\\Patents\\new_graph.txt", false));

    getIndexSizeAllDatasets();
  }


  public static void getIndexSizeAllDatasets() {
    try {
      String dataset, dir, graphPath, entityPath;
      double minx = -180, miny = -90, maxx = 180, maxy = 90;
      int pieces_x = 128, pieces_y = 128, MAX_HOP = 3;
      double MG = 1.0, MR = 1.0;
      int MC = 0;
      Util.println(String.format("MG=%f, MR=%f, MAX_HOP=%d, MC=%d", MG, MR, MAX_HOP, MC));

      dataset = "Yelp";
      Util.println(dataset);
      dir = String.format("D:\\Ubuntu_shared\\GeoMinHop\\data\\%s", dataset);
      graphPath = dir + "\\graph.txt";
      entityPath = dir + "\\entity.txt";
      getIndexSize(graphPath, entityPath, minx, miny, maxx, maxy, pieces_x, pieces_y, MAX_HOP, MG,
          MR, MC);
      Util.println("\n");

      dataset = "foursquare";
      Util.println(dataset);
      dir = String.format("D:\\Ubuntu_shared\\GeoMinHop\\data\\%s", dataset);
      graphPath = dir + "\\graph.txt";
      entityPath = dir + "\\entity.txt";
      getIndexSize(graphPath, entityPath, minx, miny, maxx, maxy, pieces_x, pieces_y, MAX_HOP, MG,
          MR, MC);
      Util.println("\n");

      dataset = "Gowalla";
      Util.println(dataset);
      dir = String.format("D:\\Ubuntu_shared\\GeoMinHop\\data\\%s", dataset);
      graphPath = dir + "\\graph.txt";
      entityPath = dir + "\\entity.txt";
      getIndexSize(graphPath, entityPath, minx, miny, maxx, maxy, pieces_x, pieces_y, MAX_HOP, MG,
          MR, MC);
      Util.println("\n");

    } catch (Exception e) {
      e.printStackTrace();
      System.exit(-1);
    }
  }

  public static void getIndexSize(String graphPath, String entityPath, double minx, double miny,
      double maxx, double maxy, int pieces_x, int pieces_y, int MAX_HOP, double MG, double MR,
      int MC) {
    Util.println("Read graph from " + graphPath);
    if (!Util.pathExist(graphPath)) {
      Util.println(graphPath + " does not exist!");
      System.exit(-1);
    }
    ArrayList<ArrayList<Integer>> graph = GraphUtil.ReadGraph(graphPath);

    Util.println("Read entities from " + entityPath);
    if (!Util.pathExist(entityPath)) {
      Util.println(entityPath + " does not exist!");
      System.exit(-1);
    }
    ArrayList<Entity> entities = GraphUtil.ReadEntity(entityPath);

    ArrayList<VertexGeoReachList> index = IndexConstruct.ConstructIndexList(graph, entities, minx,
        miny, maxx, maxy, pieces_x, pieces_y, MAX_HOP);

    /**
     * Generate type list time.
     */
    ArrayList<ArrayList<Integer>> typesList = IndexConstruct.generateTypeListForList(index, MAX_HOP,
        minx, miny, maxx, maxy, pieces_x, pieces_y, MG, MR, MC);

    /**
     * Compute index size.
     */
    for (int i = 0; i < MAX_HOP; i++) {

      Iterator<VertexGeoReachList> iterator1 = index.iterator();
      Iterator<ArrayList<Integer>> iterator2 = typesList.iterator();

      int size = 0;
      while (iterator1.hasNext() && iterator2.hasNext()) {
        VertexGeoReachList vertexGeoReach = iterator1.next();
        ArrayList<Integer> types = iterator2.next();
        ArrayList<Integer> reachgrid = vertexGeoReach.ReachGrids.get(i);
        int type = types.get(i);
        switch (type) {
          case 0: // ReachGrid
            RoaringBitmap r = new RoaringBitmap();
            for (int gridID : reachgrid)
              r.add(gridID);
            size += r.serializedSizeInBytes();
          case 1: // RMBR
            size += 8 * 4; // each location value is a double type (8 bytes)
          case 2: // GeoB
            size += 1; // 1 byte.
        }

      }
      Util.println(String.format("%d-hop index size: %d", i + 1, size));
    }
  }

  /**
   * Return the graph size as bytes. Each node is 16 bytes and each edge is 34 bytes.
   * 
   * @param graphPath
   * @return
   */
  public static int getGraphDBSize(String graphPath, boolean bidirectional) {
    ArrayList<ArrayList<Integer>> graph = GraphUtil.ReadGraph(graphPath);
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
