package experiment;

import java.util.ArrayList;
import java.util.Iterator;
import org.roaringbitmap.RoaringBitmap;
import commons.Entity;
import commons.Util;
import commons.VertexGeoReachList;
import construction.IndexConstruct;

public class IniTime {

  public static void getIniTime(String graphPath, String entityPath, double minx, double miny,
      double maxx, double maxy, int pieces_x, int pieces_y, int MAX_HOP, double MG, double MR,
      int MC) {
    Util.Print("Read graph from " + graphPath);
    if (!Util.pathExist(graphPath)) {
      Util.Print(graphPath + " does not exist!");
      System.exit(-1);
    }
    ArrayList<ArrayList<Integer>> graph = Util.ReadGraph(graphPath);

    Util.Print("Read entities from " + entityPath);
    if (!Util.pathExist(entityPath)) {
      Util.Print(entityPath + " does not exist!");
      System.exit(-1);
    }
    ArrayList<Entity> entities = Util.ReadEntity(entityPath);

    ArrayList<VertexGeoReachList> index = IndexConstruct.ConstructIndexList(graph, entities, minx,
        miny, maxx, maxy, pieces_x, pieces_y, MAX_HOP);

    long start, time;

    /**
     * Generate type list time.
     */
    start = System.currentTimeMillis();
    ArrayList<ArrayList<Integer>> typesList = IndexConstruct.generateTypeListForList(index, MAX_HOP,
        minx, miny, maxx, maxy, pieces_x, pieces_y, MG, MR, MC);
    time = System.currentTimeMillis() - start;
    Util.Print("generate type list time: " + time);

    /**
     * Bitmap compression time
     */
    for (int i = 0; i < MAX_HOP; i++) {

      start = System.currentTimeMillis();

      Iterator<VertexGeoReachList> iterator1 = index.iterator();
      Iterator<ArrayList<Integer>> iterator2 = typesList.iterator();

      while (iterator1.hasNext() && iterator2.hasNext()) {
        VertexGeoReachList vertexGeoReach = iterator1.next();
        ArrayList<Integer> types = iterator2.next();
        ArrayList<Integer> reachgrid = vertexGeoReach.ReachGrids.get(i);
        int type = types.get(i);
        if (type == 0) {
          RoaringBitmap r = new RoaringBitmap();
          for (int gridID : reachgrid)
            r.add(gridID);
          Util.Serialize_RoarBitmap_ToString(r);
        }
        time = System.currentTimeMillis() - start;
        Util.Print(String.format("%d-hop bitmap time: %d", i + 1, time));
      }
    }
  }

  public static void main(String[] args) {
    try {
      String dataset, dir, graphPath, entityPath;
      double minx = -180, miny = -90, maxx = 180, maxy = 90;
      int pieces_x = 128, pieces_y = 128, MAX_HOP = 3;
      double MG = 0.8, MR = 0.8;
      int MC = 0;

      dataset = "Yelp";
      dir = String.format("D:\\Ubuntu_shared\\GeoMinHop\\data\\%s", dataset);
      graphPath = dir + "\\graph.txt";
      entityPath = dir + "\\entity.txt";
      getIniTime(graphPath, entityPath, minx, miny, maxx, maxy, pieces_x, pieces_y, MAX_HOP, MG, MR,
          MC);

      dataset = "foursquare";
      dir = String.format("D:\\Ubuntu_shared\\GeoMinHop\\data\\%s", dataset);
      graphPath = dir + "\\graph.txt";
      entityPath = dir + "\\entity.txt";
      getIniTime(graphPath, entityPath, minx, miny, maxx, maxy, pieces_x, pieces_y, MAX_HOP, MG, MR,
          MC);

      dataset = "Gowalla";
      dir = String.format("D:\\Ubuntu_shared\\GeoMinHop\\data\\%s", dataset);
      graphPath = dir + "\\graph.txt";
      entityPath = dir + "\\entity.txt";
      getIniTime(graphPath, entityPath, minx, miny, maxx, maxy, pieces_x, pieces_y, MAX_HOP, MG, MR,
          MC);



    } catch (Exception e) {
      e.printStackTrace();
      System.exit(-1);
    }



  }

}
