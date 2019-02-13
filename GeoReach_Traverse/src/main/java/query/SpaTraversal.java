package query;

import java.io.File;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.LinkedList;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Relationship;
import org.neo4j.graphdb.Transaction;
import org.neo4j.graphdb.factory.GraphDatabaseFactory;
import org.roaringbitmap.buffer.ImmutableRoaringBitmap;
import commons.Config;
import commons.Labels.GraphRel;
import commons.MyRectangle;
import commons.Util;

public class SpaTraversal {

  public static Config config = new Config();
  public String lon_name = config.GetLongitudePropertyName();
  public String lat_name = config.GetLatitudePropertyName();

  static String GeoReachTypeName = config.getGeoReachTypeName();
  static String reachGridName = config.getReachGridName();
  static String rmbrName = config.getRMBRName();
  static String geoBName = config.getGeoBName();

  public int MAX_HOP;
  public GraphDatabaseService dbservice;
  MyRectangle total_range;
  int pieces_x, pieces_y;
  double resolution_x, resolution_y;

  // query related variables
  int length;
  MyRectangle queryRectangle;
  int lb_x, lb_y, rt_x, rt_y;
  HashSet<Long> visited;
  ArrayList<HashSet<Long>> prunedVertices;
  LinkedList<Long> curPath;
  // public ArrayList<LinkedList<Long>> paths;

  // tracking variables
  public long resultCount, visitedCount, GeoReachPruneCount, PrunedVerticesWorkCount;
  public long dbTime, checkTime;

  public SpaTraversal(String db_path, int MAX_HOP, MyRectangle total_range, int pieces_x,
      int pieces_y) {
    try {
      if (Util.pathExist(db_path))
        dbservice = new GraphDatabaseFactory().newEmbeddedDatabase(new File(db_path));
      else
        throw new Exception(db_path + "does not exist!");

      this.MAX_HOP = MAX_HOP;
      this.total_range = total_range;
      this.pieces_x = pieces_x;
      this.pieces_y = pieces_y;
      resolution_x = (total_range.max_x - total_range.min_x) / (double) pieces_x;
      resolution_y = (total_range.max_y - total_range.min_y) / (double) pieces_y;
    } catch (Exception e) {
      e.printStackTrace();
      System.exit(-1);
    }

  }

  public void traverse(ArrayList<Node> startNodes, int length, MyRectangle queryRectangle) {
    this.length = length;
    this.queryRectangle = queryRectangle;
    // paths = new ArrayList<LinkedList<Long>>();
    visited = new HashSet<Long>();
    curPath = new LinkedList<>();

    lb_x = (int) ((queryRectangle.min_x - this.total_range.min_x) / this.resolution_x);
    lb_y = (int) ((queryRectangle.min_y - this.total_range.min_y) / this.resolution_y);
    rt_x = (int) ((queryRectangle.max_x - this.total_range.min_x) / this.resolution_x);
    rt_y = (int) ((queryRectangle.max_y - this.total_range.min_y) / this.resolution_y);

    prunedVertices = new ArrayList<>(MAX_HOP);
    for (int i = 0; i < MAX_HOP; i++)
      prunedVertices.add(new HashSet<>());

    // tracking variables
    resultCount = 0;
    visitedCount = 0;
    GeoReachPruneCount = 0;
    PrunedVerticesWorkCount = 0;

    dbTime = 0;
    checkTime = 0;

    Transaction tx = dbservice.beginTx();
    for (Node node : startNodes)
      helper(node, 0);
    tx.success();
    tx.close();
  }

  /**
   * 
   * @param node start node
   * @param curHop
   */
  public void helper(Node node, int curHop) {
    long start = System.currentTimeMillis();
    long id = node.getId();
    dbTime += System.currentTimeMillis() - start;
    // if (id == 1299743 && curHop ==1)
    // {
    // Util.Print("here");
    // }
    if (visited.contains(id) == false) {
      if (curHop == length) {
        if (node.hasProperty(lon_name)) {
          double lon = (Double) node.getProperty(lon_name);
          double lat = (Double) node.getProperty(lat_name);
          if (Util.Location_In_Rect(lon, lat, queryRectangle)) {
            LinkedList<Long> path = new LinkedList<Long>(curPath);
            path.add(id);
            // paths.add(path);
            resultCount++;
          }
        }
        return;
      }

      // if cannot satisfy the GeoReach validation
      // if (node.getId() == 1299743 && curHop == 1)
      // {
      // Util.Print("here");
      // }

      int distance = length - curHop;
      // node has been pruned at this query vertex under GeoReach validation
      // Util.Print(hop_offset);
      if (distance <= MAX_HOP) {
        if (prunedVertices.get(distance - 1).contains(id)) {
          PrunedVerticesWorkCount++;
          return;
        }

        if (validate(node, distance, queryRectangle) == false) {
          prunedVertices.get(distance - 1).add(id);
          GeoReachPruneCount++;
          return;
        }
      }


      curPath.add(id);
      visited.add(id);
      // Util.Print(curPath);
      start = System.currentTimeMillis();
      Iterable<Relationship> rels = node.getRelationships(GraphRel.GRAPH_LINK);
      dbTime += System.currentTimeMillis() - start;

      for (Relationship relationship : rels) {
        start = System.currentTimeMillis();
        Node neighbor = relationship.getOtherNode(node);
        dbTime += System.currentTimeMillis() - start;
        visitedCount++;
        helper(neighbor, curHop + 1);
      }
      visited.remove(id);
      curPath.removeLast();
    }
  }

  /**
   * Validate whether current can satisfy the SpaExpand operator.
   *
   * @param node
   * @param distance
   * @param queryRectangle
   * @return
   */
  public boolean validate(Node node, int distance, MyRectangle queryRectangle) {
    try {
      long start = System.currentTimeMillis();
      int type = (int) node.getProperty(GeoReachTypeName + "_" + distance);

      switch (type) {
        case 0:
          String ser = node.getProperty(reachGridName + "_" + distance).toString();
          dbTime += System.currentTimeMillis() - start;

          start = System.currentTimeMillis();
          ImmutableRoaringBitmap reachgrid = Util.getImmutableRoaringBitmap(ser);

          for (int i = lb_x; i <= rt_x; i++) {
            for (int j = lb_y; j <= rt_y; j++) {
              int grid_id = i * pieces_y + j;
              if (reachgrid.contains(grid_id)) {
                return true;
              }
            }
          }
          checkTime += System.currentTimeMillis() - start;

          return false;
        case 1:
          MyRectangle rmbr =
              new MyRectangle(node.getProperty(rmbrName + "_" + distance).toString());
          dbTime += System.currentTimeMillis() - start;
          if (Util.intersect(rmbr, queryRectangle)) {
            checkTime += System.currentTimeMillis() - start;
            return true;
          } else {
            checkTime += System.currentTimeMillis() - start;
            return false;
          }
        case 2:
          boolean geoB = (boolean) node.getProperty(geoBName + "_" + distance);
          dbTime += System.currentTimeMillis() - start;
          return geoB;
        default:
          throw new Exception(
              String.format("%s for %s is %d", GeoReachTypeName + "_" + distance, node, type));
      }
    } catch (Exception e) {
      Util.print(String.format("Something wrong happen in validate(%s, %d, %s)", node, distance,
          queryRectangle));
      e.printStackTrace();
      System.exit(-1);
    }
    Util.print(String.format("Something wrong happen in validate(%s, %d, %s)", node, distance,
        queryRectangle));
    return true;
  }

  /**
   * Always call this to shutdown the dbservice.
   */
  public void shutdown() {
    dbservice.shutdown();
  }
}
