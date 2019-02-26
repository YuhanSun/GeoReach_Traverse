package query;

import java.io.File;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Relationship;
import org.neo4j.graphdb.Transaction;
import org.neo4j.graphdb.factory.GraphDatabaseFactory;
import commons.Config;
import commons.Labels.GraphRel;
import commons.MyRectangle;
import commons.Util;

public class SimpleGraphTraversal {

  public static Config config = new Config();
  public String lon_name = config.GetLongitudePropertyName();
  public String lat_name = config.GetLatitudePropertyName();

  public GraphDatabaseService dbservice;

  // query related variables
  int length;
  MyRectangle queryRectangle;
  HashSet<Long> visited;
  LinkedList<Long> curPath;
  // public ArrayList<LinkedList<Long>> paths;

  // tracking variables
  public long resultCount, visitedCount;

  public SimpleGraphTraversal(String db_path) {
    try {
      if (Util.pathExist(db_path))
        dbservice = new GraphDatabaseFactory().newEmbeddedDatabase(new File(db_path));
      else
        throw new Exception(db_path + "does not exist!");
    } catch (Exception e) {
      e.printStackTrace();
      System.exit(-1);
    }
  }

  public SimpleGraphTraversal(GraphDatabaseService service) {
    this.dbservice = service;
  }

  /**
   * DFS way of traversal.
   * 
   * @param startNodes
   * @param length
   * @param queryRectangle
   */
  public void traverse(List<Node> startNodes, int length, MyRectangle queryRectangle) {
    // query variables initialization
    this.length = length;
    this.queryRectangle = queryRectangle;
    // paths = new ArrayList<LinkedList<Long>>();
    visited = new HashSet<Long>();
    curPath = new LinkedList<>();

    // tracking variables initialization
    resultCount = 0;
    visitedCount = 0;

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
    long id = node.getId();
    if (visited.add(id)) {
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
        visited.remove(id);
        return;
      }

      curPath.add(id);
      // Util.Print(curPath);
      Iterable<Relationship> rels = node.getRelationships(GraphRel.GRAPH_LINK);
      for (Relationship relationship : rels) {
        Node neighbor = relationship.getOtherNode(node);
        visitedCount++;
        helper(neighbor, curHop + 1);
      }
      visited.remove(id);
      curPath.removeLast();
    }
  }

  /**
   * Always call this to shutdown the dbservice.
   */
  public void shutdown() {
    dbservice.shutdown();
  }
}
