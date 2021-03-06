package query;

import java.io.File;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.LinkedList;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Result;
import org.neo4j.graphdb.Transaction;
import org.neo4j.graphdb.factory.GraphDatabaseFactory;
import commons.Config;
import commons.Labels.GraphRel;
import commons.MyRectangle;
import commons.Util;

public class Neo4jCypherTraversal {
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
  public long resultCount, pageAccessCount;

  public Neo4jCypherTraversal(String db_path) {
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

  public void traverse(ArrayList<Long> ids, int length, MyRectangle queryRectangle) {
    // query variables initialization
    this.length = length;
    this.queryRectangle = queryRectangle;

    // tracking variables initialization
    resultCount = 0;
    pageAccessCount = 0;

    Transaction tx = dbservice.beginTx();

    String query = "profile match p = (start)";
    for (int i = 0; i < length; i++) {
      query += String.format("-[:%s]-(a%d)", GraphRel.GRAPH_LINK.toString(), i);
    }
    query += String.format(" where id(start) in %s", ids);
    query += String.format(" and %s < a%d.%s < %s and %s < a%d.%s < %s",
        String.valueOf(queryRectangle.min_x), length - 1, lon_name,
        String.valueOf(queryRectangle.max_x), String.valueOf(queryRectangle.min_y), length - 1,
        lat_name, String.valueOf(queryRectangle.max_y));
    query += " return p";
    Util.println(query);
    Result result = dbservice.execute(query);
    while (result.hasNext()) {
      resultCount++;
      result.next();
    }
    pageAccessCount = Util.GetTotalDBHits(result.getExecutionPlanDescription());

    tx.success();
    tx.close();
  }

  /**
   * Always call this to shutdown the dbservice.
   */
  public void shutdown() {
    dbservice.shutdown();
  }
}
