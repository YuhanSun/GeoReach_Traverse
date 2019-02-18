package commons;

import java.util.ArrayList;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Transaction;

public class UtilTest {

  @BeforeClass
  public static void setUpBeforeClass() throws Exception {}

  @AfterClass
  public static void tearDownAfterClass() throws Exception {}

  @Test
  public void pathExistTest() {
    String path = "D:\\Ubuntu_shared\\GeReachHop\\data";
    Util.println(Util.pathExist(path));
  }

  @Test
  public void getNodesByIDsTest() {
    String dbPath =
        "/home/yuhansun/Documents/GeoReachHop/Gowalla_10/neo4j-community-3.1.1_128_128_100_100_0_3/data/databases/graph.db";
    GraphDatabaseService databaseService = Neo4jGraphUtility.getDatabaseService(dbPath);
    Transaction tx = databaseService.beginTx();
    ArrayList<Long> ids = new ArrayList<>();
    ids.add((long) 1299706);
    ArrayList<Node> nodes = Util.getNodesByIDs(databaseService, ids);
    Util.println(nodes);
    for (Node node : nodes) {
      Util.println(node);
      Util.println(node.getAllProperties());
    }
    tx.success();
    tx.close();

    tx = databaseService.beginTx();
    Util.println(nodes);
    for (Node node : nodes) {
      Util.println(node);
      Util.println(node.getAllProperties());
    }
    tx.success();
    tx.close();

    databaseService.shutdown();
  }
}
