package construction;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Label;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.ResourceIterator;
import org.neo4j.graphdb.Transaction;
import commons.Neo4jGraphUtility;
import commons.Util;

public class MaintenanceTest {
  private String dbPath =
      "D:\\Ubuntu_shared\\GeoReachHop\\data\\Yelp\\neo4j-community-3.1.1_128_128_1_100_0_3_test\\data\\databases\\graph.db";
  private GraphDatabaseService dbservice = null;

  @Before
  public void setUp() throws Exception {
    dbservice = Neo4jGraphUtility.getDatabaseService(dbPath);
  }

  @After
  public void tearDown() throws Exception {
    dbservice.shutdown();
  }

  @Test
  public void readWriteTest() {
    Transaction tx = dbservice.beginTx();
    ResourceIterator<Node> testNodes = dbservice.findNodes(Label.label("GRAPH_0"));
    Node testNode = null;
    while (testNodes.hasNext()) {
      testNode = testNodes.next();
      break;
    }
    Util.print(testNode);

    Util.print("before modification: " + testNode.hasProperty("test"));

    testNode.setProperty("test", "first");
    Util.print("after modification: " + testNode.hasProperty("test"));

    tx.success();
    tx.close();
  }
}
