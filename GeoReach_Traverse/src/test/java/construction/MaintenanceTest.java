package construction;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Label;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.ResourceIterable;
import org.neo4j.graphdb.ResourceIterator;
import org.neo4j.graphdb.Transaction;
import commons.Entity;
import commons.Neo4jGraphUtility;
import commons.Util;
import dataprocess.LoadData;

public class MaintenanceTest {
  // private String dbPath =
  // "D:\\Ubuntu_shared\\GeoReachHop\\data\\Yelp\\neo4j-community-3.1.1_128_128_1_100_0_3_test\\data\\databases\\graph.db";
  String dbPath = null, homeDir = null, dataDir = null, mapPath = null;
  private GraphDatabaseService dbservice = null;

  ArrayList<ArrayList<Integer>> graph = null;
  List<Entity> entities = null;
  List<Integer> labelList = null;

  /**
   * Initialize the test graph, including variables: graph, entities, labelList.
   */
  public void iniSmallGraph() {
    int nodeCount = 7;
    graph = new ArrayList<>(nodeCount);
    entities = new ArrayList<>(nodeCount);
    labelList = new ArrayList<>(nodeCount);
    labelList.addAll(Arrays.asList(new Integer[] {0, 0, 0, 0, 1, 1, 1}));

    for (int i = 0; i < nodeCount; i++) {
      graph.add(new ArrayList<>());
      entities.add(new Entity(i));
    }

    entities.get(4).setSpatialEntityLocation(0.5, 0.5);
    entities.get(5).setSpatialEntityLocation(2.3, 1.9);
    entities.get(6).setSpatialEntityLocation(3.7, 4.6);

    graph.get(1).add(2);
    graph.get(1).add(3);

    graph.get(2).add(3);
    graph.get(2).add(4);
    graph.get(2).add(6);

    graph.get(3).add(5);
  }

  @Before
  public void setUp() throws Exception {
    // dbservice = Neo4jGraphUtility.getDatabaseService(dbPath);
    ClassLoader classLoader = getClass().getClassLoader();
    File file = new File(classLoader.getResource("").getFile());
    homeDir = file.getAbsolutePath();
    dbPath = homeDir + "/smallGraphDb";

    dataDir = homeDir + "/smallGraph";
    if (!Util.pathExist(dataDir)) {
      new File(dataDir).mkdirs();
    }
    mapPath = dataDir + "/node_map_RTree.txt";
    Util.print(mapPath);
    File testFile = new File(mapPath);
    Util.print(testFile.exists());

    // db directory does not exist
    if (!Util.pathExist(dbPath)) {
      mapPath = file.getAbsolutePath() + "/smallGraph/node_map_RTree.txt";
      iniSmallGraph();
      new LoadData().loadAllEntityAndCreateIdMap(entities, labelList, dbPath, mapPath);
    }
  }

  @Test
  public void loadDbTest() {
    GraphDatabaseService service = Neo4jGraphUtility.getDatabaseService(dbPath);
    Transaction tx = service.beginTx();
    ResourceIterable<Node> nodes = service.getAllNodes();
    for (Node node : nodes) {
      Util.print(node.getAllProperties());
    }
    tx.success();
    tx.close();
    service.shutdown();
  }

  @After
  public void tearDown() throws Exception {
    // dbservice.shutdown();
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

  @Test
  public void readTest() throws Exception {
    ClassLoader classLoader = getClass().getClassLoader();
    File dir = new File(classLoader.getResource("").getFile());
    Util.print(dir.toString());
    File file = new File(classLoader.getResource("test/test.txt").getFile());
    Util.print(file.getAbsolutePath());
    BufferedReader reader = new BufferedReader(new FileReader(file));
    String line = null;
    while ((line = reader.readLine()) != null) {
      Util.print(line);
    }
    reader.close();
  }

  @Test
  public void addEdgeTest() {

  }
}
