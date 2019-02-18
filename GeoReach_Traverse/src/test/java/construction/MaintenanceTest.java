package construction;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;
import org.apache.commons.io.FileUtils;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Label;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.ResourceIterable;
import org.neo4j.graphdb.ResourceIterator;
import org.neo4j.graphdb.Transaction;
import org.roaringbitmap.buffer.ImmutableRoaringBitmap;
import commons.ArrayUtil;
import commons.Entity;
import commons.EnumVariables.GeoReachOutputFormat;
import commons.EnumVariables.GeoReachType;
import commons.GeoReachIndexUtil;
import commons.MyRectangle;
import commons.Neo4jGraphUtility;
import commons.SpaceManager;
import commons.Util;
import commons.VertexGeoReachList;
import dataprocess.LoadData;

public class MaintenanceTest {
  // private String dbPath =
  // "D:\\Ubuntu_shared\\GeoReachHop\\data\\Yelp\\neo4j-community-3.1.1_128_128_1_100_0_3_test\\data\\databases\\graph.db";
  String dbPath = null, homeDir = null, dataDir = null, mapPath = null;
  private GraphDatabaseService dbservice = null;

  ArrayList<ArrayList<Integer>> graph = null;
  ArrayList<Entity> entities = null;
  ArrayList<Integer> labelList = null;

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

    graph.get(2).add(1);
    graph.get(2).add(3);
    graph.get(2).add(4);
    graph.get(2).add(6);


    graph.get(3).add(1);
    graph.get(3).add(2);
    graph.get(3).add(5);

    graph.get(4).add(2);
    graph.get(5).add(3);
    graph.get(6).add(2);
  }

  /**
   * Clear the db directory and load the smallGraph.
   *
   * @throws Exception
   */
  public void loadGraph() throws Exception {
    // db directory exist, delete the directory (itself included)
    if (Util.pathExist(dbPath)) {
      FileUtils.deleteDirectory(new File(dbPath));
    }

    iniSmallGraph();
    LoadData loadData = new LoadData();
    loadData.loadAllEntityAndCreateIdMap(entities, labelList, dbPath, mapPath);
    loadData.LoadGraphEdges(mapPath, dbPath, graph);
  }

  /**
   * Set up all the paths, create dataDir if it does not exist.
   *
   * @throws Exception
   */
  @Before
  public void setUp() throws Exception {
    ClassLoader classLoader = getClass().getClassLoader();
    File file = new File(classLoader.getResource("").getFile());
    homeDir = file.getAbsolutePath();
    dbPath = homeDir + "/smallGraph.db";

    dataDir = homeDir + "/smallGraph";
    if (!Util.pathExist(dataDir)) {
      new File(dataDir).mkdirs();
    }
    mapPath = dataDir + "/node_map_RTree.txt";

  }

  @Test
  public void printDB() {
    if (dbservice == null) {
      dbservice = Neo4jGraphUtility.getDatabaseService(dbPath);
    }
    Transaction tx = dbservice.beginTx();
    ResourceIterable<Node> nodes = dbservice.getAllNodes();
    for (Node node : nodes) {
      Util.print(String.format("%s: %s", node, node.getAllProperties()));
    }
    tx.close();
    tx.success();
  }

  @Test
  public void printDbInTransaction() {
    ResourceIterable<Node> nodes = dbservice.getAllNodes();
    for (Node node : nodes) {
      Util.print(String.format("%s: %s", node, node.getAllProperties()));
    }
  }

  @After
  public void tearDown() throws Exception {
    if (dbservice != null) {
      dbservice.shutdown();
    }
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

  // space related variables
  double minx = 0, miny = 0, maxx = 10, maxy = 10;
  int pieces_x = 10, pieces_y = 10;
  SpaceManager spaceManager = new SpaceManager(minx, miny, maxx, maxy, pieces_x, pieces_y);

  // index related variables
  int MAX_HOP = 3;
  double MG = 2, MR = 2;
  int MC = 0;

  /**
   * Construct and load the index for the original smallGraph.
   *
   * @throws Exception
   */
  @Test
  public void constructAndLoadIndex() throws Exception {
    ArrayList<VertexGeoReachList> index =
        IndexConstruct.ConstructIndexList(graph, entities, spaceManager, MAX_HOP);

    ArrayList<ArrayList<Integer>> typesList =
        IndexConstruct.generateTypeListForList(index, MAX_HOP, spaceManager, MG, MR, MC);

    // Generate the List format index.
    GeoReachOutputFormat format = GeoReachOutputFormat.LIST;
    String filename =
        GeoReachIndexUtil.getIndexFileNormalName(pieces_x, pieces_y, MG, MR, MC, MAX_HOP, format);
    String outputPath = dataDir + "/" + filename;
    GeoReachIndexUtil.outputGeoReachForList(index, outputPath, typesList, format);

    // Output to BIGMAP format.
    format = GeoReachOutputFormat.BITMAP;
    filename =
        GeoReachIndexUtil.getIndexFileNormalName(pieces_x, pieces_y, MG, MR, MC, MAX_HOP, format);
    outputPath = dataDir + "/" + filename;
    GeoReachIndexUtil.outputGeoReachForList(index, outputPath, typesList, format);

    // Load the Index into db
    Loader loader = new Loader();
    long[] graph_pos_map_list = Util.readMapToArray(mapPath);
    loader.load(outputPath, dbPath, graph_pos_map_list);

    if (dbservice == null) {
      dbservice = Neo4jGraphUtility.getDatabaseService(dbPath);
    }
    Transaction tx = dbservice.beginTx();
    Maintenance maintenance = new Maintenance(spaceManager, MAX_HOP, MG, MR, MC, dbservice);
    validateAllNodeIndex(maintenance, index, typesList, graph_pos_map_list);
    printDbInTransaction();
    tx.success();
    tx.close();
  }

  /**
   * Modify
   * 
   * @throws Exception
   */
  @Test
  public void addEdgeMGMRTest() throws Exception {
    // test for ReachGrid
    MG = 2;
    MR = 2;
    Util.print(String.format("MG: %f, MR: %f", MG, MR));
    addEdgeTest();

    // test for RMBR
    MG = -1;
    MR = 2;
    Util.print(String.format("MG: %f, MR: %f", MG, MR));
    addEdgeTest();

    // test for GeoB
    MG = -1;
    MR = -1;
    Util.print(String.format("MG: %f, MR: %f", MG, MR));
    addEdgeTest();
  }

  @Test
  public void addEdgeTest() throws Exception {
    loadGraph();
    constructAndLoadIndex();
    ArrayList<VertexGeoReachList> index = null;
    ArrayList<ArrayList<Integer>> typesList = null;
    Transaction tx = dbservice.beginTx();
    Maintenance maintenance = new Maintenance(spaceManager, MAX_HOP, MG, MR, MC, dbservice);
    Node src = null, trg = null;
    long[] graph_pos_map_list = Util.readMapToArray(mapPath);
    int srcId, trgId;

    // add edge (0, 3)
    srcId = 0;
    trgId = 3;
    Util.print(String.format("add edge (%d, %d)", srcId, trgId));
    graph.get(srcId).add(trgId);
    graph.get(trgId).add(srcId);
    index = IndexConstruct.ConstructIndexList(graph, entities, spaceManager, MAX_HOP);
    typesList = IndexConstruct.generateTypeListForList(index, MAX_HOP, spaceManager, MG, MR, MC);
    src = dbservice.getNodeById(srcId);
    trg = dbservice.getNodeById(trgId);
    maintenance.addEdgeAndUpdateIndex(src, trg);
    validateAllNodeIndex(maintenance, index, typesList, graph_pos_map_list);


    // add edge (0, 1)
    srcId = 0;
    trgId = 1;
    Util.print(String.format("add edge (%d, %d)", srcId, trgId));
    graph.get(srcId).add(trgId);
    graph.get(trgId).add(srcId);
    index = IndexConstruct.ConstructIndexList(graph, entities, spaceManager, MAX_HOP);
    typesList = IndexConstruct.generateTypeListForList(index, MAX_HOP, spaceManager, MG, MR, MC);
    src = dbservice.getNodeById(srcId);
    trg = dbservice.getNodeById(trgId);
    maintenance.addEdgeAndUpdateIndex(src, trg);
    validateAllNodeIndex(maintenance, index, typesList, graph_pos_map_list);

    // add edge (0, 5)
    srcId = 0;
    trgId = 5;
    Util.print(String.format("add edge (%d, %d)", srcId, trgId));
    graph.get(srcId).add(trgId);
    graph.get(trgId).add(srcId);
    index = IndexConstruct.ConstructIndexList(graph, entities, spaceManager, MAX_HOP);
    typesList = IndexConstruct.generateTypeListForList(index, MAX_HOP, spaceManager, MG, MR, MC);
    src = dbservice.getNodeById(srcId);
    trg = dbservice.getNodeById(trgId);
    maintenance.addEdgeAndUpdateIndex(src, trg);
    validateAllNodeIndex(maintenance, index, typesList, graph_pos_map_list);

    // add edge (2, 5)
    srcId = 2;
    trgId = 5;
    Util.print(String.format("add edge (%d, %d)", srcId, trgId));
    graph.get(srcId).add(trgId);
    graph.get(trgId).add(srcId);
    index = IndexConstruct.ConstructIndexList(graph, entities, spaceManager, MAX_HOP);
    typesList = IndexConstruct.generateTypeListForList(index, MAX_HOP, spaceManager, MG, MR, MC);
    src = dbservice.getNodeById(srcId);
    trg = dbservice.getNodeById(trgId);
    maintenance.addEdgeAndUpdateIndex(src, trg);
    validateAllNodeIndex(maintenance, index, typesList, graph_pos_map_list);

    // add edge (4, 5)
    srcId = 4;
    trgId = 5;
    Util.print(String.format("add edge (%d, %d)", srcId, trgId));
    graph.get(srcId).add(trgId);
    graph.get(trgId).add(srcId);
    index = IndexConstruct.ConstructIndexList(graph, entities, spaceManager, MAX_HOP);
    typesList = IndexConstruct.generateTypeListForList(index, MAX_HOP, spaceManager, MG, MR, MC);
    src = dbservice.getNodeById(srcId);
    trg = dbservice.getNodeById(trgId);
    maintenance.addEdgeAndUpdateIndex(src, trg);
    validateAllNodeIndex(maintenance, index, typesList, graph_pos_map_list);

    tx.success();
    tx.close();
    maintenance.shutdown();
    dbservice = null;
  }

  /**
   * Validate the correctness of the index of all the nodes in the database.
   *
   * @param index the baseline index generated using in memory function
   * @param typesList the GeoReachTypes for all nodes in the <code>index</code>
   * @param graph_pos_map_list the map from [0, n-1] id to neo4j pos id
   * @return
   * @throws Exception
   */
  public void validateAllNodeIndex(Maintenance maintenance, List<VertexGeoReachList> index,
      ArrayList<ArrayList<Integer>> typesList, long[] graph_pos_map_list) throws Exception {
    if (index.size() != typesList.size()) {
      throw new Exception("index size is different from that of typesList!");
    }

    Iterator<VertexGeoReachList> iterIndex = index.iterator();
    Iterator<ArrayList<Integer>> iterTypes = typesList.iterator();
    int id = 0;
    while (iterIndex.hasNext() && iterTypes.hasNext()) {
      VertexGeoReachList nodeIndex = iterIndex.next();
      List<Integer> types = iterTypes.next();
      long neo4jID = graph_pos_map_list[id];
      Node node = dbservice.getNodeById(neo4jID);
      if (!validateSingleNodeIndex(maintenance, nodeIndex, types, node)) {
        throw new Exception(String.format("Node %d at %s index inconsistency!", id, node));
      }
      id++;
    }
  }

  /**
   * Validate the correctness of the index on a node.
   *
   * @param nodeIndex
   * @param types
   * @param node
   * @return
   * @throws Exception
   */
  public boolean validateSingleNodeIndex(Maintenance maintenance, VertexGeoReachList nodeIndex,
      List<Integer> types, Node node) throws Exception {
    for (int i = 0; i < types.size(); i++) {
      int hop = i + 1;
      GeoReachType typeOnNode = maintenance.getGeoReachType(node, hop);
      GeoReachType typeOnIndex = GeoReachIndexUtil.getGeoReachType(types.get(i));
      if (!typeOnIndex.equals(typeOnNode)) {
        Util.print(String.format(
            "GeoReachType on %d hop of %s inconsistency. Type on node is %s while on index is %s!",
            hop, node, typeOnNode, typeOnIndex));
        return false;
      }
      switch (typeOnNode) {
        case ReachGrid:
          ImmutableRoaringBitmap irbNode = maintenance.getReachGrid(node, hop);
          List<Integer> reachGridNode = ArrayUtil.iterableToList(irbNode);

          List<Integer> reachGridIndex = nodeIndex.ReachGrids.get(i);
          if (!ArrayUtil.isSortedListEqual(reachGridIndex, reachGridNode)) {
            Util.print(String.format(
                "ReachGrid on %d hop of %s inconsistency! On node is %s, on index is %s!", hop,
                node, reachGridNode, reachGridIndex));
            return false;
          }
          break;
        case RMBR:
          MyRectangle rmbrNode = maintenance.getRMBR(node, hop);
          MyRectangle rmbrIndex = nodeIndex.RMBRs.get(i);
          if (!rmbrNode.isEqual(rmbrIndex)) {
            Util.print(
                String.format("RMBR on %d hop of %s inconsistency! On node is %s, on index is %s!",
                    hop, node, rmbrNode, rmbrIndex));
            return false;
          }
          break;
        default:
          boolean geoBNode = maintenance.getGeoB(node, hop);
          boolean geoBIndex = nodeIndex.GeoBs.get(i);
          if (geoBNode != geoBIndex) {
            Util.print(
                String.format("GeoB on %d hop of %s inconsistency! On node is %s, on index is %s!",
                    hop, node, geoBNode, geoBIndex));
            return false;
          }
      }
    }
    return true;
  }
}
