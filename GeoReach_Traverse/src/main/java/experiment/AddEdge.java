package experiment;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Random;
import org.apache.commons.io.FileUtils;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Transaction;
import commons.Config;
import commons.Edge;
import commons.Entity;
import commons.EnumVariables.Expand;
import commons.EnumVariables.GeoReachOutputFormat;
import commons.EnumVariables.MaintenanceStrategy;
import commons.GeoReachIndexUtil;
import commons.GraphUtil;
import commons.Neo4jGraphUtility;
import commons.ReadWriteUtil;
import commons.SpaceManager;
import commons.Util;
import commons.VertexGeoReachList;
import construction.IndexConstruct;
import construction.Loader;
import construction.Maintenance;
import dataprocess.LoadData;

public class AddEdge {

  public static Config config = new Config();
  public static String password = config.getPassword();

  /**
   * Paths related to data files, including db.
   */
  public String homeDir, dataDir;
  public String dataset;
  public String graphPath, entityPath, labelListPath;
  public String dbPath;

  /**
   * Paths related to experiment.
   */
  public String experimentDir;// root dir of queryDir and resultDir
  public String queryDir; // contains the edges
  public String resultDir;// contains the result
  public String edgePath;
  public String testDir;

  /**
   * Dir or file name related to Add edge experiment.
   */
  public final static String afterAddAccurateDirName = "after_add_accurate";
  public final static String afterAddLightweightDirName = "after_add_lightweight";

  ArrayList<ArrayList<Integer>> graph = null;
  ArrayList<Entity> entities = null;
  ArrayList<Integer> labelList = null;

  String mapPath;
  long[] graph_pos_map_list;

  public static void main(String[] args) throws Exception {
    try {
      AddEdge addEdge = new AddEdge();
      addEdge.iniPaths();

      // Run once to generate the inserted edges
      // addEdge.generateEdges();

      // Evaluate three set-up of MG, MR and test the run time of edge insertion
      // addEdge.iniPaths();
      // addEdge.evaluate();

      // Generate the db with the accurate index after insertion.
      // addEdge.iniPaths();
      // addEdge.generateAccurateDbAfterAddEdge();

      addEdge.evaluateQuery();

    } catch (Exception e) {
      e.printStackTrace();
      System.exit(-1);
    }
  }

  public void iniPaths() {
    // Only need to modify homeDir.
    // ClassLoader classLoader = new AddEdge().getClass().getClassLoader();
    // File file = new File(classLoader.getResource("").getFile());
    // homeDir = file.getAbsolutePath();
    homeDir = "D:\\Ubuntu_shared\\GeoReachHop";

    // data dirs
    dataset = "Yelp";
    dataDir = homeDir + "/data/" + dataset;
    graphPath = dataDir + "/" + config.getGraphFileName();
    entityPath = dataDir + "/" + config.getEntityFileName();
    labelListPath = dataDir + "/" + config.getLabelListFileName();

    // experiment dirs
    // experimentDir = "/Users/zhouyang/Google_Drive/Projects/GeoReachHop";
    experimentDir = "D:\\Google_Drive\\Projects\\GeoReachHop";
    queryDir = String.format("%s/query/%s", experimentDir, dataset);
    resultDir = experimentDir + "/add_edge";
    edgePath = queryDir + "/edges.txt";
    testDir = dataDir + "/" + afterAddLightweightDirName;
  }

  public static int piecesX = 128, piecesY = 128;
  public static int MC = 0;
  public static int MAX_HOP = 3;
  public static double minx = -180, miny = -90, maxx = 180, maxy = 90;
  public static SpaceManager spaceManager =
      new SpaceManager(minx, miny, maxx, maxy, piecesX, piecesY);

  public void evaluateQuery() throws Exception {
    double MG, MR;
    MaintenanceStrategy strategy;
    Expand expand;

    // MG = 2.0;
    // MR = 2.0;
    // strategy = MaintenanceStrategy.LIGHTWEIGHT;
    // expand = Expand.SIMPLEGRAPHTRAVERSAL;
    // runQueryLightweightStrategy(MG, MR, strategy, expand);

    // MG = -1.0;
    // MR = 2.0;
    // strategy = MaintenanceStrategy.LIGHTWEIGHT;
    // expand = Expand.SPATRAVERSAL;
    // iniPaths();
    // runQueryLightweightStrategy(MG, MR, strategy, expand);
    // strategy = MaintenanceStrategy.RECONSTRUCT;
    // iniPaths();
    // runQueryLightweightStrategy(MG, MR, strategy, expand);
    //
    // MG = -1.0;
    // MR = -1.0;
    // strategy = MaintenanceStrategy.LIGHTWEIGHT;
    // expand = Expand.SPATRAVERSAL;
    // iniPaths();
    // runQueryLightweightStrategy(MG, MR, strategy, expand);
    // strategy = MaintenanceStrategy.RECONSTRUCT;
    // iniPaths();
    // runQueryLightweightStrategy(MG, MR, strategy, expand);

    MG = 0.5;
    MR = 2.0;
    strategy = MaintenanceStrategy.LIGHTWEIGHT;
    expand = Expand.SPATRAVERSAL;
    iniPaths();
    runQueryLightweightStrategy(MG, MR, strategy, expand);
    strategy = MaintenanceStrategy.RECONSTRUCT;
    iniPaths();
    runQueryLightweightStrategy(MG, MR, strategy, expand);

  }

  public void runQueryLightweightStrategy(double MG, double MR, MaintenanceStrategy strategy,
      Expand expand) throws Exception {
    boolean clearCache = false;
    boolean hotDB = true;
    entities = GraphUtil.ReadEntity(entityPath);
    int spaCount = Util.GetSpatialEntityCount(entities);
    String curDataDir = null;
    if (strategy.equals(MaintenanceStrategy.LIGHTWEIGHT)) {
      curDataDir = dataDir + "/" + afterAddLightweightDirName;
      resultDir += "/" + afterAddLightweightDirName;

    } else {
      curDataDir = dataDir + "/" + afterAddAccurateDirName;
      resultDir += "/" + afterAddAccurateDirName;
    }
    if (!Util.pathExist(resultDir)) {
      new File(resultDir).mkdirs();
    }
    dbPath =
        curDataDir + "/" + Neo4jGraphUtility.getDbNormalName(piecesX, piecesY, MG, MR, MC, MAX_HOP);
    mapPath = curDataDir + "/"
        + Neo4jGraphUtility.getGraphNeo4jIdMapNormalName(piecesX, piecesY, MG, MR, MC, MAX_HOP);
    Util.println("Read map from " + mapPath);
    graph_pos_map_list = ReadWriteUtil.readMapToArray(mapPath);

    String startIDPath = queryDir + "/" + config.getStartIDFileName();
    int offset = 0, groupCount = 5, groupSize = 500;
    int length = 3;
    double startSelectivity = Math.pow(10, -4), endSelectivity = Math.pow(10, -1) * 2;
    int selectivityTimes = 10;

    SelectivityNumber.evaluateQuery(dbPath, expand, clearCache, hotDB, MG, MR, graph_pos_map_list,
        spaceManager, resultDir, dataset, queryDir, startIDPath, offset, groupCount, groupSize,
        spaCount, MAX_HOP, length, startSelectivity, endSelectivity, selectivityTimes);
  }

  // public void runQuery() throws Exception {
  // boolean clearCache = false;
  // entities = GraphUtil.ReadEntity(entityPath);
  // int spaCount = Util.GetSpatialEntityCount(entities);
  // String curDataDir = dataDir + "/" + afterAddAccurateDirName;
  // double MG, MR;
  //
  // MG = 2;
  // MR = 2;
  // dbPath =
  // curDataDir + "/" + Neo4jGraphUtility.getDbNormalName(piecesX, piecesY, MG, MR, MC, MAX_HOP);
  // mapPath = curDataDir + "/"
  // + Neo4jGraphUtility.getGraphNeo4jIdMapNormalName(piecesX, piecesY, MG, MR, MC, MAX_HOP);
  // Util.println("Read map from " + mapPath);
  // graph_pos_map_list = ReadWriteUtil.readMapToArray(mapPath);
  // resultDir += "/" + afterAddAccurateDirName;
  // if (!Util.pathExist(resultDir)) {
  // new File(resultDir).mkdirs();
  // }
  // String startIDPath = queryDir + "/" + config.getStartIDFileName();
  // int offset = 0, groupCount = 2, groupSize = 500;
  // int length = 3;
  // double startSelectivity = Math.pow(10, -4), endSelectivity = Math.pow(10, -1) * 2;
  // int selectivityTimes = 10;
  //
  // SelectivityNumber.evaluateQuery(dbPath, Expand.SPATRAVERSAL, clearCache, MG, MR,
  // graph_pos_map_list, spaceManager, resultDir, dataset, queryDir, startIDPath, offset,
  // groupCount, groupSize, spaCount, MAX_HOP, length, startSelectivity, endSelectivity,
  // selectivityTimes);
  // }

  /**
   * Generate the accurate SIP for comparison.
   *
   * @throws Exception
   */
  public void generateAccurateDbAfterAddEdge() throws Exception {
    readGraph();
    addEdgesToGraph();
    dataDir += "/" + afterAddAccurateDirName;
    if (Util.pathExist(dataDir)) {
      new File(dataDir).mkdirs();
    }

    double MG, MR;

    // MG = 2;
    // MR = 2;
    // generateAccurateDbAfterAddEdges(MG, MR);
    //
    // MG = -1;
    // MR = 2;
    // generateAccurateDbAfterAddEdges(MG, MR);
    //
    // MG = -1;
    // MR = -1;
    // generateAccurateDbAfterAddEdges(MG, MR);

    MG = 0.5;
    MR = 2.0;
    generateAccurateDbAfterAddEdges(MG, MR);
  }

  public void generateAccurateDbAfterAddEdges(double MG, double MR) throws Exception {
    String dbFileName = Neo4jGraphUtility.getDbNormalName(piecesX, piecesY, MG, MR, MC, MAX_HOP);

    dbPath = dataDir + "/" + dbFileName;
    // mapPath is initialized before loadGraphAndIndex because its value will be used.
    mapPath = dataDir + "/"
        + Neo4jGraphUtility.getGraphNeo4jIdMapNormalName(piecesX, piecesY, MG, MR, MC, MAX_HOP);
    loadGraphAndIndex(MG, MR);
  }

  public void reIniPaths() {
    graphPath = dataDir + "/" + config.getGraphFileName();
    entityPath = dataDir + "/" + config.getEntityFileName();
    labelListPath = dataDir + "/" + config.getLabelListFileName();
  }

  /**
   * Add all the edges in edge.txt to the original graph. This can ensure that the neighbors are
   * sorted by id.
   *
   * @throws Exception
   */
  public void addEdgesToGraph() throws Exception {
    List<Edge> edges = GraphUtil.readEdges(edgePath);
    List<Collection<Integer>> treeSetGraph = GraphUtil.convertListGraphToTreeSetGraph(graph);
    for (Edge edge : edges) {
      int start = (int) edge.start;
      int end = (int) edge.end;
      treeSetGraph.get(start).add(end);
    }
    graph = GraphUtil.convertCollectionGraphToArrayList(treeSetGraph);
  }


  /**
   * Evaluate the insertion speed with different MG, MR.
   *
   * @throws Exception
   */
  public void evaluate() throws Exception {
    double MG, MR;
    readGraph();
    if (!Util.pathExist(dataDir)) {
      new File(dataDir).mkdirs();
    }
    if (Util.pathExist(testDir)) {
      new File(testDir).mkdirs();
    }

    // // All reachgrid
    // MG = 2;
    // MR = 2;
    // evaluate(MG, MR, 0);
    //
    // // All rmbr
    // MG = -1;
    // MR = 2;
    // evaluate(MG, MR, 0);
    //
    // // All GeoB
    // MG = -1;
    // MR = -1;
    // evaluate(MG, MR, 0);

    MG = 0.5;
    MR = 2.0;
    evaluate(MG, MR, 0);

  }

  /**
   * Copy the original db to the testDbPath and add edges there.
   *
   * @param MG
   * @param MR
   * @param testCount how many edges in the edge.txt file will be tested
   * @throws Exception
   */
  public void evaluate(double MG, double MR, int testCount) throws Exception {
    String dbFileName = Neo4jGraphUtility.getDbNormalName(piecesX, piecesY, MG, MR, MC, MAX_HOP);
    dbPath = dataDir + "/" + dbFileName;
    // mapPath is initialized before loadGraphAndIndex because its value will be used.
    String mapFileName =
        Neo4jGraphUtility.getGraphNeo4jIdMapNormalName(piecesX, piecesY, MG, MR, MC, MAX_HOP);
    mapPath = dataDir + "/" + mapFileName;
    loadGraphAndIndex(MG, MR);

    String testDbPath = testDir + "/" + dbFileName;
    // If the testDb dir exists, remove it.
    // Because it was inserted edges and is different from the original graph.
    if (Util.pathExist(testDbPath)) {
      Util.println(String.format("Delete %s...", testDbPath));
      FileUtils.deleteDirectory(new File(testDbPath));
    }
    String newMapPath = testDir + mapFileName;
    if (Util.pathExist(newMapPath)) {
      Util.println(String.format("Delete %s...", newMapPath));
      new File(newMapPath).delete();
    }
    // Copy the original db to the test dir.
    Util.println(String.format("Copy %s to %s...", dbPath, testDir));
    FileUtils.copyDirectoryToDirectory(new File(dbPath), new File(testDir));
    Util.println(String.format("Copy %s to %s...", mapPath, testDir));
    FileUtils.copyFileToDirectory(new File(mapPath), new File(testDir), true);

    GraphDatabaseService service = Neo4jGraphUtility.getDatabaseService(testDbPath);
    SpaceManager spaceManager = new SpaceManager(minx, miny, maxx, maxy, piecesX, piecesY);
    Maintenance maintenance = new Maintenance(spaceManager, MAX_HOP, MG, MR, MC, service);
    Util.println("Read edges from " + edgePath + "...");
    List<Edge> edges = GraphUtil.readEdges(edgePath);
    if (testCount == 0) {
      testCount = edges.size();
    }
    List<Edge> edgesNeo4j = new ArrayList<>(testCount);
    int i = 0;
    for (Edge edge : edges) {
      edgesNeo4j
          .add(new Edge(graph_pos_map_list[(int) edge.start], graph_pos_map_list[(int) edge.end]));
      i++;
      if (i == testCount) {
        break;
      }
    }
    Util.println("maintenance test...");
    addEdgeMaintenance(edgesNeo4j, maintenance);
    service.shutdown();
  }

  /**
   * If the dbPath does not exist, load the graph and index from scratch. Read the id map from
   * mapPath anyhow.
   *
   * @param MG
   * @param MR
   * @throws Exception
   */
  public void loadGraphAndIndex(double MG, double MR) throws Exception {
    if (!Util.pathExist(dbPath)) {
      Util.println(String.format("load graph into %s...", dbPath));
      loadGraph();

      Util.println("Read map from " + mapPath);
      graph_pos_map_list = ReadWriteUtil.readMapToArray(mapPath);

      Util.println("Construct and load index...");
      constructAndLoadIndex(MG, MR);
    } else {
      Util.println("Read map from " + mapPath);
      graph_pos_map_list = ReadWriteUtil.readMapToArray(mapPath);
    }
  }

  /**
   * Construct and load the index.
   *
   * @throws Exception
   */
  public void constructAndLoadIndex(double MG, double MR) throws Exception {
    ArrayList<VertexGeoReachList> index =
        IndexConstruct.ConstructIndexList(graph, entities, spaceManager, MAX_HOP);

    ArrayList<ArrayList<Integer>> typesList =
        IndexConstruct.generateTypeListForList(index, MAX_HOP, spaceManager, MG, MR, MC);

    // Generate the List format index.
    GeoReachOutputFormat format = GeoReachOutputFormat.LIST;
    String filename =
        GeoReachIndexUtil.getIndexFileNormalName(piecesX, piecesY, MG, MR, MC, MAX_HOP, format);
    String outputPath = dataDir + "/" + filename;
    GeoReachIndexUtil.outputGeoReachForList(index, outputPath, typesList, format);

    // Output to BIGMAP format.
    format = GeoReachOutputFormat.BITMAP;
    filename =
        GeoReachIndexUtil.getIndexFileNormalName(piecesX, piecesY, MG, MR, MC, MAX_HOP, format);
    outputPath = dataDir + "/" + filename;
    GeoReachIndexUtil.outputGeoReachForList(index, outputPath, typesList, format);

    // Load the Index into db
    Loader loader = new Loader();
    loader.load(outputPath, dbPath, graph_pos_map_list);
  }

  private void loadGraph() throws Exception {
    LoadData loadData = new LoadData();
    loadData.loadAllEntityAndCreateIdMap(entities, labelList, dbPath, mapPath);
    loadData.LoadGraphEdges(mapPath, dbPath, graph);
  }

  private void readGraph() {
    graph = GraphUtil.ReadGraph(graphPath);
    entities = GraphUtil.ReadEntity(entityPath);
    labelList = ReadWriteUtil.readIntegerArray(labelListPath);
  }

  /**
   * <code>edges</code> contains the long type neo4j id.
   *
   * @param edges
   * @param maintenance
   * @throws Exception
   */
  public static void addEdgeMaintenance(List<Edge> edges, Maintenance maintenance)
      throws Exception {
    GraphDatabaseService service = maintenance.service;
    Transaction tx = service.beginTx();
    long time = System.currentTimeMillis();
    for (Edge edge : edges) {
      // Util.println(edge.toString());
      Node src = service.getNodeById(edge.start);
      Node trg = service.getNodeById(edge.end);
      maintenance.addEdgeAndUpdateIndex(src, trg);
    }
    long totalTime = System.currentTimeMillis() - time;
    Util.println(String.format("MG = %s, MR = %s", String.valueOf(maintenance.MG),
        String.valueOf(maintenance.MR)));
    Util.println("total time: " + totalTime);
    double avgTime = (double) totalTime / edges.size();
    Util.println("average time: " + avgTime);
    time = System.currentTimeMillis();
    tx.success();
    tx.close();
    Util.println("commit time: " + String.valueOf(System.currentTimeMillis() - time));
  }

  /**
   * Generate the edges for experiment.
   *
   * @throws Exception
   */
  public void generateEdges() throws Exception {
    String outputPath = queryDir + "/" + config.getEdgeFileName();
    generateEdges(graphPath, entityPath, labelListPath, 0.05, outputPath);
  }

  public static void generateEdges(String graphPath, String entityPath, String labelListPath,
      double targetRatio, String outputPath) throws IOException {
    Util.println("Read graph from " + graphPath);
    List<Collection<Integer>> graph =
        GraphUtil.convertListGraphToTreeSetGraph(GraphUtil.ReadGraph(graphPath));
    Util.println("Read entity from " + entityPath);
    ArrayList<Entity> entities = GraphUtil.ReadEntity(entityPath);
    Util.println("Read labels from " + labelListPath);
    ArrayList<Integer> labelList = ReadWriteUtil.readIntegerArray(labelListPath);

    int edgeCount = GraphUtil.getEdgeCount(graph);
    int targetCount = (int) (edgeCount * targetRatio);
    Util.println("Generate edges output to " + outputPath);
    generateEdges(graph, entities, labelList, targetCount, outputPath);
  }

  /**
   * Generate random edges between vertexes.
   *
   * @param graph
   * @param entities
   * @param labelList
   * @param targetCount
   * @param outputPath
   * @throws IOException
   */
  public static void generateEdges(List<Collection<Integer>> graph, List<Entity> entities,
      List<Integer> labelList, int targetCount, String outputPath) throws IOException {
    FileWriter writer = new FileWriter(outputPath);
    Random random = new Random();
    int size = graph.size();
    int count = 0;
    while (count < targetCount) {
      int id1 = random.nextInt(size);
      while (true) {
        int id2 = random.nextInt(size);
        if (id1 != id2) {
          if (!graph.get(id1).contains(id2)) {
            graph.get(id1).add(id2);
            graph.get(id2).add(id1);
            writer.write(String.format("%d,%d\n", id1, id2));
            count++;
            break;
          }
        }
      }
    }
    writer.close();
  }

}
