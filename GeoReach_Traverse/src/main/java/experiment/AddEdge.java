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

      // Run once to generate the inserted edges
      // addEdge.iniPaths();
      // addEdge.generateEdges();
      // addEdge.generateEdgesAttach();

      // Evaluate three set-up of MG, MR and test the run time of edge insertion
      addEdge.iniPaths();
      addEdge.evaluateEdgeInsertion();

      // Generate the db with the accurate index after insertion.
      // addEdge.iniPaths();
      // addEdge.generateAccurateDbAfterAddEdge();

      // addEdge.evaluateQuery();

    } catch (Exception e) {
      e.printStackTrace();
      System.exit(-1);
    }
  }

  public void iniPaths(String homeDir, String experimentDir, String dataset) {
    this.homeDir = homeDir;
    this.experimentDir = experimentDir;

    // data dirs
    this.dataset = dataset;
    dataDir = homeDir + "/data/" + dataset;
    graphPath = dataDir + "/" + config.getGraphFileName();
    entityPath = dataDir + "/" + config.getEntityFileName();
    labelListPath = dataDir + "/" + config.getLabelListFileName();

    // experiment dirs
    queryDir = String.format("%s/query/%s", experimentDir, dataset);
    resultDir = experimentDir + "/add_edge";
    edgePath = queryDir + "/edges.txt";
  }

  public void iniPaths() {
    // Only need to modify homeDir.
    // ClassLoader classLoader = new AddEdge().getClass().getClassLoader();
    // File file = new File(classLoader.getResource("").getFile());
    // homeDir = file.getAbsolutePath();
    homeDir = "D:\\Ubuntu_shared\\GeoReachHop";

    dataset = "Yelp";
    // experiment dirs
    // experimentDir = "/Users/zhouyang/Google_Drive/Projects/GeoReachHop";
    experimentDir = "D:\\Google_Drive\\Projects\\GeoReachHop";
    iniPaths(homeDir, experimentDir, dataset);
  }

  public static int piecesX = 128, piecesY = 128;
  public static int MC = 0;
  public static int MAX_HOP = 3;
  public static double minx = -180, miny = -90, maxx = 180, maxy = 90;
  public static SpaceManager spaceManager =
      new SpaceManager(minx, miny, maxx, maxy, piecesX, piecesY);

  public void evaluateInsertionByQuery() throws Exception {
    double MG, MR;
    MaintenanceStrategy strategy;
    Expand expand;

    MG = 1.0;
    MR = 2.0;
    expand = Expand.SIMPLEGRAPHTRAVERSAL;
    strategy = MaintenanceStrategy.LIGHTWEIGHT;
    iniPaths();
    evaluateInsertionByQuery(MG, MR, strategy, expand);
    // strategy = MaintenanceStrategy.RECONSTRUCT;
    // iniPaths();
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

    // MG = 0.5;
    // MR = 2.0;
    // strategy = MaintenanceStrategy.LIGHTWEIGHT;
    // expand = Expand.SPATRAVERSAL;
    // iniPaths();
    // runQueryLightweightStrategy(MG, MR, strategy, expand);
    // strategy = MaintenanceStrategy.RECONSTRUCT;
    // iniPaths();
    // runQueryLightweightStrategy(MG, MR, strategy, expand);

  }

  /**
   * Assume that the db exists.
   *
   * @param MG
   * @param MR
   * @param strategy
   * @param expand
   * @throws Exception
   */
  public void evaluateInsertionByQuery(double MG, double MR, MaintenanceStrategy strategy,
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
    int offset = 0, groupCount = 20, groupSize = 500;
    int length = 3;
    double startSelectivity = Math.pow(10, -4), endSelectivity = Math.pow(10, -1) * 2;
    int selectivityTimes = 10;

    SelectivityNumber.evaluateQuery(dbPath, expand, clearCache, hotDB, MG, MR, graph_pos_map_list,
        spaceManager, resultDir, dataset, queryDir, startIDPath, offset, groupCount, groupSize,
        spaCount, MAX_HOP, length, startSelectivity, endSelectivity, selectivityTimes);
  }

  /**
   * Generate the accurate SIP for comparison.
   *
   * @throws Exception
   */
  public void generateAccurateDbAfterAddEdge() throws Exception {
    readGraph();
    addEdgesToGraph();
    dataDir += "/" + afterAddAccurateDirName;
    if (!Util.pathExist(dataDir)) {
      new File(dataDir).mkdirs();
    }

    double MG, MR;

    // MG = 1.0;
    // MR = 2.0;
    // generateAccurateDbAfterAddEdges(MG, MR);

    MG = -1;
    MR = 2;
    generateAccurateDbAfterAddEdges(MG, MR);

    MG = -1;
    MR = -1;
    generateAccurateDbAfterAddEdges(MG, MR);

    MG = 0.5;
    MR = 2.0;
    generateAccurateDbAfterAddEdges(MG, MR);
  }

  /**
   * Generate the new db with edges being inserted. If the directory exists, it will be deleted. The
   * graph will be reloaded and index will be reconstructed and loaded as well.
   * 
   * @param MG
   * @param MR
   * @throws Exception
   */
  public void generateAccurateDbAfterAddEdges(double MG, double MR) throws Exception {
    String dbFileName = Neo4jGraphUtility.getDbNormalName(piecesX, piecesY, MG, MR, MC, MAX_HOP);

    dbPath = dataDir + "/" + dbFileName;
    if (Util.pathExist(dbPath)) {
      FileUtils.deleteDirectory(new File(dbPath));
    }
    // mapPath is initialized before loadGraphAndIndex because its value will be used in
    // loadGraphAndIndex.
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
      treeSetGraph.get(end).add(start);
    }
    graph = GraphUtil.convertCollectionGraphToArrayList(treeSetGraph);
  }


  /**
   * Evaluate the insertion speed with different MG, MR.
   *
   * @throws Exception
   */
  public void evaluateEdgeInsertion() throws Exception {
    readGraph();
    if (!Util.pathExist(dataDir)) {
      new File(dataDir).mkdirs();
    }

    // // All reachgrid
    evaluateEdgeInsersion(1.0, 2.0, 0.25, MaintenanceStrategy.LIGHTWEIGHT);
    evaluateEdgeInsersion(1.0, 2.0, 0.5, MaintenanceStrategy.LIGHTWEIGHT);
    evaluateEdgeInsersion(1.0, 2.0, 0.75, MaintenanceStrategy.LIGHTWEIGHT);
    evaluateEdgeInsersion(1.0, 2.0, 1.0, MaintenanceStrategy.LIGHTWEIGHT);
    //
    // // All rmbr
    evaluateEdgeInsersion(-1, 2.0, 0.25, MaintenanceStrategy.LIGHTWEIGHT);
    evaluateEdgeInsersion(-1, 2.0, 0.5, MaintenanceStrategy.LIGHTWEIGHT);
    evaluateEdgeInsersion(-1, 2.0, 0.75, MaintenanceStrategy.LIGHTWEIGHT);
    evaluateEdgeInsersion(-1, 2.0, 1.0, MaintenanceStrategy.LIGHTWEIGHT);
    //
    // // All GeoB
    evaluateEdgeInsersion(-1, -1, 0.25, MaintenanceStrategy.LIGHTWEIGHT);
    evaluateEdgeInsersion(-1, -1, 0.5, MaintenanceStrategy.LIGHTWEIGHT);
    evaluateEdgeInsersion(-1, -1, 0.75, MaintenanceStrategy.LIGHTWEIGHT);
    evaluateEdgeInsersion(-1, -1, 1.0, MaintenanceStrategy.LIGHTWEIGHT);

    // MG = 0.5, ReachGrid + RMBR
    evaluateEdgeInsersion(0.5, 2.0, 0.25, MaintenanceStrategy.LIGHTWEIGHT);
    evaluateEdgeInsersion(0.5, 2.0, 0.5, MaintenanceStrategy.LIGHTWEIGHT);
    evaluateEdgeInsersion(0.5, 2.0, 0.75, MaintenanceStrategy.LIGHTWEIGHT);
    evaluateEdgeInsersion(0.5, 2.0, 1.0, MaintenanceStrategy.LIGHTWEIGHT);
  }

  /**
   * Copy the original db to the testDbPath and add edges there.
   *
   * @param MG
   * @param MR
   * @param testCount how many edges in the edge.txt file will be tested
   * @throws Exception
   */
  public void evaluateEdgeInsersion(double MG, double MR, double testRatio,
      MaintenanceStrategy strategy) throws Exception {
    String dbFileName = Neo4jGraphUtility.getDbNormalName(piecesX, piecesY, MG, MR, MC, MAX_HOP);
    dbPath = dataDir + "/" + dbFileName;
    // mapPath is initialized before loadGraphAndIndex because its value will be used.
    String mapFileName =
        Neo4jGraphUtility.getGraphNeo4jIdMapNormalName(piecesX, piecesY, MG, MR, MC, MAX_HOP);
    mapPath = dataDir + "/" + mapFileName;
    loadGraphAndIndex(MG, MR);

    if (!Util.pathExist(testDir)) {
      new File(testDir).mkdirs();
    }
    switch (strategy) {
      case LIGHTWEIGHT:
        testDir = dataDir + "/" + afterAddLightweightDirName;
        break;
      case RECONSTRUCT:
        testDir = dataDir + "/" + afterAddAccurateDirName;
        break;
    }
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
    int testCount = (int) (edges.size() * testRatio);
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
    ResultRecord resultRecord = addEdgeMaintenance(edgesNeo4j, maintenance, strategy);
    service.shutdown();

    String resultPath = resultDir + "/add_edge_time.txt";
    FileWriter writer = new FileWriter(resultPath, true);
    writer.write(String.format("MAX_HOP=%d, MG=%s, MR=%s, strategy=%s\n", MAX_HOP,
        String.valueOf(MG), String.valueOf(MR), strategy));
    writer.write(String.format("insertion count: %d\n", edgesNeo4j.size()));
    writer.write(String.format("total time: %d\n", resultRecord.runTime));
    writer.write(
        String.format("average time: %f\n", (double) resultRecord.runTime / edgesNeo4j.size()));
    writer.write(String.format("commit time: %d\n", resultRecord.commitTime));
    writer.write(String.format("visited count: %d\n", resultRecord.visitedCount));
    writer.write(String.format("average visited count: %f\n",
        (double) resultRecord.visitedCount / edgesNeo4j.size()));

    writer.write("\n");
    writer.close();
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

  public void readGraph() {
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
  public static ResultRecord addEdgeMaintenance(List<Edge> edges, Maintenance maintenance,
      MaintenanceStrategy strategy) throws Exception {
    GraphDatabaseService service = maintenance.service;
    Transaction tx = service.beginTx();
    long totalTime = 0, visitedCount = 0;
    for (Edge edge : edges) {
      // Util.println(edge.toString());
      Node src = service.getNodeById(edge.start);
      Node trg = service.getNodeById(edge.end);
      long time = System.currentTimeMillis();
      if (strategy.equals(MaintenanceStrategy.LIGHTWEIGHT)) {
        maintenance.addEdgeAndUpdateIndexLightweight(src, trg);
      } else {
        maintenance.addEdgeAndUpdateIndexReconstruct(src, trg);
      }
      totalTime += System.currentTimeMillis() - time;
      visitedCount += maintenance.visitedCount;
    }
    Util.println(String.format("MG = %s, MR = %s", String.valueOf(maintenance.MG),
        String.valueOf(maintenance.MR)));
    Util.println("total time: " + totalTime);
    double avgTime = (double) totalTime / edges.size();
    Util.println("average time: " + avgTime);
    long time = System.currentTimeMillis();
    tx.success();
    tx.close();
    long commitTime = System.currentTimeMillis() - time;
    Util.println("commit time: " + String.valueOf(commitTime));
    return new ResultRecord(totalTime, commitTime, visitedCount, -1);
  }


  public void generateEdgesAttach() throws Exception {
    Util.println("Read graph from " + graphPath);
    this.graph = GraphUtil.ReadGraph(graphPath);
    List<Collection<Integer>> graph = GraphUtil.convertListGraphToTreeSetGraph(this.graph);
    int edgeCount = GraphUtil.getEdgeCount(graph);
    double addRatio = 0.05;
    int targetCount = (int) (addRatio * edgeCount);
    addEdgesToGraph();
    graph = GraphUtil.convertListGraphToTreeSetGraph(this.graph);
    List<Edge> edges = generateEdges(graph, targetCount);
    String outputPath = queryDir + "/" + config.getEdgeFileName();
    ReadWriteUtil.writeEdges(edges, outputPath, true);
  }

  /**
   * Generate the edges for experiment.
   *
   * @throws Exception
   */
  public void generateEdges() throws Exception {
    String outputPath = queryDir + "/" + config.getEdgeFileName();
    generateEdges(graphPath, 0.05, outputPath);
  }

  public static void generateEdges(String graphPath, double targetRatio, String outputPath)
      throws IOException {
    Util.println("Read graph from " + graphPath);
    List<Collection<Integer>> graph =
        GraphUtil.convertListGraphToTreeSetGraph(GraphUtil.ReadGraph(graphPath));

    int edgeCount = GraphUtil.getEdgeCount(graph);
    int targetCount = (int) (edgeCount * targetRatio);
    Util.println("Generate edges output to " + outputPath);
    List<Edge> edges = generateEdges(graph, targetCount);
    ReadWriteUtil.writeEdges(edges, outputPath, false);
  }

  // public static void generateEdges(List<Collection<Integer>> graph, List<Entity> entities,
  // List<Integer> labelList, int targetCount, String outputPath) throws IOException {
  // FileWriter writer = new FileWriter(outputPath);
  // Random random = new Random();
  // int size = graph.size();
  // int count = 0;
  // while (count < targetCount) {
  // int id1 = random.nextInt(size);
  // while (true) {
  // int id2 = random.nextInt(size);
  // if (id1 != id2) {
  // if (!graph.get(id1).contains(id2)) {
  // graph.get(id1).add(id2);
  // graph.get(id2).add(id1);
  // writer.write(String.format("%d,%d\n", id1, id2));
  // count++;
  // break;
  // }
  // }
  // }
  // }
  // writer.close();
  // }

  public static List<Edge> generateEdges(List<Collection<Integer>> graph, int targetCount) {
    List<Edge> edges = new ArrayList<>(targetCount);
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
            edges.add(new Edge(id1, id2));
            count++;
            break;
          }
        }
      }
    }
    return edges;
  }
}
