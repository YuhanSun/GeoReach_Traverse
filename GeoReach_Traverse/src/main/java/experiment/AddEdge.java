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
import commons.GraphUtil;
import commons.Neo4jGraphUtility;
import commons.ReadWriteUtil;
import commons.SpaceManager;
import commons.Util;
import construction.Maintenance;
import dataprocess.LoadData;

public class AddEdge {

  public static Config config = new Config();

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

  ArrayList<ArrayList<Integer>> graph = null;
  ArrayList<Entity> entities = null;
  ArrayList<Integer> labelList = null;

  String mapPath;
  long[] graph_pos_map_list;

  public static void main(String[] args) throws Exception {
    // TODO Auto-generated method stub
    AddEdge addEdge = new AddEdge();
    addEdge.iniPaths();

    // generateEdges();
    addEdge.evaluate();
  }

  public void iniPaths() {
    // Only need to modify homeDir.
    ClassLoader classLoader = new AddEdge().getClass().getClassLoader();
    File file = new File(classLoader.getResource("").getFile());
    homeDir = file.getAbsolutePath();

    // data dirs
    dataset = "Yelp";
    dataDir = homeDir + "/data/" + dataset;
    graphPath = dataDir + "/" + config.getGraphFileName();
    entityPath = dataDir + "/" + config.getEntityFileName();
    labelListPath = dataDir + "/" + config.getLabelListFileName();

    // experiment dirs
    experimentDir = "/Users/zhouyang/Google_Drive/Projects";
    queryDir = String.format("%s/query/%s", experimentDir, dataset);
    resultDir = experimentDir + "/add_edge";
  }

  public static int piecesX = 128, piecesY = 128;
  public static int MC = 0;
  public static int MAX_HOP = 1;
  public static double minx = -180, miny = -90, maxx = 180, maxy = 90;

  public void evaluate() throws Exception {
    double MG = 2.0, MR = 2.0;
    evaluate(MG, MR, MC);

  }

  public void evaluate(double MG, double MR, int MC) throws Exception {
    String dbFileName = Neo4jGraphUtility.getDbNormalName(piecesX, piecesY, MG, MR, MC);
    if (!Util.pathExist(dataDir)) {
      new File(dataDir).mkdirs();
    }

    dbPath = dataDir + "/" + dbFileName;
    // mapPath is initialized before loadGraphAndIndex because its value will be used.
    mapPath = dataDir + "/"
        + Neo4jGraphUtility.getGraphNeo4jIdMapNormalName(piecesX, piecesY, MG, MR, MC);
    if (!Util.pathExist(dbPath)) {
      loadGraphAndIndex();
    }
    graph_pos_map_list = ReadWriteUtil.readMapToArray(mapPath);

    String testDbFileName = "test_" + dbFileName;
    String testDbPath = dataDir + "/" + testDbFileName;
    // If the testDb dir exists, remove it.
    // Because it was inserted edges and is different from the original graph.
    if (Util.pathExist(testDbPath)) {
      FileUtils.deleteDirectory(new File(dbPath));
    }
    // Copy the original db to the test db dir.
    FileUtils.copyDirectoryToDirectory(new File(dbPath), new File(testDbPath));

    GraphDatabaseService service = Neo4jGraphUtility.getDatabaseService(testDbPath);
    SpaceManager spaceManager = new SpaceManager(minx, miny, maxx, maxy, piecesX, piecesY);
    Maintenance maintenance = new Maintenance(spaceManager, MAX_HOP, MG, MR, MC, service);
    List<Edge> edges = GraphUtil.readEdges(queryDir + "/edges.txt");
    addEdgeMaintenance(edges, maintenance);
    service.shutdown();
  }

  private void loadGraphAndIndex() throws Exception {
    readGraph();
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
    for (Edge edge : edges) {
      Node src = service.getNodeById(edge.start);
      Node trg = service.getNodeById(edge.end);
      maintenance.addEdgeAndUpdateIndex(src, trg);
    }
    tx.success();
    tx.close();
  }

  /**
   * Generate the edges for experiment.
   *
   * @throws Exception
   */
  public void generateEdges() throws Exception {
    String outputPath =
        "/Users/zhouyang/Google_Drive/Projects/GeoReachHop/query/" + dataset + "/edges.txt";
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
          }
        }
      }
    }
    writer.close();
  }

}
