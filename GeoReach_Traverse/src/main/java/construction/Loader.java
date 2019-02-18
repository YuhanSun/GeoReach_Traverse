package construction;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;
import org.neo4j.unsafe.batchinsert.BatchInserter;
import org.neo4j.unsafe.batchinsert.BatchInserters;
import commons.Config;
import commons.Entity;
import commons.EnumVariables.Datasets;
import commons.EnumVariables.system;
import commons.Util;

/**
 * Load GeoReach index
 * 
 * @author ysun138
 *
 */
public class Loader {
  Config config;
  String dataset, version;
  system systemName;
  double minx, miny, maxx, maxy;

  ArrayList<ArrayList<Integer>> graph;
  ArrayList<Entity> entities;
  public long[] graph_pos_map_list;
  public String entityPath, graph_pos_map_path, graphPath;

  String GeoReachTypeName, reachGridName, rmbrName, geoBName;

  public Loader(Config config) {
    this.config = config;
    initParameters();
  }

  public Loader() {
    config = new Config();
    GeoReachTypeName = config.getGeoReachTypeName();
    reachGridName = config.getReachGridName();
    rmbrName = config.getRMBRName();
    geoBName = config.getGeoBName();
  }

  public static void main(String[] args) {
    // TODO Auto-generated method stub
    // load();
    loadServer();

  }

  public static void loadServer() {
    Loader loader = new Loader();
    String dir = "/hdd2/data/ysun138";
    String indexPath = dir + "/128_128_100_100_0_2_bitmap.txt";
    String dbFolder = "neo4j-community-3.1.1";
    String dbPath = dir + "/" + dbFolder + "/data/databases/graph.db";
    Util.println("load from " + indexPath + " \nto " + dbPath);
    if (!Util.pathExist(indexPath)) {
      Util.println(indexPath + " does not exist");
      System.exit(-1);
    }
    if (!Util.pathExist(dbPath)) {
      Util.println(dbPath + " does not exist");
      System.exit(-1);
    }

    Config config = new Config();
    loader.GeoReachTypeName = config.getGeoReachTypeName();
    loader.reachGridName = config.getReachGridName();
    loader.rmbrName = config.getRMBRName();
    loader.geoBName = config.getGeoBName();

    /**
     * set graph id to neo4j id map
     */
    String graph_pos_map_path = dir + "/node_map_RTree.txt";
    HashMap<String, String> graph_pos_map = Util.ReadMap(graph_pos_map_path);
    loader.graph_pos_map_list = new long[graph_pos_map.size()];
    for (String key_str : graph_pos_map.keySet()) {
      int key = Integer.parseInt(key_str);
      int pos_id = Integer.parseInt(graph_pos_map.get(key_str));
      loader.graph_pos_map_list[key] = pos_id;
    }
    Util.println("graph pos map size: " + graph_pos_map.size());

    loader.load(indexPath, dbPath);
  }

  public static void load() {
    Config config = new Config();
    config.setDatasetName(Datasets.Gowalla_10.name());

    Loader loader = new Loader(config);
    String dir = "D:\\Ubuntu_shared\\GeoReachHop\\data\\Gowalla_10";
    String indexPath = dir + "\\128_128_0_100_0_3_bitmap.txt";
    String dbFolder = "neo4j-community-3.1.1_Gowalla_10_128_128_100_100_0_3";
    String dbPath = dir + "\\" + dbFolder + "\\data\\databases\\graph.db";
    Util.println("load from " + indexPath + " \nto " + dbPath);
    loader.load(indexPath, dbPath);
  }

  /**
   * Load from index file of format BITMAP.
   *
   * @param indexPath
   * @param dbPath
   */
  public void load(String indexPath, String dbPath, long[] graph_pos_map_list) {
    int lineIndex = 0;
    String line = "";
    BatchInserter inserter = null;
    BufferedReader reader = null;
    int id;
    try {
      Map<String, String> config = new HashMap<String, String>();
      config.put("dbms.pagecache.memory", "100g");
      if (Util.pathExist(dbPath) == false)
        throw new Exception(dbPath + " does not exist!");
      inserter = BatchInserters.inserter(new File(dbPath).getAbsoluteFile(), config);

      reader = new BufferedReader(new FileReader(new File(indexPath)));

      lineIndex++;
      line = reader.readLine();
      String[] strList = line.split(",");
      int nodeCount = Integer.parseInt(strList[0]);
      int MAX_HOP = Integer.parseInt(strList[1]);

      for (int i = 0; i < nodeCount; i++) {
        lineIndex++;
        id = Integer.parseInt(reader.readLine());
        long neo4j_ID = graph_pos_map_list[id];
        for (int j = 1; j <= MAX_HOP; j++) {
          lineIndex++;
          line = reader.readLine();
          strList = line.split(":");
          int type = Integer.parseInt(strList[0]);
          switch (type) {
            case 0:
              inserter.setNodeProperty(neo4j_ID, reachGridName + "_" + j, strList[1]);
              break;
            case 1:
              inserter.setNodeProperty(neo4j_ID, rmbrName + "_" + j, strList[1]);
              break;
            case 2:
              if (strList[1].equals("true"))
                inserter.setNodeProperty(neo4j_ID, geoBName + "_" + j, true);
              else {
                inserter.setNodeProperty(neo4j_ID, geoBName + "_" + j, false);
              }
              break;
            default:
              throw new Exception(String.format("Vertex %d hop %d has type %d!", id, j, type));
          }
          inserter.setNodeProperty(neo4j_ID, GeoReachTypeName + "_" + j, type);
        }
      }

      inserter.shutdown();

    } catch (Exception e) {
      if (reader != null)
        try {
          reader.close();
        } catch (IOException e1) {
          // TODO Auto-generated catch block
          e1.printStackTrace();
        }

      if (inserter != null)
        inserter.shutdown();
      Util.println(String.format("line %d: %s", lineIndex, line));
      e.printStackTrace();
      System.exit(-1);
    }
  }

  /**
   * Load from index file of format BITMAP.
   *
   * @param indexPath
   * @param dbPath
   */
  public void load(String indexPath, String dbPath) {
    int lineIndex = 0;
    String line = "";
    BatchInserter inserter = null;
    BufferedReader reader = null;
    int id;
    try {
      Map<String, String> config = new HashMap<String, String>();
      config.put("dbms.pagecache.memory", "100g");
      if (Util.pathExist(dbPath) == false)
        throw new Exception(dbPath + " does not exist!");
      inserter = BatchInserters.inserter(new File(dbPath).getAbsoluteFile(), config);

      reader = new BufferedReader(new FileReader(new File(indexPath)));

      lineIndex++;
      line = reader.readLine();
      String[] strList = line.split(",");
      int nodeCount = Integer.parseInt(strList[0]);
      int MAX_HOP = Integer.parseInt(strList[1]);

      for (int i = 0; i < nodeCount; i++) {
        lineIndex++;
        id = Integer.parseInt(reader.readLine());
        long neo4j_ID = graph_pos_map_list[id];
        for (int j = 1; j <= MAX_HOP; j++) {
          lineIndex++;
          line = reader.readLine();
          strList = line.split(":");
          int type = Integer.parseInt(strList[0]);
          switch (type) {
            case 0:
              inserter.setNodeProperty(neo4j_ID, reachGridName + "_" + j, strList[1]);
              break;
            case 1:
              inserter.setNodeProperty(neo4j_ID, rmbrName + "_" + j, strList[1]);
              break;
            case 2:
              if (strList[1].equals("true"))
                inserter.setNodeProperty(neo4j_ID, geoBName + "_" + j, true);
              else {
                inserter.setNodeProperty(neo4j_ID, geoBName + "_" + j, false);
              }
              break;
            default:
              throw new Exception(String.format("Vertex %d hop %d has type %d!", id, j, type));
          }
          inserter.setNodeProperty(neo4j_ID, GeoReachTypeName + "_" + j, type);
        }
      }

      inserter.shutdown();

    } catch (Exception e) {
      if (reader != null)
        try {
          reader.close();
        } catch (IOException e1) {
          // TODO Auto-generated catch block
          e1.printStackTrace();
        }

      if (inserter != null)
        inserter.shutdown();
      Util.println(String.format("line %d: %s", lineIndex, line));
      e.printStackTrace();
      System.exit(-1);
    }
  }


  public void initParameters() {
    systemName = config.getSystemName();
    version = config.GetNeo4jVersion();
    dataset = config.getDatasetName();

    GeoReachTypeName = config.getGeoReachTypeName();
    reachGridName = config.getReachGridName();
    rmbrName = config.getRMBRName();
    geoBName = config.getGeoBName();

    switch (systemName) {
      case Ubuntu:
        entityPath = String.format("/mnt/hgfs/Ubuntu_shared/GeoMinHop/data/%s/entity.txt", dataset);
        graphPath = String.format("/mnt/hgfs/Ubuntu_shared/GeoMinHop/data/%s/graph.txt", dataset);
        graph_pos_map_path =
            "/mnt/hgfs/Ubuntu_shared/GeoMinHop/data/" + dataset + "/node_map_RTree.txt";
        break;
      case Windows:
        entityPath = String.format("D:\\Ubuntu_shared\\GeoMinHop\\data\\%s\\entity.txt", dataset);
        graphPath = String.format("D:\\Ubuntu_shared\\GeoMinHop\\data\\%s\\graph.txt", dataset);
        graph_pos_map_path =
            "D:\\Ubuntu_shared\\GeoMinHop\\data\\" + dataset + "\\node_map_RTree.txt";
      default:
        break;
    }

    /**
     * set whole space range
     */
    if (dataset.contains("Gowalla") || dataset.contains("Yelp") || dataset.contains("foursquare")) {
      minx = -180;
      miny = -90;
      maxx = 180;
      maxy = 90;
    }
    if (dataset.contains("Patents") || dataset.contains("go_uniprot")) {
      minx = 0;
      miny = 0;
      maxx = 1000;
      maxy = 1000;
    }

    /**
     * set graph id to neo4j id map
     */
    HashMap<String, String> graph_pos_map = Util.ReadMap(graph_pos_map_path);
    graph_pos_map_list = new long[graph_pos_map.size()];
    for (String key_str : graph_pos_map.keySet()) {
      int key = Integer.parseInt(key_str);
      int pos_id = Integer.parseInt(graph_pos_map.get(key_str));
      graph_pos_map_list[key] = pos_id;
    }
    Util.println(graph_pos_map.size());
  }

}
