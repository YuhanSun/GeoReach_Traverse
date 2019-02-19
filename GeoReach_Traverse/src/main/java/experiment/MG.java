package experiment;

import java.util.ArrayList;
import java.util.HashMap;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Transaction;
import commons.Config;
import commons.Entity;
import commons.EnumVariables.GeoReachOutputFormat;
import commons.EnumVariables.system;
import commons.GeoReachIndexUtil;
import commons.GraphUtil;
import commons.MyRectangle;
import commons.ReadWriteUtil;
import commons.Util;
import commons.VertexGeoReach;
import construction.IndexConstruct;
import construction.Loader;
import query.SpaTraversal;

public class MG {
  Config config;
  String dataset, neo4j_version;
  system systemName;
  String password;
  int MAX_HOPNUM;
  double minx, miny, maxx, maxy;

  ArrayList<ArrayList<Integer>> graph;
  ArrayList<Entity> entities;
  String dbPath, entityPath, mapPath, graphPath;

  String dbDir, projectDir, resultDir, queryDir;
  private int spaCount;
  private long[] graph_pos_map_list;
  private String graph_pos_map_path;
  private String dataDir;
  Integer testMAXHOP = null;

  // int[] MGs = new int[] {0, 25, 50, 75, 100};
  // int[] MGs = new int[] {0, 50, 100};
  // int[] MGs = new int[] {0, 5, 10, 15, 20};
  int[] MGs = new int[] {0, 1, 2, 3};

  public static void main(String[] args) {
    Config config = new Config();
    // config.setDatasetName(Datasets.Gowalla_10.name());
    config.setDatasetName("Patents_2_random_80");
    config.setMAXHOPNUM(2);
    MG mg = new MG(config);
    mg.testMAXHOP = 2;
    // mg.generateIndex();
    // mg.loadIndex();
    mg.query();
  }

  public void generateIndex() {
    int pieces_x = 128, pieces_y = 128, MC = 0;
    int MR = 100;

    String dir = "D:\\Ubuntu_shared\\GeoReachHop\\data";
    ArrayList<VertexGeoReach> index = IndexConstruct.ConstructIndex(graph, entities, minx, miny,
        maxx, maxy, pieces_x, pieces_y, MAX_HOPNUM);
    // for (int MG = 0; MG <= 100; MG += 10)
    for (int MG : MGs) {
      Util.println("\nMG: " + MG);

      ArrayList<ArrayList<Integer>> typesList = IndexConstruct.generateTypeList(index, MAX_HOPNUM,
          minx, miny, maxx, maxy, pieces_x, pieces_y, MG / 100.0, MR / 100.0, MC);

      GeoReachOutputFormat format = GeoReachOutputFormat.BITMAP;
      String suffix = "bitmap";
      String indexPath = String.format("%s\\%s\\MG\\%d_%d_%d_%d_%d_%d_%s.txt", dir, dataset,
          pieces_x, pieces_y, MG, MR, MC, MAX_HOPNUM, suffix);
      Util.println("Output index to " + indexPath);
      GeoReachIndexUtil.outputGeoReach(index, indexPath, typesList, format);

      format = GeoReachOutputFormat.LIST;
      suffix = "list";
      indexPath = String.format("%s\\%s\\MG\\%d_%d_%d_%d_%d_%d_%s.txt", dir, dataset, pieces_x,
          pieces_y, MG, MR, MC, MAX_HOPNUM, suffix);
      Util.println("Output index to " + indexPath);
      GeoReachIndexUtil.outputGeoReach(index, indexPath, typesList, format);
    }

    // for (int MG = 1; MG <= 9; MG += 1)
    // {
    // Util.Print("\nMG: " + MG);
    // String indexPath = String.format("%s\\%s\\MG\\%d_%d_%d_%d_%d_%d_%s.txt",
    // dir, dataset, pieces_x, pieces_y, MG, MR, MC, MAX_HOPNUM, suffix);
    // Util.Print("Output index to " + indexPath);
    //
    // ArrayList<ArrayList<Integer>> typesList = IndexConstruct.generateTypeList(index, MAX_HOPNUM,
    // minx, miny, maxx, maxy,
    // pieces_x, pieces_y, MG/100.0, MR/100.0, MC);
    //
    // Util.outputGeoReach(index, indexPath, typesList, format);
    // }
  }

  public void loadIndex() {
    int pieces_x = 128, pieces_y = 128, MC = 0;
    int MR = 100;

    int format = 1;
    String suffix = "";
    if (format == 0)
      suffix = "list";
    else
      suffix = "bitmap";

    String dir = "D:\\Ubuntu_shared\\GeoReachHop\\data";

    // for (int MG = 25; MG <= 75; MG += 25)
    for (int MG : MGs) {
      Util.println("\nMG: " + MG);
      Loader loader = new Loader(config);

      String indexPath = String.format("%s\\%s\\MG\\%d_%d_%d_%d_%d_%d_%s.txt", dir, dataset,
          pieces_x, pieces_y, MG, MR, MC, MAX_HOPNUM, suffix);

      String dbPath =
          String.format("%s\\%s\\MG\\%s_%d_%d_%d_%d_%d_%d" + "\\data\\databases\\graph.db", dir,
              dataset, neo4j_version, pieces_x, pieces_y, MG, MR, MC, MAX_HOPNUM);

      Util.println(String.format("Load from %s\nto %s", indexPath, dbPath));
      loader.load(indexPath, dbPath);
    }
  }

  public void query() {
    try {
      int pieces_x = 128, pieces_y = 128, MC = 0;
      double MR = 1.0;
      double selectivity = 0.0001;
      int length = 3;
      MyRectangle total_range = new MyRectangle(minx, miny, maxx, maxy);

      // Read start ids
      String startIDPath = String.format("%s/startID.txt", queryDir);
      Util.println("start id path: " + startIDPath);
      ArrayList<Integer> allIDs = ReadWriteUtil.readIntegerArray(startIDPath);
      Util.println(allIDs);

      int experimentCount = 500;
      int groupCount = 1;
      ArrayList<ArrayList<Long>> startIDsList = new ArrayList<>();
      for (int i = 0; i < groupCount; i++)
        startIDsList.add(new ArrayList<>());

      int offset = 0;
      for (int i = offset; i < offset + experimentCount * groupCount; i++) {
        int id = allIDs.get(i);
        int index = i % groupCount;
        startIDsList.get(index).add(graph_pos_map_list[id]);
      }


      int repeatTime = 20;
      ArrayList<ArrayList<Long>> startIDsListRepeat = new ArrayList<>();
      for (ArrayList<Long> startIDs : startIDsList) {
        for (int i = 0; i < repeatTime; i++)
          startIDsListRepeat.add(new ArrayList<>(startIDs));
      }
      startIDsList = startIDsListRepeat;

      long start, time;

      String result_detail_path = null, result_avg_path = null;
      switch (systemName) {
        case Ubuntu:
          result_detail_path =
              String.format("%s/%s_spaTraversal_%d_detail.txt", resultDir, dataset, testMAXHOP);
          result_avg_path =
              String.format("%s/%s_spaTraversal_%d_avg.txt", resultDir, dataset, testMAXHOP);
          break;
        case Windows:
          // result_detail_path = String.format("%s\\risotree_PN_%d_%d.txt", resultDir, nodeCount,
          // query_id);
          // result_avg_path = String.format("%s\\risotree_PN_%d_%d_avg.txt.txt", resultDir,
          // nodeCount, query_id);
          break;
      }

      String write_line = String.format("%s\t%d\n", dataset, length);
      ReadWriteUtil.WriteFile(result_detail_path, true, write_line);
      ReadWriteUtil.WriteFile(result_avg_path, true, write_line);

      String head_line = "time\tdbTime\tcheckTime\t"
          + "visited_count\tGeoReachPruned\tHistoryPruned\tresult_count\n";
      ReadWriteUtil.WriteFile(result_avg_path, true, "MG\t" + head_line);

      // for ( double MG = 0; MG < 1.01; MG += 0.25) //Gowalla
      for (int MGint : MGs) {
        double MG = MGint / 100.0;
        String dbRootFolder = String.format("%s_%d_%d_%d_%d_%d_%d", neo4j_version, pieces_x,
            pieces_y, (int) (MG * 100), (int) (MR * 100), MC, MAX_HOPNUM);
        dbPath = String.format("%s/%s/MG/%s/data/databases/graph.db", dbDir, dataset, dbRootFolder);

        {
          int name_suffix = (int) (selectivity * spaCount);

          String queryrect_path = null;
          switch (systemName) {
            case Ubuntu:
              queryrect_path = String.format("%s/queryrect_%d.txt", queryDir, name_suffix);
              break;
            case Windows:
              queryrect_path = String.format("%s\\queryrect_%d.txt", queryDir, name_suffix);
              break;
          }
          Util.println("query rectangle path: " + queryrect_path);

          write_line = selectivity + "\n" + head_line;
          ReadWriteUtil.WriteFile(result_detail_path, true, write_line);

          ArrayList<MyRectangle> queryrect = ReadWriteUtil.ReadQueryRectangle(queryrect_path);

          ArrayList<Long> total_time = new ArrayList<Long>();
          ArrayList<Long> dbTime = new ArrayList<>();
          ArrayList<Long> checkTime = new ArrayList<>();
          ArrayList<Long> visitedcount = new ArrayList<Long>();
          ArrayList<Long> resultCount = new ArrayList<Long>();
          ArrayList<Long> GeoReachPrunedCount = new ArrayList<Long>();
          ArrayList<Long> HistoryPrunedCount = new ArrayList<Long>();

          for (int i = 0; i < startIDsList.size(); i++) {
            Util.println(dbPath);
            SpaTraversal spaTraversal =
                new SpaTraversal(dbPath, testMAXHOP, total_range, pieces_x, pieces_y);
            ArrayList<Long> startIDs = startIDsList.get(i);
            Transaction tx = spaTraversal.dbservice.beginTx();
            ArrayList<Node> startNodes = Util.getNodesByIDs(spaTraversal.dbservice, startIDs);
            tx.success();
            tx.close();

            MyRectangle rectangle = queryrect.get(i);
            if (rectangle.area() == 0.0) {
              double delta = Math.pow(0.1, 10);
              rectangle = new MyRectangle(rectangle.min_x - delta, rectangle.min_y - delta,
                  rectangle.max_x + delta, rectangle.max_y + delta);
            }

            Util.println(String.format("%d : %s", i, rectangle.toString()));
            // Util.Print(ids);

            Util.clearAndSleep(password, 5000);

            start = System.currentTimeMillis();
            spaTraversal.traverse(startNodes, length, rectangle);
            time = System.currentTimeMillis() - start;

            total_time.add(time);
            visitedcount.add(spaTraversal.visitedCount);
            resultCount.add(spaTraversal.resultCount);
            dbTime.add(spaTraversal.dbTime);
            checkTime.add(spaTraversal.checkTime);
            GeoReachPrunedCount.add(spaTraversal.GeoReachPruneCount);
            HistoryPrunedCount.add(spaTraversal.PrunedVerticesWorkCount);


            write_line = String.format("%d\t%d\t", total_time.get(i), visitedcount.get(i));
            write_line +=
                String.format("%d\t%d\t", GeoReachPrunedCount.get(i), HistoryPrunedCount.get(i));
            write_line += String.format("%d\n", resultCount.get(i));
            ReadWriteUtil.WriteFile(result_detail_path, true, write_line);

            spaTraversal.dbservice.shutdown();
          }

          write_line = String.valueOf(MG) + "\t";
          write_line += String.format("%d\t%d\t%d\t%d\t", Util.Average(total_time),
              Util.Average(dbTime), Util.Average(checkTime), Util.Average(visitedcount));
          write_line += String.format("%d\t%d\t%d\n", Util.Average(GeoReachPrunedCount),
              Util.Average(HistoryPrunedCount), Util.Average(resultCount));
          ReadWriteUtil.WriteFile(result_avg_path, true, write_line);

        }
      }
      ReadWriteUtil.WriteFile(result_detail_path, true, "\n");
      ReadWriteUtil.WriteFile(result_avg_path, true, "\n");
    } catch (Exception e) {
    }

  }

  public MG(Config config) {
    this.config = config;
    initParameters();
  }

  public void initParameters() {
    systemName = config.getSystemName();
    neo4j_version = config.GetNeo4jVersion();
    dataset = config.getDatasetName();
    MAX_HOPNUM = config.getMaxHopNum();
    password = config.getPassword();

    dataDir = config.getDataDir();
    dbDir = config.getDBDir();
    projectDir = config.getProjectDir();

    switch (systemName) {
      case Ubuntu:
        dbPath = String.format(
            "/home/yuhansun/Documents/GeoGraphMatchData/%s_%s/data/databases/graph.db",
            neo4j_version, dataset);
        entityPath = String.format("/mnt/hgfs/Ubuntu_shared/GeoMinHop/data/%s/entity.txt", dataset);
        graphPath = String.format("/mnt/hgfs/Ubuntu_shared/GeoMinHop/data/%s/graph.txt", dataset);
        graph_pos_map_path = dataDir + "/" + dataset + "/node_map_RTree.txt";

        resultDir = String.format("%s/MG", projectDir);
        queryDir = String.format("%s/query/%s", projectDir, dataset);
        break;
      case Windows:
        dbPath = String.format(
            "D:\\Ubuntu_shared\\GeoMinHop\\data\\%s\\%s_%s\\data\\databases\\graph.db", dataset,
            neo4j_version, dataset);
        entityPath = String.format("D:\\Ubuntu_shared\\GeoMinHop\\data\\%s\\entity.txt", dataset);
        graphPath = String.format("D:\\Ubuntu_shared\\GeoMinHop\\data\\%s\\graph.txt", dataset);
        graph_pos_map_path =
            "D:\\Ubuntu_shared\\GeoMinHop\\data\\" + dataset + "\\node_map_RTree.txt";
      default:
        break;
    }

    Util.println("Read graph from " + graphPath);
    graph = GraphUtil.ReadGraph(graphPath);

    Util.println("Read entity from " + entityPath);
    entities = GraphUtil.ReadEntity(entityPath);

    spaCount = Util.GetSpatialEntityCount(entities);

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

    HashMap<String, String> graph_pos_map = ReadWriteUtil.ReadMap(graph_pos_map_path);
    graph_pos_map_list = new long[graph_pos_map.size()];
    for (String key_str : graph_pos_map.keySet()) {
      int key = Integer.parseInt(key_str);
      int pos_id = Integer.parseInt(graph_pos_map.get(key_str));
      graph_pos_map_list[key] = pos_id;
    }
  }
}
