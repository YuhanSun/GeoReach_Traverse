package experiment;

import java.io.File;
import java.text.DecimalFormat;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Transaction;
import org.neo4j.graphdb.factory.GraphDatabaseFactory;
import commons.ArrayUtil;
import commons.Config;
import commons.Entity;
import commons.EnumVariables.Datasets;
import commons.EnumVariables.Expand;
import commons.EnumVariables.system;
import commons.ExperimentUtil;
import commons.GraphUtil;
import commons.MyRectangle;
import commons.Neo4jGraphUtility;
import commons.ReadWriteUtil;
import commons.SpaceManager;
import commons.Util;
import query.Neo4jCypherTraversal;
import query.SimpleGraphTraversal;
import query.SpaTraversal;


public class SelectivityNumber {

  public static Config config = new Config();
  public String dataset;
  public String version;
  public system systemName;
  public static String password = config.getPassword();
  public int MAX_HOPNUM;
  public MyRectangle totalRange;

  public String dataDir, projectDir, dbDir;
  public String db_path;
  public String graph_pos_map_path;
  public String entityPath;
  public long[] graph_pos_map_list;

  // public String querygraphDir;
  public String queryDir;
  public String resultDir;

  public boolean TEST_FORMAT;

  // non-spatial ratio 20
  // static double startSelectivity = 0.000001;
  // static double endSelectivity = 0.002;

  // non-spatial ratio 80
  // double startSelectivity = 0.00001;
  // double endSelectivity = 0.2;

  // foursquare_100 and Gowalla
  public double startSelectivity = 0.000001;
  public double endSelectivity = 0.2;

  // Yelp
  // public double startSelectivity = 0.0001;
  // public double endSelectivity = 0.2;

  // Patents
  // double startSelectivity = 0.00001;
  // double endSelectivity = 0.002;

  // for switching point detect
  // static double startSelectivity = 0.01;
  // static double endSelectivity = 0.2;

  // static double startSelectivity = 0.0001;
  // static double endSelectivity = 0.2;

  public int spaCount;

  public SelectivityNumber(Config config) {
    this.config = config;
    initializeParameters();
  }

  public int pieces_x = 96, pieces_y = 96;
  public double MG = 1.0, MR = 1.0;
  public int MC = 0;
  public int length = 3;
  public int times = 10;

  public void initializeParameters() {
    TEST_FORMAT = false;
    dataset = config.getDatasetName();
    version = config.GetNeo4jVersion();
    systemName = config.getSystemName();
    password = config.getPassword();
    MAX_HOPNUM = config.getMaxHopNum();

    /**
     * set whole space range
     */
    if (dataset.contains("Gowalla") || dataset.contains("Yelp") || dataset.contains("foursquare"))
      totalRange = new MyRectangle(-180, -90, 180, 90);
    if (dataset.contains("Patents") || dataset.contains("go_uniprot"))
      totalRange = new MyRectangle(0, 0, 1000, 1000);


    dbDir = config.getDBDir();
    projectDir = config.getProjectDir();
    dataDir = config.getDataDir();

    switch (systemName) {
      case Ubuntu:
        String dbFolder = String.format("%s_%d_%d_%d_%d_%d_%d", version, pieces_x, pieces_y,
            (int) (MG * 100), (int) (MR * 100), MC, 3);
        db_path = String.format("%s/%s/%s/data/databases/graph.db", dbDir, dataset, dbFolder);
        graph_pos_map_path = dataDir + "/" + dataset + "/node_map_RTree.txt";
        entityPath = String.format("%s/%s/entity.txt", dataDir, dataset);
        // querygraphDir =
        // String.format("/mnt/hgfs/Google_Drive/Projects/risotree/query/query_graph/%s", dataset);
        queryDir = String.format("%s/query/%s", projectDir, dataset);
        resultDir = String.format("%s/selectivity_number", projectDir);
        // resultDir =
        // String.format("/mnt/hgfs/Google_Drive/Experiment_Result/Riso-Tree/%s/switch_point",
        // dataset);
        break;
      case Windows:
        // db_path =
        // String.format("D:\\Ubuntu_shared\\GeoMinHop\\data\\%s\\%s_%s\\data\\databases\\graph.db",
        // dataset, version, dataset);
        // graph_pos_map_path = "D:\\Ubuntu_shared\\GeoMinHop\\data\\" + dataset +
        // "\\node_map_RTree.txt";
        // entityPath = String.format("D:\\Ubuntu_shared\\GeoMinHop\\data\\%s\\entity.txt",
        // dataset);
        // querygraphDir = String.format("D:\\Ubuntu_shared\\GeoMinHop\\query\\query_graph\\%s",
        // dataset);
        // spaPredicateDir = String.format("D:\\Ubuntu_shared\\GeoMinHop\\query\\spa_predicate\\%s",
        // dataset);
        // resultDir = String.format("D:\\Google_Drive\\Experiment_Result\\Riso-Tree\\%s", dataset);
        break;
    }
    Util.println("entity path: " + entityPath);
    ArrayList<Entity> entities = GraphUtil.ReadEntity(entityPath);
    spaCount = Util.GetSpatialEntityCount(entities);

    HashMap<String, String> graph_pos_map = ReadWriteUtil.ReadMap(graph_pos_map_path);
    graph_pos_map_list = new long[graph_pos_map.size()];
    for (String key_str : graph_pos_map.keySet()) {
      int key = Integer.parseInt(key_str);
      int pos_id = Integer.parseInt(graph_pos_map.get(key_str));
      graph_pos_map_list[key] = pos_id;
    }
  }

  public static boolean clearCacheFlag = true;
  public static boolean hotDB = false;
  static DecimalFormat df = new DecimalFormat("0E0");

  public static void main(String[] args) {
    try {
      Config config = new Config();

      // ArrayList<String> dataset_a = new ArrayList<String>(Arrays.asList(
      // Config.Datasets.Gowalla_100.name(),
      // Config.Datasets.foursquare_100.name(),
      // Config.Datasets.Patents_100_random_80.name(),
      // Config.Datasets.go_uniprot_100_random_80.name()));

      config.setDatasetName(Datasets.Gowalla_10.name());
      config.setMAXHOPNUM(2);
      SelectivityNumber selectivityNumber = new SelectivityNumber(config);

      // Read start ids
      String startIDPath = String.format("%s/startID.txt", selectivityNumber.queryDir);
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
        startIDsList.get(index).add(selectivityNumber.graph_pos_map_list[id]);
      }


      int repeatTime = 20;
      ArrayList<ArrayList<Long>> startIDsListRepeat = new ArrayList<>();
      for (ArrayList<Long> startIDs : startIDsList) {
        for (int i = 0; i < repeatTime; i++)
          startIDsListRepeat.add(new ArrayList<>(startIDs));
      }
      startIDsList = startIDsListRepeat;

      clearCacheFlag = false;
      hotDB = true;
      // selectivityNumber.simpleTraversal(startIDsList);
      selectivityNumber.spaTraversal(startIDsList);
      // selectivityNumber.neo4jCypherTraveral(startIDsList);
    } catch (Exception e) {
      // TODO: handle exception
      e.printStackTrace();
      System.exit(-1);
    }
  }

  public static void evaluateQuery(String dbPath, Expand expand, boolean clearCache, double MG,
      double MR, long[] graph_pos_map_list, SpaceManager spaceManager, String resultDir,
      String dataset, String queryDir, String startIDPath, int offset, int groupCount,
      int groupSize, int spaCount, int MAX_HOP, int length, double startSelectivity,
      double endSelectivity, int selectivityTimes) throws Exception {
    {
      // Create the group nodes list
      GraphDatabaseService service = Neo4jGraphUtility.getDatabaseService(dbPath);
      ArrayList<Integer> allIDs = ReadWriteUtil.readIntegerArray(startIDPath);
      List<List<Node>> groupsNodes = new ArrayList<>(groupCount);
      for (int i = 0; i < groupCount; i++)
        groupsNodes.add(new ArrayList<>());
      Transaction tx = service.beginTx();
      for (int i = offset; i < offset + groupSize * groupCount; i++) {
        int id = allIDs.get(i);
        int index = i % groupCount;
        long neo4jId = graph_pos_map_list[id];
        groupsNodes.get(index).add(service.getNodeById(neo4jId));
      }
      tx.success();
      tx.close();

      // Read query rectangles list.
      List<List<MyRectangle>> rectanglesList = new LinkedList<>();
      double selectivity = startSelectivity;
      while (selectivity <= endSelectivity) {
        String queryrect_path = String.format("%s/%s", queryDir,
            ExperimentUtil.getRectangleFileName(spaCount, selectivity));
        Util.println("query rectangle path: " + queryrect_path);

        List<MyRectangle> queryrect = ReadWriteUtil.ReadQueryRectangle(queryrect_path);
        for (int i = 0; i < queryrect.size(); i++) {
          MyRectangle rectangle = queryrect.get(i);
          if (rectangle.area() == 0.0) {
            double delta = Math.pow(0.1, 10);
            rectangle = new MyRectangle(rectangle.min_x - delta, rectangle.min_y - delta,
                rectangle.max_x + delta, rectangle.max_y + delta);
          }
          queryrect.set(i, rectangle);
        }
        rectanglesList.add(queryrect);
        selectivity *= selectivityTimes;
      }

      String result_detail_path =
          String.format("%s/%s_spaTraversal_detail.txt", resultDir, dataset);
      String result_avg_path = String.format("%s/%s_spaTraversal_avg.txt", resultDir, dataset);

      String write_line =
          String.format("%s\n" + "length=%d, offset=%d, groupCount=%d, groupSize=%d" + "\n",
              dataset, length, offset, groupCount, groupSize);
      ReadWriteUtil.WriteFile(result_detail_path, true, write_line);
      ReadWriteUtil.WriteFile(result_avg_path, true, write_line);

      write_line = String.format("MAXHOP=%d, pieces=%d, MG=%f, MR=%f\n", MAX_HOP,
          spaceManager.getPiecesX(), MG, MR);
      ReadWriteUtil.WriteFile(result_avg_path, true, write_line);

      String head_line = "time\tvisited_count\tGeoReachPruned\tHistoryPruned\tresult_count\n";
      ReadWriteUtil.WriteFile(result_avg_path, true, "selectivity\t" + head_line);

      List<List<ResultRecord>> recordsList = SelectivityNumber.evaluateTraversalSelectivities(
          service, MAX_HOP, spaceManager, groupsNodes, rectanglesList, length, expand, clearCache);

      // Output the result
      selectivity = startSelectivity;
      for (List<ResultRecord> records : recordsList) {
        write_line = df.format(selectivity) + "\n" + head_line;
        ReadWriteUtil.WriteFile(result_detail_path, true, write_line);

        // Output detail for each traversal query.
        for (ResultRecord record : records) {
          // if (clearCacheFlag)
          // Util.clearAndSleep(password, 5000);
          // start = System.currentTimeMillis();
          // spaTraversal.traverse(startNodes, length, rectangle);
          // time = System.currentTimeMillis() - start;
          write_line = String.format("%d\t%d\t", record.runTime, record.visitedCount);
          write_line +=
              String.format("%d\t%d\t", record.GeoReachPrunedCount, record.HistoryPrunedCount);
          write_line += String.format("%d\n", record.resultCount);
          ReadWriteUtil.WriteFile(result_detail_path, true, write_line);
        }

        // For each selectivity, remove the first result
        if (hotDB) {
          records.remove(0);
        }

        // Output the average for one selectivity.
        write_line = df.format(selectivity) + "\t";
        write_line += String.format("%d\t%d\t", ExperimentUtil.getRuntimeAvg(records),
            ExperimentUtil.getVisitedCountAvg(records));
        write_line +=
            String.format("%d\t%d\t%d\n", ExperimentUtil.getGeoReachPrunedCountAvg(records),
                ExperimentUtil.getHistoryPrunedCountAvg(records),
                ExperimentUtil.getResultCountAvg(records));
        ReadWriteUtil.WriteFile(result_avg_path, true, write_line);

        selectivity *= selectivityTimes;
      }

      ReadWriteUtil.WriteFile(result_detail_path, true, "\n");
      ReadWriteUtil.WriteFile(result_avg_path, true, "\n");
    }
  }


  /**
   * Evaluate rectangle sets with different selectivities.
   *
   * @param service
   * @param MAX_HOP
   * @param spaceManager
   * @param groupsNodes
   * @param rectanglesList
   * @param expand
   * @return
   * @throws Exception
   */
  public static List<List<ResultRecord>> evaluateTraversalSelectivities(
      GraphDatabaseService service, int MAX_HOP, SpaceManager spaceManager,
      List<List<Node>> groupsNodes, List<List<MyRectangle>> rectanglesList, int length,
      Expand expand, boolean clearCache) throws Exception {
    List<List<ResultRecord>> recordsList = new ArrayList<>(rectanglesList.size());
    int i = 0;
    for (List<MyRectangle> rectangles : rectanglesList) {
      Util.println(String.format("selectivity: %d th", i));
      List<ResultRecord> records = evaluateTraversal(service, MAX_HOP, spaceManager, groupsNodes,
          rectangles, length, expand, clearCache);
      recordsList.add(records);
      i++;
    }
    return recordsList;
  }

  /**
   * Evaluate a set of node list for a list of rectangles with the same selectivity.
   *
   * @param service
   * @param MAX_HOP
   * @param spaceManager
   * @param groupsNodes
   * @param rectangles
   * @param expand
   * @return each ResultRecord in the returned list is the result for running a traversal from a
   *         list of nodes to a rectangle
   * @throws Exception
   */
  public static List<ResultRecord> evaluateTraversal(GraphDatabaseService service, int MAX_HOP,
      SpaceManager spaceManager, List<List<Node>> groupsNodes, List<MyRectangle> rectangles,
      int length, Expand expand, boolean clearCache) throws Exception {
    if (groupsNodes.size() > rectangles.size()) {
      throw new Exception(String.format("groupNodes has size of %d, while rectangles %d!",
          groupsNodes.size(), rectangles.size()));
    }

    List<ResultRecord> resultRecords = new ArrayList<>(groupsNodes.size());
    Iterator<List<Node>> iterator1 = groupsNodes.iterator();
    Iterator<MyRectangle> iterator2 = rectangles.iterator();
    switch (expand) {
      case SPATRAVERSAL:
        SpaTraversal spaTraversal = new SpaTraversal(service, MAX_HOP, spaceManager);
        while (iterator1.hasNext() && iterator2.hasNext()) {
          if (clearCache) {
            Util.clearAndSleep(password, 5000);
          }
          long start = System.currentTimeMillis();
          spaTraversal.traverse(iterator1.next(), length, iterator2.next());
          long runTime = System.currentTimeMillis() - start;
          ResultRecord record =
              new ResultRecord(runTime, spaTraversal.visitedCount, spaTraversal.resultCount,
                  spaTraversal.GeoReachPruneCount, spaTraversal.PrunedVerticesWorkCount);
          resultRecords.add(record);
          Util.println(record);
        }
        break;
      case SIMPLEGRAPHTRAVERSAL:
        SimpleGraphTraversal simpleGraphTraversal = new SimpleGraphTraversal(service);
        while (iterator1.hasNext() && iterator2.hasNext()) {
          long start = System.currentTimeMillis();
          simpleGraphTraversal.traverse(iterator1.next(), length, iterator2.next());
          long runTime = System.currentTimeMillis() - start;
          ResultRecord record = new ResultRecord(runTime, simpleGraphTraversal.visitedCount,
              simpleGraphTraversal.resultCount);
          resultRecords.add(record);
          Util.println(record);
        }
        break;
      default:
        break;
    }

    return resultRecords;
  }

  public void spaTraversal(ArrayList<ArrayList<Long>> startIDsList) {
    SpaTraversal spaTraversal = null;
    try {
      long start;
      long time;

      String result_detail_path = null, result_avg_path = null;
      switch (systemName) {
        case Ubuntu:
          result_detail_path = String.format("%s/%s_spaTraversal_detail.txt", resultDir, dataset);
          result_avg_path = String.format("%s/%s_spaTraversal_avg.txt", resultDir, dataset);
          break;
        case Windows:
          // result_detail_path = String.format("%s\\risotree_PN_%d_%d.txt", resultDir, nodeCount,
          // query_id);
          // result_avg_path = String.format("%s\\risotree_PN_%d_%d_avg.txt.txt", resultDir,
          // nodeCount, query_id);
          break;
      }

      String write_line = String.format("%s\tlength:%d\n", dataset, length);
      if (!TEST_FORMAT) {
        ReadWriteUtil.WriteFile(result_detail_path, true, write_line);
        ReadWriteUtil.WriteFile(result_avg_path, true, write_line);
      }

      write_line = String.format("MAXHOP=%d, pieces=%d, MG=%f", MAX_HOPNUM, pieces_x, MG);
      write_line = "MAXHOP = " + MAX_HOPNUM + ", pieces = " + pieces_x;
      ReadWriteUtil.WriteFile(result_avg_path, true, write_line + "\n");

      String head_line = "time\tvisited_count\tGeoReachPruned\tHistoryPruned\tresult_count\n";
      if (!TEST_FORMAT)
        ReadWriteUtil.WriteFile(result_avg_path, true, "selectivity\t" + head_line);

      double selectivity = startSelectivity;
      while (selectivity <= endSelectivity) {
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

        write_line = df.format(selectivity) + "\n" + head_line;
        if (!TEST_FORMAT)
          ReadWriteUtil.WriteFile(result_detail_path, true, write_line);

        Util.println("queryrect path: " + queryrect_path);
        List<MyRectangle> queryrect = ReadWriteUtil.ReadQueryRectangle(queryrect_path);

        Util.println("db path: " + db_path);
        spaTraversal = new SpaTraversal(db_path, MAX_HOPNUM, totalRange, pieces_x, pieces_y);

        ArrayList<Long> total_time = new ArrayList<Long>();
        ArrayList<Long> visitedcount = new ArrayList<Long>();
        ArrayList<Long> resultCount = new ArrayList<Long>();
        ArrayList<Long> GeoReachPrunedCount = new ArrayList<Long>();
        ArrayList<Long> HistoryPrunedCount = new ArrayList<Long>();

        for (int i = 0; i < startIDsList.size(); i++) {
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

          if (!TEST_FORMAT) {
            Util.println(String.format("%d : %s", i, rectangle.toString()));
            Util.println(startIDs);

            if (clearCacheFlag)
              Util.clearAndSleep(password, 5000);
            start = System.currentTimeMillis();
            spaTraversal.traverse(startNodes, length, rectangle);
            time = System.currentTimeMillis() - start;

            total_time.add(time);
            visitedcount.add(spaTraversal.visitedCount);
            resultCount.add(spaTraversal.resultCount);
            GeoReachPrunedCount.add(spaTraversal.GeoReachPruneCount);
            HistoryPrunedCount.add(spaTraversal.PrunedVerticesWorkCount);


            write_line = String.format("%d\t%d\t", total_time.get(i), visitedcount.get(i));
            write_line +=
                String.format("%d\t%d\t", GeoReachPrunedCount.get(i), HistoryPrunedCount.get(i));
            write_line += String.format("%d\n", resultCount.get(i));
            if (!TEST_FORMAT)
              ReadWriteUtil.WriteFile(result_detail_path, true, write_line);
          }

          spaTraversal.dbservice.shutdown();
          spaTraversal.dbservice =
              new GraphDatabaseFactory().newEmbeddedDatabase(new File(db_path));

        }
        spaTraversal.dbservice.shutdown();

        if (hotDB) {
          total_time.remove(0);
          visitedcount.remove(0);
          GeoReachPrunedCount.remove(0);
          HistoryPrunedCount.remove(0);
          resultCount.remove(0);
        }

        write_line = df.format(selectivity) + "\t";
        write_line += String.format("%d\t%d\t", ArrayUtil.Average(total_time),
            ArrayUtil.Average(visitedcount));
        write_line += String.format("%d\t%d\t%d\n", ArrayUtil.Average(GeoReachPrunedCount),
            ArrayUtil.Average(HistoryPrunedCount), ArrayUtil.Average(resultCount));
        if (!TEST_FORMAT)
          ReadWriteUtil.WriteFile(result_avg_path, true, write_line);

        selectivity *= times;
        Util.clearAndSleep(password, 5000);
      }
      ReadWriteUtil.WriteFile(result_detail_path, true, "\n");
      ReadWriteUtil.WriteFile(result_avg_path, true, "\n");
    } catch (Exception e) {
      e.printStackTrace();
      if (spaTraversal.dbservice != null)
        spaTraversal.dbservice.shutdown();
      System.exit(-1);
    }
  }

  public void simpleTraversal(ArrayList<ArrayList<Long>> startIDsList) {
    try {
      long start;
      long time;

      String result_detail_path = null, result_avg_path = null;
      switch (systemName) {
        case Ubuntu:
          result_detail_path =
              String.format("%s/%s_simpleTraversal_detail.txt", resultDir, dataset);
          result_avg_path = String.format("%s/%s_simpleTraversal_avg.txt", resultDir, dataset);
          break;
        case Windows:
          // result_detail_path = String.format("%s\\risotree_PN_%d_%d.txt", resultDir, nodeCount,
          // query_id);
          // result_avg_path = String.format("%s\\risotree_PN_%d_%d_avg.txt.txt", resultDir,
          // nodeCount, query_id);
          break;
      }

      String write_line = String.format("%s\t%d\n", dataset, length);
      if (!TEST_FORMAT) {
        ReadWriteUtil.WriteFile(result_detail_path, true, write_line);
        ReadWriteUtil.WriteFile(result_avg_path, true, write_line);
      }

      String head_line = "time\tvisited_count\tresult_count\n";
      if (!TEST_FORMAT)
        ReadWriteUtil.WriteFile(result_avg_path, true, "selectivity\t" + head_line);

      double selectivity = startSelectivity;
      while (selectivity <= endSelectivity) {
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

        write_line = df.format(selectivity) + "\n" + head_line;
        if (!TEST_FORMAT)
          ReadWriteUtil.WriteFile(result_detail_path, true, write_line);

        List<MyRectangle> queryrect = ReadWriteUtil.ReadQueryRectangle(queryrect_path);

        SimpleGraphTraversal simpleGraphTraversal = new SimpleGraphTraversal(db_path);

        ArrayList<Long> total_time = new ArrayList<Long>();
        ArrayList<Long> visitedcount = new ArrayList<Long>();
        ArrayList<Long> resultCount = new ArrayList<Long>();

        for (int i = 0; i < startIDsList.size(); i++) {
          ArrayList<Long> startIDs = startIDsList.get(i);
          Util.println("start ids: " + startIDs);
          Transaction tx = simpleGraphTraversal.dbservice.beginTx();
          ArrayList<Node> startNodes = Util.getNodesByIDs(simpleGraphTraversal.dbservice, startIDs);
          tx.success();
          tx.close();

          MyRectangle rectangle = queryrect.get(i);
          if (rectangle.area() == 0.0) {
            double delta = Math.pow(0.1, 10);
            rectangle = new MyRectangle(rectangle.min_x - delta, rectangle.min_y - delta,
                rectangle.max_x + delta, rectangle.max_y + delta);
          }

          if (!TEST_FORMAT) {
            Util.println(String.format("%d : %s", i, rectangle.toString()));
            Util.println(startIDs);

            if (clearCacheFlag)
              Util.clearAndSleep(password, 5000);

            start = System.currentTimeMillis();
            simpleGraphTraversal.traverse(startNodes, length, rectangle);
            time = System.currentTimeMillis() - start;

            total_time.add(time);
            visitedcount.add(simpleGraphTraversal.visitedCount);
            resultCount.add(simpleGraphTraversal.resultCount);

            write_line = String.format("%d\t%d\t", total_time.get(i), visitedcount.get(i));
            write_line += String.format("%d\n", resultCount.get(i));
            if (!TEST_FORMAT)
              ReadWriteUtil.WriteFile(result_detail_path, true, write_line);
          }

          simpleGraphTraversal.dbservice.shutdown();
          simpleGraphTraversal.dbservice =
              new GraphDatabaseFactory().newEmbeddedDatabase(new File(db_path));

        }
        simpleGraphTraversal.dbservice.shutdown();

        if (hotDB) {
          total_time.remove(0);
          visitedcount.remove(0);
          resultCount.remove(0);
        }
        write_line = df.format(selectivity) + "\t";
        write_line += String.format("%d\t%d\t", ArrayUtil.Average(total_time),
            ArrayUtil.Average(visitedcount));
        write_line += String.format("%d\n", ArrayUtil.Average(resultCount));
        if (!TEST_FORMAT)
          ReadWriteUtil.WriteFile(result_avg_path, true, write_line);

        selectivity *= times;
        Util.clearAndSleep(password, 5000);
      }
      ReadWriteUtil.WriteFile(result_detail_path, true, "\n");
      ReadWriteUtil.WriteFile(result_avg_path, true, "\n");
    } catch (Exception e) {
      e.printStackTrace();
      System.exit(-1);
    }
  }

  public void neo4jCypherTraveral(ArrayList<ArrayList<Long>> startIDsList) {
    try {
      long start;
      long time;

      String result_detail_path = null, result_avg_path = null;
      switch (systemName) {
        case Ubuntu:
          result_detail_path = String.format("%s/%s_neo4jCypher_detail.txt", resultDir, dataset);
          result_avg_path = String.format("%s/%s_neo4jCypher_avg.txt", resultDir, dataset);
          break;
        case Windows:
          // result_detail_path = String.format("%s\\risotree_PN_%d_%d.txt", resultDir, nodeCount,
          // query_id);
          // result_avg_path = String.format("%s\\risotree_PN_%d_%d_avg.txt.txt", resultDir,
          // nodeCount, query_id);
          break;
      }

      String write_line = String.format("%s\t%d\n", dataset, length);
      if (!TEST_FORMAT) {
        ReadWriteUtil.WriteFile(result_detail_path, true, write_line);
        ReadWriteUtil.WriteFile(result_avg_path, true, write_line);
      }

      String head_line = "time\tpage_access\tresult_count\n";
      if (!TEST_FORMAT)
        ReadWriteUtil.WriteFile(result_avg_path, true, "selectivity\t" + head_line);

      double selectivity = startSelectivity;
      int times = 10;
      while (selectivity <= endSelectivity) {
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
        if (!TEST_FORMAT)
          ReadWriteUtil.WriteFile(result_detail_path, true, write_line);

        List<MyRectangle> queryrect = ReadWriteUtil.ReadQueryRectangle(queryrect_path);

        Neo4jCypherTraversal neo4jCypherTraversal = new Neo4jCypherTraversal(db_path);

        ArrayList<Long> total_time = new ArrayList<Long>();
        ArrayList<Long> pageAccessCount = new ArrayList<Long>();
        ArrayList<Long> resultCount = new ArrayList<Long>();

        for (int i = 0; i < startIDsList.size(); i++) {
          ArrayList<Long> startIDs = startIDsList.get(i);
          Util.println("start ids: " + startIDs);

          MyRectangle rectangle = queryrect.get(i);
          if (rectangle.area() == 0.0) {
            double delta = Math.pow(0.1, 10);
            rectangle = new MyRectangle(rectangle.min_x - delta, rectangle.min_y - delta,
                rectangle.max_x + delta, rectangle.max_y + delta);
          }

          if (!TEST_FORMAT) {
            Util.println(String.format("%d : %s", i, rectangle.toString()));
            Util.println(startIDs);

            start = System.currentTimeMillis();
            neo4jCypherTraversal.traverse(startIDs, length, rectangle);
            time = System.currentTimeMillis() - start;

            total_time.add(time);
            pageAccessCount.add(neo4jCypherTraversal.pageAccessCount);
            resultCount.add(neo4jCypherTraversal.resultCount);

            write_line = String.format("%d\t%d\t", total_time.get(i), pageAccessCount.get(i));
            write_line += String.format("%d\n", resultCount.get(i));
            if (!TEST_FORMAT)
              ReadWriteUtil.WriteFile(result_detail_path, true, write_line);
          }

          neo4jCypherTraversal.dbservice.shutdown();

          Util.clearAndSleep(password, 5000);

          neo4jCypherTraversal.dbservice =
              new GraphDatabaseFactory().newEmbeddedDatabase(new File(db_path));

        }
        neo4jCypherTraversal.dbservice.shutdown();

        write_line = String.valueOf(selectivity) + "\t";
        write_line += String.format("%d\t%d\t", ArrayUtil.Average(total_time),
            ArrayUtil.Average(pageAccessCount));
        write_line += String.format("%d\n", ArrayUtil.Average(resultCount));
        if (!TEST_FORMAT)
          ReadWriteUtil.WriteFile(result_avg_path, true, write_line);

        selectivity *= times;
      }
      ReadWriteUtil.WriteFile(result_detail_path, true, "\n");
      ReadWriteUtil.WriteFile(result_avg_path, true, "\n");
    } catch (Exception e) {
      e.printStackTrace();
      System.exit(-1);
    }
  }
}
