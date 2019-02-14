package construction;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.TreeSet;
import commons.Config;
import commons.Entity;
import commons.EnumVariables.GeoReachOutputFormat;
import commons.EnumVariables.system;
import commons.GeoReachIndexUtil;
import commons.MyRectangle;
import commons.SpaceManager;
import commons.Util;
import commons.VertexGeoReach;
import commons.VertexGeoReachList;
import kwaysort.KWayMerging;

/**
 * Construct the GeoReach Index. Do not consider MC now.
 *
 * @author Yuhan Sun
 *
 */
public class IndexConstruct {

  public Config config;
  public String dataset, version;
  public system systemName;
  public int MAX_HOP;
  public double minx, miny, maxx, maxy;

  public ArrayList<ArrayList<Integer>> graph;
  public ArrayList<Entity> entities;
  public String dbPath, entityPath, graphPath;

  /**
   * Example of how to use such class. Construct from scratch.
   */
  public void constructList(String dir) {
    if (Util.pathExist(graphPath))
      Util.print(graphPath);
    else {
      Util.print(graphPath + " not exist");
      System.exit(-1);
    }

    if (Util.pathExist(entityPath))
      Util.print(entityPath);
    else {
      Util.print(entityPath + " not exist");
      System.exit(-1);
    }

    String suffix = "whole";
    String outputPath =
        String.format("%s%d_%d_%d_%s.txt", dir, pieces_x, pieces_y, MAX_HOP, suffix);
    if (Util.pathExist(dir))
      Util.print(dir);
    else {
      Util.print(outputPath + " not exist");
      System.exit(-1);
    }

    Util.print("Read entities from " + entityPath);
    if (entities == null)
      entities = Util.ReadEntity(entityPath);
    Util.print("entities size: " + entities.size() + "\n");

    Util.print("Read graph from " + graphPath);
    if (graph == null)
      graph = Util.ReadGraph(graphPath);
    Util.print("graph size: " + graph.size());

    ArrayList<VertexGeoReachList> index =
        ConstructIndexList(graph, entities, minx, miny, maxx, maxy, pieces_x, pieces_y, MAX_HOP);

    // ouput whole index
    Util.print("output index to " + outputPath);
    GeoReachIndexUtil.outputGeoReachList(index, outputPath);

    // output single index for load
    ArrayList<ArrayList<Integer>> typesList = generateTypeListForList(index, MAX_HOP, minx, miny,
        maxx, maxy, pieces_x, pieces_y, MG, MR, MC);

    int format = 1;
    suffix = "bitmap";
    outputPath = String.format("%s%d_%d_%d_%d_%d_%d_%s.txt", dir, pieces_x, pieces_y,
        (int) (MG * 100), (int) (MR * 100), MC, MAX_HOP, suffix);
    Util.print("output to " + outputPath);
    GeoReachIndexUtil.outputGeoReachForList(index, outputPath, typesList,
        GeoReachOutputFormat.BITMAP);

    format = 0;
    suffix = "list";
    outputPath = String.format("%s%d_%d_%d_%d_%d_%d_%s.txt", dir, pieces_x, pieces_y,
        (int) (MG * 100), (int) (MR * 100), MC, MAX_HOP, suffix);
    Util.print("output to " + outputPath);
    GeoReachIndexUtil.outputGeoReachForList(index, outputPath, typesList,
        GeoReachOutputFormat.LIST);
  }

  /**
   * Example of how to use such class. Construct from stracth.
   */
  public void construct() {
    // TODO Auto-generated method stub
    Util.print("Read entities from " + entityPath);
    if (entities == null)
      entities = Util.ReadEntity(entityPath);
    Util.print("entities size: " + entities.size() + "\n");

    Util.print("Read graph from " + graphPath);
    if (graph == null)
      graph = Util.ReadGraph(graphPath);
    Util.print("graph size: " + graph.size());

    ArrayList<VertexGeoReach> index =
        ConstructIndex(graph, entities, minx, miny, maxx, maxy, pieces_x, pieces_y, MAX_HOP);

    String suffix = "whole";
    String outputPath = String.format("D:\\Ubuntu_shared\\GeoReachHop\\data\\%s\\%d_%d_%d_%s.txt",
        dataset, pieces_x, pieces_y, MAX_HOP, suffix);
    Util.print("output index to " + outputPath);
    GeoReachIndexUtil.outputGeoReach(index, outputPath); // output all three types.

    // ArrayList<VertexGeoReachList> index = ConstructIndexList(graph, entities,
    // minx, miny, maxx, maxy,
    // pieces_x, pieces_y, MAX_HOPNUM);
    //
    // //ouput whole index
    // String suffix = "whole_test";
    // String outputPath =
    // String.format("D:\\Ubuntu_shared\\GeoReachHop\\data\\%s\\%d_%d_%d_%s.txt",
    // dataset, pieces_x, pieces_y, MAX_HOPNUM, suffix);
    // Util.outputGeoReachList(index, outputPath);

    // output single index
    // ArrayList<ArrayList<Integer>> typesList = generateTypeList(index, MAX_HOPNUM,
    // minx, miny, maxx, maxy,
    // pieces_x, pieces_y, MG, MR, MC);
    //
    // int format = 0;
    // String suffix = "bitmap";
    // String outputPath =
    // String.format("D:\\Ubuntu_shared\\GeoReachHop\\data\\%s\\%d_%d_%d_%d_%d_%d_%s.txt",
    // dataset, pieces_x, pieces_y, (int)(MG * 100), (int)(MR * 100), MC, MAX_HOPNUM, suffix);
    // Util.outputGeoReach(index, outputPath, typesList, format);
    //
    // format = 1;
    // suffix = "list";
    // outputPath =
    // String.format("D:\\Ubuntu_shared\\GeoReachHop\\data\\%s\\%d_%d_%d_%d_%d_%d_%s.txt",
    // dataset, pieces_x, pieces_y, (int)(MG * 100), (int)(MR * 100), MC, MAX_HOPNUM, suffix);
    // Util.outputGeoReach(index, outputPath, typesList, format);
  }

  /**
   * Construct final index by reading the local whole index.
   *
   * @param dir
   * @param inputFileName
   */
  public void constructFromFile(String dir, String inputFileName) {
    if (Util.pathExist(dir))
      Util.print("directory is " + dir);
    else {
      Util.print(dir + " does not exist");
      System.exit(-1);
    }
    String indexPath = dir + inputFileName;
    Util.print("read index from " + indexPath);
    ArrayList<VertexGeoReachList> index = GeoReachIndexUtil.readGeoReachWhole(indexPath);

    Util.print("generate type list");
    ArrayList<ArrayList<Integer>> typesList = generateTypeListForList(index, MAX_HOP, minx, miny,
        maxx, maxy, pieces_x, pieces_y, MG, MR, MC);

    int format = 0;
    String suffix = "list";
    String outputPath = String.format("%s%d_%d_%d_%d_%d_%d_%s.txt", dir, pieces_x, pieces_y,
        (int) (MG * 100), (int) (MR * 100), MC, MAX_HOP, suffix);
    Util.print("output to " + outputPath);
    GeoReachIndexUtil.outputGeoReachForList(index, outputPath, typesList,
        GeoReachOutputFormat.LIST);

    format = 1;
    suffix = "bitmap";
    outputPath = String.format("%s%d_%d_%d_%d_%d_%d_%s.txt", dir, pieces_x, pieces_y,
        (int) (MG * 100), (int) (MR * 100), MC, MAX_HOP, suffix);
    Util.print("output to " + outputPath);
    GeoReachIndexUtil.outputGeoReachForList(index, outputPath, typesList,
        GeoReachOutputFormat.BITMAP);
  }

  int pieces_x = 128, pieces_y = 128, MC = 0;
  double MG = 1.0, MR = 1.0;

  public static void main(String[] args) {

    Util.print(args[0]);
    Util.print(args[1]);
    Util.print(args[2]);
    Util.print(args[3]);

    IndexConstruct indexConstruct = new IndexConstruct();
    indexConstruct.graphPath = args[0];
    indexConstruct.entityPath = args[1];

    indexConstruct.minx = -180;
    indexConstruct.miny = -90;
    indexConstruct.maxx = 180;
    indexConstruct.maxy = 90;

    String dir = args[2];
    indexConstruct.MAX_HOP = Integer.parseInt(args[3]);
    indexConstruct.constructList(dir);

    // String dir = args[0];
    // String filename = args[1];
    //
    // Config config = new Config();
    //// String dir = "D:\\Ubuntu_shared\\GeoReachHop\\data\\wikidata\\";
    //// String filename = "128_128_2_whole.txt";
    // config.setDatasetName(Datasets.wikidata.name());
    // config.setMAXHOPNUM(2);
    // IndexConstruct indexConstruct = new IndexConstruct(config);
    // indexConstruct.constructFromFile(dir, filename);

    // config.setDatasetName(Datasets.wikidata.name());
    // IndexConstruct indexConstruct = new IndexConstruct(config);
    // indexConstruct.construct();
    // try {
    // System.in.read();
    // } catch (IOException e) {
    // // TODO Auto-generated catch block
    // e.printStackTrace();
    // }
  }

  /**
   * This has to be called anytime to set up parameters.
   */
  public IndexConstruct(Config config) {
    this.config = config;
    initParameters();
  }

  public IndexConstruct() {}

  public void initParameters() {
    systemName = config.getSystemName();
    version = config.GetNeo4jVersion();
    dataset = config.getDatasetName();
    MAX_HOP = config.getMaxHopNum();
    switch (systemName) {
      case Ubuntu:
        dbPath = String.format(
            "/home/yuhansun/Documents/GeoGraphMatchData/%s_%s/data/databases/graph.db", version,
            dataset);
        entityPath = String.format("/mnt/hgfs/Ubuntu_shared/GeoMinHop/data/%s/entity.txt", dataset);
        graphPath = String.format("/mnt/hgfs/Ubuntu_shared/GeoMinHop/data/%s/graph.txt", dataset);
        break;
      case Windows:
        dbPath = String.format(
            "D:\\Ubuntu_shared\\GeoMinHop\\data\\%s\\%s_%s\\data\\databases\\graph.db", dataset,
            version, dataset);
        entityPath = String.format("D:\\Ubuntu_shared\\GeoMinHop\\data\\%s\\entity.txt", dataset);
        graphPath = String.format("D:\\Ubuntu_shared\\GeoMinHop\\data\\%s\\graph.txt", dataset);
      default:
        break;
    }
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
  }

  public static ArrayList<VertexGeoReach> ConstructIndex(ArrayList<ArrayList<Integer>> graph,
      ArrayList<Entity> entities, SpaceManager spaceManager, int MAX_HOP) {
    return ConstructIndex(graph, entities, spaceManager.getMinx(), spaceManager.getMiny(),
        spaceManager.getMaxx(), spaceManager.getMaxy(), spaceManager.getPiecesX(),
        spaceManager.getPiecesY(), MAX_HOP);
  }

  /**
   * Generate index where ReachGrid is treeset.
   *
   * @param graph
   * @param entities
   * @param minx
   * @param miny
   * @param maxx
   * @param maxy
   * @param pieces_x
   * @param pieces_y
   * @param MAX_HOP
   * @return
   */
  public static ArrayList<VertexGeoReach> ConstructIndex(ArrayList<ArrayList<Integer>> graph,
      ArrayList<Entity> entities, double minx, double miny, double maxx, double maxy, int pieces_x,
      int pieces_y, int MAX_HOP) {
    int nodeCount = graph.size();
    double resolution_x = (maxx - minx) / pieces_x;
    double resolution_y = (maxy - miny) / pieces_y;
    ArrayList<VertexGeoReach> index = new ArrayList<VertexGeoReach>(nodeCount);
    for (int i = 0; i < nodeCount; i++) {
      VertexGeoReach vertexGeoReach = new VertexGeoReach(MAX_HOP);
      index.add(vertexGeoReach);
    }

    long start = System.currentTimeMillis();

    /**
     * Construct the 1-hop GeoReach. It will be stored at index 0.
     */
    Util.print("Construct 1-hop");
    int id = 0;
    for (ArrayList<Integer> neighbors : graph) {
      VertexGeoReach vertexGeoReach = index.get(id);
      for (int neighborID : neighbors) {
        Entity entity = entities.get(neighborID);
        if (entity.IsSpatial) {
          int idX = (int) ((entity.lon - minx) / resolution_x);
          int idY = (int) ((entity.lat - miny) / resolution_y);
          idX = Math.min(pieces_x - 1, idX);
          idY = Math.min(pieces_y - 1, idY);
          int gridID = idX * pieces_x + idY;

          // Util.Print(vertexGeoReach.ReachGrids.size());
          TreeSet<Integer> reachgrid = vertexGeoReach.ReachGrids.get(0);
          if (reachgrid == null) {
            reachgrid = new TreeSet<>();
            reachgrid.add(gridID);
            vertexGeoReach.ReachGrids.set(0, reachgrid);
          } else
            reachgrid.add(gridID);

          MyRectangle rmbr = vertexGeoReach.RMBRs.get(0);
          if (rmbr == null) {
            rmbr = new MyRectangle(entity.lon, entity.lat, entity.lon, entity.lat);
            vertexGeoReach.RMBRs.set(0, rmbr);
          } else
            rmbr.MBR(new MyRectangle(entity.lon, entity.lat, entity.lon, entity.lat));
        }
      }
      id++;
    }

    Util.print("1-hop time: " + (System.currentTimeMillis() - start) + "\n");
    start = System.currentTimeMillis();
    /**
     * 2-hop and more hops
     */
    for (int i = 1; i < MAX_HOP; i++) {
      Util.print(String.format("Construct %d-hop", i + 1));
      id = 0;
      for (ArrayList<Integer> neighbors : graph) {
        VertexGeoReach vertexGeoReach = index.get(id);

        TreeSet<Integer> targetReachGrid = vertexGeoReach.ReachGrids.get(i);
        MyRectangle targetRMBR = vertexGeoReach.RMBRs.get(i);
        for (int neighborID : neighbors) {
          VertexGeoReach neighborGeoReach = index.get(neighborID);
          TreeSet<Integer> reachgrid = neighborGeoReach.ReachGrids.get(i - 1);
          if (reachgrid != null) {
            if (targetReachGrid == null) {
              targetReachGrid = new TreeSet<>(reachgrid);
              vertexGeoReach.ReachGrids.set(i, targetReachGrid);
            } else
              targetReachGrid.addAll(reachgrid);
          }

          MyRectangle rmbr = neighborGeoReach.RMBRs.get(i - 1);
          if (rmbr != null) {
            if (targetRMBR == null) {
              targetRMBR = new MyRectangle(rmbr);
              vertexGeoReach.RMBRs.set(i, targetRMBR);
            } else
              targetRMBR.MBR(rmbr);

          }
        }
        id++;
      }
      Util.print(String.format("%d-hop time: %d\n", i + 1, System.currentTimeMillis() - start));
      start = System.currentTimeMillis();
    }

    return index;
  }

  public static ArrayList<VertexGeoReachList> ConstructIndexList(
      ArrayList<ArrayList<Integer>> graph, ArrayList<Entity> entities, SpaceManager spaceManager,
      int MAX_HOP) {
    return ConstructIndexList(graph, entities, spaceManager.getMinx(), spaceManager.getMiny(),
        spaceManager.getMaxx(), spaceManager.getMaxy(), spaceManager.getPiecesX(),
        spaceManager.getPiecesY(), MAX_HOP);
  }

  /**
   * Generate index where Reachgrid is list.
   *
   * @param graph
   * @param entities
   * @param minx
   * @param miny
   * @param maxx
   * @param maxy
   * @param pieces_x
   * @param pieces_y
   * @param MAX_HOP
   * @return
   */
  public static ArrayList<VertexGeoReachList> ConstructIndexList(
      ArrayList<ArrayList<Integer>> graph, ArrayList<Entity> entities, double minx, double miny,
      double maxx, double maxy, int pieces_x, int pieces_y, int MAX_HOP) {
    int nodeCount = graph.size();
    double resolution_x = (maxx - minx) / pieces_x;
    double resolution_y = (maxy - miny) / pieces_y;
    ArrayList<VertexGeoReachList> index = new ArrayList<>(nodeCount);
    for (int i = 0; i < nodeCount; i++) {
      VertexGeoReachList vertexGeoReach = new VertexGeoReachList(MAX_HOP);
      index.add(vertexGeoReach);
    }

    long start = System.currentTimeMillis();

    /**
     * Construct the 1-hop GeoReach. It will be stored at index 0.
     */
    Util.print("Construct 1-hop");
    int id = 0;
    for (ArrayList<Integer> neighbors : graph) {
      VertexGeoReachList vertexGeoReach = index.get(id);
      TreeSet<Integer> reachgrid = null;
      MyRectangle rmbr = null;
      for (int neighborID : neighbors) {
        Entity entity = entities.get(neighborID);
        if (entity.IsSpatial) {
          int idX = (int) ((entity.lon - minx) / resolution_x);
          int idY = (int) ((entity.lat - miny) / resolution_y);
          idX = Math.min(pieces_x - 1, idX);
          idY = Math.min(pieces_y - 1, idY);
          int gridID = idX * pieces_x + idY;

          // TreeSet<Integer> reachgrid = vertexGeoReach.ReachGrids.get(0);
          if (reachgrid == null)
            reachgrid = new TreeSet<>();
          reachgrid.add(gridID);


          // MyRectangle rmbr = vertexGeoReach.RMBRs.get(0);
          if (rmbr == null) {
            rmbr = new MyRectangle(entity.lon, entity.lat, entity.lon, entity.lat);
            // vertexGeoReach.RMBRs.set(0, rmbr);
          } else
            rmbr.MBR(new MyRectangle(entity.lon, entity.lat, entity.lon, entity.lat));
        }
      }
      if (reachgrid == null)
        vertexGeoReach.ReachGrids.set(0, null);
      else {
        ArrayList<Integer> list = new ArrayList<>(reachgrid);
        vertexGeoReach.ReachGrids.set(0, list);
      }
      vertexGeoReach.RMBRs.set(0, rmbr);
      id++;
    }

    Util.print("1-hop time: " + (System.currentTimeMillis() - start) + "\n");
    start = System.currentTimeMillis();
    /**
     * 2-hop and more hops
     */
    for (int i = 1; i < MAX_HOP; i++) {
      Util.print(String.format("Construct %d-hop", i + 1));
      id = 0;

      for (ArrayList<Integer> neighbors : graph) {
        VertexGeoReachList vertexGeoReach = index.get(id);
        ArrayList<ArrayList<Integer>> temp = new ArrayList<>(neighbors.size());

        // ArrayList<Integer> targetReachGrid = vertexGeoReach.ReachGrids.get(i);
        MyRectangle targetRMBR = vertexGeoReach.RMBRs.get(i);
        for (int neighborID : neighbors) {
          VertexGeoReachList neighborGeoReach = index.get(neighborID);
          ArrayList<Integer> reachgrid = neighborGeoReach.ReachGrids.get(i - 1);
          if (reachgrid != null)
            temp.add(reachgrid);

          MyRectangle rmbr = neighborGeoReach.RMBRs.get(i - 1);
          if (rmbr != null) {
            if (targetRMBR == null) {
              targetRMBR = new MyRectangle(rmbr);
              vertexGeoReach.RMBRs.set(i, targetRMBR);
            } else
              targetRMBR.MBR(rmbr);
          }
        }

        if (temp.size() > 0) {
          KWayMerging kWayMerging = new KWayMerging(temp.size());
          ArrayList<Integer> targetReachGrid = kWayMerging.mergeKList(temp);
          vertexGeoReach.ReachGrids.set(i, targetReachGrid);
        }

        id++;
      }
      Util.print(String.format("%d-hop time: %d\n", i + 1, System.currentTimeMillis() - start));
      start = System.currentTimeMillis();
    }

    return index;
  }

  public static ArrayList<ArrayList<Integer>> generateTypeList(ArrayList<VertexGeoReach> index,
      int MAX_HOP, SpaceManager spaceManager, double MG, double MR, int MC) {
    return generateTypeList(index, MAX_HOP, spaceManager.getMinx(), spaceManager.getMiny(),
        spaceManager.getMaxx(), spaceManager.getMaxy(), spaceManager.getPiecesX(),
        spaceManager.getPiecesY(), MG, MR, MC);
  }

  /**
   * Generate index type for each vertex
   * 
   * @param index
   * @param MAX_HOP
   * @param minx
   * @param miny
   * @param maxx
   * @param maxy
   * @param pieces_x
   * @param pieces_y
   * @param MG
   * @param MR
   * @param MC
   * @return
   */
  public static ArrayList<ArrayList<Integer>> generateTypeList(ArrayList<VertexGeoReach> index,
      int MAX_HOP, double minx, double miny, double maxx, double maxy, int pieces_x, int pieces_y,
      double MG, double MR, int MC) {
    long start = System.currentTimeMillis();
    double total_area = (maxx - minx) * (maxy - miny);
    double resolution_x = (maxx - minx) / pieces_x;
    double resolution_y = (maxy - miny) / pieces_y;

    ArrayList<ArrayList<Integer>> typesList = new ArrayList<>();
    for (VertexGeoReach vertexGeoReach : index) {
      ArrayList<Integer> types = new ArrayList<>();
      for (int i = 0; i < MAX_HOP; i++) {
        TreeSet<Integer> reachgrid = vertexGeoReach.ReachGrids.get(i);
        MyRectangle rmbr = vertexGeoReach.RMBRs.get(i);

        int type = 0;
        if (reachgrid != null) {
          int idX_min = (int) ((rmbr.min_x - minx) / resolution_x);
          int idY_min = (int) ((rmbr.min_y - miny) / resolution_y);
          int idX_max = (int) ((rmbr.max_x - minx) / resolution_x);
          int idY_max = (int) ((rmbr.max_y - miny) / resolution_y);

          idX_min = Math.min(pieces_x - 1, idX_min);
          idY_min = Math.min(pieces_y - 1, idY_min);
          idX_max = Math.min(pieces_x - 1, idX_max);
          idY_max = Math.min(pieces_y - 1, idY_max);

          if (reachgrid.size() > (idX_max - idX_min + 1) * (idY_max - idY_min + 1) * MG) {
            type = 1;
            if (rmbr.area() > total_area * MR) {
              vertexGeoReach.GeoBs.set(i, true);
              type = 2;
            }
          }
        } else {
          type = 2;
          vertexGeoReach.GeoBs.set(i, false);
        }

        types.add(type);
      }
      typesList.add(types);
    }

    Util.print("\nConstruct types time: " + (System.currentTimeMillis() - start));

    ArrayList<ArrayList<Integer>> statis = new ArrayList<>(MAX_HOP);
    for (int i = 0; i < MAX_HOP; i++)
      statis.add(new ArrayList<>());
    for (int i = 0; i < typesList.size(); i++) {
      ArrayList<Integer> types = typesList.get(i);
      for (int j = 0; j < MAX_HOP; j++) {
        int type = types.get(j);
        statis.get(j).add(type);
      }
    }

    for (ArrayList<Integer> types : statis)
      Util.print(Util.histogram(types));

    return typesList;
  }

  public static ArrayList<ArrayList<Integer>> generateTypeListForList(
      ArrayList<VertexGeoReachList> index, int MAX_HOP, SpaceManager spaceManager, double MG,
      double MR, int MC) {
    return generateTypeListForList(index, MAX_HOP, spaceManager.getMinx(), spaceManager.getMiny(),
        spaceManager.getMaxx(), spaceManager.getMaxy(), spaceManager.getPiecesX(),
        spaceManager.getPiecesY(), MG, MR, MC);
  }

  /**
   * 
   * @param index
   * @param MAX_HOP
   * @param minx
   * @param miny
   * @param maxx
   * @param maxy
   * @param pieces_x
   * @param pieces_y
   * @param MG
   * @param MR
   * @param MC
   * @return
   */
  public static ArrayList<ArrayList<Integer>> generateTypeListForList(
      ArrayList<VertexGeoReachList> index, int MAX_HOP, double minx, double miny, double maxx,
      double maxy, int pieces_x, int pieces_y, double MG, double MR, int MC) {
    long start = System.currentTimeMillis();
    double total_area = (maxx - minx) * (maxy - miny);
    double resolution_x = (maxx - minx) / pieces_x;
    double resolution_y = (maxy - miny) / pieces_y;

    ArrayList<ArrayList<Integer>> typesList = new ArrayList<>();
    for (VertexGeoReachList vertexGeoReach : index) {
      ArrayList<Integer> types = new ArrayList<>();
      for (int i = 0; i < MAX_HOP; i++) {
        ArrayList<Integer> reachgrid = vertexGeoReach.ReachGrids.get(i);
        MyRectangle rmbr = vertexGeoReach.RMBRs.get(i);

        int type = 0;
        if (reachgrid != null) {
          int idX_min = (int) ((rmbr.min_x - minx) / resolution_x);
          int idY_min = (int) ((rmbr.min_y - miny) / resolution_y);
          int idX_max = (int) ((rmbr.max_x - minx) / resolution_x);
          int idY_max = (int) ((rmbr.max_y - miny) / resolution_y);

          idX_min = Math.min(pieces_x - 1, idX_min);
          idY_min = Math.min(pieces_y - 1, idY_min);
          idX_max = Math.min(pieces_x - 1, idX_max);
          idY_max = Math.min(pieces_y - 1, idY_max);

          if (reachgrid.size() > (idX_max - idX_min + 1) * (idY_max - idY_min + 1) * MG) {
            type = 1;
            if (rmbr.area() > total_area * MR) {
              vertexGeoReach.GeoBs.set(i, true);
              type = 2;
            }
          }
        } else {
          type = 2;
          vertexGeoReach.GeoBs.set(i, false);
        }

        types.add(type);
      }
      typesList.add(types);
    }

    Util.print("\nConstruct types time: " + (System.currentTimeMillis() - start));

    // Compute statis of types. Should be outside the function.
    // ArrayList<ArrayList<Integer>> statis = new ArrayList<>(MAX_HOP);
    // for (int i = 0; i < MAX_HOP; i++)
    // statis.add(new ArrayList<>());
    // for (int i = 0; i < typesList.size(); i++) {
    // ArrayList<Integer> types = typesList.get(i);
    // for (int j = 0; j < MAX_HOP; j++) {
    // int type = types.get(j);
    // statis.get(j).add(type);
    // }
    // }
    //
    // for (ArrayList<Integer> types : statis)
    // Util.Print(Util.histogram(types));

    return typesList;
  }

  /**
   * This part is the baseline for correctness proof
   */

  public void getReachbleVertices() {
    /**
     * setting up
     */
    int pieces_x = 128, pieces_y = 128;
    double resolution_x = (maxx - minx) / pieces_x;
    double resolution_y = (maxy - miny) / pieces_y;

    int startID = 0, hops = 1;
    if (graph == null)
      graph = Util.ReadGraph(graphPath);
    if (entities == null)
      entities = Util.ReadEntity(entityPath);

    HashSet<Integer> curList = new HashSet<>(), nextList = new HashSet<>();
    curList.add(startID);
    for (int i = 0; i < hops; i++) {
      Util.print("hop: " + (i + 1));
      for (int id : curList) {
        ArrayList<Integer> neighbors = graph.get(id);
        for (int neighborID : neighbors)
          nextList.add(neighborID);
      }

      Util.print(String.format("reachable vertices size:\t%d\t%s", nextList.size(), nextList));

      TreeSet<Integer> spatialVertices = new TreeSet<>();
      for (int id : nextList)
        if (entities.get(id).IsSpatial)
          spatialVertices.add(id);
      Util.print(String.format("reachable spatial vertices size:\t%d\t%s", spatialVertices.size(),
          spatialVertices));

      TreeSet<Integer> reachgrids = new TreeSet<>();
      for (int id : spatialVertices) {
        Entity entity = entities.get(id);
        int idX = (int) ((entity.lon - minx) / resolution_x);
        int idY = (int) ((entity.lat - miny) / resolution_y);
        idX = Math.min(pieces_x - 1, idX);
        idY = Math.min(pieces_y - 1, idY);
        int gridID = idX * pieces_y + idY;
        reachgrids.add(gridID);
      }
      Util.print(String.format("reachable grids size:\t%d\t%s", reachgrids.size(), reachgrids));

      curList = nextList;
      nextList = new HashSet<>();
    }
  }

  // public TreeSet<Integer> updateReachGrid(TreeSet<Integer> target, TreeSet<Integer> source)
  // {
  // if (source == null)
  // return;
  // else
  // {
  // if (target == null)
  // target = new TreeSet<>(source);
  // else
  // target.addAll(source);
  // }
  // }
  //
  // public void updateRMBR(MyRectangle target, MyRectangle source)
  // {
  // if (source == null)
  // return;
  // else
  // {
  // if (target == null)
  // target = new MyRectangle(source);
  // else
  // target.MBR(source);
  // }
  // }
}
