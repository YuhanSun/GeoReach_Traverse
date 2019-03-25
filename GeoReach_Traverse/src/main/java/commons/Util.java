package commons;

import java.io.BufferedReader;
import java.io.DataOutputStream;
import java.io.File;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.OutputStream;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Base64;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Queue;
import java.util.Random;
import java.util.logging.Logger;
import org.neo4j.graphdb.ExecutionPlanDescription;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Node;
import org.neo4j.unsafe.batchinsert.BatchInserter;
import org.roaringbitmap.RoaringBitmap;
import org.roaringbitmap.buffer.ImmutableRoaringBitmap;
import commons.EnumVariables.BoundaryLocationStatus;

public class Util {

  private static final Logger LOGGER = Logger.getLogger(Util.class.getName());

  public static void close(BufferedReader reader) throws Exception {
    if (reader != null) {
      reader.close();
    }
  }

  public static void close(FileWriter writer) throws Exception {
    if (writer != null) {
      writer.close();
    }
  }

  public static void close(BatchInserter inserter) {
    LOGGER.info("shut down batch inserter...");
    if (inserter != null) {
      inserter.shutdown();
    }
    LOGGER.info("shut down is done.");
  }

  public static void extendBoundary(int[] boundary, int[] xyId) {
    extendBoundary(boundary, xyId[0], xyId[1]);
  }

  public static void extendBoundary(int[] boundary, int idX, int idY) {
    boundary[0] = Math.min(boundary[0], idX);
    boundary[1] = Math.min(boundary[1], idY);
    boundary[2] = Math.max(boundary[2], idX);
    boundary[3] = Math.max(boundary[3], idY);
  }

  /**
   * Detect location of the [idX, idY] w.r.t. the boundary.
   *
   * @param boundary
   * @param xyId
   * @return
   */
  public static BoundaryLocationStatus locate(int[] boundary, int[] xyId) {
    return locate(boundary, xyId[0], xyId[1]);
  }

  /**
   * Detect location of the [idX, idY] w.r.t. the boundary.
   *
   * @param boundary
   * @param idX
   * @param idY
   * @return
   */
  public static BoundaryLocationStatus locate(int[] boundary, int idX, int idY) {
    if (idX < boundary[0] || idY < boundary[1] || idX > boundary[2] || idY > boundary[3]) {
      return BoundaryLocationStatus.OUTSIDE;
    } else {
      if (idX == boundary[0] || idY == boundary[1] || idX == boundary[2] || idY == boundary[3]) {
        return BoundaryLocationStatus.ONBOUNDARY;
      } else {
        return BoundaryLocationStatus.INSIDE;
      }
    }
  }

  public static void println(Object o) {
    System.out.println(o);
  }

  public static boolean compareFile(String file1, String file2) {
    BufferedReader reader1 = null;
    BufferedReader reader2 = null;
    try {
      reader1 = new BufferedReader(new FileReader(new File(file1)));
      reader2 = new BufferedReader(new FileReader(new File(file2)));
      String line1 = "";
      String line2 = "";
      while (true) {
        line1 = reader1.readLine();
        line2 = reader2.readLine();

        if (line1 == null || line2 == null)
          break;

        if (line1.equals(line2))
          continue;
        else {
          Util.println(line1);
          Util.println(line2);
          return false;
        }
      }
      if (line1 != null || line2 != null) {
        Util.println("line count different!");
        return false;
      }
      reader1.close();
      reader2.close();
      return true;
    } catch (Exception e) {
      e.printStackTrace();
    }
    return false;
  }

  /**
   * Get the boundary of all the entities.
   * 
   * @param entities
   * @return
   */
  public static MyRectangle GetEntityRange(ArrayList<Entity> entities) {
    Entity p_Entity;
    MyRectangle range = null;
    int i = 0;
    while (i < entities.size()) {
      p_Entity = entities.get(i);
      if (p_Entity.IsSpatial) {
        range = new MyRectangle(p_Entity.lon, p_Entity.lat, p_Entity.lon, p_Entity.lat);
        break;
      }
      ++i;
    }
    while (i < entities.size()) {
      p_Entity = entities.get(i);
      if (p_Entity.lon < range.min_x) {
        range.min_x = p_Entity.lon;
      }
      if (p_Entity.lat < range.min_y) {
        range.min_y = p_Entity.lat;
      }
      if (p_Entity.lon > range.max_x) {
        range.max_x = p_Entity.lon;
      }
      if (p_Entity.lat > range.max_y) {
        range.max_y = p_Entity.lat;
      }
      ++i;
    }
    return range;
  }

  /**
   * Generate label.txt file based on entity file The label.txt has only 0 and 1 label where 0 is
   * non-spatial and 1 is spatial.
   * 
   * @param entityPath
   * @param labelListPath
   */
  public static void getLabelListFromEntity(String entityPath, String labelListPath) {
    ArrayList<Entity> entities = GraphUtil.ReadEntity(entityPath);
    ArrayList<Integer> labelList = new ArrayList<Integer>(entities.size());
    for (Entity entity : entities) {
      if (entity.IsSpatial)
        labelList.add(1);
      else
        labelList.add(0);
    }
    Util.println("Write label list to: " + labelListPath);

    ArrayList<String> labelListString = new ArrayList<>(labelList.size());
    for (int label : labelList)
      labelListString.add(String.valueOf(label));

    ReadWriteUtil.WriteArray(labelListPath, labelListString);
  }

  /**
   * This function has to be used in transaction.
   * 
   * @param databaseService
   * @param ids
   * @return
   */
  public static ArrayList<Node> getNodesByIDs(GraphDatabaseService databaseService,
      List<Long> ids) {
    ArrayList<Node> nodes = new ArrayList<>();
    for (long id : ids)
      nodes.add(databaseService.getNodeById(id));
    return nodes;
  }

  public static String clearAndSleep(String password, int sleepTime) {
    String res = clearCache(password);
    Thread.currentThread();
    try {
      Thread.sleep(sleepTime);
    } catch (InterruptedException e) {
      e.printStackTrace();
    }
    return res;
  }

  public static String clearCache(String password) {
    String[] cmd = new String[] {"/bin/bash", "-c",
        "echo " + password + " | sudo -S sh -c \"sync; echo 3 > /proc/sys/vm/drop_caches\""};
    String result = null;
    try {
      String line;
      Process process = Runtime.getRuntime().exec(cmd);
      process.waitFor();
      BufferedReader br = new BufferedReader(new InputStreamReader(process.getInputStream()));
      StringBuffer sb = new StringBuffer();
      while ((line = br.readLine()) != null) {
        sb.append(line).append("\n");
      }
      result = sb.toString();
      result = String.valueOf(result) + "\n";
    } catch (Exception e) {
      e.printStackTrace();
    }
    return result;
  }

  /**
   * Get db hits. Directly use getDBHits will just get the hits of the final step. It is not
   * feasible.
   * 
   * @param plan
   * @return
   */
  public static long GetTotalDBHits(ExecutionPlanDescription plan) {
    long dbhits = 0;
    Queue<ExecutionPlanDescription> queue = new LinkedList<ExecutionPlanDescription>();
    if (plan.hasProfilerStatistics())
      queue.add(plan);
    while (queue.isEmpty() == false) {
      ExecutionPlanDescription planDescription = queue.poll();
      dbhits += planDescription.getProfilerStatistics().getDbHits();
      for (ExecutionPlanDescription planDescription2 : planDescription.getChildren())
        queue.add(planDescription2);
    }
    return dbhits;
  }

  public static boolean pathExist(String path) {
    File file = new File(path);

    if (file.exists())
      return true;
    else
      return false;
  }

  public static boolean intersect(MyRectangle rect1, MyRectangle rect2) {
    if (rect1.min_x > rect2.max_x || rect1.min_y > rect2.max_y || rect1.max_x < rect2.min_x
        || rect1.max_y < rect2.min_y)
      return false;
    else
      return true;
  }

  public static int GetSpatialEntityCount(ArrayList<Entity> entities) {
    int count = 0;
    for (Entity entity : entities)
      if (entity.IsSpatial)
        count++;
    return count;
  }

  /**
   * Generate a set of random values for a given range.
   * 
   * @param graph_size [0, graph_size - 1]
   * @param node_count the wanted set size
   * @return a set of values
   */
  public static HashSet<Long> GenerateRandomInteger(long graph_size, int node_count) {
    HashSet<Long> ids = new HashSet<Long>();
    Random random = new Random();
    while (ids.size() < node_count) {
      Long id = (long) (random.nextDouble() * (double) graph_size);
      ids.add(id);
    }
    return ids;
  }

  /**
   * Get node count from a graph file. The graph has to be the correct format.
   * 
   * @param filepath
   * @return
   */
  public static int GetNodeCountGeneral(String filepath) {
    int node_count = 0;
    File file = null;
    BufferedReader reader = null;
    try {
      try {
        file = new File(filepath);
        reader = new BufferedReader(new FileReader(file));
        String str = reader.readLine();
        String[] l = str.split(" ");
        node_count = Integer.parseInt(l[0]);
      } catch (Exception e) {
        e.printStackTrace();
        try {
          reader.close();
        } catch (IOException e1) {
          e1.printStackTrace();
        }
      }
    } finally {
      try {
        reader.close();
      } catch (IOException e) {
        e.printStackTrace();
      }
    }
    return node_count;
  }

  public static ImmutableRoaringBitmap getImmutableRoaringBitmap(String string) {
    ByteBuffer newbb = ByteBuffer.wrap(Base64.getDecoder().decode(string));
    ImmutableRoaringBitmap immutableRoaringBitmap = new ImmutableRoaringBitmap(newbb);
    return immutableRoaringBitmap;
  }

  public static String roarBitmapSerializeToString(RoaringBitmap r) {
    r.runOptimize();
    ByteBuffer outbb = ByteBuffer.allocate(r.serializedSizeInBytes());
    try {
      r.serialize(new DataOutputStream(new OutputStream() {
        ByteBuffer mBB;

        OutputStream init(ByteBuffer mbb) {
          mBB = mbb;
          return this;
        }

        public void close() {}

        public void flush() {}

        public void write(int b) {
          mBB.put((byte) b);
        }

        public void write(byte[] b) {
          mBB.put(b);
        }

        public void write(byte[] b, int off, int l) {
          mBB.put(b, off, l);
        }
      }.init(outbb)));
    } catch (IOException e) {
      e.printStackTrace();
    }
    outbb.flip();
    String serializedstring = Base64.getEncoder().encodeToString(outbb.array());
    return serializedstring;
  }

  public static HashMap<Integer, Integer> histogram(List<Integer> list) {
    HashMap<Integer, Integer> res = new HashMap<>();
    for (int element : list) {
      if (res.containsKey(element))
        res.put(element, res.get(element) + 1);
      else
        res.put(element, 1);
    }
    return res;
  }


  /**
   * Decide whether rect1 is located in rect2.
   *
   * @param rect1
   * @param rect2
   * @return
   */
  public static boolean rectLocatedInRect(MyRectangle rect1, MyRectangle rect2) {
    if (rect1 == null) {
      return true;
    }
    if (Location_In_Rect(rect1.min_x, rect1.min_y, rect2) == false
        || Location_In_Rect(rect1.max_x, rect1.max_y, rect2) == false) {
      return false;
    }
    return true;
  }

  /**
   * Decide whether a location is located within a rectangle
   * 
   * @param lon
   * @param lat
   * @param rect
   * @return
   */
  public static boolean Location_In_Rect(double lon, double lat, MyRectangle rect) {
    if (lat < rect.min_y || lat > rect.max_y || lon < rect.min_x || lon > rect.max_x) {
      return false;
    }
    return true;
  }
}
