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
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Queue;
import java.util.Random;
import java.util.Set;
import org.neo4j.graphdb.ExecutionPlanDescription;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Node;
import org.roaringbitmap.RoaringBitmap;
import org.roaringbitmap.buffer.ImmutableRoaringBitmap;
import commons.EnumVariables.BoundaryLocationStatus;

public class Util {

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
    ArrayList<Entity> entities = Util.ReadEntity(entityPath);
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
   * write map to file
   * 
   * @param filename
   * @param app append or not
   * @param map
   */
  public static void WriteMap(String filename, boolean app, Map<Object, Object> map) {
    try {
      FileWriter fWriter = new FileWriter(filename, app);
      Set<Entry<Object, Object>> set = map.entrySet();
      Iterator<Entry<Object, Object>> iterator = set.iterator();
      while (iterator.hasNext()) {
        Entry<Object, Object> element = iterator.next();
        fWriter.write(
            String.format("%s,%s\n", element.getKey().toString(), element.getValue().toString()));
      }
      fWriter.close();
    } catch (Exception e) {
      // TODO: handle exception
    }
  }

  public static long Average(ArrayList<Long> arraylist) {
    if (arraylist.size() == 0)
      return -1;
    long sum = 0;
    for (long element : arraylist)
      sum += element;
    return sum / arraylist.size();
  }

  /**
   * This function has to be used in transaction.
   * 
   * @param databaseService
   * @param ids
   * @return
   */
  public static ArrayList<Node> getNodesByIDs(GraphDatabaseService databaseService,
      ArrayList<Long> ids) {
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

  public static ArrayList<MyRectangle> ReadQueryRectangle(String filepath) {
    ArrayList<MyRectangle> queryrectangles;
    queryrectangles = new ArrayList<MyRectangle>();
    BufferedReader reader = null;
    File file = null;
    try {
      file = new File(filepath);
      reader = new BufferedReader(new FileReader(file));
      String temp = null;
      while ((temp = reader.readLine()) != null) {
        if (temp.contains("%"))
          continue;
        String[] line_list = temp.split("\t");
        MyRectangle rect =
            new MyRectangle(Double.parseDouble(line_list[0]), Double.parseDouble(line_list[1]),
                Double.parseDouble(line_list[2]), Double.parseDouble(line_list[3]));
        queryrectangles.add(rect);
      }
      reader.close();
    } catch (Exception e) {
      e.printStackTrace();
    } finally {
      if (reader != null)
        try {
          reader.close();
        } catch (IOException e) {
          // TODO Auto-generated catch block
          e.printStackTrace();
        }
    }
    return queryrectangles;
  }

  public static void WriteFile(String filename, boolean app, String str) {
    try {
      FileWriter fw = new FileWriter(filename, app);
      fw.write(str);
      fw.close();
    } catch (IOException e) {
      e.printStackTrace();
    }
  }

  /**
   * read integer arraylist
   * 
   * @param path
   * @return
   */
  public static ArrayList<Integer> readIntegerArray(String path) {
    String line = null;
    ArrayList<Integer> arrayList = new ArrayList<Integer>();
    try {
      BufferedReader reader = new BufferedReader(new FileReader(new File(path)));
      while ((line = reader.readLine()) != null) {
        int x = Integer.parseInt(line);
        arrayList.add(x);
      }
      reader.close();
    } catch (Exception e) {
      // TODO: handle exception
      e.printStackTrace();
    }
    return arrayList;
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

  public static void WriteFile(String filename, boolean app, List<String> lines) {
    try {
      FileWriter fw = new FileWriter(filename, app);
      int i = 0;
      while (i < lines.size()) {
        fw.write(String.valueOf(lines.get(i)) + "\n");
        ++i;
      }
      fw.close();
    } catch (IOException e) {
      e.printStackTrace();
    }
  }

  public static void WriteFile(String filename, boolean app, Set<String> lines) {
    try {
      FileWriter fw = new FileWriter(filename, app);
      for (String line : lines)
        fw.write(line + "\n");
      fw.close();
    } catch (IOException e) {
      e.printStackTrace();
    }
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

  /**
   * Read map from file
   * 
   * @param filename
   * @return
   */
  public static HashMap<String, String> ReadMap(String filename) {
    try {
      HashMap<String, String> map = new HashMap<String, String>();
      BufferedReader reader = new BufferedReader(new FileReader(new File(filename)));
      String line = null;
      while ((line = reader.readLine()) != null) {
        String[] liStrings = line.split(",");
        map.put(liStrings[0], liStrings[1]);
      }
      reader.close();
      return map;
    } catch (Exception e) {
      // TODO: handle exception
      e.printStackTrace();
    }
    Util.println("nothing in ReadMap(" + filename + ")");
    return null;
  }

  /**
   * Read a map to a long[] array. So the key has to be in the [0, count-1].
   *
   * @param filename
   * @return
   */
  public static long[] readMapToArray(String filename) {
    HashMap<String, String> graph_pos_map = Util.ReadMap(filename);
    long[] graph_pos_map_list = new long[graph_pos_map.size()];
    for (String key_str : graph_pos_map.keySet()) {
      int key = Integer.parseInt(key_str);
      int pos_id = Integer.parseInt(graph_pos_map.get(key_str));
      graph_pos_map_list[key] = pos_id;
    }
    return graph_pos_map_list;
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

  /**
   * Read entities
   * 
   * @param entity_path
   * @return
   */
  public static ArrayList<Entity> ReadEntity(String entity_path) {
    ArrayList<Entity> entities = null;
    BufferedReader reader = null;
    String str = null;
    int id = 0;
    try {
      if (!Util.pathExist(entity_path))
        throw new Exception(entity_path + " does not exist");

      reader = new BufferedReader(new FileReader(new File(entity_path)));
      str = reader.readLine();
      int node_count = Integer.parseInt(str);
      entities = new ArrayList<Entity>(node_count);
      while ((str = reader.readLine()) != null) {
        Entity entity;
        String[] str_l = str.split(",");
        int flag = Integer.parseInt(str_l[1]);
        if (flag == 0) {
          entity = new Entity(id);
          entities.add(entity);
        } else {
          entity = new Entity(id, Double.parseDouble(str_l[2]), Double.parseDouble(str_l[3]));
          entities.add(entity);
        }
        ++id;
      }
      reader.close();
    } catch (Exception e) {
      Util.println(String.format("error happens in entity id %d", id));
      e.printStackTrace();
      System.exit(-1);
    }
    return entities;
  }
}
