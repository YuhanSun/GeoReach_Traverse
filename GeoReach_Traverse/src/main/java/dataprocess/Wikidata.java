package dataprocess;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.TreeSet;
import java.util.logging.Level;
import java.util.logging.Logger;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import org.json.simple.JSONArray;
import org.json.simple.JSONObject;
import org.json.simple.parser.JSONParser;
import com.google.gson.JsonObject;
import commons.Entity;
import commons.GraphUtil;
import commons.ReadWriteUtil;
import commons.Util;

public class Wikidata {

  /**
   * entity with Q-start
   */
  private final static Pattern entityPattern =
      Pattern.compile("<http://www.wikidata.org/entity/Q(\\d+)>");

  /**
   * property treated as entity in the subject.
   */
  private final static Pattern propertyEntityPattern =
      Pattern.compile("<http://www.wikidata.org/entity/P(\\d+)>");

  /**
   * property treated as predicate
   */
  private final static Pattern propertyPredicatePattern =
      Pattern.compile("<http://www.wikidata.org/prop/direct/P(\\d+)>");

  // private final static Pattern patternContainLanguageMark =
  // Pattern.compile("\\\"(.*?)\\\"@\\S+(-*)(\\S*)");

  private final static String labelStr = "rdf-schema#label";
  private final static String labelPropertyName = "name";
  private final static String descriptionStr = "<http://schema.org/description>";
  private final static String descriptionPropertyName = "description";

  private final static String enStr = "@en";
  private final static String instanceOfStr = "<http://www.wikidata.org/prop/direct/P31>";
  private final static String coordinateStr = "<http://www.wikidata.org/prop/direct/P625>";

  private static final Logger LOGGER = Logger.getLogger(Wikidata.class.getName());
  private static Level loggingLevel = Level.ALL;

  // for test
  // static String dir = "D:/Project_Data/wikidata-20180308-truthy-BETA.nt";
  // static String fullfilePath = dir + "/slice_100000.nt";

  static String dir = "/hdd/code/yuhansun/data/wikidata";
  static String fullfilePath = dir + "/wikidata-20180308-truthy-BETA.nt";
  static String sliceDataPath = dir + "/slice_100000.nt";
  static String logPath = dir + "/extract.log";
  static String locationPath = dir + "/locations.txt";
  static String entityMapPath = dir + "/entity_map.txt";
  static String graphPath = dir + "/graph.txt";
  static String entityPath = dir + "/entity.txt";

  static String propertiesJsonFile = dir + "/properties_from_query.json";
  static String propertyMapPath = dir + "/property_map.txt";
  static String edgePath = dir + "/graph_edges.txt";
  static String entityPropertiesPath = dir + "/entity_properties.txt";
  static String entityStringLabelMapPath = dir + "/entity_string_label.txt";


  public static void main(String[] args) throws Exception {
    // TODO Auto-generated method stub
    // extract();

    // extractEntityMap();
    // extractEntityToEntityRelation();
    // checkGraphVerticesCount();
    // generateEntityFile();

    // readGraphTest();

    // //test code
    // String string = "<http://www.wikidata.org/entity/Q26>";
    // Util.Print(isEntity(string));
    // Util.Print(getEntityID(string));
    // //test code
    // String string = "<https://www.wikidata.org/wiki/Property:P1151>";
    // Util.Print(isProperty(string));
    // Util.Print(getPropertyID(string));


    // getRange();
    // checkLocation();
    // findEntitiesNotOnEarth();
    // removeLocationOutOfEarth();
    // removeLocationOutOfBound();
    // getEdgeCount();

    // extractPropertyID();

    // getLabelCount();
    // extractLabels();
    extractProperties();

    // extractPropertyLabelMap();

    // convert data for GraphFrame
    // GraphUtil.convertGraphToEdgeFormat(graphPath, edgePath);
    // GraphUtil.extractSpatialEntities(entityPath, dir + "/entity_spatial.txt");

    // edgeCountCheck();
  }


  /**
   * Check the number of edges in graph.txt and edges.txt.
   *
   * @throws Exception
   */
  public static void edgeCountCheck() throws Exception {
    ArrayList<ArrayList<Integer>> graph = GraphUtil.ReadGraph(graphPath);
    LOGGER.log(loggingLevel, "Edge count in graph: {0}", GraphUtil.getEdgeCount(graph));
    LOGGER.log(loggingLevel, "Edge count in edges file: {0}",
        Files.lines(Paths.get(edgePath)).count());
  }

  public static void checkPropertyEntityID() {
    HashMap<Long, Integer> idMap = readMap(entityMapPath);
    ArrayList<Integer> propertySet = ReadWriteUtil.readIntegerArray(dir + "\\propertyID.txt");
    int count = 0;
    for (int id : propertySet) {
      if (idMap.containsKey((long) id)) {
        Util.println(String.format("%d,%d", id, idMap.get((long) id)));
        count++;
      }
    }
    Util.println("count: " + count);
  }

  /**
   * Extract all node properties from the source file.
   *
   * @throws Exception
   */
  public static void extractProperties() throws Exception {
    Map<Integer, String> propertyMap = readPropertyMap(propertyMapPath);
    BufferedReader reader = new BufferedReader(new FileReader(fullfilePath));
    FileWriter writer = new FileWriter(entityPropertiesPath);
    String line = null;
    long entityId = -1;
    JsonObject properties = null;
    while ((line = reader.readLine()) != null) {
      String[] spo = decodeRow(line);
      if (!isQEntity(spo[0])) {
        continue;
      }
      long curEntityId = getQEntityID(spo[0]);
      if (curEntityId != entityId) {
        // entityId = -1, initialize the properties for the first entity.
        if (properties == null) {
          properties = new JsonObject();
        } else {
          // output the properties as json format for this entity.
          properties.addProperty("id", entityId);
          writer.write(properties.toString() + "\n");
        }
        entityId = curEntityId;
      }

      String predicate = spo[1];
      String object = spo[2];
      // extract the label and description in language English.
      if (object.endsWith(enStr)) {
        if (predicate.contains(labelStr)) {
          properties.addProperty(labelPropertyName, object);
        } else if (predicate.equals(descriptionStr)) {
          properties.addProperty(descriptionPropertyName, object);
        }
      } else if (isQEntity(object)) {
        // skip the entity-to-entity edges.
        continue;
      } else if (isPropertyPredicate(predicate)) {

        if (!(object.endsWith("\"") && object.startsWith("\""))) {
          continue;
        }

        object = object.substring(1, object.length() - 1);

        // only extract the rows with existing property predicate.
        int propertyId = getPropertyPredicateID(predicate);
        LOGGER.log(loggingLevel, propertyId + "");
        String propertyName = propertyMap.get(propertyId);
        // the propertyId does not exist in latest properties ids.
        if (propertyName == null) {
          continue;
        }
        if (properties.has(propertyName)) {
          properties.remove(propertyName);
        }
        properties.addProperty(propertyName, object);
      }
    }

    if (properties.size() > 0) {
      properties.addProperty("id", entityId);
      writer.write(properties.toString() + "\n");
    }

    reader.close();
    writer.close();
  }

  /**
   * Extract all the string labels for all Q entities.
   * 
   * @throws Exception
   */
  public static void extractStringLabels() throws Exception {
    Map<Integer, Long> entityIdMap = readEntityIdMap(entityMapPath);
    // get the reversed map from <graphid, Qid> to generate the map <Qid, graphid>.
    Map<Long, Integer> map = new HashMap<>();
    for (int key : entityIdMap.keySet()) {
      map.put(entityIdMap.get(key), key);
    }

    BufferedReader reader = new BufferedReader(new FileReader(fullfilePath));
    FileWriter writer = new FileWriter(entityPropertiesPath);
    String line = null;
    while ((line = reader.readLine()) != null) {
      String[] spo = decodeRow(line);
      if (!isQEntity(spo[0])) {
        continue;
      }

      String predicate = spo[1];
      String object = spo[2];
      // extract the label and description in language English.
      if (object.endsWith(enStr) && predicate.contains(labelStr)) {
        object = object.substring(1, object.length() - 4);
        long curEntityId = getQEntityID(spo[0]);
        writer.write(String.format("%d,%s", map.get(curEntityId), object));
      }
    }
    reader.close();
    writer.close();
  }

  /**
   * Extract the map <id, label> from properties.json. Since the json file is not up-to-date, I
   * change the code extractProperty() to get the correct result.
   *
   * @throws Exception
   */
  public static void extractPropertyLabelMap() throws Exception {
    FileWriter writer = new FileWriter(propertyMapPath);
    JSONParser parser = new JSONParser();
    LOGGER.log(loggingLevel, "read from " + propertiesJsonFile);
    JSONArray jsonArray = (JSONArray) parser.parse(new FileReader(propertiesJsonFile));
    for (Object object : jsonArray) {
      JSONObject propertyMap = (JSONObject) object;
      LOGGER.log(loggingLevel, propertyMap.toString());
      String property = propertyMap.get("property").toString();
      String label = propertyMap.get("propertyLabel").toString();
      int propertyId =
          Integer.parseInt(property.replaceAll("http://www.wikidata.org/entity/P", ""));
      writer.write(String.format("%d,%s\n", propertyId, label));
    }
    writer.close();
  }

  /**
   * Extract all property id.
   */
  public static void extractPropertyID() {
    BufferedReader reader = null;
    String line = "";
    HashSet<Long> idSet = new HashSet<>();
    int lineIndex = 0;
    try {
      reader = new BufferedReader(new FileReader(new File(fullfilePath)));
      while ((line = reader.readLine()) != null) {
        lineIndex++;
        String[] strings = line.split(" ");
        String subject = strings[0];
        long id = getPropertySubjectID(subject);
        if (id != -1)
          idSet.add(id);

        String object = strings[2];
        id = getPropertySubjectID(object);
        if (id != -1)
          idSet.add(id);

        if (lineIndex % 10000000 == 0)
          Util.println(lineIndex);
      }
      reader.close();
      ArrayList<String> output = new ArrayList<>(idSet.size());
      for (long id : idSet)
        output.add(String.valueOf(id));

      ReadWriteUtil.WriteArray(dir + "\\propertyID.txt", output);
    } catch (Exception e) {
      // TODO: handle exception
      e.printStackTrace();
    }
  }

  public static void extractLabelMap() {

  }

  /**
   * Extract labels of format <graphID, list of labels>.
   */
  public static void extractEntityLabels() {
    BufferedReader reader = null;
    String line = "";
    HashMap<Long, Integer> idMap = readMap(entityMapPath);
    TreeSet<Integer> hasLabelVertices = new TreeSet<>();
    HashMap<Integer, TreeSet<Integer>> labels = new HashMap<>();
    int count = 0;
    try {
      reader = new BufferedReader(new FileReader(new File(fullfilePath)));
      while ((line = reader.readLine()) != null) {
        String[] strList = decodeRow(line);
        String predicate = strList[1];
        if (predicate.equals(instanceOfStr)) {
          count++;
          String subject = strList[0];
          String object = strList[2];

          if (!isQEntity(subject) || !isQEntity(object))
            continue;

          int graphID = idMap.get(getQEntityID(subject));
          hasLabelVertices.add(graphID);

          // labelId is the mapped id of the entity.
          int labelID = idMap.get(getQEntityID(object));
          if (!labels.containsKey(graphID))
            labels.put(graphID, new TreeSet<>());
          labels.get(graphID).add(labelID);

          if (count % 1000000 == 0)
            Util.println(count);
        }
      }

      // String filePath = dir + "\\hasLabelVertices.txt";
      // ArrayList<String> outputArray = new ArrayList<>(hasLabelVertices.size());
      // for (int id : hasLabelVertices)
      // outputArray.add(String.valueOf(id));
      // ReadWriteUtil.WriteArray(filePath, outputArray);

      Util.println(labels);
      String filePath = dir + "/graph_label.txt";
      FileWriter writer = new FileWriter(filePath);
      FileWriter logwriter = new FileWriter(logPath, true);
      writer.write(labels.size() + "\n");
      for (int key = 0; key < idMap.size(); key++) {
        TreeSet<Integer> keyLabels = labels.get(key);
        if (keyLabels == null) {
          logwriter.write(String.format("%d does not have label\n", key));
          writer.write(String.format("%d,0\n", key));
          continue;
        }
        writer.write(String.format("%d,%d", key, keyLabels.size()));
        for (int id : keyLabels)
          writer.write(String.format(",%d", id));
        writer.write("\n");
      }
      writer.close();
      logwriter.close();

    } catch (Exception e) {
      Util.println(line);
      e.printStackTrace();
    }
  }

  /**
   * Extract labels of format <labelID, set of graphIds>.
   */
  public static void getLabelCount() {
    BufferedReader reader = null;
    String line = "";
    HashMap<Long, Integer> idMap = readMap(entityMapPath);
    TreeSet<Integer> hasLabelVertices = new TreeSet<>();
    HashMap<Integer, TreeSet<Integer>> labels = new HashMap<>();
    int count = 0;
    try {
      reader = new BufferedReader(new FileReader(new File(fullfilePath)));
      while ((line = reader.readLine()) != null) {
        String[] strList = line.split(" ");
        String predicate = strList[1];
        if (predicate.equals(instanceOfStr)) {
          count++;
          String subject = strList[0];
          String object = strList[2];

          if (!isQEntity(subject) || !isQEntity(object))
            continue;

          int graphID = idMap.get(getQEntityID(subject));
          hasLabelVertices.add(graphID);

          int labelID = idMap.get(getQEntityID(object));
          if (!labels.containsKey(labelID))
            labels.put(labelID, new TreeSet<>());
          labels.get(labelID).add(graphID);

          if (count % 1000000 == 0)
            Util.println(count);
        }
      }

      String filePath = dir + "\\hasLabelVertices.txt";
      ArrayList<String> outputArray = new ArrayList<>(hasLabelVertices.size());
      for (int id : hasLabelVertices)
        outputArray.add(String.valueOf(id));
      ReadWriteUtil.WriteArray(filePath, outputArray);

      filePath = dir + "\\labels.txt";
      FileWriter writer = new FileWriter(new File(filePath));
      writer.write(labels.size() + "\n");
      for (int key : labels.keySet()) {
        TreeSet<Integer> verticesID = labels.get(key);
        writer.write(String.format("%d,%d", key, verticesID.size()));
        for (int id : verticesID)
          writer.write(String.format(",%d", id));
        writer.write("\n");
      }
      writer.close();

    } catch (Exception e) {
      Util.println(line);
      e.printStackTrace();
    }
  }

  public static void getEdgeCount() {
    ArrayList<ArrayList<Integer>> graph = GraphUtil.ReadGraph(graphPath);
    int edgeCount = 0;
    for (ArrayList<Integer> neighbors : graph)
      edgeCount += neighbors.size();
    Util.println(edgeCount);
  }

  public static void removeLocationOutOfBound() {
    ArrayList<Entity> entities = GraphUtil.ReadEntity(entityPath);
    int count = 0;
    for (Entity entity : entities) {
      if (entity.IsSpatial) {
        if (entity.lon < -180 || entity.lon > 180 || entity.lat < -90 || entity.lat > 90) {
          count++;
          entity.IsSpatial = false;
          entity.lon = 0;
          entity.lat = 0;
        }
      }
    }
    Util.println(count);
    GraphUtil.writeEntityToFile(entities, entityPath);
  }

  public static void removeLocationOutOfEarth() {
    BufferedReader reader = null;
    HashMap<Long, Integer> idMap = readMap(entityMapPath);
    ArrayList<Entity> entities = GraphUtil.ReadEntity(entityPath);
    String outearthPath = dir + "\\outofearth_local.csv";
    String line = "";
    try {
      reader = new BufferedReader(new FileReader(new File(outearthPath)));
      while ((line = reader.readLine()) != null) {
        // String[] strList = line.split(",");
        // long wikiID = getEntityID(strList[0]);

        long wikiID = Long.parseLong(line);

        // if (!idMap.containsKey(wikiID))
        // continue;

        int graphID = idMap.get(wikiID);
        Entity entity = entities.get(graphID);
        entity.IsSpatial = false;
        entity.lon = 0;
        entity.lat = 0;
      }
      GraphUtil.writeEntityToFile(entities, dir + "\\new_entity.txt");
    } catch (Exception e) {
      // TODO: handle exception
      Util.println(line);
      e.printStackTrace();
    }
  }

  public static void findEntitiesNotOnEarth() {
    String outputPath = dir + "\\outofearth_local.csv";
    FileWriter writer = null;
    BufferedReader reader = null;
    String line = "";
    int predicateCount = 0;
    try {
      writer = new FileWriter(new File(outputPath));
      reader = new BufferedReader(new FileReader(new File(fullfilePath)));
      while ((line = reader.readLine()) != null) {
        String[] strList = line.split(" ");
        String predicate = strList[1];
        if (predicate.matches(propertyPredicatePattern.pattern())) {
          predicateCount++;
          long propertyID = getPropertyPredicateID(predicate);
          if (propertyID == 376) {
            String object = strList[2];
            if (object.matches(entityPattern.pattern())) {
              long planetID = getQEntityID(object);
              if (planetID != 2) {
                String subject = strList[0];
                if (isQEntity(subject)) {
                  long subjectWikiID = getQEntityID(subject);
                  writer.write(subjectWikiID + "\n");
                }
              }
            }
          }
          if (predicateCount % 10000000 == 0)
            Util.println(predicateCount);
        }
      }
      reader.close();
      writer.close();
    } catch (Exception e) {
      e.printStackTrace();
      System.exit(-1);
    }
  }

  /**
   * Find some entities are not on earth.
   */
  public static void checkLocation() {
    ArrayList<Entity> entities = GraphUtil.ReadEntity(entityPath);
    HashMap<String, String> map = ReadWriteUtil.ReadMap(entityMapPath);
    int count = 0;
    for (Entity entity : entities) {
      if (entity.IsSpatial) {
        if (entity.lon < -180 || entity.lon > 180 || entity.lat < -90 || entity.lat > 90) {
          Util.println(entity + " " + map.get("" + entity.id));
          count++;
        }
      }
    }
    Util.println(count);
  }

  public static void getRange() {
    ArrayList<Entity> entities = GraphUtil.ReadEntity(entityPath);
    // ArrayList<Entity> entities = Util.ReadEntity(dir + "\\new_entity.txt");
    Util.println(Util.GetEntityRange(entities));
  }

  public static void readGraphTest() {
    ArrayList<ArrayList<Integer>> graph = GraphUtil.ReadGraph(graphPath);
  }

  /**
   * Check graph file first line and real number of vertices
   */
  public static void checkGraphVerticesCount() {
    BufferedReader reader = null;
    try {
      reader = new BufferedReader(new FileReader(new File(graphPath)));
      String line = reader.readLine();
      int nodeCount = Integer.parseInt(line);
      Util.println(nodeCount);

      int index = 0;

      while ((line = reader.readLine()) != null) {
        index++;
      }
      reader.close();
      Util.println(index);
    } catch (Exception e) {
      // TODO: handle exception
      e.printStackTrace();
    }
  }

  public static void extractEntityMap() {
    BufferedReader reader;
    FileWriter writer;
    FileWriter logWriter;
    String line = "";
    long lineIndex = 0;

    HashSet<Long> startIdSet = new HashSet<>();
    ArrayList<Long> map = new ArrayList<>(95000000);
    HashSet<Long> leafVerticesSet = new HashSet<>();

    try {
      reader = new BufferedReader(new FileReader(new File(fullfilePath)));
      writer = new FileWriter(entityMapPath);
      logWriter = new FileWriter(logPath);

      long curWikiID = -1;

      while ((line = reader.readLine()) != null) {
        String[] strList = line.split(" ");
        String subject = strList[0];

        if (subject.matches(entityPattern.pattern())) {
          long startID = getQEntityID(subject);
          if (startID != curWikiID) {
            if (startIdSet.contains(startID)) {
              throw new Exception(startID + "already exists before here!");
            } else {
              map.add(startID);
              startIdSet.add(startID);
              leafVerticesSet.remove(startID);
              // Util.Print(startID);
            }
            curWikiID = startID;
          }
        }

        String object = strList[2];
        if (object.matches(entityPattern.pattern())) {
          long endID = getQEntityID(object);
          if (startIdSet.contains(endID) == false)
            leafVerticesSet.add(endID);
        }

        lineIndex++;
        if (lineIndex % 10000000 == 0)
          Util.println(lineIndex);

        if (lineIndex == 10000000)
          break;
      }

      Util.println("leaf count: " + leafVerticesSet.size());
      for (long key : leafVerticesSet)
        map.add(key);

      lineIndex = 0;
      for (long id : map) {
        writer.write(String.format("%d,%d\n", lineIndex, id));
        lineIndex++;
      }

      reader.close();
      writer.close();
      logWriter.close();

    } catch (Exception e) {
      Util.println(String.format("line %d:\n%s", lineIndex, line));
      e.printStackTrace();
    }
  }

  /**
   * Generate the graph.txt file (single directional).
   */
  public static void extractEntityToEntityRelation() {
    BufferedReader reader;
    FileWriter writer;
    FileWriter logWriter;
    String line = "";
    long lineIndex = 0;

    try {
      HashMap<Long, Integer> idMap = readMap(entityMapPath);
      Util.println(idMap.size());

      reader = new BufferedReader(new FileReader(new File(fullfilePath)));
      writer = new FileWriter(graphPath);
      logWriter = new FileWriter(logPath);

      writer.write(idMap.size() + "\n");

      long curWikiID = 26;
      TreeSet<Integer> neighbors = new TreeSet<>();
      // Process node by node. Assume that all the spo for the same node are clustered rather than
      // interleaved.
      while ((line = reader.readLine()) != null) {
        String[] strList = line.split(" ");
        String subject = strList[0];

        if (subject.matches(entityPattern.pattern())) {
          long startID = getQEntityID(subject);
          if (curWikiID != startID) {
            writer.write(String.format("%d,%d", idMap.get(curWikiID), neighbors.size()));
            for (int neighbor : neighbors)
              writer.write("," + neighbor);
            writer.write("\n");
            neighbors = new TreeSet<>();
            curWikiID = startID;
          }

          String object = strList[2];
          if (object.matches(entityPattern.pattern())) {
            long endID = getQEntityID(object);
            int graphID = idMap.get(endID);
            neighbors.add(graphID);
          }
        }

        lineIndex++;
        if (lineIndex % 10000000 == 0)
          Util.println(lineIndex);

        // if (lineIndex == 10000000)
        // break;
      }
      reader.close();

      int leafID = idMap.get(curWikiID);
      leafID++;
      for (; leafID < idMap.size(); leafID++)
        writer.write(String.format("%d,0\n", leafID));

      writer.close();
      logWriter.close();

    } catch (Exception e) {
      Util.println(String.format("line %d:\n%s", lineIndex, line));
      e.printStackTrace();
    }
  }

  /**
   * Generate the final entity file from location.txt.
   */
  public static void generateEntityFile() {
    BufferedReader reader = null;
    int lineIndex = 0;
    String line = "";
    try {
      Util.println("read map from " + entityMapPath);
      HashMap<Long, Integer> idMap = readMap(entityMapPath);
      Util.println("initialize entities");
      ArrayList<Entity> entities = new ArrayList<>(idMap.size());
      for (int i = 0; i < idMap.size(); i++)
        entities.add(new Entity(i));

      Util.println("read locations from " + locationPath);
      // Process the spatial entities.
      reader = new BufferedReader(new FileReader(new File(locationPath)));
      while ((line = reader.readLine()) != null) {
        String[] strList = line.split(",");
        long wikiID = Long.parseLong(strList[0]);
        int graphID = idMap.get(wikiID);
        String location = strList[1];
        strList = location.split("Point\\(");
        location = strList[1];
        location = location.replace(")", "");
        strList = location.split(" ");
        double lon = Double.parseDouble(strList[0]);
        double lat = Double.parseDouble(strList[1]);
        Entity entity = entities.get(graphID);
        entity.IsSpatial = true;
        entity.lon = lon;
        entity.lat = lat;
        lineIndex++;
      }
      reader.close();

      GraphUtil.writeEntityToFile(entities, entityPath);
    } catch (Exception e) {
      Util.println(lineIndex);
      Util.println(line);
      e.printStackTrace();
    }
  }

  /**
   * Extract only the spatial entities. The input example is '''<http://www.wikidata.org/entity/Q26>
   * <http://www.wikidata.org/prop/direct/P625> "Point(-5.84
   * 54.590933333333)"^^<http://www.opengis.net/ont/geosparql#wktLiteral> .''' The output format is
   * '''wikiId,POINT(lon lat)'''.
   */
  public static void extract() {
    BufferedReader reader;
    FileWriter writer;
    FileWriter logWriter;
    String line = "";
    int p276Count = 0, p625Count = 0; // p276 is not for coordinate
    long index = 0;
    try {
      reader = new BufferedReader(new FileReader(new File(fullfilePath)));
      writer = new FileWriter(locationPath);
      logWriter = new FileWriter(logPath);
      while ((line = reader.readLine()) != null) {
        String[] strList = line.split(" ");
        String predicate = strList[1];
        if (predicate.equals(coordinateStr)) {
          p625Count++;
          if (line.contains("\"")) {
            String subject = strList[0];
            Long wikiID = getQEntityID(subject);
            strList = line.split("\"");
            String pointString = strList[1];
            writer.write(wikiID + "," + pointString + "\n");
          } else
            logWriter.write(line + "\n");
        }

        index++;

        if (index % 10000000 == 0)
          Util.println(index);

        // if (index == 100000)
        // break;
      }
      reader.close();
      writer.close();
      logWriter.close();

      Util.println("p625Count: " + p625Count);
    } catch (Exception e) {
      Util.println(String.format("line %d:\n%s", index, line));
      e.printStackTrace();
    }
  }

  /**
   * Read the map <wikiID, graphID>.
   *
   * @param mapPath
   * @return
   */
  public static HashMap<Long, Integer> readMap(String mapPath) {
    LOGGER.log(loggingLevel, "readMap from {0}", mapPath);
    BufferedReader reader = null;
    String line = "";
    try {
      reader = new BufferedReader(new FileReader(new File(mapPath)));
      HashMap<Long, Integer> idMap = new HashMap<>();
      while ((line = reader.readLine()) != null) {
        String[] strList = line.split(",");
        int graphID = Integer.parseInt(strList[0]);
        long wikiID = Long.parseLong(strList[1]);
        idMap.put(wikiID, graphID);
      }
      reader.close();
      return idMap;
    } catch (Exception e) {
      if (reader != null)
        try {
          reader.close();
        } catch (IOException e1) {
          e1.printStackTrace();
        }
    }
    return null;
  }

  /**
   * Extract the id of a property when it is a predicate.
   *
   * @param string
   * @return
   * @throws Exception
   */
  public static int getPropertyPredicateID(String string) throws Exception {
    Matcher matcher = propertyPredicatePattern.matcher(string);
    if (matcher.find()) {
      return Integer.parseInt(matcher.group(1));
    }
    throw new Exception(string + " is not a property in predicate!");
  }

  public static boolean isPropertyPredicate(String string) {
    return string.matches(propertyPredicatePattern.pattern());
  }

  // public static boolean isProperty(String string) {
  // if (string.matches("<https://www.wikidata.org/wiki/Property:P\\d+>"))
  // return true;
  // else
  // return false;
  // }


  /**
   * Get the id of a property in subject.
   * 
   * @param string
   * @return
   * @throws Exception
   */
  public static long getPropertySubjectID(String string) throws Exception {
    Matcher m = propertyEntityPattern.matcher(string);
    if (m.find()) {
      return Long.parseLong(m.group(1));
    }
    throw new Exception(string + " is not a property entity!");
  }

  public static boolean isPropertySubject(String string) {
    return string.matches(propertyEntityPattern.pattern());
  }

  /**
   * Extract the id of an entity. An entity means Q-entity.
   *
   * @param string
   * @return
   * @throws Exception
   */
  public static long getQEntityID(String string) throws Exception {
    Matcher m = entityPattern.matcher(string);
    if (m.find()) {
      return Long.parseLong(m.group(1));
    }
    throw new Exception(string + " is not an Q-entity!");
  }

  /**
   * Whether is an Q-entity.
   *
   * @param string
   * @return
   */
  public static boolean isQEntity(String string) {
    return string.matches(entityPattern.pattern());
  }

  /**
   * Decode the row into [subject, predicate, object].
   *
   * @param line
   * @return
   */
  public static String[] decodeRow(String line) {
    return line.split(" ");
  }

  public static Map<Integer, String> readPropertyMap(String filepath) {
    LOGGER.log(loggingLevel, "read map from " + filepath);
    HashMap<String, String> map = ReadWriteUtil.ReadMap(filepath);
    HashMap<Integer, String> propertyMap = new HashMap<>();
    for (String key : map.keySet()) {
      propertyMap.put(Integer.parseInt(key), map.get(key));
    }
    return propertyMap;
  }

  public static Map<Integer, Long> readEntityIdMap(String filepath) {
    LOGGER.log(loggingLevel, "read map from " + filepath);
    HashMap<String, String> map = ReadWriteUtil.ReadMap(filepath);
    HashMap<Integer, Long> entityIdMap = new HashMap<>();
    for (String key : map.keySet()) {
      entityIdMap.put(Integer.parseInt(key), Long.parseLong(map.get(key)));
    }
    return entityIdMap;
  }
}
