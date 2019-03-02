package commons;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.FileWriter;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.TreeSet;
import org.roaringbitmap.RoaringBitmap;
import commons.EnumVariables.GeoReachOutputFormat;
import commons.EnumVariables.GeoReachType;

public class GeoReachIndexUtil {

  /**
   * Decide whether current gridCount can satisfy MG constraint.
   *
   * @param gridCount number of grid cells
   * @param idXMin
   * @param idYMin
   * @param idXMax
   * @param idYMax
   * @param MG
   * @return
   */
  public static boolean validateMG(int gridCount, int idXMin, int idYMin, int idXMax, int idYMax,
      double MG) {
    if (gridCount == 1) {
      return false;
    }
    return gridCount < ((idXMax - idXMin + 1) * (idYMax - idYMin + 1) * MG);
  }

  public static boolean validateMR(MyRectangle rectangle, double totalArea, double MR) {
    // area of rectangle <= totalArea * MR
    return rectangle.area() < totalArea * MR;
  }

  public static GeoReachType getGeoReachType(int type) throws Exception {
    switch (type) {
      case 0:
        return GeoReachType.ReachGrid;
      case 1:
        return GeoReachType.RMBR;
      case 2:
        return GeoReachType.GeoB;
      default:
        throw new Exception(String.format("Type %d does not exist!", type));
    }
  }

  public static String getIndexFileNormalName(int pieces_x, int pieces_y, double MG, double MR,
      int MC, int MAX_HOP, GeoReachOutputFormat format) throws Exception {
    String suffix = "";
    if (format.equals(GeoReachOutputFormat.BITMAP)) {
      suffix = "bitmap";
    } else {
      suffix = "list";
    }
    return String.format("%d_%d_%d_%d_%d_%d_%s.txt", pieces_x, pieces_y, (int) (MG * 100),
        (int) (MR * 100), MC, MAX_HOP, suffix);
  }

  /**
   * Output everything. Each vertex for each step contains all the ReachGrid and RMBR information
   * 
   * @param index
   * @param filepath
   */
  public static void outputGeoReach(ArrayList<VertexGeoReach> index, String filepath) {
    int id = 0;
    FileWriter writer = null;
    try {
      int nodeCount = index.size();
      int MAX_HOP = index.get(0).ReachGrids.size();
      writer = new FileWriter(filepath);
      writer.write(String.format("%d,%d\n", nodeCount, MAX_HOP));
      for (VertexGeoReach vertexGeoReach : index) {
        writer.write(id + "\n");
        writer.write(vertexGeoReach.toString());
        id++;
      }
      writer.close();
    } catch (Exception e) {
      e.printStackTrace();
      System.exit(-1);
    }
  }

  /**
   * Output everything. Each vertex for each step contains all the ReachGrid and RMBR information
   * 
   * @param index
   * @param filepath
   */
  public static void outputGeoReachList(ArrayList<VertexGeoReachList> index, String filepath) {
    int id = 0;
    FileWriter writer = null;
    try {
      int nodeCount = index.size();
      int MAX_HOP = index.get(0).ReachGrids.size();
      writer = new FileWriter(filepath);
      writer.write(String.format("%d,%d\n", nodeCount, MAX_HOP));
      for (VertexGeoReachList vertexGeoReach : index) {
        writer.write(id + "\n");
        writer.write(vertexGeoReach.toString());
        id++;
      }
      writer.close();
    } catch (Exception e) {
      e.printStackTrace();
      System.exit(-1);
    }
  }

  public static void readGeoReachWhole(String filepath,
      ArrayList<ArrayList<ArrayList<Integer>>> reachgridsList,
      ArrayList<ArrayList<MyRectangle>> rectanglesList) {
    int id = 0;
    String line = null;
    BufferedReader reader = null;
    try {
      reader = new BufferedReader(new FileReader(new File(filepath)));
      line = reader.readLine();
      String[] strList = line.split(",");
      int nodeCount = Integer.parseInt(strList[0]);
      int MAX_HOPNUM = Integer.parseInt(strList[1]);

      reachgridsList = new ArrayList<>(nodeCount);
      rectanglesList = new ArrayList<>(nodeCount);

      // each vertex
      for (int i = 0; i < nodeCount; i++) {
        // VertexGeoReachList vertexGeoReachList = new VertexGeoReachList(MAX_HOPNUM);
        ArrayList<ArrayList<Integer>> reachgrids = new ArrayList<>(MAX_HOPNUM);
        ArrayList<MyRectangle> rectangles = new ArrayList<>(MAX_HOPNUM);

        id = Integer.parseInt(reader.readLine());

        // each hop
        for (int j = 0; j < MAX_HOPNUM; j++) {
          line = reader.readLine();
          strList = line.split(";");

          String reachGridStr = strList[0];
          String rmbrStr = strList[1];

          // reachGrid
          if (!reachGridStr.equals("null")) {
            reachGridStr = reachGridStr.substring(1, reachGridStr.length() - 1);
            strList = reachGridStr.split(", ");
            ArrayList<Integer> reachGrid = new ArrayList<>(strList.length);
            for (String string : strList)
              reachGrid.add(Integer.parseInt(string));
            // vertexGeoReachList.ReachGrids.set(j, reachGrid);
            reachgrids.add(reachGrid);
          } else
            reachgrids.add(null);

          // rmbr
          if (!rmbrStr.equals("null")) {
            MyRectangle rmbr = new MyRectangle(rmbrStr);
            // vertexGeoReachList.RMBRs.set(j, rmbr);
            rectangles.add(rmbr);
          } else
            rectangles.add(null);
        }
        // index.add(vertexGeoReachList);
        reachgridsList.add(reachgrids);
        rectanglesList.add(rectangles);
      }
      reader.close();
    } catch (Exception e) {
      Util.println("id: " + id + "\n" + line);
      e.printStackTrace();
      System.exit(-1);
    }
  }

  public static ArrayList<VertexGeoReachList> readGeoReachWhole(String filepath) {
    int id = 0;
    String line = null;
    BufferedReader reader = null;
    try {
      reader = new BufferedReader(new FileReader(new File(filepath)));
      line = reader.readLine();
      String[] strList = line.split(",");
      int nodeCount = Integer.parseInt(strList[0]);
      int MAX_HOPNUM = Integer.parseInt(strList[1]);
      ArrayList<VertexGeoReachList> index = new ArrayList<>(nodeCount);
      // each vertex
      for (int i = 0; i < nodeCount; i++) {
        VertexGeoReachList vertexGeoReachList = new VertexGeoReachList(MAX_HOPNUM);
        id = Integer.parseInt(reader.readLine());

        // each hop
        for (int j = 0; j < MAX_HOPNUM; j++) {
          line = reader.readLine();
          strList = line.split(";");

          String reachGridStr = strList[0];
          String rmbrStr = strList[1];
          // String geoBStr = strList[2];

          // reachGrid
          if (!reachGridStr.equals("null")) {
            reachGridStr = reachGridStr.substring(1, reachGridStr.length() - 1);
            strList = reachGridStr.split(", ");
            ArrayList<Integer> reachGrid = new ArrayList<>(strList.length);
            for (String string : strList)
              reachGrid.add(Integer.parseInt(string));
            vertexGeoReachList.ReachGrids.set(j, reachGrid);
          }

          // rmbr
          if (!rmbrStr.equals("null")) {
            MyRectangle rmbr = new MyRectangle(rmbrStr);
            vertexGeoReachList.RMBRs.set(j, rmbr);
          }
        }
        index.add(vertexGeoReachList);
      }
      reader.close();
      return index;
    } catch (Exception e) {
      Util.println("id: " + id + "\n" + line);
      e.printStackTrace();
      System.exit(-1);
    }
    return null;
  }

  /**
   * Output index based on type
   * 
   * @param index
   * @param filepath
   * @param types
   * @param format 0 represents list while 1 represents bitmap
   */
  public static void outputGeoReach(ArrayList<VertexGeoReach> index, String filepath,
      ArrayList<ArrayList<Integer>> typesList, GeoReachOutputFormat format) {
    int id = 0;
    FileWriter writer = null;
    try {
      int nodeCount = index.size();
      int MAX_HOP = index.get(0).ReachGrids.size();
      writer = new FileWriter(filepath);
      writer.write(String.format("%d,%d\n", nodeCount, MAX_HOP));

      Iterator<VertexGeoReach> iterator1 = index.iterator();
      Iterator<ArrayList<Integer>> iterator2 = typesList.iterator();

      while (iterator1.hasNext() && iterator2.hasNext()) {
        VertexGeoReach vertexGeoReach = iterator1.next();
        ArrayList<Integer> types = iterator2.next();

        writer.write(id + "\n");
        for (int i = 0; i < MAX_HOP; i++) {
          int type = types.get(i);
          writer.write("" + type + ":");
          switch (type) {
            case 0:
              TreeSet<Integer> reachgrid = vertexGeoReach.ReachGrids.get(i);
              if (format.equals(GeoReachOutputFormat.BITMAP)) {
                RoaringBitmap r = new RoaringBitmap();
                for (int gridID : reachgrid)
                  r.add(gridID);
                String bitmap_ser = Util.roarBitmapSerializeToString(r);
                writer.write(bitmap_ser + "\n");
              } else {
                writer.write(reachgrid.toString() + "\n");
              }
              break;
            case 1:
              writer.write(vertexGeoReach.RMBRs.get(i).toString() + "\n");
              break;
            case 2:
              writer.write(vertexGeoReach.GeoBs.get(i).toString() + "\n");
              break;
            default:
              throw new Exception(String.format("Wrong type %d for vertex %d!", type, id));
          }
        }
        id++;
      }
      writer.close();
    } catch (Exception e) {
      Util.println("Error happens when output index for vertex " + id);
      Util.println("Type List: " + typesList.get(id));
      e.printStackTrace();
      System.exit(-1);
    }
  }

  /**
   * Output index based on type. Will overwrite if the same filename exists.
   *
   * @param index
   * @param filepath
   * @param types
   * @param format 0 represents list while 1 represents bitmap
   */
  public static void outputGeoReachForList(ArrayList<VertexGeoReachList> index, String filepath,
      ArrayList<ArrayList<Integer>> typesList, GeoReachOutputFormat format) {
    int id = 0;
    FileWriter writer = null;
    try {
      int nodeCount = index.size();
      int MAX_HOP = index.get(0).ReachGrids.size();
      writer = new FileWriter(filepath);
      writer.write(String.format("%d,%d\n", nodeCount, MAX_HOP));

      Iterator<VertexGeoReachList> iterator1 = index.iterator();
      Iterator<ArrayList<Integer>> iterator2 = typesList.iterator();

      while (iterator1.hasNext() && iterator2.hasNext()) {
        VertexGeoReachList vertexGeoReach = iterator1.next();
        ArrayList<Integer> types = iterator2.next();

        writer.write(id + "\n");
        for (int i = 0; i < MAX_HOP; i++) {
          int type = types.get(i);
          writer.write("" + type + ":");
          switch (type) {
            case 0:
              ArrayList<Integer> reachgrid = vertexGeoReach.ReachGrids.get(i);
              if (format.equals(GeoReachOutputFormat.BITMAP)) {
                RoaringBitmap r = new RoaringBitmap();
                for (int gridID : reachgrid)
                  r.add(gridID);
                String bitmap_ser = Util.roarBitmapSerializeToString(r);
                writer.write(bitmap_ser + "\n");
              } else {
                writer.write(reachgrid.toString() + "\n");
              }
              break;
            case 1:
              writer.write(vertexGeoReach.RMBRs.get(i).toString() + "\n");
              break;
            case 2:
              writer.write(vertexGeoReach.GeoBs.get(i).toString() + "\n");
              break;
            default:
              throw new Exception(String.format("Wrong type %d for vertex %d!", type, id));
          }
        }
        id++;
      }
      writer.close();
    } catch (Exception e) {
      Util.println("Error happens when output index for vertex " + id);
      Util.println("Type List: " + typesList.get(id));
      e.printStackTrace();
      System.exit(-1);
    }
  }

}
