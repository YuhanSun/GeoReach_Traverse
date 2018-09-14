package construction;

import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Node;
import org.roaringbitmap.RoaringBitmap;
import org.roaringbitmap.buffer.ImmutableRoaringBitmap;
import commons.Config;
import commons.EnumVariables.UpdateStatus;
import commons.MyRectangle;
import commons.Util;

public class Maintenance {

  public static Config config = new Config();
  public static String lon_name = config.GetLongitudePropertyName();
  public static String lat_name = config.GetLatitudePropertyName();

  public static String GeoReachTypeName = config.getGeoReachTypeName();
  public static String reachGridName = config.getReachGridName();
  public static String rmbrName = config.getRMBRName();
  public static String geoBName = config.getGeoBName();

  public static boolean addEdge(GraphDatabaseService dbservice, Node src, Node trg, int bound) {



    return false;
  }

  /**
   * Update specific hop GeoReach of both nodes. Has to make sure that (hop-1) GeoReach is correct.
   * Update src using trg.
   *
   * @param dbservice
   * @param src
   * @param trg
   * @param hop
   * @return
   */
  public static UpdateStatus update(GraphDatabaseService dbservice, Node src, Node trg, int hop,
      double minx, double miny, double maxx, double maxy, int pieces_x, int pieces_y) {
    double resolution_x = (maxx - minx) / pieces_x;
    double resolution_y = (maxy - miny) / pieces_y;
    if (hop == 1) {
      if (trg.hasProperty(lon_name)) {
        double lon = (double) trg.getProperty(lon_name);
        double lat = (double) trg.getProperty(lat_name);
        String typePropertyName = GeoReachTypeName + "_1";
        int type = (int) src.getProperty(typePropertyName);
        String propertyName;
        switch (type) {
          case 0:
            int idX = (int) ((lon - minx) / resolution_x);
            int idY = (int) ((lat - miny) / resolution_y);
            idX = Math.min(pieces_x - 1, idX);
            idY = Math.min(pieces_y - 1, idY);
            int gridID = idX * pieces_x + idY;

            propertyName = reachGridName + "_1";
            String reachGridRbStr = (String) src.getProperty(propertyName);
            ImmutableRoaringBitmap reachGridIrb = Util.getImmutableRoaringBitmap(reachGridRbStr);
            int[] boundary = Util.getXYBoundary(reachGridIrb, pieces_x, pieces_y);
            if (idX < boundary[0] || idY < boundary[1] || idX > boundary[2] || idY > boundary[3]) {
              RoaringBitmap reachGridRb = new RoaringBitmap(reachGridIrb);
              reachGridRb.add(gridID);
              src.setProperty(propertyName, reachGridRb);
              return UpdateStatus.UpdateOutside;
            }
            // Not update
            if (reachGridIrb.contains(gridID)) {
              if (idX == boundary[0] || idY == boundary[1] || idX == boundary[2]
                  || idY == boundary[3]) {
                return UpdateStatus.NotUpdateOnBoundary;
              } else {
                return UpdateStatus.NotUpdateInside;
              }
            } else {
              RoaringBitmap reachGridRb = new RoaringBitmap(reachGridIrb);
              reachGridRb.add(gridID);
              src.setProperty(propertyName, reachGridRb);
              if (idX == boundary[0] || idY == boundary[1] || idX == boundary[2]
                  || idY == boundary[3]) {
                return UpdateStatus.UpdateOnBoundary;
              } else {
                return UpdateStatus.UpdateInside;
              }
            }
          case 1:
            propertyName = rmbrName + "_1";
            MyRectangle rmbr = new MyRectangle(src.getProperty(propertyName).toString());
            if (Util.Location_In_Rect(lon, lat, rmbr)) {
              return UpdateStatus.NotUpdateInside;
            } else {
              rmbr.MBR(new MyRectangle(lon, lat, lon, lat));
              src.setProperty(propertyName, rmbr.toString());
              return UpdateStatus.UpdateOutside;
            }
          case 2:
            propertyName = geoBName + "_1";
            boolean geoB = (boolean) src.getProperty(propertyName);
            if (geoB) {
              return UpdateStatus.NotUpdateInside;
            } else {
              src.setProperty(typePropertyName, 1);
              src.setProperty(rmbrName + "_1", new MyRectangle(lon, lat, lon, lat));
              return UpdateStatus.UpdateOutside;
            }
        }
      } else {
        return UpdateStatus.NotUpdateInside;
      }
    }

    else {

    }

    return UpdateStatus.NotUpdateInside;
  }

  // static UpdateStatus updateReachGrid(ImmutableRoaringBitmap immutableRoaringBitmap,
  // RoaringBitmap roaringBitmap, Node trg, int hop, int piecesX, int piecesY) {
  //
  // }

  /**
   * Update ReachGrid with another ReachGrid.
   *
   * @param immutableRoaringBitmap
   * @param roaringBitmap
   * @param reachGrid
   * @param piecesX
   * @param piecesY
   * @return
   */
  static UpdateStatus updateReachGridWithReachGrid(ImmutableRoaringBitmap immutableRoaringBitmap,
      RoaringBitmap roaringBitmap, ImmutableRoaringBitmap reachGrid, int piecesX, int piecesY) {
    UpdateStatus status = UpdateStatus.NotUpdateInside;
    int[] boundary = Util.getXYBoundary(reachGrid, piecesX, piecesY);
    roaringBitmap = immutableRoaringBitmap.toRoaringBitmap();
    for (int id : reachGrid) {
      int[] xyId = Util.getXYId(id, piecesX, piecesY);
      UpdateStatus boundaryStatus = Util.locate(boundary, xyId);
      if (immutableRoaringBitmap.contains(id)) {
        if (boundaryStatus == UpdateStatus.NotUpdateOnBoundary
            && status == UpdateStatus.NotUpdateInside) {
          status = UpdateStatus.NotUpdateOnBoundary;
        }
      } else {
        roaringBitmap.add(id);
        if (status == UpdateStatus.NotUpdateInside) {
          switch (boundaryStatus) {
            case NotUpdateInside:
              status = UpdateStatus.UpdateInside;
            case NotUpdateOnBoundary:
              status = UpdateStatus.UpdateOnBoundary;
            case UpdateOutside:
              status = UpdateStatus.UpdateOutside;
          }
        }
      }
    }
    return status;
  }

  static UpdateStatus updateReachGridWithRMBR(ImmutableRoaringBitmap immutableRoaringBitmap,
      RoaringBitmap roaringBitmap, MyRectangle rmbr, double minx, double miny, double maxx,
      double maxy, double resolutionX, double resolutionY, int piecesX, int piecesY) {
    UpdateStatus status = UpdateStatus.NotUpdateInside;
    int[] boundary = Util.getXYBoundary(immutableRoaringBitmap, piecesX, piecesY);
    roaringBitmap = immutableRoaringBitmap.toRoaringBitmap();

    int[] boundaryRMBR = Util.getXYBoundary(rmbr, minx, miny, resolutionX, resolutionY);
    // Definitely UpdateOutside
    if (boundaryRMBR[0] < boundary[0] || boundaryRMBR[1] < boundary[1]
        || boundaryRMBR[2] > boundary[2] || boundaryRMBR[3] > boundary[3]) {
      status = UpdateStatus.UpdateOutside;
      for (int i = boundaryRMBR[0]; i <= boundaryRMBR[2]; i++) {
        for (int j = boundaryRMBR[1]; j <= boundaryRMBR[3]; j++) {
          int id = i * piecesX + j;
          roaringBitmap.add(id);
        }
      }
    } else {
      // boundary overlap
      if (boundaryRMBR[0] == boundary[0] || boundaryRMBR[1] == boundary[1]
          || boundaryRMBR[2] == boundary[2] || boundaryRMBR[3] == boundary[3]) {
        status = UpdateStatus.NotUpdateOnBoundary;
        for (int i = boundaryRMBR[0]; i <= boundaryRMBR[2]; i++) {
          for (int j = boundaryRMBR[1]; j <= boundaryRMBR[3]; j++) {
            int id = i * piecesX + j;
            if (!immutableRoaringBitmap.contains(id))
              roaringBitmap.add(id);
            status = UpdateStatus.UpdateOnBoundary;
          }
        }
      } else { // rmbr inside boundary of reachgrid
        for (int i = boundaryRMBR[0]; i <= boundaryRMBR[2]; i++) {
          for (int j = boundaryRMBR[1]; j <= boundaryRMBR[3]; j++) {
            int id = i * piecesX + j;
            if (!immutableRoaringBitmap.contains(id)) {
              roaringBitmap.add(id);
              status = UpdateStatus.UpdateInside;
            }
          }
        }
      }
    }
    return status;
  }
}
