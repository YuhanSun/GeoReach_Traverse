package construction;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Relationship;
import org.roaringbitmap.RoaringBitmap;
import org.roaringbitmap.buffer.ImmutableRoaringBitmap;
import commons.Config;
import commons.EnumVariables.GeoReachType;
import commons.EnumVariables.UpdateStatus;
import commons.Labels;
import commons.MyRectangle;
import commons.SpaceManager;
import commons.Util;

/**
 * An update unit contains all types of SIP.
 *
 * @author Yuhan Sun
 *
 */
class UpdateUnit {
  public GeoReachType type;
  public boolean GeoB;
  public ImmutableRoaringBitmap immutableRoaringBitmap;
  public MyRectangle rmbr;

  public UpdateUnit(GeoReachType type, ImmutableRoaringBitmap immutableRoaringBitmap,
      MyRectangle rmbr, boolean GeoB) {
    this.type = type;
    this.immutableRoaringBitmap = immutableRoaringBitmap;
    this.rmbr = rmbr;
    this.GeoB = GeoB;
  }
}


public class Maintenance {

  public static Config config = new Config();
  public static String lon_name = config.GetLongitudePropertyName();
  public static String lat_name = config.GetLatitudePropertyName();

  public static String GeoReachTypeName = config.getGeoReachTypeName();
  public static String reachGridName = config.getReachGridName();
  public static String rmbrName = config.getRMBRName();
  public static String geoBName = config.getGeoBName();

  public double minx, miny, maxx, maxy;
  public int pieces_x, pieces_y;
  public int MAX_HOP;
  public double resolutionX, resolutionY;
  public SpaceManager spaceManager;

  public Maintenance(double minx, double miny, double maxx, double maxy, int piecesX, int piecesY,
      int MAX_HOP) {
    spaceManager = new SpaceManager(minx, miny, maxx, maxy, piecesX, piecesY);
    this.MAX_HOP = MAX_HOP;
  }

  public void addEdge(Node src, Node trg) throws Exception {
    HashMap<Integer, UpdateUnit> updateUnits = createUpdateUnit(trg);
    /**
     * Get the minimum hop that has update potential. It can reduce the neighbor search boundary.
     */
    int minHop = 0;
    for (; minHop <= MAX_HOP - 1; minHop++) {
      UpdateUnit updateUnit = updateUnits.get(minHop);
      // If the unit contains ReachGrid or RMBR or GeoB = true, continue.
      if (!updateUnit.type.equals(GeoReachType.GeoB) || updateUnit.GeoB) {
        break;
      }
    }

    HashMap<Node, HashSet<Integer>> currentUpdateNodes = new HashMap<>();
    // The hop id on trg to be updated on the src
    HashSet<Integer> srcUpdateHops = new HashSet<>();
    for (int i = minHop; i <= MAX_HOP - 1; i++) {
      srcUpdateHops.add(i);
    }
    currentUpdateNodes.put(src, srcUpdateHops);
    for (int hop = MAX_HOP - 1; hop >= minHop; hop--) {
      HashMap<Node, HashSet<Integer>> nextUpdateNodes = new HashMap<>();
      for (Node node : currentUpdateNodes.keySet()) {
        HashSet<Integer> updateHops = currentUpdateNodes.get(node);
        update(node, updateUnits, updateHops, nextUpdateNodes);
      }
      currentUpdateNodes = nextUpdateNodes;
    }
  }

  /**
   * Create the update unit.
   *
   * @param node
   * @throws Exception
   */
  private HashMap createUpdateUnit(Node node) throws Exception {
    HashMap<Integer, UpdateUnit> updateUnits = new HashMap<>();
    // handle the SIP(node, 0)
    if (node.hasProperty(lon_name)) {
      double lon = (Double) node.getProperty(lon_name);
      double lat = (Double) node.getProperty(lat_name);
      int id = spaceManager.getId(lon, lat);
      RoaringBitmap roaringBitmap = new RoaringBitmap();
      roaringBitmap.add(id);
      MyRectangle rmbr = new MyRectangle(lon, lat, lon, lat);

      updateUnits.put(0,
          new UpdateUnit(GeoReachType.RMBR, roaringBitmap.toMutableRoaringBitmap(), rmbr, true));
    } else {
      updateUnits.put(0, new UpdateUnit(GeoReachType.GeoB, null, null, false));
    }
    // handle SIP(node, 1) to SIP(node, B-1)
    for (int hop = 1; hop < MAX_HOP; hop++) {
      GeoReachType type = getGeoReachType(node, hop);
      ImmutableRoaringBitmap immutableRoaringBitmap = null;
      MyRectangle rmbr = null;
      boolean geoB = false;
      switch (type) {
        case ReachGrid:
          geoB = (boolean) node.getProperty(geoBName + "_" + hop);
          break;
        case RMBR:
          rmbr = new MyRectangle(node.getProperty(rmbrName + "_" + hop).toString());
          immutableRoaringBitmap = spaceManager.getCoverIdOfRectangle(rmbr);
          geoB = true;
          break;
        case GeoB:
          String ser = node.getProperty(reachGridName + "_" + hop).toString();
          immutableRoaringBitmap = Util.getImmutableRoaringBitmap(ser);
          rmbr = spaceManager.getMbrOfReachGrid(immutableRoaringBitmap);
        default:
          throw new Exception(String.format("type %d does not exist!", type));
      }
      updateUnits.put(hop, new UpdateUnit(GeoReachType.RMBR, immutableRoaringBitmap, rmbr, geoB));
    }
    return updateUnits;
  }

  /**
   * Update a node for all the required hops.
   *
   * @param node
   * @param updateUnits
   * @param updateHops the required hops to be updated
   * @param nextUpdateNodes
   * @throws Exception
   */
  public void update(Node node, HashMap<Integer, UpdateUnit> updateUnits,
      HashSet<Integer> updateHops, HashMap<Node, HashSet<Integer>> nextUpdateNodes)
      throws Exception {
    for (int updateHop : updateHops) {
      UpdateStatus updateStatus = update(node, updateUnits, updateHop);
      int nextHop = updateHop + 1;
      if (!updateStatus.equals(UpdateStatus.UpdateInside) && (nextHop <= MAX_HOP)) {
        Iterable<Relationship> iterable = node.getRelationships(Labels.GraphRel.GRAPH_LINK);
        Iterator<Relationship> iterator = iterable.iterator();
        while (iterator.hasNext()) {
          Relationship relationship = iterator.next();
          Node neighbor = relationship.getOtherNode(node);
          if (!nextUpdateNodes.containsKey(neighbor)) {
            nextUpdateNodes.put(neighbor, new HashSet<>());
          }
          if (!nextUpdateNodes.get(neighbor).contains(nextHop)) {
            nextUpdateNodes.get(neighbor).add(nextHop);
          }
        }
      }
    }
  }

  /**
   * Update a node's specific hop GeoReach.
   *
   * @param src
   * @param updateUnits
   * @param updateHop
   * @return
   * @throws Exception
   */
  public UpdateStatus update(Node src, HashMap<Integer, UpdateUnit> updateUnits, int updateHop)
      throws Exception {
    int srcUpdateHop = updateHop + 1;
    GeoReachType geoReachType = getGeoReachType(src, srcUpdateHop);
    switch (geoReachType) {
      case ReachGrid:

        break;

      default:
        break;
    }
    return UpdateStatus.NotUpdateInside;
  }

  public UpdateStatus updateReachGridWithReachGrid() {

  }

  /**
   * Get the GeoReach type of a node for a hop.
   *
   * @param node
   * @param hop
   * @return
   * @throws Exception
   */
  public GeoReachType getGeoReachType(Node node, int hop) throws Exception {
    String typePropertyName = GeoReachTypeName + "_" + hop;
    if (!node.hasProperty(typePropertyName)) {
      throw new Exception(String.format("Type property %s is not found!", typePropertyName));
    }
    int type = (int) node.getProperty(typePropertyName);
    switch (type) {
      case 0:
        return GeoReachType.ReachGrid;
      case 1:
        return GeoReachType.RMBR;
      case 2:
        return GeoReachType.GeoB;
      default:
        throw new Exception(String.format("type %d does not exist!", type));
    }
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
  public UpdateStatus update(GraphDatabaseService dbservice, Node src, Node trg, int hop) {
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
            int gridID = idX * pieces_y + idY;

            propertyName = reachGridName + "_1";
            String reachGridRbStr = (String) src.getProperty(propertyName);
            ImmutableRoaringBitmap reachGridIrb = Util.getImmutableRoaringBitmap(reachGridRbStr);
            int[] boundary = spaceManager.getXYBoundary(reachGridIrb);
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
  public UpdateStatus updateReachGridWithReachGrid(ImmutableRoaringBitmap immutableRoaringBitmap,
      RoaringBitmap roaringBitmap, ImmutableRoaringBitmap reachGrid, int piecesX, int piecesY) {
    UpdateStatus status = UpdateStatus.NotUpdateInside;
    int[] boundary = spaceManager.getXYBoundary(reachGrid);
    roaringBitmap = immutableRoaringBitmap.toRoaringBitmap();
    for (int id : reachGrid) {
      int[] xyId = spaceManager.getXYId(id);
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

  public UpdateStatus updateReachGridWithRMBR(ImmutableRoaringBitmap immutableRoaringBitmap,
      RoaringBitmap roaringBitmap, MyRectangle rmbr, double minx, double miny, double maxx,
      double maxy, double resolutionX, double resolutionY, int piecesX, int piecesY) {
    UpdateStatus status = UpdateStatus.NotUpdateInside;
    int[] boundary = spaceManager.getXYBoundary(immutableRoaringBitmap);
    roaringBitmap = immutableRoaringBitmap.toRoaringBitmap();

    int[] boundaryRMBR = spaceManager.getXYBoundary(rmbr);
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
