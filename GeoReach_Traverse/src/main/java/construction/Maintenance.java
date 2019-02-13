package construction;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Relationship;
import org.roaringbitmap.RoaringBitmap;
import org.roaringbitmap.buffer.ImmutableRoaringBitmap;
import commons.Config;
import commons.EnumVariables.BoundaryLocationStatus;
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
  public boolean geoB;
  public ImmutableRoaringBitmap reachGrid;
  public MyRectangle rmbr;

  public UpdateUnit(GeoReachType type, ImmutableRoaringBitmap ReachGrid, MyRectangle rmbr,
      boolean GeoB) {
    this.type = type;
    this.reachGrid = ReachGrid;
    this.rmbr = rmbr;
    this.geoB = GeoB;
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

  // MGRatio and MRRatio
  public double MG, MR;


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
      if (!updateUnit.type.equals(GeoReachType.GeoB) || updateUnit.geoB) {
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
  private HashMap<Integer, UpdateUnit> createUpdateUnit(Node node) throws Exception {
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
          immutableRoaringBitmap = getReachGrid(node, hop);
          rmbr = spaceManager.getMbrOfReachGrid(immutableRoaringBitmap);
          geoB = true;
          break;
        case RMBR:
          rmbr = getRMBR(node, hop);
          immutableRoaringBitmap = spaceManager.getCoverIdOfRectangle(rmbr);
          geoB = true;
          break;
        case GeoB:
          // No need to set ReachGrid and RMBR because the update does not require.
          geoB = getGeoB(node, hop);
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
    UpdateStatus status = null;
    int srcUpdateHop = updateHop + 1;
    GeoReachType geoReachType = getGeoReachType(src, srcUpdateHop);
    UpdateUnit updateUnit = updateUnits.get(updateHop);
    switch (geoReachType) {
      case ReachGrid:
        ImmutableRoaringBitmap irb = getReachGrid(src, srcUpdateHop);
        RoaringBitmap rb = new RoaringBitmap(irb);
        int[] xyBoundary = spaceManager.getXYBoundary(rb);
        status = updateReachGridWithReachGrid(rb, updateUnit.reachGrid, xyBoundary);
        // If any new cell is added, check the MG.
        if (!status.equals(UpdateStatus.NotUpdateInside)
            && !status.equals(UpdateStatus.NotUpdateOnBoundary)) {
          if (!validateMG(rb, xyBoundary)) {
            // keep the GeoReach Type but modify ReachGrid value in db
            setReachGrid(src, srcUpdateHop, rb);
          } else {
            // validate MR
            removeGeoReach(src, GeoReachType.ReachGrid, srcUpdateHop);
            MyRectangle newRMBR = spaceManager.getMbrOfBoundary(xyBoundary);
            // remove ReachGrid, change GeoReachType_hop to GeoB, and set GeoB_hop to true
            if (validateMR(newRMBR)) {
              setGeoReachType(src, srcUpdateHop, GeoReachType.GeoB);
              src.setProperty(getGeoReachKey(GeoReachType.GeoB, srcUpdateHop), true);
            } else {
              setGeoReachType(src, srcUpdateHop, GeoReachType.RMBR);
              src.setProperty(getGeoReachKey(GeoReachType.RMBR, srcUpdateHop), newRMBR.toString());
            }
          }
        }
        break;
      case RMBR:
        MyRectangle srcRect = getRMBR(src, srcUpdateHop);
        status = srcRect.MBR(updateUnit.rmbr);
        if (status.equals(UpdateStatus.UpdateOutside)) {
          if (validateMR(srcRect)) {
            removeGeoReach(src, GeoReachType.RMBR, srcUpdateHop);
            setGeoReachType(src, srcUpdateHop, GeoReachType.GeoB);
            src.setProperty(getGeoReachKey(GeoReachType.GeoB, srcUpdateHop), true);
          } else {
            src.setProperty(getGeoReachKey(GeoReachType.RMBR, srcUpdateHop), srcRect.toString());
          }
        }
      case GeoB:
        boolean srcGeoB = getGeoB(src, srcUpdateHop);
        // srcGeoB is true, no update is needed.
        if (srcGeoB) {
          status = UpdateStatus.NotUpdateInside;
        } else {
          // updateUnit geoB is false, no update is needed.
          if (!updateUnit.geoB) {
            status = UpdateStatus.NotUpdateInside;
          } else {
            status = UpdateStatus.UpdateOutside;
            src.setProperty(getGeoReachKey(GeoReachType.GeoB, srcUpdateHop), true);
          }
        }
      default:
        throw new Exception(String.format("Type %s does not exist!", geoReachType.toString()));
    }
    return status;
  }

  private UpdateStatus updateReachGridWithReachGrid(RoaringBitmap rb, ImmutableRoaringBitmap irb,
      int[] boundary) throws Exception {
    if (irb == null) {
      return UpdateStatus.NotUpdateInside;
    }
    BoundaryLocationStatus status = BoundaryLocationStatus.INSIDE;
    boolean hasDiff = false;
    for (int id : irb) {
      int[] xyId = spaceManager.getXYId(id);
      // Check whether cell is contained
      if (!rb.contains(id) && !hasDiff) {
        rb.add(id);
        hasDiff = true;
      }
      // update spatial relation status
      BoundaryLocationStatus boundaryStatus = Util.locate(boundary, xyId);
      status = updateBoundaryLocationStatus(status, boundaryStatus);
    }
    return getUpdateStatus(status, hasDiff);
  }

  /**
   * Decide whether <code>rb</code> needs to be replaced by rmbr
   *
   * @param rb
   * @param boundary
   * @return <code>true</code> means needs to be replaced by rmbr
   */
  public boolean validateMG(RoaringBitmap rb, int[] boundary) {
    // # of cells in rb > # of cells in the coverage MBR * MG
    return (boundary[4] > (boundary[2] - boundary[0] + 1) * (boundary[3] - boundary[1] + 1) * MG);
  }

  public boolean validateMR(MyRectangle rectangle) {
    // area of rectangle > totalArea * MR
    return rectangle.area() > spaceManager.getTotalArea() * MR;
  }

  /**
   * Get the GeoReach type of a node for a hop.
   *
   * @param node
   * @param hop
   * @return
   * @throws Exception
   */
  private GeoReachType getGeoReachType(Node node, int hop) throws Exception {
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

  public void setGeoReachType(Node node, int hop, GeoReachType type) throws Exception {
    int setType = 0;
    switch (type) {
      case ReachGrid:
        setType = 0;
        break;
      case RMBR:
        setType = 1;
        break;
      case GeoB:
        setType = 2;
        break;
      default:
        throw new Exception(String.format("Type %s does not exist!", type.toString()));
    }
    node.setProperty(GeoReachTypeName + "_" + hop, setType);
  }

  public void removeGeoReach(Node node, GeoReachType geoReachType, int hop) throws Exception {
    node.removeProperty(getGeoReachKey(geoReachType, hop));
  }

  /**
   * Get the GeoReach property name.
   *
   * @param geoReachType
   * @param hop
   * @return
   * @throws Exception
   */
  public String getGeoReachKey(GeoReachType geoReachType, int hop) throws Exception {
    switch (geoReachType) {
      case ReachGrid:
        return String.format("%s_%d", reachGridName, hop);
      case RMBR:
        return String.format("%s_%d", rmbrName, hop);
      case GeoB:
        return String.format("%s_%d", geoBName, hop);
      default:
        throw new Exception(String.format("GeoReachType %s does not exist!", geoReachType));
    }
  }

  /**
   * Assume that Type of hop is ReachGrid and the property exists.
   *
   * @param node
   * @param hop
   * @return
   */
  public ImmutableRoaringBitmap getReachGrid(Node node, int hop) {
    String rbString = (String) node.getProperty(reachGridName + "_" + hop);
    return Util.getImmutableRoaringBitmap(rbString);
  }

  public void setReachGrid(Node node, int hop, RoaringBitmap rb) throws Exception {
    String string = Util.roarBitmapSerializeToString(rb);
    node.setProperty(getGeoReachKey(GeoReachType.ReachGrid, hop), string);
  }

  public MyRectangle getRMBR(Node node, int hop) {
    return new MyRectangle(node.getProperty(rmbrName + "_" + hop).toString());
  }

  private boolean getGeoB(Node node, int hop) {
    return (boolean) node.getProperty(geoBName + "_" + hop);
  }

  /**
   * Get the final status of a ReachGrid update.
   *
   * @param status
   * @param hasDiff
   * @return
   * @throws Exception
   */
  private static UpdateStatus getUpdateStatus(BoundaryLocationStatus status, boolean hasDiff)
      throws Exception {
    switch (status) {
      case OUTSIDE:
        return UpdateStatus.UpdateOutside;
      case ONBOUNDARY:
        if (hasDiff) {
          return UpdateStatus.UpdateOnBoundary;
        } else {
          return UpdateStatus.NotUpdateOnBoundary;
        }
      case INSIDE:
        if (hasDiff) {
          return UpdateStatus.UpdateInside;
        } else {
          return UpdateStatus.NotUpdateInside;
        }
      default:
        throw new Exception(String.format("Status %s not defined!", status));
    }
  }

  private static BoundaryLocationStatus updateBoundaryLocationStatus(BoundaryLocationStatus status,
      BoundaryLocationStatus cur) throws Exception {
    switch (cur) {
      case ONBOUNDARY:
        if (status.equals(BoundaryLocationStatus.INSIDE)) {
          status = cur;
        }
        break;
      case OUTSIDE:
        if (status.equals(BoundaryLocationStatus.INSIDE)
            || status.equals(BoundaryLocationStatus.ONBOUNDARY)) {
          status = cur;
        }
        break;
      case INSIDE:
        break;
      default:
        throw new Exception(String.format("BoundaryLocationStatus %s does not exist!", cur));
    }
    return status;
  }
}
