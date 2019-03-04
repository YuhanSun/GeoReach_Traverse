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
import commons.EnumVariables.BoundaryLocationStatus;
import commons.EnumVariables.GeoReachType;
import commons.EnumVariables.UpdateStatus;
import commons.GeoReachIndexUtil;
import commons.Labels;
import commons.Labels.GraphRel;
import commons.MyRectangle;
import commons.Neo4jGraphUtility;
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
  public String rbString;
  public MyRectangle rmbr;

  public UpdateUnit(GeoReachType type, ImmutableRoaringBitmap ReachGrid, MyRectangle rmbr,
      boolean GeoB) {
    this.type = type;
    this.reachGrid = ReachGrid;
    if (reachGrid != null) {
      this.rbString = Util.roarBitmapSerializeToString(new RoaringBitmap(ReachGrid));
    }
    this.rmbr = rmbr;
    this.geoB = GeoB;
  }
}


public class Maintenance {

  public static Config config = new Config();
  public static String lon_name = config.GetLongitudePropertyName();
  public static String lat_name = config.GetLatitudePropertyName();

  public String GeoReachTypeName = config.getGeoReachTypeName();
  public String reachGridName = config.getReachGridName();
  public String rmbrName = config.getRMBRName();
  public String geoBName = config.getGeoBName();
  public String reachGridListName = config.getReachGridListName();

  public double minx, miny, maxx, maxy;
  public int pieces_x, pieces_y;
  public int MAX_HOP;
  public double resolutionX, resolutionY;
  public SpaceManager spaceManager;

  // MGRatio and MRRatio
  public double MG, MR;
  public int MC;

  public String dbPath;
  public GraphDatabaseService service;

  // Experiment variable
  public int visitedCount;

  public Maintenance(double minx, double miny, double maxx, double maxy, int piecesX, int piecesY,
      int MAX_HOP, double MG, Double MR, int MC, GraphDatabaseService service) {
    spaceManager = new SpaceManager(minx, miny, maxx, maxy, piecesX, piecesY);
    this.MAX_HOP = MAX_HOP;
    this.MG = MG;
    this.MR = MR;
    this.MC = MC;
    this.service = service;
  }

  public Maintenance(SpaceManager spaceManager, int MAX_HOP, double MG, double MR, int MC,
      GraphDatabaseService service) {
    this(spaceManager.getMinx(), spaceManager.getMiny(), spaceManager.getMaxx(),
        spaceManager.getMaxy(), spaceManager.getPiecesX(), spaceManager.getPiecesY(), MAX_HOP, MG,
        MR, MC, service);
  }

  public void addEdgeAndUpdateIndexLightweight(Node src, Node trg) throws Exception {
    visitedCount = 0;
    src.createRelationshipTo(trg, GraphRel.GRAPH_LINK);
    updateOneDirectionAddEdge(src, trg);
    updateOneDirectionAddEdge(trg, src);
  }

  /**
   * Update GeoReach index of src using trg. Assume the edge is added from <code>src</code> to
   * <code>trg</code>.
   *
   * @param src
   * @param trg
   * @throws Exception
   */
  public void updateOneDirectionAddEdge(Node src, Node trg) throws Exception {
    HashMap<Integer, UpdateUnit> updateUnits = createUpdateUnit(trg);
    /**
     * Get the minimum hop that has update potential. It can reduce the neighbor search boundary.
     */
    int minHop = 0;
    for (; minHop <= MAX_HOP - 1; minHop++) {
      UpdateUnit updateUnit = updateUnits.get(minHop);
      // continue to next hop because it does not have the update potential.
      if (updateUnit.type.equals(GeoReachType.GeoB) && !updateUnit.geoB) {
        continue;
      }
      // this hop has the update potential. It determines that the graph search boundary is B -
      // minHop - 1
      break;
    }

    HashMap<Node, HashSet<Integer>> currentUpdateNodes = new HashMap<>();
    // The hop index updated on the src
    HashSet<Integer> srcUpdateHops = new HashSet<>();
    for (int i = minHop + 1; i <= MAX_HOP; i++) {
      srcUpdateHops.add(i);
    }
    currentUpdateNodes.put(src, srcUpdateHops);
    int dist = 0;
    // hop on src to be updated
    for (int hop = MAX_HOP; hop >= minHop + 1; hop--) {
      visitedCount += currentUpdateNodes.size();
      HashMap<Node, HashSet<Integer>> nextUpdateNodes = new HashMap<>();
      for (Node node : currentUpdateNodes.keySet()) {
        HashSet<Integer> curSrcupdateHops = currentUpdateNodes.get(node);
        update(node, updateUnits, curSrcupdateHops, dist, nextUpdateNodes);
      }
      currentUpdateNodes = nextUpdateNodes;
      dist++;
    }
  }

  /**
   * Create the update unit using [0, B-1] GeoReach on <code>node</code>.
   *
   * @param node
   * @throws Exception
   */
  private HashMap<Integer, UpdateUnit> createUpdateUnit(Node node) throws Exception {
    HashMap<Integer, UpdateUnit> updateUnits = new HashMap<>();
    // Neo4jGraphUtility.printNode(node);
    // handle the SIP(node, 0)
    if (node.hasProperty(lon_name)) {
      double lon = (Double) node.getProperty(lon_name);
      double lat = (Double) node.getProperty(lat_name);
      int id = spaceManager.getId(lon, lat);
      RoaringBitmap roaringBitmap = new RoaringBitmap();
      roaringBitmap.add(id);
      MyRectangle rmbr = new MyRectangle(lon, lat, lon, lat);

      GeoReachType geoReachType = GeoReachType.ReachGrid;
      if (!validateMG(roaringBitmap, spaceManager.getXYBoundary(roaringBitmap))) {
        geoReachType = GeoReachType.RMBR;
        if (!validateMR(rmbr)) {
          geoReachType = GeoReachType.GeoB;
        }
      }

      updateUnits.put(0,
          new UpdateUnit(geoReachType, roaringBitmap.toMutableRoaringBitmap(), rmbr, true));
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
          break;
      }
      updateUnits.put(hop, new UpdateUnit(type, immutableRoaringBitmap, rmbr, geoB));
    }
    return updateUnits;
  }

  /**
   * Update a node for all the required hops.
   *
   * @param node
   * @param updateUnits
   * @param srcUpdateHops the required hops to be updated on node
   * @param nextUpdateNodes
   * @throws Exception
   */
  public void update(Node node, HashMap<Integer, UpdateUnit> updateUnits,
      HashSet<Integer> srcUpdateHops, int dist, HashMap<Node, HashSet<Integer>> nextUpdateNodes)
      throws Exception {
    for (int srcUpdateHop : srcUpdateHops) {
      UpdateStatus updateStatus = update(node, updateUnits, srcUpdateHop, dist);
      if (!updateStatus.equals(UpdateStatus.NotUpdateInside) && (srcUpdateHop < MAX_HOP)) {
        Iterable<Relationship> iterable = node.getRelationships(Labels.GraphRel.GRAPH_LINK);
        Iterator<Relationship> iterator = iterable.iterator();
        while (iterator.hasNext()) {
          Relationship relationship = iterator.next();
          Node neighbor = relationship.getOtherNode(node);
          if (!nextUpdateNodes.containsKey(neighbor)) {
            nextUpdateNodes.put(neighbor, new HashSet<>());
          }
          int nextUpdateHop = srcUpdateHop + 1;
          if (!nextUpdateNodes.get(neighbor).contains(nextUpdateHop)) {
            nextUpdateNodes.get(neighbor).add(nextUpdateHop);
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
   * @param srcUpdateHop the hop on <code>src</code>
   * @return
   * @throws Exception
   */
  public UpdateStatus update(Node src, HashMap<Integer, UpdateUnit> updateUnits, int srcUpdateHop,
      int dist) throws Exception {
    UpdateStatus status = null;
    GeoReachType geoReachType = getGeoReachType(src, srcUpdateHop);
    int trgHopInUnit = srcUpdateHop - dist - 1;
    // the hop in unit that is used to update the src
    UpdateUnit updateUnit = updateUnits.get(trgHopInUnit);
    switch (geoReachType) {
      case ReachGrid:
        ImmutableRoaringBitmap irb = getReachGrid(src, srcUpdateHop);
        RoaringBitmap rb = new RoaringBitmap(irb);
        int[] xyBoundary = spaceManager.getXYBoundary(rb);
        status = updateReachGridWithReachGrid(rb, updateUnit.reachGrid, xyBoundary);
        // If any new cell is added, check the MG.
        if (!status.equals(UpdateStatus.NotUpdateInside)
            && !status.equals(UpdateStatus.NotUpdateOnBoundary)) {
          if (validateMG(rb, xyBoundary)) {
            // keep the GeoReach Type but modify ReachGrid value in db
            setReachGrid(src, srcUpdateHop, rb);
          } else {
            // validate MR
            removeGeoReach(src, GeoReachType.ReachGrid, srcUpdateHop);
            MyRectangle newRMBR = spaceManager.getMbrOfBoundary(xyBoundary);
            // remove ReachGrid, change GeoReachType_hop to GeoB, and set GeoB_hop to true
            if (validateMR(newRMBR)) {
              setGeoReachType(src, srcUpdateHop, GeoReachType.RMBR);
              src.setProperty(getGeoReachKey(GeoReachType.RMBR, srcUpdateHop), newRMBR.toString());
            } else {
              setGeoReachType(src, srcUpdateHop, GeoReachType.GeoB);
              src.setProperty(getGeoReachKey(GeoReachType.GeoB, srcUpdateHop), true);
            }
          }
        }
        break;
      case RMBR:
        MyRectangle srcRect = getRMBR(src, srcUpdateHop);
        status = srcRect.MBR(updateUnit.rmbr);
        if (status.equals(UpdateStatus.UpdateOutside)) {
          if (validateMR(srcRect)) {
            src.setProperty(getGeoReachKey(GeoReachType.RMBR, srcUpdateHop), srcRect.toString());
          } else {
            removeGeoReach(src, GeoReachType.RMBR, srcUpdateHop);
            setGeoReachType(src, srcUpdateHop, GeoReachType.GeoB);
            src.setProperty(getGeoReachKey(GeoReachType.GeoB, srcUpdateHop), true);
          }
        }
        break;
      case GeoB:
        status = updateGeoB(src, srcUpdateHop, updateUnit);
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
      if (!rb.contains(id)) {
        rb.add(id);
        if (!hasDiff) {
          hasDiff = true;
        }
      }
      // update spatial relation status
      BoundaryLocationStatus boundaryStatus = Util.locate(boundary, xyId);
      status = updateBoundaryLocationStatus(status, boundaryStatus);
    }
    return getUpdateStatus(status, hasDiff);
  }

  /**
   * Update the GeoB of src.
   *
   * @param src
   * @param srcUpdateHop
   * @param updateUnit
   * @return
   * @throws Exception
   */
  private UpdateStatus updateGeoB(Node src, int srcUpdateHop, UpdateUnit updateUnit)
      throws Exception {
    boolean srcGeoB = getGeoB(src, srcUpdateHop);
    if (srcGeoB) { // srcGeoB = true, it never requires update
      return UpdateStatus.NotUpdateInside;
    }
    switch (updateUnit.type) {
      case GeoB:
        if (updateUnit.geoB) {
          src.setProperty(getGeoReachKey(GeoReachType.GeoB, srcUpdateHop), true);
          return UpdateStatus.UpdateOutside;
        }
        break;
      case RMBR:
        setGeoReachType(src, srcUpdateHop, GeoReachType.RMBR);
        src.setProperty(getGeoReachKey(GeoReachType.RMBR, srcUpdateHop),
            updateUnit.rmbr.toString());
        removeGeoReach(src, GeoReachType.GeoB, srcUpdateHop);
        break;
      default:
        setGeoReachType(src, srcUpdateHop, GeoReachType.ReachGrid);
        src.setProperty(getGeoReachKey(GeoReachType.ReachGrid, srcUpdateHop), updateUnit.rbString);
        // debug
        // src.setProperty(reachGridListName + "_" + srcUpdateHop,
        // ArrayUtil.iterableToList(updateUnit.reachGrid).toString());

        removeGeoReach(src, GeoReachType.GeoB, srcUpdateHop);
    }
    return UpdateStatus.UpdateOutside;
  }

  /**
   * Decide whether <code>rb</code> is valid and no need to downgrade to rmbr.
   *
   * @param rb
   * @param boundary
   * @return <code>true</code> means needs to be replaced by rmbr
   */
  public boolean validateMG(RoaringBitmap rb, int[] boundary) {
    // # of cells in rb <= # of cells in the coverage MBR * MG
    return GeoReachIndexUtil.validateMG(boundary[4], boundary[0], boundary[1], boundary[2],
        boundary[3], MG);
    // return (boundary[4] <= (boundary[2] - boundary[0] + 1) * (boundary[3] - boundary[1] + 1) *
    // MG);
  }

  /**
   * Decide whether a rectangle is valid.
   *
   * @param rectangle
   * @return
   */
  public boolean validateMR(MyRectangle rectangle) {
    // area of rectangle > totalArea * MR
    return GeoReachIndexUtil.validateMR(rectangle, spaceManager.getTotalArea(), MR);
    // return rectangle.area() <= spaceManager.getTotalArea() * MR;
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

  public void setGeoReachType(Node node, int hop, GeoReachType type) {
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
    }
    node.setProperty(GeoReachTypeName + "_" + hop, setType);
  }

  public void removeGeoReach(Node node, GeoReachType geoReachType, int hop) {
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
  public String getGeoReachKey(GeoReachType geoReachType, int hop) {
    switch (geoReachType) {
      case ReachGrid:
        return String.format("%s_%d", reachGridName, hop);
      case RMBR:
        return String.format("%s_%d", rmbrName, hop);
      default:
        return String.format("%s_%d", geoBName, hop);
    }
  }

  /**
   * Assume that Type of hop is ReachGrid and the property exists.
   *
   * @param node
   * @param hop
   * @return
   * @throws Exception
   */
  public ImmutableRoaringBitmap getReachGrid(Node node, int hop) throws Exception {
    String property = reachGridName + "_" + hop;
    String rbString = (String) Neo4jGraphUtility.getNodeProperty(node, property);
    return Util.getImmutableRoaringBitmap(rbString);
  }

  public void setReachGrid(Node node, int hop, RoaringBitmap rb) throws Exception {
    String string = Util.roarBitmapSerializeToString(rb);
    node.setProperty(getGeoReachKey(GeoReachType.ReachGrid, hop), string);
    // debug
    // node.setProperty(reachGridListName + "_" + hop, ArrayUtil.iterableToList(rb).toString());
  }

  public MyRectangle getRMBR(Node node, int hop) throws Exception {
    String property = rmbrName + "_" + hop;
    return new MyRectangle(Neo4jGraphUtility.getNodeProperty(node, property).toString());
  }

  public boolean getGeoB(Node node, int hop) throws Exception {
    String property = geoBName + "_" + hop;
    return (boolean) Neo4jGraphUtility.getNodeProperty(node, property);
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
      default: // INSIDE
        if (hasDiff) {
          return UpdateStatus.UpdateInside;
        } else {
          return UpdateStatus.NotUpdateInside;
        }
    }
  }

  private static BoundaryLocationStatus updateBoundaryLocationStatus(BoundaryLocationStatus status,
      BoundaryLocationStatus cur) {
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
    }
    return status;
  }

  /**
   * Always call this to shutdown the dbservice.
   */
  public void shutdown() {
    service.shutdown();
  }
}
