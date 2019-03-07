package construction;

import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Relationship;
import org.roaringbitmap.RoaringBitmap;
import org.roaringbitmap.buffer.ImmutableRoaringBitmap;
import org.roaringbitmap.buffer.MutableRoaringBitmap;
import commons.Config;
import commons.EnumVariables.BoundaryLocationStatus;
import commons.EnumVariables.GeoReachType;
import commons.EnumVariables.MaintenanceStrategy;
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
  public HashMap<Integer, Integer> reconstructCount = new HashMap<>();

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

  private boolean isSpatialNode(Node node) {
    return node.hasProperty(lon_name);
  }

  private double getLongitude(Node node) {
    return (double) node.getProperty(lon_name);
  }

  private double getLatitude(Node node) {
    return (double) node.getProperty(lat_name);
  }

  public void addEdgeAndUpdateIndex(Node src, Node trg, MaintenanceStrategy strategy)
      throws Exception {
    switch (strategy) {
      case LIGHTWEIGHT:
        addEdgeAndUpdateIndexLightweight(src, trg);
        break;
      case RECONSTRUCT:
        addEdgeAndUpdateIndexReconstruct(src, trg);
        break;
      default:
        break;
    }
  }

  public void addEdgeAndUpdateIndexReconstruct(Node src, Node trg) throws Exception {
    visitedCount = 0;
    src.createRelationshipTo(trg, GraphRel.GRAPH_LINK);
    reconstruct(src, trg);
    reconstruct(trg, src);
  }

  private void reconstruct(Node src, Node trg) throws Exception {
    HashMap<Integer, UpdateUnit> updateUnits = createUpdateUnit(trg, MAX_HOP);
    List<GeoReachType> types = getGeoReachTypes(trg);
    // B-hop type is not needed, while 0-hop is. To make it consistent with updateUnits.
    types.remove(types.size() - 1);
    if (updateUnits.get(0).geoB) {
      types.add(0, GeoReachType.RMBR);
    } else {
      types.add(0, GeoReachType.GeoB);
    }

    int reconstructMaxHop = MAX_HOP;
    for (; reconstructMaxHop > 0; reconstructMaxHop--) {
      if (isReconstructedNeeded(src, reconstructMaxHop, updateUnits, types)) {
        break;
      }
    }
    if (reconstructMaxHop > 0) {
      int curCount = reconstructCount.getOrDefault(reconstructMaxHop, 0);
      reconstructCount.put(reconstructMaxHop, curCount + 1);
    }
    // debug
    // Util.println("reconstruct max hop: " + reconstructMaxHop);

    // replace the updateUnits with accurate value
    HashMap<Integer, UpdateUnit> geoReachAfterInsertion =
        reconstructGeoReach(src, reconstructMaxHop);
    for (int key : geoReachAfterInsertion.keySet()) {
      updateUnits.put(key, geoReachAfterInsertion.get(key));
    }

    HashMap<Node, HashSet<Integer>> nextUpdateNodes = new HashMap<>();
    HashSet<Integer> srcUpdateHops = new HashSet<>();
    for (int hop = 1; hop <= MAX_HOP; hop++) {
      srcUpdateHops.add(hop);
    }
    update(src, updateUnits, srcUpdateHops, -1, nextUpdateNodes);

    HashMap<Node, HashSet<Integer>> currentUpdateNodes = nextUpdateNodes;

    int dist = 0; // distance to the trg node
    // hop on trg to be updated
    for (; dist < MAX_HOP - 1;) {
      visitedCount += currentUpdateNodes.size();
      nextUpdateNodes = new HashMap<>();
      for (Node node : currentUpdateNodes.keySet()) {
        HashSet<Integer> curSrcUpdateHops = currentUpdateNodes.get(node);
        update(node, updateUnits, curSrcUpdateHops, dist, nextUpdateNodes);
      }
      currentUpdateNodes = nextUpdateNodes;
      dist++;
    }
  }

  private boolean isReconstructedNeeded(Node src, int srcHop,
      HashMap<Integer, UpdateUnit> updateUnits, List<GeoReachType> types) throws Exception {
    int trgHop = srcHop - 1;
    if (trgHop < 0) {
      throw new Exception("target hop < 0");
    }
    if (getGeoReachType(src, srcHop).equals(GeoReachType.ReachGrid)
        && types.get(trgHop).equals(GeoReachType.RMBR)
        && updateUnits.get(trgHop).reachGrid.getCardinality() > 1) {
      return true;
    }
    return false;
  }

  // private HashMap<Integer, UpdateUnit> isNodeGeoReachModified(Node node,
  // HashMap<Integer, UpdateUnit> geoReachAfterInsertion) throws Exception {
  // HashMap<Integer, UpdateUnit> updateUnits = new HashMap<>();
  // for (int hop = 1; hop <= MAX_HOP; hop++) {
  // GeoReachType type = getGeoReachType(node, hop);
  // UpdateUnit unit = geoReachAfterInsertion.get(hop);
  // switch (type) {
  // case ReachGrid:
  // ImmutableRoaringBitmap irbPrev = getReachGrid(node, hop);
  // ImmutableRoaringBitmap irbAfter = unit.reachGrid;
  // ImmutableRoaringBitmap irbDiff = ImmutableRoaringBitmap.andNot(irbAfter, irbPrev);
  // UpdateUnit diffUnit = new UpdateUnit(null, irbDiff, unit.rmbr, unit.geoB);
  // updateUnits.put(hop, diffUnit);
  // break;
  //
  // default:
  // break;
  // }
  // }
  // return updateUnits;
  // }

  // /**
  // * Reconstruct GeoReach for [0, B] hops.
  // *
  // * @param trg
  // * @return
  // */
  // private HashMap<Integer, UpdateUnit> reconstructGeoReach(Node trg) {
  // return reconstructGeoReach(trg, MAX_HOP);
  // }

  private HashMap<Integer, UpdateUnit> reconstructGeoReach(Node node, int bound) {
    HashMap<Integer, UpdateUnit> updateUnits = new HashMap<>();
    Collection<Node> curLevelNodes = new HashSet<>();
    curLevelNodes.add(node);
    for (int i = 0; i <= bound; i++) {
      UpdateUnit unit = constructUnit(curLevelNodes);
      updateUnits.put(i, unit);

      Collection<Node> nextLevelNodes = new HashSet<>();
      for (Node curNode : curLevelNodes) {
        Iterable<Relationship> relationships = curNode.getRelationships();
        for (Relationship relationship : relationships) {
          Node neighbor = relationship.getOtherNode(curNode);
          nextLevelNodes.add(neighbor);
        }
      }
      curLevelNodes = nextLevelNodes;
    }
    return updateUnits;
  }

  /**
   * Construct the UpdateUnit for a collection of nodes. Type is not important in current case. But
   * may need to be modified if used for other purpose.
   *
   * @param nodes
   * @return
   */
  private UpdateUnit constructUnit(Collection<Node> nodes) {
    MutableRoaringBitmap rb = new MutableRoaringBitmap();
    MyRectangle rmbr = null;
    boolean GeoB = false;
    for (Node node : nodes) {
      if (isSpatialNode(node)) {
        double lon = getLongitude(node);
        double lat = getLatitude(node);
        int id = spaceManager.getId(lon, lat);
        rb.add(id);
        if (rmbr == null) {
          rmbr = new MyRectangle(lon, lat, lon, lat);
        } else {
          rmbr.MBR(new MyRectangle(lon, lat, lon, lat));
        }
        GeoB = true;
      }
    }
    switch (rb.getCardinality()) {
      case 0:
        return new UpdateUnit(GeoReachType.GeoB, null, null, GeoB);
      case 1:
        return new UpdateUnit(GeoReachType.RMBR, rb, rmbr, GeoB);
      default:
        return new UpdateUnit(GeoReachType.ReachGrid, rb, rmbr, GeoB);
    }
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
    int dist = 0; // distance to the src node
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
    return createUpdateUnit(node, MAX_HOP - 1);
  }

  private HashMap<Integer, UpdateUnit> createUpdateUnit(Node node, int max_hop) throws Exception {
    HashMap<Integer, UpdateUnit> updateUnits = new HashMap<>();
    // Neo4jGraphUtility.printNode(node);
    // handle the SIP(node, 0)
    if (isSpatialNode(node)) {
      double lon = getLongitude(node);
      double lat = getLatitude(node);
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
    for (int hop = 1; hop <= max_hop; hop++) {
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
   * @param srcUpdateHops
   * @param dist
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
    // Debug
    // Util.println("trghop: " + trgHopInUnit);
    // Util.println("dist: " + dist);
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
        if (Util.rectLocatedInRect(updateUnit.rmbr, srcRect)) {
          return UpdateStatus.NotUpdateInside;
        }
        ImmutableRoaringBitmap irbSrc = spaceManager.getCoverIdOfRectangle(srcRect);
        int[] xyBoundarySrc = spaceManager.getXYBoundary(srcRect);
        RoaringBitmap rbSrc = new RoaringBitmap(irbSrc);
        UpdateStatus reachGridStatus =
            updateReachGridWithReachGrid(rbSrc, updateUnit.reachGrid, xyBoundarySrc);
        if (!reachGridStatus.equals(UpdateStatus.NotUpdateInside)
            && !reachGridStatus.equals(UpdateStatus.NotUpdateOnBoundary)) {
          if (validateMG(rbSrc, xyBoundarySrc)) {
            // change the GeoReach Type to reachgrid and modify ReachGrid value in db
            removeGeoReach(src, GeoReachType.RMBR, srcUpdateHop);
            setGeoReachType(src, srcUpdateHop, GeoReachType.ReachGrid);
            setReachGrid(src, srcUpdateHop, rbSrc);
            return reachGridStatus;
          } else {
            status = srcRect.MBR(updateUnit.rmbr);
            if (validateMR(srcRect)) {
              src.setProperty(getGeoReachKey(GeoReachType.RMBR, srcUpdateHop), srcRect.toString());
            } else {
              removeGeoReach(src, GeoReachType.RMBR, srcUpdateHop);
              setGeoReachType(src, srcUpdateHop, GeoReachType.GeoB);
              src.setProperty(getGeoReachKey(GeoReachType.GeoB, srcUpdateHop), true);
            }
            return status;
          }
        }
        return reachGridStatus;
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
      if (boundaryStatus.equals(BoundaryLocationStatus.OUTSIDE)) {
        Util.extendBoundary(boundary, xyId);
      }
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
        } else {
          return UpdateStatus.NotUpdateInside;
        }
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
   * Get the GeoReach types of [1, B].
   *
   * @param node
   * @return
   * @throws Exception
   */
  public List<GeoReachType> getGeoReachTypes(Node node) throws Exception {
    List<GeoReachType> types = new LinkedList<>();
    for (int hop = 1; hop <= MAX_HOP; hop++) {
      types.add(getGeoReachType(node, hop));
    }
    return types;
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
    return GeoReachIndexUtil.getGeoReachType(type);
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

  /**
   * Assume that Type of hop is RMBR and the property exists.
   *
   * @param node
   * @param hop
   * @return
   * @throws Exception
   */
  public MyRectangle getRMBR(Node node, int hop) throws Exception {
    String property = rmbrName + "_" + hop;
    return new MyRectangle(Neo4jGraphUtility.getNodeProperty(node, property).toString());
  }

  /**
   * Assume that Type of hop is GeoB and the property exists.
   *
   * @param node
   * @param hop
   * @return
   * @throws Exception
   */
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
