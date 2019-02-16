package commons;

import java.util.ArrayList;
import commons.EnumVariables.GeoReachType;

/**
 * GeoReach index for each vertex ReachGrids is list.
 * 
 * @author ysun138
 *
 */
public class VertexGeoReachList {
  // public ArrayList<Integer> types;
  public ArrayList<ArrayList<Integer>> ReachGrids;
  public ArrayList<MyRectangle> RMBRs;
  public ArrayList<Boolean> GeoBs;

  public VertexGeoReachList(int MAX_HOP) {
    ReachGrids = new ArrayList<ArrayList<Integer>>(MAX_HOP);
    RMBRs = new ArrayList<MyRectangle>(MAX_HOP);
    GeoBs = new ArrayList<Boolean>(MAX_HOP);
    for (int i = 0; i < MAX_HOP; i++) {
      ReachGrids.add(null);
      RMBRs.add(null);
      GeoBs.add(null);
    }
  }

  public Object getGeoReach(int hop, GeoReachType type) {
    switch (type) {
      case ReachGrid:
        return ReachGrids.get(hop);
      case RMBR:
        return RMBRs.get(hop);
      default:
        return GeoBs.get(hop);
    }
  }

  @Override
  public String toString() {
    String string = "";
    for (int i = 0; i < ReachGrids.size(); i++) {
      string += ReachGrids.get(i);
      string += ";" + RMBRs.get(i);
      string += ";" + GeoBs.get(i);
      string += "\n";
    }
    return string;
  }
}
