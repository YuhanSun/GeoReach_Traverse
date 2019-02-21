package commons;

import commons.EnumVariables.Datasets;
import commons.EnumVariables.system;

public class Config {
  public Config() {

  }

  private String SERVER_ROOT_URI = "http://localhost:7474/db/data";

  private String longitude_property_name = "lon";
  private String latitude_property_name = "lat";
  private String password = "syh19910205";

  // attention here, these settings change a lot
  private String neo4j_version = "neo4j-community-3.1.1";
  private system operatingSystem = system.Windows;
  private String dataset = Datasets.Yelp.name();

  // Project dir: experiment query and result
  private String projectDir =
      operatingSystem.equals(system.Windows) ? "D:\\Google_Drive\\Projects\\GeoReachHop"
          : "/mnt/hgfs/Google_Drive/Projects/GeoReachHop";

  // data dir: graph entity data for construction usage
  private String dataDir =
      operatingSystem.equals(system.Windows) ? "D:\\Ubuntu_shared\\GeoMinHop\\data"
          : "/mnt/hgfs/Ubuntu_shared/GeoMinHop/data";

  // dbDir: database and index dir
  private String dbDir =
      operatingSystem.equals(system.Windows) ? "D:\\Ubuntu_shared\\GeoReachHop\\data"
          : "/home/yuhansun/Documents/GeoReachHop";

  private int MAX_HOPNUM = 3;
  private int MAX_HMBR_HOPNUM = 3;
  private int nonspatial_label_count = 100;

  private String Rect_minx_name = "minx";
  private String Rect_miny_name = "miny";
  private String Rect_maxx_name = "maxx";
  private String Rect_maxy_name = "maxy";

  private String GeoReachTypeName = "GeoReachType";
  private String reachGridName = "ReachGrid";
  private String rmbrName = "RMBR";
  private String geoBName = "GeoB";

  private String graphFileName = "graph.txt";
  private String entityFileName = "entity.txt";
  private String labelListFileName = "label.txt";

  private String edgeFileName = "edges.txt";

  public String getEdgeFileName() {
    return edgeFileName;
  }

  public void setEdgeFileName(String edgeFileName) {
    this.edgeFileName = edgeFileName;
  }

  public String getGraphFileName() {
    return graphFileName;
  }

  public void setGraphFileName(String graphFileName) {
    this.graphFileName = graphFileName;
  }

  public String getEntityFileName() {
    return entityFileName;
  }

  public void setEntityFileName(String entityFileName) {
    this.entityFileName = entityFileName;
  }

  public String getLabelListFileName() {
    return labelListFileName;
  }

  public void setLabelListFileName(String labelListFileName) {
    this.labelListFileName = labelListFileName;
  }


  public void setDatasetName(String pName) {
    this.dataset = pName;
  }

  public void setMAXHOPNUM(int pMAXHOPNUM) {
    this.MAX_HOPNUM = pMAXHOPNUM;
  }

  public String GetServerRoot() {
    return SERVER_ROOT_URI;
  }

  public String GetLongitudePropertyName() {
    return longitude_property_name;
  }

  public String GetLatitudePropertyName() {
    return latitude_property_name;
  }

  public String[] GetRectCornerName() {
    String[] rect_corner_name = new String[4];
    rect_corner_name[0] = this.Rect_minx_name;
    rect_corner_name[1] = this.Rect_miny_name;
    rect_corner_name[2] = this.Rect_maxx_name;
    rect_corner_name[3] = this.Rect_maxy_name;
    return rect_corner_name;
  }

  public String GetNeo4jVersion() {
    return neo4j_version;
  }

  public int getMaxHopNum() {
    return MAX_HOPNUM;
  }

  public int getMaxHMBRHopNum() {
    return MAX_HMBR_HOPNUM;
  }

  public system getSystemName() {
    return operatingSystem;
  }

  public String getDatasetName() {
    return dataset;
  }

  public String getPassword() {
    return password;
  }

  public int getNonSpatialLabelCount() {
    return nonspatial_label_count;
  }

  public String getReachGridName() {
    return reachGridName;
  }

  public String getRMBRName() {
    return rmbrName;
  }

  public String getGeoBName() {
    return geoBName;
  }

  public String getProjectDir() {
    return projectDir;
  }

  public String getDataDir() {
    return dataDir;
  }

  public String getGeoReachTypeName() {
    return GeoReachTypeName;
  }

  public String getDBDir() {
    return dbDir;
  }
}
