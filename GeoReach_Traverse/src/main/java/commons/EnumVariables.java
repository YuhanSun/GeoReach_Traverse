package commons;

public class EnumVariables {
  public static enum system {
    Ubuntu, Windows
  }

  public static enum Explain_Or_Profile {
    Explain, Profile, Nothing
  }

  public static enum Datasets {
    Patents_100_random_80, Patents_100_random_60, Patents_100_random_40, Patents_100_random_20,

    Patents_10_random_20, Patents_10_random_80, Patents_1_random_80, Patents_1_random_20, Patents_2_random_80, go_uniprot_100_random_80, foursquare, foursquare_10, foursquare_100, Gowalla, Gowalla_10, Gowalla_100, Gowalla_25, Gowalla_50, Yelp, Yelp_10, Yelp_100,

    wikidata, wikidata_2,
  }

  public static enum UpdateStatus {
    UpdateInside, // only for ReachGrid
    UpdateOnBoundary, // only for ReachGrid
    UpdateOutside, // update outside current boundary (for all three)
    NotUpdateInside, // no update (for all three)
    NotUpdateOnBoundary, // no update but boundary grid is touched (for ReachGrid)
  }

  /**
   * The relative spatial relation.
   *
   * @author Yuhan Sun
   *
   */
  public static enum BoundaryLocationStatus {
    INSIDE, ONBOUNDARY, OUTSIDE,
  }

  public static enum GeoReachType {
    ReachGrid, RMBR, GeoB
  }

  public static enum GeoReachOutputFormat {
    BITMAP, LIST,
  }

  public static enum Expand {
    SPATRAVERSAL, SIMPLEGRAPHTRAVERSAL,
  }

  public static enum MaintenanceStrategy {
    LIGHTWEIGHT, RECONSTRUCT,
  }
}
