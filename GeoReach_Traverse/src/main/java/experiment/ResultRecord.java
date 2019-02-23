package experiment;

public class ResultRecord {
  public long runTime;
  public long dbTime;
  public long checkTime;
  public long visitedCount;
  public long resultCount;
  public long GeoReachPrunedCount;
  public long HistoryPrunedCount;

  public ResultRecord(long runTime, long visitetCount, long resultCount, long GeoReachPrunedCount,
      long HistoryPrunedCount) {
    this.runTime = runTime;
    this.resultCount = resultCount;
    this.GeoReachPrunedCount = GeoReachPrunedCount;
    this.HistoryPrunedCount = HistoryPrunedCount;
  }
}
