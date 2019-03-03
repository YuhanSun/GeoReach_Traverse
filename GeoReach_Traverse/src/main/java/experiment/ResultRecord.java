package experiment;

public class ResultRecord {
  public long runTime;
  public long dbTime;
  public long checkTime;
  public long commitTime;
  public long visitedCount;
  public long resultCount;
  public long GeoReachPrunedCount;
  public long HistoryPrunedCount;

  public ResultRecord(long runTime, long visitetCount, long resultCount, long GeoReachPrunedCount,
      long HistoryPrunedCount) {
    this.runTime = runTime;
    this.visitedCount = visitetCount;
    this.resultCount = resultCount;
    this.GeoReachPrunedCount = GeoReachPrunedCount;
    this.HistoryPrunedCount = HistoryPrunedCount;
  }

  public ResultRecord(long runTime, long visitedCount, long resultCount) {
    this.runTime = runTime;
    this.visitedCount = visitedCount;
    this.resultCount = resultCount;
  }

  public ResultRecord(long runTime, long commitTime, long visitedCount, long resultCount) {
    this(runTime, visitedCount, resultCount);
    this.commitTime = commitTime;
  }

  @Override
  public String toString() {
    return String.format(
        "runTime:%d, visitedCount:%d, resultCount:%d, GeoReachPrunedCount:%d, HistoryPrunedCount:%d",
        runTime, visitedCount, resultCount, GeoReachPrunedCount, HistoryPrunedCount);
  }
}
