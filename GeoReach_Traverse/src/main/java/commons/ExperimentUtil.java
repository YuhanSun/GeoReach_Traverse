package commons;

import java.util.ArrayList;
import java.util.List;
import experiment.ResultRecord;

public class ExperimentUtil {
  public static String getRectangleFileName(int spaCount, double selectivity) {
    return String.format("queryrect_%d.txt", (int) (spaCount * selectivity));
  }

  public static long getRuntimeAvg(List<ResultRecord> records) {
    List<Long> times = new ArrayList<>(records.size());
    for (ResultRecord record : records) {
      times.add(record.runTime);
    }
    return ArrayUtil.Average(times);
  }

  public static long getVisitedCountAvg(List<ResultRecord> records) {
    List<Long> times = new ArrayList<>(records.size());
    for (ResultRecord record : records) {
      times.add(record.visitedCount);
    }
    return ArrayUtil.Average(times);
  }

  public static long getGeoReachPrunedCountAvg(List<ResultRecord> records) {
    List<Long> times = new ArrayList<>(records.size());
    for (ResultRecord record : records) {
      times.add(record.GeoReachPrunedCount);
    }
    return ArrayUtil.Average(times);
  }

  public static long getHistoryPrunedCountAvg(List<ResultRecord> records) {
    List<Long> times = new ArrayList<>(records.size());
    for (ResultRecord record : records) {
      times.add(record.HistoryPrunedCount);
    }
    return ArrayUtil.Average(times);
  }

  public static long getResultCountAvg(List<ResultRecord> records) {
    List<Long> times = new ArrayList<>(records.size());
    for (ResultRecord record : records) {
      times.add(record.resultCount);
    }
    return ArrayUtil.Average(times);
  }
}
