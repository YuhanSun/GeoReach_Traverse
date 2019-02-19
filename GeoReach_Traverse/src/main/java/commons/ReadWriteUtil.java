package commons;

import java.io.File;
import java.io.FileWriter;
import java.util.List;

public class ReadWriteUtil {
  public static <T> void WriteArray(String filename, List<T> arrayList) {
    FileWriter fileWriter = null;
    try {
      fileWriter = new FileWriter(new File(filename));
      for (T line : arrayList)
        fileWriter.write(line.toString() + "\n");
      fileWriter.close();
    } catch (Exception e) {
      e.printStackTrace();
    }
  }

}
