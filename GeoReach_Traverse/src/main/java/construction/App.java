package construction;

import java.util.ArrayList;
import commons.EnumVariables.UpdateStatus;
import commons.Util;

/**
 * Hello world!
 *
 */
public class App {
  public static void main(String[] args) {
    double MG = 1.0;
    Util.println(String.format("%f", MG));
    Util.println(String.format("%s", String.valueOf(MG)));
  }

  public static void test(UpdateStatus status) {
    status = UpdateStatus.UpdateOutside;
    Util.println(status);
  }

  public static void test(ArrayList<Integer> arrayList) {
    // arrayList = new ArrayList<>();
    arrayList.add(0);
  }
}
