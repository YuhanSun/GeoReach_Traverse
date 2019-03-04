package construction;

import org.roaringbitmap.buffer.MutableRoaringBitmap;
import commons.Util;

/**
 * Hello world!
 *
 */
public class App {
  public static void main(String[] args) {
    Util.println("here");
    bitmapAndNotTest();
  }

  public static void bitmapAndNotTest() {
    int arr1[] = {10, 20, 30};
    int arr2[] = {20, 25, 30, 40, 50};
    MutableRoaringBitmap b1 = new MutableRoaringBitmap();
    for (int e : arr1) {
      b1.add(e);
    }

    MutableRoaringBitmap b2 = new MutableRoaringBitmap();
    for (int e : arr2) {
      b2.add(e);
    }

    Util.println(MutableRoaringBitmap.andNot(b1, b2));
  }

  public static void formatTest() {
    double MG = 1.0;
    Util.println(String.format("%f", MG));
    Util.println(String.format("%s", String.valueOf(MG)));
  }

  // public static void test(UpdateStatus status) {
  // status = UpdateStatus.UpdateOutside;
  // Util.println(status);
  // }
  //
  // public static void test(ArrayList<Integer> arrayList) {
  // // arrayList = new ArrayList<>();
  // arrayList.add(0);
  // }

}
