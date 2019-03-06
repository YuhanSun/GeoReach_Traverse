package construction;

import org.roaringbitmap.buffer.MutableRoaringBitmap;
import commons.EnumVariables.UpdateStatus;
import commons.MyRectangle;
import commons.SpaceManager;
import commons.Util;

/**
 * Hello world!
 *
 */
public class App {
  public static void main(String[] args) {
    test();
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

  public static void test(UpdateStatus status) {
    status = UpdateStatus.UpdateOutside;
    Util.println(status);
  }

  public static void test() {
    double minx = -180, miny = -90, maxx = 180, maxy = 90;
    int pieces_x = 128, pieces_y = 128;

    SpaceManager spaceManager = new SpaceManager(minx, miny, maxx, maxy, pieces_x, pieces_y);
    String string = "(-73.5835215, 45.5232245, -73.5835215, 45.5232245)";
    Util.println(string);
    MyRectangle rectangle = new MyRectangle(string);
    Util.println(spaceManager.getCoverIdOfRectangle(rectangle));

    string = "(-88.2361709, 40.1103909, -88.2361709, 40.1103909)";
    Util.println(string);
    rectangle = new MyRectangle(string);
    Util.println(spaceManager.getCoverIdOfRectangle(rectangle));

    string = "(-88.243876, 40.1065381, -88.195252, 40.117741911204)";
    Util.println(string);
    rectangle = new MyRectangle(string);
    Util.println(spaceManager.getCoverIdOfRectangle(rectangle));

    string = "(-80.003366, 40.448827, -80.003366, 40.448827)";
    Util.println(string);
    rectangle = new MyRectangle(string);
    Util.println(spaceManager.getCoverIdOfRectangle(rectangle));

  }

}
