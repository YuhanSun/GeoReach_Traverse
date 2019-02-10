package commons;

import static org.junit.Assert.assertEquals;
import java.util.Arrays;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import org.roaringbitmap.RoaringBitmap;

public class SpaceManagerTest {

  @BeforeClass
  public static void setUpBeforeClass() throws Exception {}

  @AfterClass
  public static void tearDownAfterClass() throws Exception {}

  @Test
  public void getXYIdTest() {
    /**
     * test the first function
     */
    SpaceManager spaceManager = new SpaceManager(0, 0, 0, 0, 10, 10);
    int[] xy = spaceManager.getXYId(0);
    assertEquals(0, xy[0]);
    assertEquals(0, xy[1]);

    xy = spaceManager.getXYId(3);
    assertEquals(0, xy[0]);
    assertEquals(3, xy[1]);

    xy = spaceManager.getXYId(23);
    assertEquals(2, xy[0]);
    assertEquals(3, xy[1]);

    xy = spaceManager.getXYId(39);
    assertEquals(3, xy[0]);
    assertEquals(9, xy[1]);

    /**
     * test the second function
     */
    spaceManager = new SpaceManager(0.1, 0.2, 1.6, 1.0, 3, 2);

    xy = spaceManager.getXYId(0.1, 0.2);
    assertEquals(0, xy[0]);
    assertEquals(0, xy[1]);

    xy = spaceManager.getXYId(0.2, 0.3);
    assertEquals(0, xy[0]);
    assertEquals(0, xy[1]);

    xy = spaceManager.getXYId(0.7, 0.7);
    assertEquals(1, xy[0]);
    assertEquals(1, xy[1]);

    xy = spaceManager.getXYId(1.6, 1.0);
    assertEquals(2, xy[0]);
    assertEquals(1, xy[1]);
  }

  @Test
  public void getIdTest() {
    SpaceManager spaceManager = new SpaceManager(0, 0, 1.0, 1.0, 10, 10);
    assertEquals(0, spaceManager.getId(0.0, 0.0));
    assertEquals(1, spaceManager.getId(0.0, 0.1));
    assertEquals(9, spaceManager.getId(0.0, 1.0));
    assertEquals(10, spaceManager.getId(0.1, 0.0));
    assertEquals(12, spaceManager.getId(0.11, 0.21));
    assertEquals(99, spaceManager.getId(1.0, 1.0));
  }

  @Test
  public void getXYBoundaryTest() {
    SpaceManager spaceManager = new SpaceManager(0, 0, 1.0, 1.0, 10, 10);

    RoaringBitmap roaringBitmap = new RoaringBitmap();
    roaringBitmap.add(3);
    roaringBitmap.add(4);
    roaringBitmap.add(12);
    int[] boundary = spaceManager
        .getXYBoundary(roaringBitmap.toMutableRoaringBitmap().toImmutableRoaringBitmap());
    assert (boundary[0] == 0);
    assert (boundary[1] == 2);
    assert (boundary[2] == 1);
    assert (boundary[3] == 4);

    roaringBitmap.add(19);
    boundary = spaceManager
        .getXYBoundary(roaringBitmap.toMutableRoaringBitmap().toImmutableRoaringBitmap());
    assert (boundary[0] == 0);
    assert (boundary[1] == 2);
    assert (boundary[2] == 1);
    assert (boundary[3] == 9);

    roaringBitmap.add(99);
    boundary = spaceManager
        .getXYBoundary(roaringBitmap.toMutableRoaringBitmap().toImmutableRoaringBitmap());
    assert (boundary[0] == 0);
    assert (boundary[1] == 2);
    assert (boundary[2] == 9);
    assert (boundary[3] == 9);

    MyRectangle rectangle = new MyRectangle(0, 0, 1.0, 1.0);
    boundary = spaceManager.getXYBoundary(rectangle);
    Util.print(Arrays.toString(boundary));
    assert (boundary[0] == 0);
    assert (boundary[1] == 0);
    assert (boundary[2] == 9);
    assert (boundary[3] == 9);
  }

}