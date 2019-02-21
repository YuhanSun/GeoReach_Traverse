package commons;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import org.roaringbitmap.RoaringBitmap;

public class ArrayUtilTest {
  @BeforeClass
  public static void setUpBeforeClass() {

  }

  @AfterClass
  public static void tearDownAfterClass() {

  }

  @Test
  public void isSortedListEqualTest() {
    List<Integer> l1 = new ArrayList<>(Arrays.asList(0, 1));
    List<Integer> l2 = new ArrayList<>(Arrays.asList(0, 1));
    assertTrue(ArrayUtil.isSortedListEqual(l1, l2));
    l2.add(3);
    assertFalse(ArrayUtil.isSortedListEqual(l1, l2));
  }

  @Test
  public void roaringBitmapTest() {
    RoaringBitmap rBitmap = new RoaringBitmap();
    rBitmap.add(-100);
    rBitmap.add(-10);
    Util.println(rBitmap);
    Iterator<Integer> iterator = rBitmap.iterator();
    while (iterator.hasNext()) {
      Util.println(iterator.next());
    }
    // rBitmap.add(50);
    // Util.println(rBitmap);
    // rBitmap.add();
    // Util.println(rBitmap);

  }
}
