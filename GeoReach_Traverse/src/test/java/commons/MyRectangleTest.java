package commons;

import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import commons.EnumVariables.UpdateStatus;

public class MyRectangleTest {

  @BeforeClass
  public static void setUpBeforeClass() throws Exception {}

  @AfterClass
  public static void tearDownAfterClass() throws Exception {}

  @Test
  public void MBRTest() {
    MyRectangle rectangle = new MyRectangle(0, 0, 1.0, 1.0);
    UpdateStatus status = rectangle.MBR(new MyRectangle(0, 0, 0.5, 0.5));
    assert (status.equals(UpdateStatus.NotUpdateInside));
    assert (Math.abs(rectangle.min_x - 0) < 0.00000001);
    assert (Math.abs(rectangle.min_y - 0) < 0.00000001);
    assert (Math.abs(rectangle.max_x - 1) < 0.00000001);
    assert (Math.abs(rectangle.max_y - 1) < 0.00000001);

    rectangle = new MyRectangle(0, 0, 1.0, 1.0);
    status = rectangle.MBR(new MyRectangle(0, 0, 1.0, 1.0));
    assert (status.equals(UpdateStatus.NotUpdateInside));
    assert (Math.abs(rectangle.min_x - 0) < 0.00000001);
    assert (Math.abs(rectangle.min_y - 0) < 0.00000001);
    assert (Math.abs(rectangle.max_x - 1) < 0.00000001);
    assert (Math.abs(rectangle.max_y - 1) < 0.00000001);

    rectangle = new MyRectangle(0, 0, 1.0, 1.0);
    status = rectangle.MBR(new MyRectangle(0.1, 0.1, 1.0, 1.0));
    assert (status.equals(UpdateStatus.NotUpdateInside));
    assert (Math.abs(rectangle.min_x - 0) < 0.00000001);
    assert (Math.abs(rectangle.min_y - 0) < 0.00000001);
    assert (Math.abs(rectangle.max_x - 1) < 0.00000001);
    assert (Math.abs(rectangle.max_y - 1) < 0.00000001);

    rectangle = new MyRectangle(0, 0, 1.0, 1.0);
    status = rectangle.MBR(new MyRectangle(0, 0, 2.0, 2.0));
    assert (status.equals(UpdateStatus.UpdateOutside));
    assert (Math.abs(rectangle.min_x - 0) < 0.00000001);
    assert (Math.abs(rectangle.min_y - 0) < 0.00000001);
    assert (Math.abs(rectangle.max_x - 2) < 0.00000001);
    assert (Math.abs(rectangle.max_y - 2.0) < 0.00000001);

    rectangle = new MyRectangle(0, 0, 1.0, 1.0);
    status = rectangle.MBR(new MyRectangle(-1, -1, 2.0, 2.0));
    assert (status.equals(UpdateStatus.UpdateOutside));
    assert (Math.abs(rectangle.min_x - -1) < 0.00000001);
    assert (Math.abs(rectangle.min_y - -1) < 0.00000001);
    assert (Math.abs(rectangle.max_x - 2) < 0.00000001);
    assert (Math.abs(rectangle.max_y - 2) < 0.00000001);

    rectangle = new MyRectangle(0, 0, 1.0, 1.0);
    status = rectangle.MBR(new MyRectangle(2, 2, 3, 3));
    assert (status.equals(UpdateStatus.UpdateOutside));
    assert (Math.abs(rectangle.min_x - 0) < 0.00000001);
    assert (Math.abs(rectangle.min_y - 0) < 0.00000001);
    assert (Math.abs(rectangle.max_x - 3) < 0.00000001);
    assert (Math.abs(rectangle.max_y - 3) < 0.00000001);
  }

}
