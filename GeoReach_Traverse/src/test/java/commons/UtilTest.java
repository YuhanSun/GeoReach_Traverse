package commons;

import java.util.ArrayList;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Transaction;
import org.roaringbitmap.RoaringBitmap;
import org.roaringbitmap.buffer.ImmutableRoaringBitmap;

public class UtilTest {

  @BeforeClass
  public static void setUpBeforeClass() throws Exception {}

  @AfterClass
  public static void tearDownAfterClass() throws Exception {}

  @Test
  public void pathExistTest() {
    String path = "D:\\Ubuntu_shared\\GeReachHop\\data";
    Util.print(Util.pathExist(path));
  }

  @Test
  public void getNodesByIDsTest() {
    String dbPath =
        "/home/yuhansun/Documents/GeoReachHop/Gowalla_10/neo4j-community-3.1.1_128_128_100_100_0_3/data/databases/graph.db";
    GraphDatabaseService databaseService = Util.getDatabaseService(dbPath);
    Transaction tx = databaseService.beginTx();
    ArrayList<Long> ids = new ArrayList<>();
    ids.add((long) 1299706);
    ArrayList<Node> nodes = Util.getNodesByIDs(databaseService, ids);
    Util.print(nodes);
    for (Node node : nodes) {
      Util.print(node);
      Util.print(node.getAllProperties());
    }
    tx.success();
    tx.close();

    tx = databaseService.beginTx();
    Util.print(nodes);
    for (Node node : nodes) {
      Util.print(node);
      Util.print(node.getAllProperties());
    }
    tx.success();
    tx.close();

    databaseService.shutdown();
  }

  @Test
  public void getXYBoundaryTest() {
    int piecesX = 10, piecesY = 10;
    RoaringBitmap roaringBitmap = new RoaringBitmap();
    roaringBitmap.add(3);
    roaringBitmap.add(4);
    roaringBitmap.add(12);
    ImmutableRoaringBitmap immutableRoaringBitmap = roaringBitmap.toMutableRoaringBitmap();

    int[] boundary = Util.getXYBoundary(immutableRoaringBitmap, piecesX, piecesY);
    // Integer[] bound = new Integer[4];
    // for (int i = 0; i < bound.length; i++) {
    // bound[i] = boundary[i];
    // }
    // Util.print(bound);
    assert (boundary[0] == 0);
    assert (boundary[1] == 2);
    assert (boundary[2] == 1);
    assert (boundary[3] == 4);
  }

}
