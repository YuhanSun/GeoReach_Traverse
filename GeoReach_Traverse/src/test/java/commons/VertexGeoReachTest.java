package commons;

import static org.junit.Assert.*;

import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

public class VertexGeoReachTest {

	@BeforeClass
	public static void setUpBeforeClass() throws Exception {
	}

	@AfterClass
	public static void tearDownAfterClass() throws Exception {
	}

	@Test
	public void constructorTest() {
		VertexGeoReach vertexGeoReach = new VertexGeoReach(3);
		Util.Print(vertexGeoReach.ReachGrids.size());
		Util.Print(vertexGeoReach.ReachGrids.get(1));
	}

}
