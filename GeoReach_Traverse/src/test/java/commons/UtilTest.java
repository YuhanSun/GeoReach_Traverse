package commons;

import static org.junit.Assert.*;

import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

public class UtilTest {

	@BeforeClass
	public static void setUpBeforeClass() throws Exception {
	}

	@AfterClass
	public static void tearDownAfterClass() throws Exception {
	}

	@Test
	public void pathExistTest() {
		String path = "D:\\Ubuntu_shared\\GeReachHop\\data";
		Util.Print(Util.pathExist(path));
	}

}
