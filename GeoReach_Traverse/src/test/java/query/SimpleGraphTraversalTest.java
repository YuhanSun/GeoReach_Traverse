package query;

import static org.junit.Assert.*;

import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Result;
import org.neo4j.graphdb.Transaction;

import commons.Config;
import commons.Config.system;
import commons.Util;

public class SimpleGraphTraversalTest {

	static Config config = new Config();
	static String dataset = config.getDatasetName();
	static String version = config.GetNeo4jVersion();
	static system systemName = config.getSystemName();

	static String db_path;
	
	@BeforeClass
	public static void setUpBeforeClass() throws Exception {
		switch (systemName) {
		case Ubuntu:
			db_path = String.format("/home/yuhansun/Documents/GeoGraphMatchData/%s_%s/data/databases/graph.db", version, dataset);
			break;
		case Windows:
			String dataDirectory = "D:\\Ubuntu_shared\\GeoMinHop\\data";
			db_path = String.format("%s\\%s\\%s_%s\\data\\databases\\graph.db", dataDirectory, dataset, version, dataset);
		default:
			break;
		}
	}

	@AfterClass
	public static void tearDownAfterClass() throws Exception {
	}

	@Test
	public void TraversalTest() {
		SimpleGraphTraversal simpleGraphTraversal = new SimpleGraphTraversal(db_path);
		long startID = 0;
		GraphDatabaseService dbservice = simpleGraphTraversal.dbservice;
		Transaction tx = dbservice.beginTx();
		Node node = dbservice.getNodeById(startID);
		String query = "match p = (s)--(a)--(b) return p";
		Result result = dbservice.execute(query);
		int count = 0;
		while (result.hasNext())
		{
			Util.Print(result.next());
			count++;
		}
		tx.success();
		tx.close();
		dbservice.shutdown();
	}

}
