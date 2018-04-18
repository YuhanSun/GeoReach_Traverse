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
import commons.MyRectangle;
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
	public void Neo4jTraversalTest() {
		try {
			Util.Print(db_path);
			SimpleGraphTraversal simpleGraphTraversal = new SimpleGraphTraversal(db_path);
//			long startID = 768514;
			long startID = 1299706;
			GraphDatabaseService dbservice = simpleGraphTraversal.dbservice;
			Transaction tx = dbservice.beginTx();
			Node node = dbservice.getNodeById(startID);
			Util.Print(node);
			Util.Print(node.getLabels());
			Util.Print(node.getAllProperties());
			String query = String.format("match p = (s)-[:GRAPH_LINK]-(a)-[:GRAPH_LINK]-(c)-[:GRAPH_LINK]-(b)"
					+ "where id(s) = %d and exists(b.%s) return p", 
					startID, "lon");
			Result result = dbservice.execute(query);
			int count = 0;
			while (result.hasNext())
			{
				result.next();
//				Util.Print(result.next());
				count++;
			}
			Util.Print(count);
			tx.success();
			tx.close();
			dbservice.shutdown();
		} catch (Exception e) {
			// TODO: handle exception
			e.printStackTrace();
		}
		
	}
	
	@Test
	public void TraversalTest() {
		try {
			Util.Print(db_path);
			SimpleGraphTraversal simpleGraphTraversal = new SimpleGraphTraversal(db_path);
			long startID = 1299706;
			GraphDatabaseService dbservice = simpleGraphTraversal.dbservice;
			Transaction tx = dbservice.beginTx();
			Node node = dbservice.getNodeById(startID);
			simpleGraphTraversal.traversal(node, 2, new MyRectangle(-180, -90, 180, 90));
			int count = simpleGraphTraversal.paths.size();
			Util.Print(count);
			tx.success();
			tx.close();
			dbservice.shutdown();
		} catch (Exception e) {
			// TODO: handle exception
			e.printStackTrace();
		}
	}
	
	@Test
	public void spaTreversalTest() {
		try {
			Util.Print(db_path);
			SpaTraversal spaTraversal = new SpaTraversal(db_path, 
					new MyRectangle(-180, -90, 180, 90), 128, 128);
			long startID = 1299706;
			GraphDatabaseService dbservice = spaTraversal.dbservice;
			Transaction tx = dbservice.beginTx();
			Node node = dbservice.getNodeById(startID);
			spaTraversal.traversal(node, 3, new MyRectangle(-180, -90, 180, 90));
			int count = spaTraversal.paths.size();
			Util.Print(count);
			tx.success();
			tx.close();
			dbservice.shutdown();
		} catch (Exception e) {
			// TODO: handle exception
			e.printStackTrace();
		}
	}
}
