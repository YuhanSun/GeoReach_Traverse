package query;

import static org.junit.Assert.*;

import java.awt.List;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.TreeSet;

import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import org.neo4j.cypher.internal.frontend.v2_3.ast.functions.Length;
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

	static String dbDir = config.getDBDir();
	static String db_path;
	
	@BeforeClass
	public static void setUpBeforeClass() throws Exception {
		switch (systemName) {
		case Ubuntu:
//			db_path = String.format("%s/%s_%s/data/databases/graph.db", dbDir, version, dataset);
			break;
		case Windows:
//			db_path = String.format("%s\\%s\\neo4j-community-3.1.1_128_128_0_100_0_3\\data\\databases\\graph.db", 
//					dbDir, dataset);
			db_path = String.format("%s\\%s\\neo4j-community-3.1.1_128_128_100_100_0_3\\data\\databases\\graph.db", 
					dbDir, dataset);
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
			GraphDatabaseService dbservice = simpleGraphTraversal.dbservice;
			Transaction tx = dbservice.beginTx();
			Node node = dbservice.getNodeById(startID);
			Util.Print(node);
			Util.Print(node.getLabels());
			Util.Print(node.getAllProperties());
			String query = String.format("profile match p = (s)-[:GRAPH_LINK]-(a)-[:GRAPH_LINK]-(c)-[:GRAPH_LINK]-(b) "
					+ "where id(s) = %d and %s < b.lon < %s and "
					+ "%s < b.lat < %s "
					+ "return p", 
					startID, String.valueOf(queryRectangle.min_x), String.valueOf(queryRectangle.max_x),
					String.valueOf(queryRectangle.min_y), String.valueOf(queryRectangle.max_y));
//			String query = String.format("match p = (s)-[:GRAPH_LINK]-(a)-[:GRAPH_LINK]-(c)-[:GRAPH_LINK]-(b) "
//					+ "where id(s) = %d and -120 < b.lon < -60 and "
//					+ "30 < b.lat < 40 "
//					+ "return p", 
//					startID, "lon");
			Result result = dbservice.execute(query);
			int count = 0;
			while (result.hasNext())
			{
				result.next();
//				Util.Print(result.next());
				count++;
			}
			Util.Print(count);
			
			long hits = Util.GetTotalDBHits(result.getExecutionPlanDescription());
			Util.Print("DB hits: " + hits);
			
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
			GraphDatabaseService dbservice = simpleGraphTraversal.dbservice;
			Transaction tx = dbservice.beginTx();
			Node node = dbservice.getNodeById(startID);
//			simpleGraphTraversal.traversal(node, 2, new MyRectangle(-120, 30, -60, 40));
			simpleGraphTraversal.traversal(node, Length, queryRectangle);
			
//			int count = simpleGraphTraversal.paths.size();
			long count = simpleGraphTraversal.resultCount;
			Util.Print(count);
			
			long visitedCount = simpleGraphTraversal.visitedCount;
			Util.Print("Visited Count: " + visitedCount);
			
			tx.success();
			tx.close();
			dbservice.shutdown();
		} catch (Exception e) {
			// TODO: handle exception
			e.printStackTrace();
		}
	}
	
	//The test query
	public static long startID = 1299708;
	ArrayList<Long> startIDs = new ArrayList<Long>(Arrays.asList(
			startID
			));
	public static int Length = 3;
	public static MyRectangle queryRectangle = new MyRectangle(-70, 30, -60, 40);
	
	@Test
	public void spaTreversalTest() {
		try {
			Util.Print(db_path);
			SpaTraversal spaTraversal = new SpaTraversal(db_path, 2, 
					new MyRectangle(-180, -90, 180, 90), 128, 128);
			GraphDatabaseService dbservice = spaTraversal.dbservice;
			Transaction tx = dbservice.beginTx();
			Node node = dbservice.getNodeById(startID);
//			spaTraversal.traversal(node, 2, new MyRectangle(-120, 30, -60, 40));
			
//			startIDs.add((long) 1299708);
			LinkedList<Node> startNodes = new LinkedList<>();
			for (long id : startIDs)
				startNodes.add(dbservice.getNodeById(id));
			
			spaTraversal.traversal(startNodes, Length, queryRectangle);
			
			Util.Print(spaTraversal.resultCount);
			Util.Print("Visited Count: " + spaTraversal.visitedCount);
			Util.Print("GeoReachPrunedCount: " + spaTraversal.GeoReachPruneCount);
			Util.Print("PrunedVerticesWorkCount: " + spaTraversal.PrunedVerticesWorkCount);
			
			tx.success();
			tx.close();
			dbservice.shutdown();
		} catch (Exception e) {
			// TODO: handle exception
			e.printStackTrace();
		}
	}
	
//	@Test
//	public void compareTest() {
//		try {
//			Util.Print(db_path);
//			SpaTraversal spaTraversal = new SpaTraversal(db_path, 
//					new MyRectangle(-180, -90, 180, 90), 128, 128);
//			long startID = 1299706;
//			GraphDatabaseService dbservice = spaTraversal.dbservice;
//			Transaction tx = dbservice.beginTx();
//			Node node = dbservice.getNodeById(startID);
//			spaTraversal.traversal(node, 2, new MyRectangle(-120, 30, -60, 40));
//			int count = spaTraversal.paths.size();
//			Util.Print(count);
//			
//			TreeSet<String> pathsStringSet1 = createPathStringList(spaTraversal.paths);
//			Util.WriteFile("D:\\temp\\spa.txt", false, pathsStringSet1);
//			
//			tx.success();
//			tx.close();
//			dbservice.shutdown();
//			
//			//
//			SimpleGraphTraversal simpleGraphTraversal = new SimpleGraphTraversal(db_path);
//			dbservice = simpleGraphTraversal.dbservice;
//			tx = dbservice.beginTx();
//			node = dbservice.getNodeById(startID);
//			simpleGraphTraversal.traversal(node, 2, new MyRectangle(-120, 30, -60, 40));
//			count = simpleGraphTraversal.paths.size();
//			Util.Print(count);
//			
//			TreeSet<String> pathsStringSet2 = createPathStringList(simpleGraphTraversal.paths);
//			Util.WriteFile("D:\\temp\\simple.txt", false, pathsStringSet2);
//			
//			tx.success();
//			tx.close();
//			dbservice.shutdown();
//			
//			//compare
//			Util.Print("Traverse not in Spa:");
//			for (String path : pathsStringSet2)
//			{
//				if (pathsStringSet1.contains(path) == false)
//					Util.Print(path);
//			}
//			
//			Util.Print("Spa not in Traverse:");
//			for (String path : pathsStringSet1)
//			{
//				if (pathsStringSet2.contains(path) == false)
//					Util.Print(path);
//			}
//			
//		} catch (Exception e) {
//			// TODO: handle exception
//			e.printStackTrace();
//		}
//	}
	
	static public TreeSet<String> createPathStringList(ArrayList<LinkedList<Long>> paths) 
	{
		TreeSet<String> pathsStringSet = new TreeSet<>();
		
		for (LinkedList<Long> path : paths)
			pathsStringSet.add(path.toString());
		
		return pathsStringSet;
	}
}
