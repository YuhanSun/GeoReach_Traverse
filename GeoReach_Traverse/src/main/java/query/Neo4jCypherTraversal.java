package query;

import java.io.File;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.LinkedList;

import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Transaction;
import org.neo4j.graphdb.factory.GraphDatabaseFactory;

import commons.Config;
import commons.Labels.GraphRel;
import commons.MyRectangle;
import commons.Util;

public class Neo4jCypherTraversal {
	public static Config config = new Config();
	public String lon_name = config.GetLongitudePropertyName();
	public String lat_name = config.GetLatitudePropertyName();
	
	public GraphDatabaseService dbservice;
	
	//query related variables
	int length;
	MyRectangle queryRectangle;
	HashSet<Long> visited;
	LinkedList<Long> curPath;
//	public ArrayList<LinkedList<Long>> paths;
	
	//tracking variables
	public long resultCount, pageAccessCount;
	
	public Neo4jCypherTraversal(String db_path)
	{
		try {
			if (Util.pathExist(db_path))
				dbservice = new GraphDatabaseFactory().newEmbeddedDatabase(new File(db_path));
			else
				throw new Exception(db_path + "does not exist!");
		} catch (Exception e) {
			e.printStackTrace();
			System.exit(-1);
		}
	}
	
	public void traverse(ArrayList<Node> startNodes, int length, MyRectangle queryRectangle)
	{
		//query variables initialization
		this.length = length;
		this.queryRectangle = queryRectangle;

		//tracking variables initialization
		resultCount = 0;
		pageAccessCount = 0;

		Transaction tx = dbservice.beginTx();
		
		String query = "match p = (start)";
		for (int i = 0; i < length; i++)
		{
			query += String.format("-[:%s]-(a%d)", GraphRel.GRAPH_LINK.toString(), i);
		}
		query += String.format(" where id(start) in %s", startNodes);
		query += String.format(" and a%d, args)

		tx.success();
		tx.close();
	}
}
