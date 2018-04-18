package query;

import java.io.File;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.LinkedList;

import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Relationship;
import org.neo4j.graphdb.factory.GraphDatabaseFactory;

import commons.Config;
import commons.MyRectangle;
import commons.Util;
import commons.Labels.GraphRel;

public class SpaTraversal {
	
	public static Config config = new Config();
	public String lon_name = config.GetLongitudePropertyName();
	public String lat_name = config.GetLatitudePropertyName();
	public int MAX_HOP = config.getMaxHopNum();
	
	static String GeoReachTypeName = config.getGeoReachTypeName();
	static String reachGridName = config.getReachGridName();
	static String rmbrName = config.getRMBRName();
	static String geoBName = config.getGeoBName();
	
	public GraphDatabaseService dbservice;
	
	//result
	public ArrayList<LinkedList<Long>> paths;
	
	public SpaTraversal(String db_path)
	{
		dbservice = new GraphDatabaseFactory().newEmbeddedDatabase(new File(db_path));
	}
	
	/**
	 * DFS way of traversal.
	 * @param node the start node
	 * @param hops number of hops
	 * @param queryRectangle the range predicate on the end node
	 */
	public void traversal(Node node, int length, MyRectangle queryRectangle)
	{
		paths = new ArrayList<LinkedList<Long>>();
		int curHop = 0;
		HashSet<Long> visited = new HashSet<Long>();
		
		ArrayList<HashSet<Long>> prunedVertices = new ArrayList<>(MAX_HOP);
		for ( int i = 0; i < MAX_HOP; i++)
			prunedVertices.add(new HashSet<>());
		
		helper(node, length, curHop, queryRectangle, visited, prunedVertices, new LinkedList<Long>());
	}
	
	/**
	 * 
	 * @param node start node
	 * @param length path length 
	 * @param curHop 
	 * @param queryRectangle
	 * @param visited current visited vertices 
	 * @param curPath
	 * @param paths
	 */
	public void helper(Node node, int length, int curHop, MyRectangle queryRectangle, 
			HashSet<Long> visited, ArrayList<HashSet<Long>> prunedVertices, LinkedList<Long> curPath)
	{
		long id = node.getId();
		if (visited.add(id))
		{
			if (curHop == length)
			{
				if (node.hasProperty(lon_name))
				{
					double lon = (Double) node.getProperty(lon_name);
					double lat = (Double) node.getProperty(lat_name);
					if (Util.Location_In_Rect(lon, lat, queryRectangle)) 
					{
						LinkedList<Long> path = new LinkedList<Long>(curPath);
						path.add(id);
						paths.add(path);
					}
				}
				visited.remove(id);
				return;
			}
			
			// node has been pruned at this query vertex under GeoReach validation
			int hop_offset = curHop - (length - MAX_HOP);
			Util.Print(hop_offset);
			if (hop_offset >= 0 && prunedVertices.get(hop_offset).contains(id))
				return;
			
			// if cannot satisfy the GeoReach validation
			if (validate(node, length - curHop, queryRectangle) == false)
				return;
			
			curPath.add(id);
			Util.Print(curPath);
			Iterable<Relationship> rels = node.getRelationships(GraphRel.GRAPH_LINK);
			for (Relationship relationship : rels)
			{
				Node neighbor = relationship.getOtherNode(node);
				helper(neighbor, length, curHop + 1, queryRectangle, 
						visited, prunedVertices, curPath);
			}
			visited.remove(id);
			curPath.removeLast();
		}
	}
	
	public boolean validate(Node node, int distance, MyRectangle queryRectangle)
	{
		try {
			int type = (int) node.getProperty(GeoReachTypeName + "_" + distance);
			switch (type) {
			case 0:
				
				break;
			case 1:
				MyRectangle rmbr = new MyRectangle(
						node.getProperty(rmbrName + "_" + distance).toString());
				if (Util.intersect(rmbr, queryRectangle))
					return true;
				else {
					return false;
				}
			case 2:
				boolean geoB = (boolean) node.getProperty(geoBName + "_" + distance);
				return geoB;
			default:
				throw new Exception(String.format("%s for %s is %d", 
						GeoReachTypeName + "_" +distance, node, type));
			}
		} catch (Exception e) {
			e.printStackTrace();
			System.exit(-1);
		}
		return true;
	}
}
