package query;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.Queue;

import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Relationship;

import commons.Config;
import commons.MyRectangle;
import commons.Util;

public class SpaTraversal {
	
	public static Config config = new Config();
	public String lon_name = config.GetLongitudePropertyName();
	public String lat_name = config.GetLatitudePropertyName();
	
	public GraphDatabaseService dbservice;
	
	//result
	public ArrayList<LinkedList<Long>> paths;
	
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
		
		ArrayList<HashSet<Long>> prunedVertices = new ArrayList<>(length - 1);
		for ( int i = 0; i < length -1; i++)
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
					if (Util.Location_In_Rect(lat, lat, queryRectangle)) 
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
			if (curHop >= 1 && prunedVertices.get(curHop-1).contains(id))
				return;
			
			// if cannot satisfy the GeoReach validation
			if (validate(node, length - curHop) == false)
				return;
			
			curPath.add(id);
			Util.Print(curPath);
			Iterable<Relationship> rels = node.getRelationships();
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
	
	public boolean validate(Node node, int distance)
	{
		return true;
	}
}
