package query;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.LinkedList;

import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Relationship;

import commons.Config;
import commons.MyRectangle;
import commons.Util;

public class SimpleGraphTraversal {
	
	public static Config config = new Config();
	public String lon_name = config.GetLongitudePropertyName();
	public String lat_name = config.GetLatitudePropertyName();
	/**
	 * DFS way of traversal.
	 * @param node the start node
	 * @param hops number of hops
	 * @param queryRectangle the range predicate on the end node
	 */
	public void traversal(Node node, int length, MyRectangle queryRectangle)
	{
		int curHop = 0;
		HashSet<Long> visited = new HashSet<Long>();
		ArrayList<LinkedList<Long>> paths = new ArrayList<LinkedList<Long>>();
		helper(node, length, curHop, queryRectangle, visited, new LinkedList<Long>(), paths);
	}
	
	public void helper(Node node, int length, int curHop, MyRectangle queryRectangle, 
			HashSet<Long> visited, LinkedList<Long> curPath, ArrayList<LinkedList<Long>> paths)
	{
		long id = node.getId();
		if (visited.add(id))
		{
			if (visited.size() == length)
			{
				if (node.hasProperty(lon_name))
				{
					double lon = (Double) node.getProperty(lon_name);
					double lat = (Double) node.getProperty(lat_name);
					if (Util.Location_In_Rect(lat, lat, queryRectangle)) {
						LinkedList<Long> path = new LinkedList<Long>(curPath);
						path.add(id);
						paths.add(path);
					}
				}
				return;
			}
			
			curPath.add(id);
			Iterable<Relationship> rels = node.getRelationships();
			for (Relationship relationship : rels)
			{
				Node neighbor = relationship.getOtherNode(node);
				helper(neighbor, length, curHop + 1, queryRectangle, 
						visited, curPath, paths);
			}
			visited.remove(node.getId());
		}
	}
}
