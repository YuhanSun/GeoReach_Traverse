package query;

import java.util.LinkedList;
import java.util.Queue;

import org.neo4j.graphdb.Node;

import commons.MyRectangle;

public class SpaTraversal {
	/**
	 * DFS way of traversal.
	 * @param node the start node
	 * @param hops number of hops
	 * @param queryRectangle the range predicate on the end node
	 */
	public void traversal(Node node, int length, MyRectangle queryRectangle)
	{
		int curHop = 0;
		helper(node, curHop, queryRectangle);
	}
	
	public void helper(Node node, int curHop, MyRectangle queryRectangle)
	{
		
	}
}
