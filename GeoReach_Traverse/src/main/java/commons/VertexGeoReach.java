package commons;

import java.util.ArrayList;
import java.util.TreeSet;

/**
 * GeoReach index for each vertex
 * @author ysun138
 *
 */
public class VertexGeoReach {
	public ArrayList<Integer> types;
	public ArrayList<TreeSet<Integer>> ReachGrids;
	public ArrayList<MyRectangle> RMBRs;
	public ArrayList<Boolean> GeoBs;
	
	public VertexGeoReach(int MAX_HOP)
	{
		ReachGrids = new ArrayList<TreeSet<Integer>>(MAX_HOP);
		RMBRs = new ArrayList<MyRectangle>(MAX_HOP);
		GeoBs = new ArrayList<Boolean>(MAX_HOP);
		for (int i = 0; i < MAX_HOP; i++)
		{
			ReachGrids.add(null);
			RMBRs.add(null);
			GeoBs.add(null);
		}
	}
}
