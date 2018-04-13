package construction;

import java.util.ArrayList;
import java.util.TreeSet;

import commons.Entity;
import commons.MyRectangle;
import commons.VertexGeoReach;

public class IndexConstruct {

	
	
	
	
	
	
	
	
	
	
	
	
	
	
	
	
	
	
	
	/**
	 * 
	 * @param graph
	 * @param entities
	 * @param MG Threshold for degrading ReachGrid to RMBR.
	 * @param MR Threshold for degrading RMBR to GeoB.
	 * @param MC Threshold for merging grid cells.
	 * @param outputPath
	 */
	public void ConstructIndex(ArrayList<ArrayList<Integer>> graph, ArrayList<Entity> entities, 
			double minx, double miny, double maxx, double maxy, 
			int pieces_x, int pieces_y,
			double MG, double MR, int MC, int MAX_HOP, String outputPath) {
		int nodeCount = graph.size();
		double resolution_x = (maxx - minx) / pieces_x;
		double resolution_y = (maxy - miny) / pieces_y;
		ArrayList<VertexGeoReach> index = new ArrayList<VertexGeoReach>(nodeCount);
		for (int i = 0; i < nodeCount; i++)
		{
			VertexGeoReach vertexGeoReach = new VertexGeoReach(MAX_HOP);
			index.add(vertexGeoReach);
		}
		
//		TreeSet<Integer> expandVertices = new TreeSet<Integer>();
//		int i = 0;
//		for (Entity entity : entities)
//		{
//			if (entity.IsSpatial)
//			{
//				int idX = (int) ((entity.lon - minx) / resolution_x);
//				int idY = (int) ((entity.lat - miny) / resolution_y);
//				idX = Math.min(pieces_x - 1, idX);
//				idY = Math.min(pieces_y - 1, idY);
//				
//				int gridID = idX * pieces_x + idY;
//				for (int neighborID : graph.get(i))
//				{
//					expandVertices.add(neighborID);
//				}
//			}
//			i++;
//		}
		
		int i = 0;
		for (ArrayList<Integer> neighbors : graph)
		{
			VertexGeoReach vertexGeoReach = index.get(i);
			for (int neighborID : neighbors)
			{
				Entity entity = entities.get(neighborID);
				if (entity.IsSpatial)
				{
					int idX = (int) ((entity.lon - minx) / resolution_x);
					int idY = (int) ((entity.lat - miny) / resolution_y);
					idX = Math.min(pieces_x - 1, idX);
					idY = Math.min(pieces_y - 1, idY);
					int gridID = idX * pieces_x + idY;
					
					if (vertexGeoReach.ReachGrids.get(0) == null)
					{
						TreeSet<Integer> reachgrid = new TreeSet<Integer>();
						reachgrid.add(gridID);
						vertexGeoReach.ReachGrids.set(0, reachgrid);
					}
					else
						vertexGeoReach.ReachGrids.get(0).add(gridID);
					
					if (vertexGeoReach.RMBRs.get(0) == null)
					{
						MyRectangle mbr = new MyRectangle(entity.lon, entity.lat, entity.lon, entity.lat);
						vertexGeoReach.RMBRs.set(0, mbr);
					}
					else
						vertexGeoReach.RMBRs.get(0).MBR(new MyRectangle(entity.lon, entity.lat, entity.lon, entity.lat));
				}
			}
			i++;
		}
		
		for (i = 0; i < MAX_HOP; i++)
		{
			
		}
	}
	
	public static void main(String[] args) {
		// TODO Auto-generated method stub

	}

}
