package construction;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.TreeSet;

import org.neo4j.graphdb.Node;

import commons.Config;
import commons.Entity;
import commons.MyRectangle;
import commons.Util;
import commons.VertexGeoReach;
import commons.Config.system;

public class IndexConstruct {

	static Config config = new Config();
	static String dataset, version;
	static system systemName;
	static int MAX_HOPNUM;
	
	static ArrayList<ArrayList<Integer>> graph;
	static ArrayList<Entity> entities;
	static String dbPath, entityPath, mapPath, graphPath;
	
	public static void construct()
	{
		// TODO Auto-generated method stub
		IndexConstruct indexConstruct = new IndexConstruct();
		if (entities == null)
			entities = Util.ReadEntity(entityPath);

		if (graph == null)
			graph = Util.ReadGraph(graphPath);

		Util.Print("graph size:  " + graph.size());
		double minx = -180, miny = -90, maxx = 180, maxy = 90; 
		int pieces_x = 128, pieces_y = 128, MC = 4;
		double MG = 0.80, MR = 0.80;
		String outputPath = String.format("D:\\Ubuntu_shared\\GeoReachHop\\data\\%s\\%d_%d_%d_%d_%d_%d.txt",
				dataset, pieces_x, pieces_y, MG * 100, MR * 100, MC, MAX_HOPNUM);
		indexConstruct.ConstructIndex(graph, entities, minx, miny, maxx, maxy, pieces_x, pieces_y, MG, MR, MC, MAX_HOPNUM, outputPath);
	}
	
	public static void main(String[] args) {
		construct();
//		getReachbleVertices();
	}
	
	public IndexConstruct()
	{
		initParameters();
	}
	
	public void initParameters()
	{
		systemName = config.getSystemName();
		version = config.GetNeo4jVersion();
		dataset = config.getDatasetName();
		MAX_HOPNUM = config.getMaxHopNum();
		switch (systemName) {
		case Ubuntu:
			dbPath = String.format("/home/yuhansun/Documents/GeoGraphMatchData/%s_%s/data/databases/graph.db", version, dataset);
			entityPath = String.format("/mnt/hgfs/Ubuntu_shared/GeoMinHop/data/%s/entity.txt", dataset);
			graphPath = String.format("/mnt/hgfs/Ubuntu_shared/GeoMinHop/data/%s/graph.txt", dataset);
			break;
		case Windows:
			dbPath = String.format("D:\\Ubuntu_shared\\GeoMinHop\\data\\%s\\%s_%s\\data\\databases\\graph.db", 
					dataset, version, dataset);
			entityPath = String.format("D:\\Ubuntu_shared\\GeoMinHop\\data\\%s\\entity.txt", dataset);
			graphPath = String.format("D:\\Ubuntu_shared\\GeoMinHop\\data\\%s\\graph.txt", dataset);
		default:
			break;
		}
	}
	
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
		
		long start = System.currentTimeMillis();
		
		/**
		 * Construct the 1-hop GeoReach.
		 * It will be stored at index 0.
		 */
		int id = 0;
		for (ArrayList<Integer> neighbors : graph)
		{
			VertexGeoReach vertexGeoReach = index.get(id);
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
					
//					Util.Print(vertexGeoReach.ReachGrids.size());
					TreeSet<Integer> reachgrid = vertexGeoReach.ReachGrids.get(0);
					if (reachgrid == null)
					{
						reachgrid = new TreeSet<>();
						reachgrid.add(gridID);
						vertexGeoReach.ReachGrids.set(0, reachgrid);	
					}
					else
						reachgrid.add(gridID);
					
					MyRectangle rmbr = vertexGeoReach.RMBRs.get(0);
					if (rmbr == null)
					{
						rmbr = new MyRectangle(entity.lon, entity.lat, entity.lon, entity.lat);
						vertexGeoReach.RMBRs.set(0, rmbr);
					}
					else
						rmbr.MBR(new MyRectangle(entity.lon, entity.lat, entity.lon, entity.lat));
				}
			}
			id++;
		}
		
		/**
		 * 2-hop and more hops
		 */
		for (int i = 1; i < MAX_HOP; i++)
		{
			id = 0;
			for (ArrayList<Integer> neighbors : graph)
			{
				VertexGeoReach vertexGeoReach = index.get(id);
				TreeSet<Integer> targetReachGrid = vertexGeoReach.ReachGrids.get(i);
				MyRectangle targetRMBR = vertexGeoReach.RMBRs.get(i);
				for (int neighborID : neighbors)
				{
					VertexGeoReach neighborGeoReach = index.get(neighborID);
					TreeSet<Integer> reachgrid = neighborGeoReach.ReachGrids.get(i-1);
					if (reachgrid != null)
					{
						if (targetReachGrid == null)
						{
							targetReachGrid = new TreeSet<>(reachgrid);
							vertexGeoReach.ReachGrids.set(i, reachgrid);	
						}
						else
							targetReachGrid.addAll(reachgrid);
					}
					
					MyRectangle rmbr = neighborGeoReach.RMBRs.get(i-1);
					if (rmbr != null)
					{
						if (targetRMBR == null)
						{
							targetRMBR = new MyRectangle(rmbr);
							vertexGeoReach.RMBRs.set(i, rmbr);
						}
						else
							rmbr.MBR(rmbr);
						
					}
					
//					updateReachGrid(vertexGeoReach.ReachGrids.get(i), neighborGeoReach.ReachGrids.get(i-1));
//					updateRMBR(vertexGeoReach.RMBRs.get(i), neighborGeoReach.RMBRs.get(i-1));
				}
				id++;
			}
		}
		long time = System.currentTimeMillis() - start;
		Util.Print(time);
		Util.outputGeoReach(index, outputPath);
	}
	
	/**
	 * This part is the baseline for correctness proof
	 */
	
	public static void getReachbleVertices()
	{
		/**
		 * setting up
		 */
		IndexConstruct indexConstruct = new IndexConstruct();
		double minx = -180, miny = -90, maxx = 180, maxy = 90; 
		int pieces_x = 128, pieces_y = 128;
		double resolution_x = (maxx - minx) / pieces_x;
		double resolution_y = (maxy - miny) / pieces_y;
		
		int startID = 0, hops = 1;
		if (graph == null) 
			graph = Util.ReadGraph(graphPath);
		if (entities == null)
			entities = Util.ReadEntity(entityPath);
		
		HashSet<Integer> curList = new HashSet<>(), nextList = new HashSet<>();
		curList.add(startID);
		for ( int i = 0; i < hops; i++)
		{
			Util.Print(i);
			for (int id : curList)
			{
				ArrayList<Integer> neighbors = graph.get(id);
				for (int neighborID : neighbors)
					nextList.add(neighborID);
			}
			
			Util.Print("hop: " + i);
			Util.Print("reachable vertices:\t" + nextList + "\tsize: " + nextList.size());
			
			TreeSet<Integer> spatialVertices = new TreeSet<>();
			for (int id : nextList)
				if (entities.get(id).IsSpatial)
					spatialVertices.add(id);
			Util.Print("reachable spatial vertices:\t" + spatialVertices + "\tsize: " + spatialVertices.size());
			
			TreeSet<Integer> reachgrids = new TreeSet<>();
			for ( int id : spatialVertices)
			{
				Entity entity = entities.get(id);
				int idX = (int) ((entity.lon - minx) / resolution_x);
				int idY = (int) ((entity.lat - miny) / resolution_y);
				idX = Math.min(pieces_x - 1, idX);
				idY = Math.min(pieces_y - 1, idY);
				int gridID = idX * pieces_x + idY;
				reachgrids.add(gridID);
			}
			Util.Print("reachable grids:\t" + reachgrids + "\tsize:\t" + reachgrids.size());
			
			curList = nextList;
			nextList = new HashSet<>();
		}
	}
	
//	public TreeSet<Integer> updateReachGrid(TreeSet<Integer> target, TreeSet<Integer> source)
//	{
//		if (source == null)
//			return;
//		else
//		{
//			if (target == null)
//				target = new TreeSet<>(source);
//			else
//				target.addAll(source);
//		}
//	}
//	
//	public void updateRMBR(MyRectangle target, MyRectangle source)
//	{
//		if (source == null)
//			return;
//		else
//		{
//			if (target == null)
//				target = new MyRectangle(source);
//			else
//				target.MBR(source);
//		}
//	}
}
