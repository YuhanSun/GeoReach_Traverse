package experiment;

import java.util.ArrayList;

import commons.Config;
import commons.Entity;
import commons.Util;
import commons.VertexGeoReach;
import commons.Config.system;
import construction.IndexConstruct;
import construction.Loader;

public class MG {
	Config config;
	String dataset, neo4j_version;
	system systemName;
	int MAX_HOPNUM;
	double minx, miny, maxx, maxy;
	
	ArrayList<ArrayList<Integer>> graph;
	ArrayList<Entity> entities;
	String dbPath, entityPath, mapPath, graphPath;
	
	public static void main(String[] args) {
		Config config = new Config();
		MG mg = new MG(config);
//		mg.generateIndex();
		mg.loadIndex();
	}
	
	public void generateIndex()
	{
		int pieces_x = 128, pieces_y = 128, MC = 0;
		int MR = 100;
		
		int format = 1;
		String suffix = "";
		if (format == 0)
			suffix = "list";
		else 
			suffix = "bitmap";
		
		String dir = "D:\\Ubuntu_shared\\GeoReachHop\\data";
		ArrayList<VertexGeoReach> index = IndexConstruct.ConstructIndex(graph, entities, 
				minx, miny, maxx, maxy, 
				pieces_x, pieces_y, MAX_HOPNUM);
		for (int MG = 0; MG <= 0; MG += 10) 
		{
			Util.Print("\nMG: " + MG);
			String indexPath = String.format("%s\\%s\\MG\\%d_%d_%d_%d_%d_%d_%s.txt",
					dir, dataset, pieces_x, pieces_y, MG, MR, MC, MAX_HOPNUM, suffix);
			Util.Print("Output index to " + indexPath);
			
			ArrayList<ArrayList<Integer>> typesList = IndexConstruct.generateTypeList(index, MAX_HOPNUM, 
					minx, miny, maxx, maxy, 
					pieces_x, pieces_y, MG/100.0, MR/100.0, MC);
			
			Util.outputGeoReach(index, indexPath, typesList, format);
		}
		
//		for (int MG = 1; MG <= 9; MG += 1) 
//		{
//			Util.Print("\nMG: " + MG);
//			String indexPath = String.format("%s\\%s\\MG\\%d_%d_%d_%d_%d_%d_%s.txt",
//					dir, dataset, pieces_x, pieces_y, MG, MR, MC, MAX_HOPNUM, suffix);
//			Util.Print("Output index to " + indexPath);
//			
//			ArrayList<ArrayList<Integer>> typesList = IndexConstruct.generateTypeList(index, MAX_HOPNUM, 
//					minx, miny, maxx, maxy, 
//					pieces_x, pieces_y, MG/100.0, MR/100.0, MC);
//			
//			Util.outputGeoReach(index, indexPath, typesList, format);
//		}
	}
	
	public void loadIndex()
	{
		int pieces_x = 128, pieces_y = 128, MC = 0;
		int MR = 100;
		
		int format = 1;
		String suffix = "";
		if (format == 0)
			suffix = "list";
		else 
			suffix = "bitmap";
		
		String dir = "D:\\Ubuntu_shared\\GeoReachHop\\data";
		
//		for (int MG = 0; MG <= 3; MG += 1) 
//		{
//			Util.Print("\nMG: " + MG);
//			Loader loader = new Loader(new Config());
//			
//			String indexPath = String.format("%s\\%s\\MG\\%d_%d_%d_%d_%d_%d_%s.txt",
//					dir, dataset, pieces_x, pieces_y, MG, MR, MC, MAX_HOPNUM, suffix);
//			
//			String dbPath = String.format("%s\\%s\\MG\\%s_%d_%d_%d_%d_%d_%d"
//					+ "\\data\\databases\\graph.db", 
//					dir, dataset, neo4j_version, pieces_x, pieces_y, MG, MR, MC, MAX_HOPNUM);
//			
//			Util.Print(String.format("Load from %s\nto %s", indexPath, dbPath));
//			loader.load(indexPath, dbPath);
//		}
		
		int MG = 4;
		{
			Util.Print("\nMG: " + MG);
			Loader loader = new Loader(new Config());
			
			String indexPath = String.format("%s\\%s\\MG\\%d_%d_%d_%d_%d_%d_%s.txt",
					dir, dataset, pieces_x, pieces_y, MG, MR, MC, MAX_HOPNUM, suffix);
			
			String dbPath = String.format("%s\\%s\\MG\\%s_%d_%d_%d_%d_%d_%d"
					+ "\\data\\databases\\graph.db", 
					dir, dataset, neo4j_version, pieces_x, pieces_y, MG, MR, MC, MAX_HOPNUM);
			
			Util.Print(String.format("Load from %s\nto %s", indexPath, dbPath));
			loader.load(indexPath, dbPath);
		}
		
	}
	
	public MG(Config config)
	{
		this.config = config;
		initParameters();
	}
	
	public void initParameters()
	{
		systemName = config.getSystemName();
		neo4j_version = config.GetNeo4jVersion();
		dataset = config.getDatasetName();
		MAX_HOPNUM = config.getMaxHopNum();
		switch (systemName) {
		case Ubuntu:
			dbPath = String.format("/home/yuhansun/Documents/GeoGraphMatchData/%s_%s/data/databases/graph.db", neo4j_version, dataset);
			entityPath = String.format("/mnt/hgfs/Ubuntu_shared/GeoMinHop/data/%s/entity.txt", dataset);
			graphPath = String.format("/mnt/hgfs/Ubuntu_shared/GeoMinHop/data/%s/graph.txt", dataset);
			break;
		case Windows:
			dbPath = String.format("D:\\Ubuntu_shared\\GeoMinHop\\data\\%s\\%s_%s\\data\\databases\\graph.db", 
					dataset, neo4j_version, dataset);
			entityPath = String.format("D:\\Ubuntu_shared\\GeoMinHop\\data\\%s\\entity.txt", dataset);
			graphPath = String.format("D:\\Ubuntu_shared\\GeoMinHop\\data\\%s\\graph.txt", dataset);
		default:
			break;
		}
		
		Util.Print("Read graph from " + graphPath);
		graph = Util.ReadGraph(graphPath);
		
		Util.Print("Read entity from " + entityPath);
		entities = Util.ReadEntity(entityPath);
		
		if (dataset.contains("Gowalla") || dataset.contains("Yelp")
				|| dataset.contains("foursquare"))
		{
			minx = -180;
			miny = -90;
			maxx = 180;
			maxy = 90; 
		}
		if (dataset.contains("Patents") || dataset.contains("go_uniprot"))
		{
			minx = 0;
			miny = 0;
			maxx = 1000;
			maxy = 1000;
		}

	}
}
