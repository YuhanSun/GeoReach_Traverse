package experiment;

import java.util.ArrayList;

import commons.Config;
import commons.Entity;
import commons.Util;
import commons.VertexGeoReach;
import commons.Config.Datasets;
import commons.Config.system;
import construction.IndexConstruct;
import construction.Loader;

public class MR {
	Config config;
	String dataset, neo4j_version;
	system systemName;
	int MAX_HOPNUM;
	double minx, miny, maxx, maxy;
	
	ArrayList<ArrayList<Integer>> graph;
	ArrayList<Entity> entities;
	String dbPath, entityPath, mapPath, graphPath;
	
	int pieces_x = 128, pieces_y = 128, MG = 0, MC = 0;
	
	public static void main(String[] args) {
		Config config = new Config();
		config.setDatasetName(Datasets.Gowalla_10.name());
		MR mr = new MR(config);
		mr.generateIndex();
//		mr.loadIndex();
	}
	
	public void generateIndex()
	{
//		int format = 1;
//		String suffix = "";
//		if (format == 0)
//			suffix = "list";
//		else 
//			suffix = "bitmap";
		
		String dir = "D:\\Ubuntu_shared\\GeoReachHop\\data";
		
		ArrayList<VertexGeoReach> index = IndexConstruct.ConstructIndex(graph, entities, 
				minx, miny, maxx, maxy, 
				pieces_x, pieces_y, MAX_HOPNUM);
		
		for (int MR = 0; MR <= 10; MR += 2) 
		{
			Util.Print("\nMR: " + MR);
			
			ArrayList<ArrayList<Integer>> typesList = IndexConstruct.generateTypeList(index, MAX_HOPNUM, 
					minx, miny, maxx, maxy, 
					pieces_x, pieces_y, MG/100.0, MR/100.0, MC);
			
			int format = 1;
			String suffix = "bitmap";
			String indexPath = String.format("%s\\%s\\MR\\%d_%d_%d_%d_%d_%d_%s.txt",
					dir, dataset, pieces_x, pieces_y, MG, MR, MC, MAX_HOPNUM, suffix);
			Util.Print("Output index to " + indexPath);
			Util.outputGeoReach(index, indexPath, typesList, format);
			
			format = 0;
			suffix = "list";
			indexPath = String.format("%s\\%s\\MR\\%d_%d_%d_%d_%d_%d_%s.txt",
					dir, dataset, pieces_x, pieces_y, MG, MR, MC, MAX_HOPNUM, suffix);
			Util.Print("Output index to " + indexPath);
			Util.outputGeoReach(index, indexPath, typesList, format);
		}
	}
	
	public void loadIndex()
	{
		int format = 1;
		String suffix = "";
		if (format == 0)
			suffix = "list";
		else 
			suffix = "bitmap";
		
		String dir = "D:\\Ubuntu_shared\\GeoReachHop\\data";
		for (int MR = 0; MR <= 100; MR += 25) 
		{
			Util.Print("\nMR: " + MR);
			Loader loader = new Loader(new Config());
			
			String indexPath = String.format("%s\\%s\\MR\\%d_%d_%d_%d_%d_%d_%s.txt",
					dir, dataset, pieces_x, pieces_y, MG, MR, MC, MAX_HOPNUM, suffix);
			
			String dbPath = String.format("%s\\%s\\MR\\%s_%d_%d_%d_%d_%d_%d"
					+ "\\data\\databases\\graph.db", 
					dir, dataset, neo4j_version, pieces_x, pieces_y, MG, MR, MC, MAX_HOPNUM);
			
			Util.Print(String.format("Load from %s\nto %s", indexPath, dbPath));
			loader.load(indexPath, dbPath);
		}
	}
	
	public MR(Config config)
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
