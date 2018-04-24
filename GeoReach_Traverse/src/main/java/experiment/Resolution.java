package experiment;

import java.util.ArrayList;
import java.util.HashMap;

import commons.Config;
import commons.Entity;
import commons.Util;
import commons.VertexGeoReach;
import commons.Config.Datasets;
import commons.Config.system;
import construction.IndexConstruct;
import construction.Loader;

public class Resolution {
	Config config;
	String dataset, neo4j_version;
	system systemName;
	String password;
	double minx, miny, maxx, maxy;
	
	ArrayList<ArrayList<Integer>> graph;
	ArrayList<Entity> entities;
	String dbPath, entityPath, mapPath, graphPath;
	
	String dbDir, projectDir, resultDir, queryDir;
	private int spaCount;
	private long[] graph_pos_map_list;
	private String graph_pos_map_path;
	private String dataDir;
	
	int MC = 0;
	double MG = 1.0, MR = 1.0;
	private int MAX_HOPNUM;

	public Resolution(Config config)
	{
		this.config = config;
		initParameters();
	}
	
	public static void main(String[] args) {
		Config config = new Config();
		config.setDatasetName(Datasets.Gowalla_10.name());
		config.setMAXHOPNUM(3);
		Resolution resolution = new Resolution(config);
		resolution.generateIndex();
//		resolution.query();
	}
	
	public void generateIndex()
	{
		String dir = "D:\\Ubuntu_shared\\GeoReachHop\\data";
		
		for (int pieces = 64; pieces <= 512; pieces *= 2) 
		{
			Util.Print("\npieces: " + pieces);
			
			ArrayList<VertexGeoReach> index = IndexConstruct.ConstructIndex(graph, entities, 
					minx, miny, maxx, maxy, 
					pieces, pieces, MAX_HOPNUM);
			ArrayList<ArrayList<Integer>> typesList = IndexConstruct.generateTypeList(index, MAX_HOPNUM, 
					minx, miny, maxx, maxy, 
					pieces, pieces, MG, MR, MC);
			
			//bitmap format
			int format = 1;
			String suffix = "bitmap";
			String indexPath = String.format("%s\\%s\\resolution\\%d_%d_%d_%d_%d_%d_%s.txt",
					dir, dataset, pieces, pieces, (int)(MG * 100), (int) (MR * 100), MC, MAX_HOPNUM, suffix);
			Util.Print("Output index to " + indexPath);
			Util.outputGeoReach(index, indexPath, typesList, format);
			
			//list format
			format = 0;
			suffix = "list";
			Util.Print("\npieces: " + pieces);
			indexPath = String.format("%s\\%s\\resolution\\%d_%d_%d_%d_%d_%d_%s.txt",
					dir, dataset, pieces, pieces, (int)(MG * 100), (int) (MR * 100), MC, MAX_HOPNUM, suffix);
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
		
		for (int pieces = 64; pieces <= 512; pieces *= 2) 
		{
			Util.Print("\npieces: " + pieces);
			Loader loader = new Loader(config);
			
			String indexPath = String.format("%s\\%s\\resolution\\%d_%d_%d_%d_%d_%d_%s.txt",
					dir, dataset, pieces, pieces, (int)(MG * 100), (int) (MR * 100), MC, MAX_HOPNUM, suffix);
			
			String dbPath = String.format("%s\\%s\\resolution\\%s_%d_%d_%d_%d_%d_%d"
					+ "\\data\\databases\\graph.db", 
					dir, dataset, neo4j_version, pieces, pieces, (int)(MG * 100), (int) (MR * 100), MC, MAX_HOPNUM);
			
			Util.Print(String.format("Load from %s\nto %s", indexPath, dbPath));
			loader.load(indexPath, dbPath);
		}
	}
	
	public void query()
	{
		for ( int pieces = 64; pieces <= 512; pieces *= 2)
		{
			
		}
	}

	public void initParameters()
	{
		systemName = config.getSystemName();
		neo4j_version = config.GetNeo4jVersion();
		dataset = config.getDatasetName();
		MAX_HOPNUM = config.getMaxHopNum();
		password = config.getPassword();
		
		dbDir = config.getDBDir();
		dataDir = config.getDataDir();
		projectDir = config.getProjectDir();
		
		switch (systemName) {
		case Ubuntu:
			entityPath = String.format("/mnt/hgfs/Ubuntu_shared/GeoMinHop/data/%s/entity.txt", dataset);
			graphPath = String.format("/mnt/hgfs/Ubuntu_shared/GeoMinHop/data/%s/graph.txt", dataset);
			graph_pos_map_path = dataDir + "/" + dataset + "/node_map_RTree.txt";
			
			resultDir = String.format("%s/MR", projectDir);
			queryDir = String.format("%s/query/%s", projectDir, dataset);
			break;
		case Windows:
			entityPath = String.format("D:\\Ubuntu_shared\\GeoMinHop\\data\\%s\\entity.txt", dataset);
			graphPath = String.format("D:\\Ubuntu_shared\\GeoMinHop\\data\\%s\\graph.txt", dataset);
			graph_pos_map_path = "D:\\Ubuntu_shared\\GeoMinHop\\data\\" + dataset + "\\node_map_RTree.txt";
		default:
			break;
		}
		
		Util.Print("Read graph from " + graphPath);
		graph = Util.ReadGraph(graphPath);
		
		Util.Print("Read entity from " + entityPath);
		entities = Util.ReadEntity(entityPath);
		
		spaCount = Util.GetSpatialEntityCount(entities);
		
		Util.Print("Read map from " + graph_pos_map_path);
		HashMap<String, String> graph_pos_map = Util.ReadMap(graph_pos_map_path);
		graph_pos_map_list = new long[graph_pos_map.size()];
		for ( String key_str : graph_pos_map.keySet())
		{
			int key = Integer.parseInt(key_str);
			int pos_id = Integer.parseInt(graph_pos_map.get(key_str));
			graph_pos_map_list[key] = pos_id;
		}
		
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
