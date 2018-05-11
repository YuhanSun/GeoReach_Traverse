package experiment;

import java.util.ArrayList;

import commons.Config;
import commons.Entity;
import commons.EnumVariables.*;


public class MaxHop {
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
	
	int pieces_x = 128, pieces_y = 128, MC = 0;
	double MG = 1.0, MR = 1.0;
	
	public static void main(String[] args) {
		Config config = new Config();
		config.setDatasetName(Datasets.Yelp.name());
		MG mg = new MG(config);
//		mg.generateIndex();
		mg.loadIndex();
	}
	
	public void query()
	{
		for ( int maxHop = 1; maxHop <= 3; maxHop++)
		{
			
		}
	}
}
