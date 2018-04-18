package experiment;

import java.util.ArrayList;

import commons.Config;
import commons.Entity;
import commons.Util;
import commons.Config.system;

public class SelectivityNumber {

	public Config config;
	public String dataset;
	public String version;
	public system systemName;
	public String password;
	public int MAX_HOPNUM;

	public String dataDir, projectDir;
	public String db_path;
	public String graph_pos_map_path;
	public String entityPath;

	public String querygraphDir;
	public String spaPredicateDir;
	public String resultDir;

	public boolean TEST_FORMAT;
	public int experimentCount = 10;

	//non-spatial ratio 20
	//	static double startSelectivity = 0.000001;
	//	static double endSelectivity = 0.002;

	//non-spatial ratio 80
//	double startSelectivity = 0.00001;
//	double endSelectivity = 0.2;
	
	//foursquare_100
	public double startSelectivity = 0.000001;
	public double endSelectivity = 0.002;
	
	//Patents
//	double startSelectivity = 0.00001;
//	double endSelectivity = 0.002;

	//for switching point detect
	//	static double startSelectivity = 0.01;
	//	static double endSelectivity = 0.2;

	//	static double startSelectivity = 0.0001;
	//	static double endSelectivity = 0.2;

	public int spaCount;

	public SelectivityNumber(Config config)
	{
		this.config = config;
		initializeParameters();
	}

	public void initializeParameters()
	{	
		TEST_FORMAT = false;
		dataset = config.getDatasetName();
		version = config.GetNeo4jVersion();
		systemName = config.getSystemName();
		password = config.getPassword();
		MAX_HOPNUM = config.getMaxHopNum();
		switch (systemName) {
		case Ubuntu:
			db_path = String.format("/home/yuhansun/Documents/GeoGraphMatchData/%s_%s/data/databases/graph.db", version, dataset);
			graph_pos_map_path = "/mnt/hgfs/Ubuntu_shared/GeoMinHop/data/" + dataset + "/node_map_RTree.txt";
			entityPath = String.format("/mnt/hgfs/Ubuntu_shared/GeoMinHop/data/%s/entity.txt", dataset);
			querygraphDir = String.format("/mnt/hgfs/Google_Drive/Projects/risotree/query/query_graph/%s", dataset);
			spaPredicateDir = String.format("/mnt/hgfs/Google_Drive/Projects/risotree/query/spa_predicate/%s", dataset);
			resultDir = String.format("/mnt/hgfs/Google_Drive/Experiment_Result/Riso-Tree/%s", dataset);
			//			resultDir = String.format("/mnt/hgfs/Google_Drive/Experiment_Result/Riso-Tree/%s/switch_point", dataset);
			break;
		case Windows:
			db_path = String.format("D:\\Ubuntu_shared\\GeoMinHop\\data\\%s\\%s_%s\\data\\databases\\graph.db", dataset, version, dataset);
			graph_pos_map_path = "D:\\Ubuntu_shared\\GeoMinHop\\data\\" + dataset + "\\node_map_RTree.txt";
			entityPath = String.format("D:\\Ubuntu_shared\\GeoMinHop\\data\\%s\\entity.txt", dataset);
			//			querygraphDir = String.format("D:\\Ubuntu_shared\\GeoMinHop\\query\\query_graph\\%s", dataset);
			//			spaPredicateDir = String.format("D:\\Ubuntu_shared\\GeoMinHop\\query\\spa_predicate\\%s", dataset);
			resultDir = String.format("D:\\Google_Drive\\Experiment_Result\\Riso-Tree\\%s", dataset);
			break;
		}
		ArrayList<Entity> entities = Util.ReadEntity(entityPath);
		spaCount = Util.GetSpatialEntityCount(entities);
	}
	
	public static void main(String[] args) {
		// TODO Auto-generated method stub

	}

}
