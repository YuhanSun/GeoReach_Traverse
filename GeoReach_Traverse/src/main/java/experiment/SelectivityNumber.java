package experiment;

import java.io.File;
import java.util.ArrayList;
import java.util.HashMap;

import org.neo4j.graphdb.factory.GraphDatabaseFactory;

import commons.Config;
import commons.Entity;
import commons.MyRectangle;
import commons.OwnMethods;
import commons.Query_Graph;
import commons.Util;
import commons.Utility;
import commons.Config.system;
import graph.RisoTreeQueryPN;

public class SelectivityNumber {

	public Config config;
	public String dataset;
	public String version;
	public system systemName;
	public String password;
	public int MAX_HOPNUM;

	public String dataDir, projectDir, dbDir;
	public String db_path;
	public String graph_pos_map_path;
	public String entityPath;

//	public String querygraphDir;
	public String queryDir;
	public String resultDir;

	public boolean TEST_FORMAT;

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

	public static int pieces_x = 128, pieces_y = 128;
	public static double MG = 1.0, MR = 1.0;
	public static int MC = 0;
	
	public void initializeParameters()
	{	
		TEST_FORMAT = false;
		dataset = config.getDatasetName();
		version = config.GetNeo4jVersion();
		systemName = config.getSystemName();
		password = config.getPassword();
		MAX_HOPNUM = config.getMaxHopNum();
		
		dbDir = config.getDBDir();
		projectDir = config.getProjectDir();
		dataDir = config.getDataDir();
		
		switch (systemName) {
		case Ubuntu:
			String dbFolder = String.format("%s_%d_%d_%d_%d_%d_%d", version, pieces_x, pieces_y, (int) (MG * 100), (int) (MR * 100), MC, 3);
			db_path = String.format("%s/%s/%s/data/databases/graph.db", dbDir, dataset, dbFolder);
			graph_pos_map_path = dataDir + "/" + dataset + "/node_map_RTree.txt";
			entityPath = String.format("%s/%s/entity.txt", dataDir, dataset);
//			querygraphDir = String.format("/mnt/hgfs/Google_Drive/Projects/risotree/query/query_graph/%s", dataset);
			queryDir = String.format("%s/query/%s", projectDir, dataset);
			resultDir = String.format("%s/selectivity_number/%s", projectDir, dataset);
			//			resultDir = String.format("/mnt/hgfs/Google_Drive/Experiment_Result/Riso-Tree/%s/switch_point", dataset);
			break;
		case Windows:
//			db_path = String.format("D:\\Ubuntu_shared\\GeoMinHop\\data\\%s\\%s_%s\\data\\databases\\graph.db", dataset, version, dataset);
//			graph_pos_map_path = "D:\\Ubuntu_shared\\GeoMinHop\\data\\" + dataset + "\\node_map_RTree.txt";
//			entityPath = String.format("D:\\Ubuntu_shared\\GeoMinHop\\data\\%s\\entity.txt", dataset);
			//			querygraphDir = String.format("D:\\Ubuntu_shared\\GeoMinHop\\query\\query_graph\\%s", dataset);
			//			spaPredicateDir = String.format("D:\\Ubuntu_shared\\GeoMinHop\\query\\spa_predicate\\%s", dataset);
//			resultDir = String.format("D:\\Google_Drive\\Experiment_Result\\Riso-Tree\\%s", dataset);
			break;
		}
		ArrayList<Entity> entities = Util.ReadEntity(entityPath);
		spaCount = Util.GetSpatialEntityCount(entities);
	}
	
	public static void main(String[] args) {
		Config config = new Config();
		
//		ArrayList<String> dataset_a = new ArrayList<String>(Arrays.asList(
//				Config.Datasets.Gowalla_100.name(), 
//				Config.Datasets.foursquare_100.name(),
//				Config.Datasets.Patents_100_random_80.name(), 
//				Config.Datasets.go_uniprot_100_random_80.name()));
		
		config.setDatasetName(Config.Datasets.foursquare_100.name());
		SelectivityNumber selectivityNumber = new SelectivityNumber(config);
		
		//Read start ids
		String startIDPath = String.format("%s/startID.txt", selectivityNumber.queryDir);
		ArrayList<Integer> startIDs = Util.readIntegerArray(startIDPath);
		
		int repeatTime = 10;
		ArrayList<ArrayList<Long>> startIDsList = new ArrayList<>();
		for ( int i = 0; i < repeatTime; i++)
			startIDsList.add(new ArrayList<>());
		
		int i = 0;
		for ( int id : startIDs)
		{
			int index = i % repeatTime;
			startIDsList.get(index).add((long) id);
			id++;
		}
		
		selectivityNumber.simpleTraversal(startIDsList);
	}
	
	public void simpleTraversal(ArrayList<ArrayList<Long>> startIDs)
	{
		try {
			long start;
			long time;

			String result_detail_path = null, result_avg_path = null;
			switch (systemName) {
			case Ubuntu:
				result_detail_path = String.format("%s/simpleTraversal.txt", resultDir);
				result_avg_path = String.format("%s/risotree_PN%d_%d_%d_avg.txt", resultDir, MAX_HOPNUM, nodeCount, query_id);
				//				result_detail_path = String.format("%s/risotree_%d_%d_test.txt", resultDir, nodeCount, query_id);
				//				result_avg_path = String.format("%s/risotree_%d_%d_avg_test.txt", resultDir, nodeCount, query_id);
				break;
			case Windows:
//				result_detail_path = String.format("%s\\risotree_PN_%d_%d.txt", resultDir, nodeCount, query_id);
//				result_avg_path = String.format("%s\\risotree_PN_%d_%d_avg.txt.txt", resultDir, nodeCount, query_id);
				break;
			}

			String write_line = String.format("%s\t%d\n", dataset, limit);
			if(!TEST_FORMAT)
			{
				OwnMethods.WriteFile(result_detail_path, true, write_line);
				OwnMethods.WriteFile(result_avg_path, true, write_line);
			}

			String head_line = "count\trange_time\tget_iterator_time\titerate_time\ttotal_time\taccess_pages\n";
			if(!TEST_FORMAT)
				OwnMethods.WriteFile(result_avg_path, true, "selectivity\t" + head_line);

			double selectivity = startSelectivity;
			int times = 10;
			while ( selectivity <= endSelectivity)
			{
				int name_suffix = (int) (selectivity * spaCount);

				String queryrect_path = null;
				switch (systemName) {
				case Ubuntu:
					queryrect_path = String.format("%s/queryrect_%d.txt", spaPredicateDir, name_suffix);
					break;
				case Windows:
					queryrect_path = String.format("%s\\queryrect_%d.txt", spaPredicateDir, name_suffix);
					break;
				}

				write_line = selectivity + "\n" + head_line;
				if(!TEST_FORMAT)
					OwnMethods.WriteFile(result_detail_path, true, write_line);

				ArrayList<MyRectangle> queryrect = OwnMethods.ReadQueryRectangle(queryrect_path);
				HashMap<String, String> graph_pos_map = OwnMethods.ReadMap(graph_pos_map_path);
				long[] graph_pos_map_list= new long[graph_pos_map.size()];
				for ( String key_str : graph_pos_map.keySet())
				{
					int key = Integer.parseInt(key_str);
					int pos_id = Integer.parseInt(graph_pos_map.get(key_str));
					graph_pos_map_list[key] = pos_id;
				}
				RisoTreeQueryPN risoTreeQueryPN = new RisoTreeQueryPN(db_path, dataset, 
						graph_pos_map_list, MAX_HOPNUM);

				ArrayList<Long> range_query_time = new ArrayList<Long>();
				ArrayList<Long> time_get_iterator = new ArrayList<Long>();
				ArrayList<Long> time_iterate = new ArrayList<Long>();
				ArrayList<Long> total_time = new ArrayList<Long>();
				ArrayList<Long> count = new ArrayList<Long>();
				ArrayList<Long> access = new ArrayList<Long>();

				for ( int i = 0; i < experimentCount; i++)
				{
					MyRectangle rectangle = queryrect.get(i);
					if ( rectangle.area() == 0.0)
					{
						double delta = Math.pow(0.1, 10);
						rectangle = new MyRectangle(rectangle.min_x - delta, rectangle.min_y - delta,
								rectangle.max_x + delta, rectangle.max_y + delta);
					}

					query_Graph.spa_predicate = new MyRectangle[query_Graph.graph.size()];

					//only handle query with one spatial predicate
					int j = 0;
					for (  ; j < query_Graph.graph.size(); j++)
						if(query_Graph.Has_Spa_Predicate[j])
							break;
					query_Graph.spa_predicate[j] = rectangle;

					if(!TEST_FORMAT)
					{
						OwnMethods.Print(String.format("%d : %s", i, rectangle.toString()));

						start = System.currentTimeMillis();
						risoTreeQueryPN.Query(query_Graph, -1);
						time = System.currentTimeMillis() - start;

						time_get_iterator.add(risoTreeQueryPN.get_iterator_time);
						time_iterate.add(risoTreeQueryPN.iterate_time);
						total_time.add(time);
						count.add(risoTreeQueryPN.result_count);
						access.add(risoTreeQueryPN.page_hit_count);OwnMethods.Print("Page access:" + risoTreeQueryPN.page_hit_count);
						range_query_time.add(risoTreeQueryPN.range_query_time);

						write_line = String.format("%d\t%d\t", count.get(i), range_query_time.get(i));
						write_line += String.format("%d\t", time_get_iterator.get(i));
						write_line += String.format("%d\t%d\t", time_iterate.get(i), total_time.get(i));
						write_line += String.format("%d\n", access.get(i));
						if(!TEST_FORMAT)
							OwnMethods.WriteFile(result_detail_path, true, write_line);
					}

					risoTreeQueryPN.dbservice.shutdown();

					OwnMethods.ClearCache(password);
					Thread.currentThread();
					Thread.sleep(5000);

					risoTreeQueryPN.dbservice = new GraphDatabaseFactory().newEmbeddedDatabase(new File(db_path));

				}
				risoTreeQueryPN.dbservice.shutdown();

				write_line = String.valueOf(selectivity) + "\t";
				write_line += String.format("%d\t%d\t", Utility.Average(count), Utility.Average(range_query_time));
				write_line += String.format("%d\t", Utility.Average(time_get_iterator));
				write_line += String.format("%d\t%d\t", Utility.Average(time_iterate), Utility.Average(total_time));
				write_line += String.format("%d\n", Utility.Average(access));
				if(!TEST_FORMAT)
					OwnMethods.WriteFile(result_avg_path, true, write_line);

				//				long larger_time = Utility.Average(total_time);
				//				if (larger_time * expe_count > 450 * 1000)
				//					expe_count = (int) (expe_count * 0.5 / (larger_time * expe_count / 450.0 / 1000.0));
				//				if(expe_count < 1)
				//					expe_count = 1;

				selectivity *= times;
			}
			OwnMethods.WriteFile(result_detail_path, true, "\n");
			OwnMethods.WriteFile(result_avg_path, true, "\n");
		} catch (Exception e) {
			e.printStackTrace();
			System.exit(-1);
		}
	}

}
