package experiment;

import java.io.File;
import java.util.ArrayList;
import java.util.HashMap;

import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Transaction;
import org.neo4j.graphdb.factory.GraphDatabaseFactory;

import commons.Config;
import commons.Entity;
import commons.MyRectangle;
import commons.Util;
import query.Neo4jCypherTraversal;
import query.SimpleGraphTraversal;
import query.SpaTraversal;
import commons.Config.system;

public class SelectivityNumber {

	public Config config;
	public String dataset;
	public String version;
	public system systemName;
	public String password;
	public int MAX_HOPNUM;
	public MyRectangle totalRange;

	public String dataDir, projectDir, dbDir;
	public String db_path;
	public String graph_pos_map_path;
	public String entityPath;
	public long[] graph_pos_map_list;

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
	public double endSelectivity = 0.2;
	
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
	public static int length = 3;
	
	public void initializeParameters()
	{	
		TEST_FORMAT = false;
		dataset = config.getDatasetName();
		version = config.GetNeo4jVersion();
		systemName = config.getSystemName();
		password = config.getPassword();
		MAX_HOPNUM = config.getMaxHopNum();
		
		/**
		 * set whole space range
		 */
		if (dataset.contains("Gowalla") || dataset.contains("Yelp")
				|| dataset.contains("foursquare"))
			totalRange = new MyRectangle(-180, -90, 180, 90);
		if (dataset.contains("Patents") || dataset.contains("go_uniprot"))
			totalRange = new MyRectangle(0, 0, 1000, 1000);

		
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
			resultDir = String.format("%s/selectivity_number", projectDir);
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
		Util.Print("entity path: " + entityPath);
		ArrayList<Entity> entities = Util.ReadEntity(entityPath);
		spaCount = Util.GetSpatialEntityCount(entities);
		
		HashMap<String, String> graph_pos_map = Util.ReadMap(graph_pos_map_path);
		graph_pos_map_list = new long[graph_pos_map.size()];
		for ( String key_str : graph_pos_map.keySet())
		{
			int key = Integer.parseInt(key_str);
			int pos_id = Integer.parseInt(graph_pos_map.get(key_str));
			graph_pos_map_list[key] = pos_id;
		}
	}
	
	public static void main(String[] args) {
		try {
			Config config = new Config();
			
//			ArrayList<String> dataset_a = new ArrayList<String>(Arrays.asList(
//					Config.Datasets.Gowalla_100.name(), 
//					Config.Datasets.foursquare_100.name(),
//					Config.Datasets.Patents_100_random_80.name(), 
//					Config.Datasets.go_uniprot_100_random_80.name()));
			
			config.setDatasetName(Config.Datasets.Gowalla_10.name());
			SelectivityNumber selectivityNumber = new SelectivityNumber(config);
			
			//Read start ids
			String startIDPath = String.format("%s/startID.txt", selectivityNumber.queryDir);
			Util.Print("start id path: " + startIDPath);
			ArrayList<Integer> startIDs = Util.readIntegerArray(startIDPath);
			Util.Print(startIDs);
			
			int experimentCount = 500;
			int repeatTime = 1;
			ArrayList<ArrayList<Long>> startIDsList = new ArrayList<>();
			for ( int i = 0; i < repeatTime; i++)
				startIDsList.add(new ArrayList<>());
			
			for ( int i = 0; i < experimentCount; i++)
			{
				int id = startIDs.get(i);
				int index = i % repeatTime;
				startIDsList.get(index).add(selectivityNumber.graph_pos_map_list[id]);
			}
			
//			selectivityNumber.simpleTraversal(startIDsList);
//			selectivityNumber.spaTraversal(startIDsList);
			selectivityNumber.neo4jCypherTraveral(startIDsList);
		} catch (Exception e) {
			// TODO: handle exception
			e.printStackTrace();
			System.exit(-1);
		}
	}
	
	public void spaTraversal(ArrayList<ArrayList<Long>> startIDsList)
	{
		try {
			long start;
			long time;

			String result_detail_path = null, result_avg_path = null;
			switch (systemName) {
			case Ubuntu:
				result_detail_path = String.format("%s/%s_spaTraversal_detail.txt", resultDir, dataset);
				result_avg_path = String.format("%s/%s_spaTraversal_avg.txt", resultDir, dataset);
				break;
			case Windows:
//				result_detail_path = String.format("%s\\risotree_PN_%d_%d.txt", resultDir, nodeCount, query_id);
//				result_avg_path = String.format("%s\\risotree_PN_%d_%d_avg.txt.txt", resultDir, nodeCount, query_id);
				break;
			}

			String write_line = String.format("%s\t%d\n", dataset, length);
			if(!TEST_FORMAT)
			{
				Util.WriteFile(result_detail_path, true, write_line);
				Util.WriteFile(result_avg_path, true, write_line);
			}

			String head_line = "time\tvisited_count\tGeoReachPruned\tHistoryPruned\tresult_count\n";
			if(!TEST_FORMAT)
				Util.WriteFile(result_avg_path, true, "selectivity\t" + head_line);

			double selectivity = startSelectivity;
			int times = 10;
			while ( selectivity <= endSelectivity)
			{
				int name_suffix = (int) (selectivity * spaCount);

				String queryrect_path = null;
				switch (systemName) {
				case Ubuntu:
					queryrect_path = String.format("%s/queryrect_%d.txt", queryDir, name_suffix);
					break;
				case Windows:
					queryrect_path = String.format("%s\\queryrect_%d.txt", queryDir, name_suffix);
					break;
				}
				Util.Print("query rectangle path: " + queryrect_path);

				write_line = selectivity + "\n" + head_line;
				if(!TEST_FORMAT)
					Util.WriteFile(result_detail_path, true, write_line);

				ArrayList<MyRectangle> queryrect = Util.ReadQueryRectangle(queryrect_path);
				
				SpaTraversal spaTraversal = new SpaTraversal(db_path, MAX_HOPNUM, totalRange, 128, 128);

				ArrayList<Long> total_time = new ArrayList<Long>();
				ArrayList<Long> visitedcount = new ArrayList<Long>();
				ArrayList<Long> resultCount = new ArrayList<Long>();
				ArrayList<Long> GeoReachPrunedCount = new ArrayList<Long>();
				ArrayList<Long> HistoryPrunedCount = new ArrayList<Long>();

				for ( int i = 0; i < startIDsList.size(); i++)
				{
					ArrayList<Long> startIDs = startIDsList.get(i);
					Transaction tx = spaTraversal.dbservice.beginTx();
					ArrayList<Node> startNodes = Util.getNodesByIDs(spaTraversal.dbservice, startIDs); 
					tx.success();
					tx.close();
					
					MyRectangle rectangle = queryrect.get(i);
					if ( rectangle.area() == 0.0)
					{
						double delta = Math.pow(0.1, 10);
						rectangle = new MyRectangle(rectangle.min_x - delta, rectangle.min_y - delta,
								rectangle.max_x + delta, rectangle.max_y + delta);
					}

					if(!TEST_FORMAT)
					{
						Util.Print(String.format("%d : %s", i, rectangle.toString()));
						Util.Print(startIDs);

						start = System.currentTimeMillis();
						spaTraversal.traverse(startNodes, length, rectangle);
						time = System.currentTimeMillis() - start;

						total_time.add(time);
						visitedcount.add(spaTraversal.visitedCount);
						resultCount.add(spaTraversal.resultCount);
						GeoReachPrunedCount.add(spaTraversal.GeoReachPruneCount);
						HistoryPrunedCount.add(spaTraversal.PrunedVerticesWorkCount);


						write_line = String.format("%d\t%d\t", total_time.get(i), visitedcount.get(i));
						write_line += String.format("%d\t%d\t", GeoReachPrunedCount.get(i), HistoryPrunedCount.get(i));
						write_line += String.format("%d\n", resultCount.get(i));
						if(!TEST_FORMAT)
							Util.WriteFile(result_detail_path, true, write_line);
					}

					spaTraversal.dbservice.shutdown();

					Util.ClearCache(password);
					Thread.currentThread();
					Thread.sleep(5000);

					spaTraversal.dbservice = new GraphDatabaseFactory().newEmbeddedDatabase(new File(db_path));

				}
				spaTraversal.dbservice.shutdown();

				write_line = String.valueOf(selectivity) + "\t";
				write_line += String.format("%d\t%d\t", Util.Average(total_time), Util.Average(visitedcount));
				write_line += String.format("%d\t%d\t%d\n", Util.Average(GeoReachPrunedCount), 
						Util.Average(HistoryPrunedCount), Util.Average(resultCount));
				if(!TEST_FORMAT)
					Util.WriteFile(result_avg_path, true, write_line);

				selectivity *= times;
			}
			Util.WriteFile(result_detail_path, true, "\n");
			Util.WriteFile(result_avg_path, true, "\n");
		} catch (Exception e) {
			e.printStackTrace();
			System.exit(-1);
		}
	}
	
	public void simpleTraversal(ArrayList<ArrayList<Long>> startIDsList)
	{
		try {
			long start;
			long time;

			String result_detail_path = null, result_avg_path = null;
			switch (systemName) {
			case Ubuntu:
				result_detail_path = String.format("%s/%s_simpleTraversal_detail.txt", resultDir, dataset);
				result_avg_path = String.format("%s/%s_simpleTraversal_avg.txt", resultDir, dataset);
				break;
			case Windows:
//				result_detail_path = String.format("%s\\risotree_PN_%d_%d.txt", resultDir, nodeCount, query_id);
//				result_avg_path = String.format("%s\\risotree_PN_%d_%d_avg.txt.txt", resultDir, nodeCount, query_id);
				break;
			}

			String write_line = String.format("%s\t%d\n", dataset, length);
			if(!TEST_FORMAT)
			{
				Util.WriteFile(result_detail_path, true, write_line);
				Util.WriteFile(result_avg_path, true, write_line);
			}

			String head_line = "time\tvisited_count\tresult_count\n";
			if(!TEST_FORMAT)
				Util.WriteFile(result_avg_path, true, "selectivity\t" + head_line);

			double selectivity = startSelectivity;
			int times = 10;
			while ( selectivity <= endSelectivity)
			{
				int name_suffix = (int) (selectivity * spaCount);

				String queryrect_path = null;
				switch (systemName) {
				case Ubuntu:
					queryrect_path = String.format("%s/queryrect_%d.txt", queryDir, name_suffix);
					break;
				case Windows:
					queryrect_path = String.format("%s\\queryrect_%d.txt", queryDir, name_suffix);
					break;
				}
				Util.Print("query rectangle path: " + queryrect_path);

				write_line = selectivity + "\n" + head_line;
				if(!TEST_FORMAT)
					Util.WriteFile(result_detail_path, true, write_line);

				ArrayList<MyRectangle> queryrect = Util.ReadQueryRectangle(queryrect_path);
				
				SimpleGraphTraversal simpleGraphTraversal = new SimpleGraphTraversal(db_path);

				ArrayList<Long> total_time = new ArrayList<Long>();
				ArrayList<Long> visitedcount = new ArrayList<Long>();
				ArrayList<Long> resultCount = new ArrayList<Long>();

				for ( int i = 0; i < startIDsList.size(); i++)
				{
					ArrayList<Long> startIDs = startIDsList.get(i);
					Util.Print("start ids: " + startIDs);
					Transaction tx = simpleGraphTraversal.dbservice.beginTx();
					ArrayList<Node> startNodes = Util.getNodesByIDs(simpleGraphTraversal.dbservice, startIDs);
					tx.success();
					tx.close();
					
					MyRectangle rectangle = queryrect.get(i);
					if ( rectangle.area() == 0.0)
					{
						double delta = Math.pow(0.1, 10);
						rectangle = new MyRectangle(rectangle.min_x - delta, rectangle.min_y - delta,
								rectangle.max_x + delta, rectangle.max_y + delta);
					}

					if(!TEST_FORMAT)
					{
						Util.Print(String.format("%d : %s", i, rectangle.toString()));
						Util.Print(startIDs);

						start = System.currentTimeMillis();
						simpleGraphTraversal.traverse(startNodes, length, rectangle);
						time = System.currentTimeMillis() - start;

						total_time.add(time);
						visitedcount.add(simpleGraphTraversal.visitedCount);
						resultCount.add(simpleGraphTraversal.resultCount);

						write_line = String.format("%d\t%d\t", total_time.get(i), visitedcount.get(i));
						write_line += String.format("%d\n", resultCount.get(i));
						if(!TEST_FORMAT)
							Util.WriteFile(result_detail_path, true, write_line);
					}

					simpleGraphTraversal.dbservice.shutdown();

					Util.ClearCache(password);
					Thread.currentThread();
					Thread.sleep(5000);

					simpleGraphTraversal.dbservice = new GraphDatabaseFactory().newEmbeddedDatabase(new File(db_path));

				}
				simpleGraphTraversal.dbservice.shutdown();

				write_line = String.valueOf(selectivity) + "\t";
				write_line += String.format("%d\t%d\t", Util.Average(total_time), Util.Average(visitedcount));
				write_line += String.format("%d\n", Util.Average(resultCount));
				if(!TEST_FORMAT)
					Util.WriteFile(result_avg_path, true, write_line);

				selectivity *= times;
			}
			Util.WriteFile(result_detail_path, true, "\n");
			Util.WriteFile(result_avg_path, true, "\n");
		} catch (Exception e) {
			e.printStackTrace();
			System.exit(-1);
		}
	}

	public void neo4jCypherTraveral(ArrayList<ArrayList<Long>> startIDsList)
	{
		try {
			long start;
			long time;

			String result_detail_path = null, result_avg_path = null;
			switch (systemName) {
			case Ubuntu:
				result_detail_path = String.format("%s/%s_neo4jCypher_detail.txt", resultDir, dataset);
				result_avg_path = String.format("%s/%s_neo4jCypher_avg.txt", resultDir, dataset);
				break;
			case Windows:
//				result_detail_path = String.format("%s\\risotree_PN_%d_%d.txt", resultDir, nodeCount, query_id);
//				result_avg_path = String.format("%s\\risotree_PN_%d_%d_avg.txt.txt", resultDir, nodeCount, query_id);
				break;
			}

			String write_line = String.format("%s\t%d\n", dataset, length);
			if(!TEST_FORMAT)
			{
				Util.WriteFile(result_detail_path, true, write_line);
				Util.WriteFile(result_avg_path, true, write_line);
			}

			String head_line = "time\tpage_access\tresult_count\n";
			if(!TEST_FORMAT)
				Util.WriteFile(result_avg_path, true, "selectivity\t" + head_line);

			double selectivity = startSelectivity;
			int times = 10;
			while ( selectivity <= endSelectivity)
			{
				int name_suffix = (int) (selectivity * spaCount);

				String queryrect_path = null;
				switch (systemName) {
				case Ubuntu:
					queryrect_path = String.format("%s/queryrect_%d.txt", queryDir, name_suffix);
					break;
				case Windows:
					queryrect_path = String.format("%s\\queryrect_%d.txt", queryDir, name_suffix);
					break;
				}
				Util.Print("query rectangle path: " + queryrect_path);

				write_line = selectivity + "\n" + head_line;
				if(!TEST_FORMAT)
					Util.WriteFile(result_detail_path, true, write_line);

				ArrayList<MyRectangle> queryrect = Util.ReadQueryRectangle(queryrect_path);
				
				Neo4jCypherTraversal neo4jCypherTraversal = new Neo4jCypherTraversal(db_path);

				ArrayList<Long> total_time = new ArrayList<Long>();
				ArrayList<Long> pageAccessCount = new ArrayList<Long>();
				ArrayList<Long> resultCount = new ArrayList<Long>();

				for ( int i = 0; i < startIDsList.size(); i++)
				{
					ArrayList<Long> startIDs = startIDsList.get(i);
					Util.Print("start ids: " + startIDs);
					
					MyRectangle rectangle = queryrect.get(i);
					if ( rectangle.area() == 0.0)
					{
						double delta = Math.pow(0.1, 10);
						rectangle = new MyRectangle(rectangle.min_x - delta, rectangle.min_y - delta,
								rectangle.max_x + delta, rectangle.max_y + delta);
					}

					if(!TEST_FORMAT)
					{
						Util.Print(String.format("%d : %s", i, rectangle.toString()));
						Util.Print(startIDs);

						start = System.currentTimeMillis();
						neo4jCypherTraversal.traverse(startIDs, length, rectangle);
						time = System.currentTimeMillis() - start;

						total_time.add(time);
						pageAccessCount.add(neo4jCypherTraversal.pageAccessCount);
						resultCount.add(neo4jCypherTraversal.resultCount);

						write_line = String.format("%d\t%d\t", total_time.get(i), pageAccessCount.get(i));
						write_line += String.format("%d\n", resultCount.get(i));
						if(!TEST_FORMAT)
							Util.WriteFile(result_detail_path, true, write_line);
					}

					neo4jCypherTraversal.dbservice.shutdown();

					Util.ClearCache(password);
					Thread.currentThread();
					Thread.sleep(5000);

					neo4jCypherTraversal.dbservice = new GraphDatabaseFactory().newEmbeddedDatabase(new File(db_path));

				}
				neo4jCypherTraversal.dbservice.shutdown();

				write_line = String.valueOf(selectivity) + "\t";
				write_line += String.format("%d\t%d\t", Util.Average(total_time), Util.Average(pageAccessCount));
				write_line += String.format("%d\n", Util.Average(resultCount));
				if(!TEST_FORMAT)
					Util.WriteFile(result_avg_path, true, write_line);

				selectivity *= times;
			}
			Util.WriteFile(result_detail_path, true, "\n");
			Util.WriteFile(result_avg_path, true, "\n");
		} catch (Exception e) {
			e.printStackTrace();
			System.exit(-1);
		}
	}
}
