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
import commons.VertexGeoReach;
import commons.Config.Datasets;
import commons.Config.system;
import construction.IndexConstruct;
import construction.Loader;
import query.SpaTraversal;

public class Resolution {
	Config config;
	String dataset, version;
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
	
	int[] piecesArray = new int[] {96};
//	int[] piecesArray = new int[] {32, 64, 96, 128};
	
	public static void main(String[] args) {
		Config config = new Config();
		config.setDatasetName(Datasets.foursquare.name());
		config.setMAXHOPNUM(3);
		Resolution resolution = new Resolution(config);
		resolution.generateIndex();
		resolution.loadIndex();
//		resolution.query();
	}
	
	public void generateIndex()
	{
		String dir = "D:\\Ubuntu_shared\\GeoReachHop\\data";
		
		for ( int pieces : piecesArray)
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
		for ( int pieces : piecesArray)
		{
			Util.Print("\npieces: " + pieces);
			Loader loader = new Loader(config);
			
			String indexPath = String.format("%s\\%s\\resolution\\%d_%d_%d_%d_%d_%d_%s.txt",
					dir, dataset, pieces, pieces, (int)(MG * 100), (int) (MR * 100), MC, MAX_HOPNUM, suffix);
			
			String dbPath = String.format("%s\\%s\\resolution\\%s_%d_%d_%d_%d_%d_%d"
					+ "\\data\\databases\\graph.db", 
					dir, dataset, version, pieces, pieces, (int)(MG * 100), (int) (MR * 100), MC, MAX_HOPNUM);
			
			Util.Print(String.format("Load from %s\nto %s", indexPath, dbPath));
			loader.load(indexPath, dbPath);
		}
	}
	
	public void query()
	{
		SpaTraversal spaTraversal = null;
		try
		{
			int length = 3;
			double startSelectivity = 0.00001;
			double endSelectivity = 0.00002;
//			double startSelectivity = 0.01;
//			double endSelectivity = 0.02;

			//Read start ids
			String startIDPath = String.format("%s/startID.txt", queryDir);
			Util.Print("start id path: " + startIDPath);
			ArrayList<Integer> allStartIDs = Util.readIntegerArray(startIDPath);
			Util.Print(allStartIDs);
			
			int experimentCount = 500;
			int repeatTime = 1;
			ArrayList<ArrayList<Long>> startIDsList = new ArrayList<>();
			for ( int i = 0; i < repeatTime; i++)
				startIDsList.add(new ArrayList<>());
			
			int offset = 500;
			for ( int i = offset; i < offset + experimentCount * repeatTime; i++)
			{
				int id = allStartIDs.get(i);
				int index = i % repeatTime;
				startIDsList.get(index).add(graph_pos_map_list[id]);
			}
			
			double selectivity = startSelectivity;
			int times = 10;
			
			String result_detail_path = null, result_avg_path = null;
			switch (systemName) {
			case Ubuntu:
				result_detail_path = String.format("%s/%s_detail.txt", resultDir, dataset);
				result_avg_path = String.format("%s/%s_avg.txt", resultDir, dataset);
				break;
			case Windows:
//				result_detail_path = String.format("%s\\risotree_PN_%d_%d.txt", resultDir, nodeCount, query_id);
//				result_avg_path = String.format("%s\\risotree_PN_%d_%d_avg.txt.txt", resultDir, nodeCount, query_id);
				break;
			}
			while ( selectivity <= endSelectivity)
			{
				Util.WriteFile(result_avg_path, true, selectivity + "\n");
				String head_line = "time\tdbTime\tcheckTime\tvisited_count\tGeoReachPruned\tHistoryPruned\tresult_count\n";
				Util.WriteFile(result_avg_path, true, "resolution\t" + head_line);
				Util.WriteFile(result_detail_path, true, selectivity + "\n");
				for ( int pieces : piecesArray)
				{
					long start;
					long time;
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
					
					String write_line = pieces + "\n" + head_line;
					Util.WriteFile(result_detail_path, true, write_line);

					Util.Print("queryrect path: " + queryrect_path);
					ArrayList<MyRectangle> queryrect = Util.ReadQueryRectangle(queryrect_path);
					
					String dbFolder = String.format("%s_%d_%d_%d_%d_%d_%d", version, pieces, pieces, (int) (MG * 100), (int) (MR * 100), MC, 3);
					String db_path = String.format("%s/%s/resolution/%s/data/databases/graph.db", dbDir, dataset, dbFolder);
					Util.Print("db path: " + db_path);
					MyRectangle totalRange = new MyRectangle(minx, miny, maxx, maxy);
					spaTraversal = new SpaTraversal(db_path, MAX_HOPNUM, totalRange, pieces, pieces);

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

						Util.Print(String.format("%d : %s", i, rectangle.toString()));
						Util.Print(startIDs);

						Util.clearAndSleep(password, 5000);
						
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
						Util.WriteFile(result_detail_path, true, write_line);

						spaTraversal.dbservice.shutdown();

						Util.clearAndSleep(password, 5000);

						spaTraversal.dbservice = new GraphDatabaseFactory().newEmbeddedDatabase(new File(db_path));

					}
					spaTraversal.dbservice.shutdown();

					write_line = String.valueOf(pieces) + "\t";
					write_line += String.format("%d\t%d\t", Util.Average(total_time), Util.Average(visitedcount));
					write_line += String.format("%d\t%d\t%d\n", Util.Average(GeoReachPrunedCount), 
							Util.Average(HistoryPrunedCount), Util.Average(resultCount));
					Util.WriteFile(result_avg_path, true, write_line);

				}
				selectivity *= times;
				Util.WriteFile(result_detail_path, true, "\n");
				Util.WriteFile(result_avg_path, true, "\n");
			}
		}
		catch(Exception e)
		{
			e.printStackTrace();
			System.exit(-1);
		}
	}

//	public void query()
//	{
//		SpaTraversal spaTraversal = null;
//		try
//		{
//			int length = 3;
//			double startSelectivity = 0.00001;
//			double endSelectivity = 0.2;
//
//			//Read start ids
//			String startIDPath = String.format("%s/startID.txt", queryDir);
//			Util.Print("start id path: " + startIDPath);
//			ArrayList<Integer> allStartIDs = Util.readIntegerArray(startIDPath);
//			Util.Print(allStartIDs);
//			
//			int experimentCount = 500;
//			int repeatTime = 1;
//			ArrayList<ArrayList<Long>> startIDsList = new ArrayList<>();
//			for ( int i = 0; i < repeatTime; i++)
//				startIDsList.add(new ArrayList<>());
//			
//			int offset = 500;
//			for ( int i = offset; i < offset + experimentCount * repeatTime; i++)
//			{
//				int id = allStartIDs.get(i);
//				int index = i % repeatTime;
//				startIDsList.get(index).add(graph_pos_map_list[id]);
//			}
//			
////			for ( int pieces = 64; pieces <= 256; pieces *= 2)
//			int pieces = 32;
//			{
//				long start;
//				long time;
//
//				String result_detail_path = null, result_avg_path = null;
//				switch (systemName) {
//				case Ubuntu:
//					result_detail_path = String.format("%s/%s_%d_detail.txt", resultDir, dataset, pieces);
//					result_avg_path = String.format("%s/%s_%d_avg.txt", resultDir, dataset, pieces);
//					break;
//				case Windows:
////					result_detail_path = String.format("%s\\risotree_PN_%d_%d.txt", resultDir, nodeCount, query_id);
////					result_avg_path = String.format("%s\\risotree_PN_%d_%d_avg.txt.txt", resultDir, nodeCount, query_id);
//					break;
//				}
//
//				String write_line = String.format("%s\t%d\n", dataset, length);
//				{
//					Util.WriteFile(result_detail_path, true, write_line);
//					Util.WriteFile(result_avg_path, true, write_line);
//				}
//
//				String head_line = "time\tdbTime\tcheckTime\tvisited_count\tGeoReachPruned\tHistoryPruned\tresult_count\n";
//				Util.WriteFile(result_avg_path, true, "selectivity\t" + head_line);
//
//				double selectivity = startSelectivity;
//				int times = 10;
//				while ( selectivity <= endSelectivity)
//				{
//					int name_suffix = (int) (selectivity * spaCount);
//
//					String queryrect_path = null;
//					switch (systemName) {
//					case Ubuntu:
//						queryrect_path = String.format("%s/queryrect_%d.txt", queryDir, name_suffix);
//						break;
//					case Windows:
//						queryrect_path = String.format("%s\\queryrect_%d.txt", queryDir, name_suffix);
//						break;
//					}
//					Util.Print("query rectangle path: " + queryrect_path);
//
//					write_line = selectivity + "\n" + head_line;
//					Util.WriteFile(result_detail_path, true, write_line);
//
//					Util.Print("queryrect path: " + queryrect_path);
//					ArrayList<MyRectangle> queryrect = Util.ReadQueryRectangle(queryrect_path);
//					
//					String dbFolder = String.format("%s_%d_%d_%d_%d_%d_%d", version, pieces, pieces, (int) (MG * 100), (int) (MR * 100), MC, 3);
//					String db_path = String.format("%s/%s/resolution/%s/data/databases/graph.db", dbDir, dataset, dbFolder);
//					Util.Print("db path: " + db_path);
//					MyRectangle totalRange = new MyRectangle(minx, miny, maxx, maxy);
//					spaTraversal = new SpaTraversal(db_path, MAX_HOPNUM, totalRange, pieces, pieces);
//
//					ArrayList<Long> total_time = new ArrayList<Long>();
//					ArrayList<Long> visitedcount = new ArrayList<Long>();
//					ArrayList<Long> resultCount = new ArrayList<Long>();
//					ArrayList<Long> GeoReachPrunedCount = new ArrayList<Long>();
//					ArrayList<Long> HistoryPrunedCount = new ArrayList<Long>();
//
//					for ( int i = 0; i < startIDsList.size(); i++)
//					{
//						ArrayList<Long> startIDs = startIDsList.get(i);
//						Transaction tx = spaTraversal.dbservice.beginTx();
//						ArrayList<Node> startNodes = Util.getNodesByIDs(spaTraversal.dbservice, startIDs); 
//						tx.success();
//						tx.close();
//
//						MyRectangle rectangle = queryrect.get(i);
//						if ( rectangle.area() == 0.0)
//						{
//							double delta = Math.pow(0.1, 10);
//							rectangle = new MyRectangle(rectangle.min_x - delta, rectangle.min_y - delta,
//									rectangle.max_x + delta, rectangle.max_y + delta);
//						}
//
//						Util.Print(String.format("%d : %s", i, rectangle.toString()));
//						Util.Print(startIDs);
//
//						start = System.currentTimeMillis();
//						spaTraversal.traverse(startNodes, length, rectangle);
//						time = System.currentTimeMillis() - start;
//
//						total_time.add(time);
//						visitedcount.add(spaTraversal.visitedCount);
//						resultCount.add(spaTraversal.resultCount);
//						GeoReachPrunedCount.add(spaTraversal.GeoReachPruneCount);
//						HistoryPrunedCount.add(spaTraversal.PrunedVerticesWorkCount);
//
//
//						write_line = String.format("%d\t%d\t", total_time.get(i), visitedcount.get(i));
//						write_line += String.format("%d\t%d\t", GeoReachPrunedCount.get(i), HistoryPrunedCount.get(i));
//						write_line += String.format("%d\n", resultCount.get(i));
//						Util.WriteFile(result_detail_path, true, write_line);
//
//						spaTraversal.dbservice.shutdown();
//
//						Util.ClearCache(password);
//						Thread.currentThread();
//						Thread.sleep(5000);
//
//						spaTraversal.dbservice = new GraphDatabaseFactory().newEmbeddedDatabase(new File(db_path));
//
//					}
//					spaTraversal.dbservice.shutdown();
//
//					write_line = String.valueOf(selectivity) + "\t";
//					write_line += String.format("%d\t%d\t", Util.Average(total_time), Util.Average(visitedcount));
//					write_line += String.format("%d\t%d\t%d\n", Util.Average(GeoReachPrunedCount), 
//							Util.Average(HistoryPrunedCount), Util.Average(resultCount));
//					Util.WriteFile(result_avg_path, true, write_line);
//
//					selectivity *= times;
//				}
//				Util.WriteFile(result_detail_path, true, "\n");
//				Util.WriteFile(result_avg_path, true, "\n");
//			}
//			
//		}
//		catch(Exception e)
//		{
//			e.printStackTrace();
//			System.exit(-1);
//		}
//	}
	
	public void initParameters()
	{
		systemName = config.getSystemName();
		version = config.GetNeo4jVersion();
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
			
			resultDir = String.format("%s/resolution", projectDir);
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
