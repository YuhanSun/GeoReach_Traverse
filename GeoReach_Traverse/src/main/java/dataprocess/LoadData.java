package dataprocess;

import java.io.File;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;
import java.util.TreeMap;

import org.neo4j.gis.spatial.EditableLayer;
import org.neo4j.gis.spatial.SpatialDatabaseService;
import org.neo4j.graphdb.Direction;
import org.neo4j.graphdb.DynamicLabel;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Label;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.ResourceIterator;
import org.neo4j.graphdb.Transaction;
import org.neo4j.graphdb.factory.GraphDatabaseFactory;
import org.neo4j.unsafe.batchinsert.BatchInserter;
import org.neo4j.unsafe.batchinsert.BatchInserters;

import commons.Config;
import commons.Entity;
import commons.EnumVariables.Datasets;
import commons.Labels.GraphLabel;
import commons.Labels.GraphRel;
import construction.Loader;
import scala.util.control.Exception;
import commons.Util;

/**
 * latest load class
 * @author ysun138
 *
 */
public class LoadData {
	static Config config = new Config();
//	static system systemName;
	static String version, dataset, lon_name, lat_name;
	static int nonspatial_label_count;
	
	static String dbPath, entityPath, mapPath, graphPath, labelListPath;
	
	static ArrayList<Entity> entities; 
	
	static String dir = "/hdd2/data/ysun138";
	
	static void initParameters()
	{
		dataset = Datasets.wikidata.name();
		dbPath = dir + "/neo4j-community-3.1.1/data/databases/graph.db";
		graphPath = dir + "/graph.txt";
		entityPath = dir + "/entity.txt";
		labelListPath = dir + "/label.txt";
		mapPath = dir + "/node_map_RTree.txt";
		
		nonspatial_label_count = 1;
		
		lon_name = config.GetLongitudePropertyName();
		lat_name = config.GetLatitudePropertyName();
		
////		systemName = config.getSystemName();
//		version = config.GetNeo4jVersion();
//		dataset = config.getDatasetName();
//		lon_name = config.GetLongitudePropertyName();
//		lat_name = config.GetLatitudePropertyName();
////		nonspatial_label_count = config.getNonSpatialLabelCount();
////		switch (systemName) {
////		case Ubuntu:
//			dbPath = String.format("/home/yuhansun/Documents/GeoGraphMatchData/%s_%s/data/databases/graph.db", version, dataset);
//			entityPath = String.format("/mnt/hgfs/Ubuntu_shared/GeoMinHop/data/%s/entity.txt", dataset);
//			labelListPath = String.format("/mnt/hgfs/Ubuntu_shared/GeoMinHop/data/%s/label.txt", dataset);
////			static String map_path = String.format("/mnt/hgfs/Ubuntu_shared/GeoMinHop/data/%s/node_map.txt", dataset);
//			/**
//			 * use this because osm node are not seen as spatial graph
//			 * but directly use RTree leaf node as the spatial vertices in the graph
//			 */
//			mapPath = String.format("/mnt/hgfs/Ubuntu_shared/GeoMinHop/data/%s/node_map_RTree.txt", dataset);
//			graphPath = String.format("/mnt/hgfs/Ubuntu_shared/GeoMinHop/data/%s/graph.txt", dataset);
//			break;
////		case Windows:
//			dbPath = String.format("D:\\Ubuntu_shared\\GeoMinHop\\data\\%s\\%s_%s\\data\\databases\\graph.db", 
//					dataset, version, dataset);
//			entityPath = String.format("D:\\Ubuntu_shared\\GeoMinHop\\data\\%s\\entity.txt", dataset);
//			labelListPath = String.format("D:\\Ubuntu_shared\\GeoMinHop\\data\\%s\\label.txt", dataset);
//			mapPath = String.format("D:\\Ubuntu_shared\\GeoMinHop\\data\\%s\\node_map_RTree.txt", dataset);
//			graphPath = String.format("D:\\Ubuntu_shared\\GeoMinHop\\data\\%s\\graph.txt", dataset);
////		default:
////			break;
////		}
		
		Util.Print("Read entity from: " + entityPath);
		entities = Util.ReadEntity(entityPath);
	}
	
//	public void LoadDataNoOSM()
//	{
//		initParameters();
//	}
//	
//	public void LoadDataNoOSM(Config pConfig)
//	{
//		this.config = pConfig;
//		initParameters();
//	}
	
	public static void main(String[] args) {
		
		try {
//			initParameters();
//			
//			batchRTreeInsert();
//			
//			if ( nonspatial_label_count == 1)
//				generateLabelList();
//			else
//			{
////				generateNonspatialLabel();
////				nonspatialLabelTest();
//			}
//			
//			Util.Print("LoadNonSpatialEntity");
//			LoadNonSpatialEntity();
//			
//			
//			GetSpatialNodeMap();
//			
//			LoadGraphEdges();
			
			loadGeoReachIndex();
			
//			CalculateCount();
			
//			Construct_RisoTree.main(null);
//			
//			loadHMBR();
			
		} catch (java.lang.Exception e) {
			e.printStackTrace();	System.exit(-1);
		}
		
	}
	
	public static void loadGeoReachIndex()
	{
		Util.Print("loadGeoReachIndex");
		
		Loader loader = new Loader();
		loader.graph_pos_map_path = dir + "/node_map_RTree.txt";
		
		Util.Print("read graph neo4j id map from " + loader.graph_pos_map_path);
		HashMap<String, String> graph_pos_map = Util.ReadMap(loader.graph_pos_map_path);
		loader.graph_pos_map_list= new long[graph_pos_map.size()];
		for ( String key_str : graph_pos_map.keySet())
		{
			int key = Integer.parseInt(key_str);
			int pos_id = Integer.parseInt(graph_pos_map.get(key_str));
			loader.graph_pos_map_list[key] = pos_id;
		}
		Util.Print("map size: " + graph_pos_map.size());
		
		String dir = "/hdd2/data/ysun138";
		String indexPath = dir + "/128_128_100_100_0_2_bitmap.txt";
		String dbFolder = "neo4j-community-3.1.1";
		String dbPath = dir + "/" + dbFolder + "/data/databases/graph.db";
		Util.Print("load from " + indexPath + " \nto " + dbPath);
		loader.load(indexPath, dbPath);
	}
	
//	/**
//	 * calculate count of spatial vertices 
//	 * enclosed by the MBR for each non-leaf 
//	 * R-Tree node.
//	 * This is important in the query algorithm
//	 */
//	public static void CalculateCount()
//	{
//		Util.Print("Calculate spatial cardinality");
//		try {
//			GraphDatabaseService databaseService = new GraphDatabaseFactory().newEmbeddedDatabase(new File(dbPath));
//			Transaction tx = databaseService.beginTx();
//			
//			Iterable<Node> geometry_nodes = RTreeUtility.getAllGeometries(databaseService, dataset);
//			Set<Node> set = new HashSet<Node>();
//			for ( Node node : geometry_nodes)
//			{
//				Node parent = node.getSingleRelationship(RTreeRel.RTREE_REFERENCE, Direction.INCOMING).getStartNode();
//				if(parent != null)
//				{
//					if(parent.hasProperty("count"))
//						parent.setProperty("count", (Integer)parent.getProperty("count") + 1);
//					else
//						parent.setProperty("count", 1);
//					set.add(parent);
//				}
//			}
//			
//			Set<Node> next_level_set = new HashSet<Node>();
//			
//			while (set.isEmpty() ==  false)
//			{
//				for (Node node : set)
//				{
//					Relationship relationship = node.getSingleRelationship(RTreeRel.RTREE_CHILD, Direction.INCOMING);
//					if ( relationship != null)
//					{
//						Node parent = relationship.getStartNode();
//						if(parent.hasProperty("count"))
//							parent.setProperty("count", (Integer)parent.getProperty("count") + (Integer)node.getProperty("count"));
//						else
//							parent.setProperty("count", (Integer)node.getProperty("count"));
//						next_level_set.add(parent);
//					}
//				}
//				
//				set = next_level_set;
//				next_level_set = new HashSet<Node>();
//			}
//			
//			tx.success();
//			tx.close();
//			databaseService.shutdown();
//			
//		} catch (Exception e) {
//			e.printStackTrace();	System.exit(-1);
//		}
//	}
	
	public static void LoadGraphEdges()
	{
		Util.Print("Load graph edges\n");
		BatchInserter inserter = null;
		try {
			Map<String, String> id_map = Util.ReadMap(mapPath);
			Map<String, String> config = new HashMap<String, String>();
			config.put("dbms.pagecache.memory", "80g");
			Util.Print("batch insert into: " + dbPath);
			inserter = BatchInserters.inserter(new File(dbPath).getAbsoluteFile(), config);
			
			ArrayList<ArrayList<Integer>> graph = Util.ReadGraph(graphPath);
			for (int i = 0; i < graph.size(); i++)
			{
				ArrayList<Integer> neighbors = graph.get(i);
				int start_neo4j_id = Integer.parseInt(id_map.get(String.valueOf(i)));
				for (int j = 0; j < neighbors.size(); j++)
				{
					int neighbor = neighbors.get(j);
					if ( i < neighbor )
					{
						int end_neo4j_id = Integer.parseInt(id_map.get(String.valueOf(neighbor)));
						inserter.createRelationship(start_neo4j_id, end_neo4j_id, GraphRel.GRAPH_LINK, null);
					}
				}
			}
			inserter.shutdown();
			
		} catch (java.lang.Exception e) {
			e.printStackTrace();	
			if (inserter != null)
				inserter.shutdown();
			System.exit(-1);
		}
	}
	
	/**
	 * Attach spatial node map to file node_map.txt
	 */
	public static void GetSpatialNodeMap()
	{
		Util.Print("Get spatial vertices map");
		try {
			Map<Object, Object> id_map = new TreeMap<Object, Object>();
			
			GraphDatabaseService databaseService = new GraphDatabaseFactory().newEmbeddedDatabase(new File(dbPath));
			Transaction tx = databaseService.beginTx();
			ResourceIterator<Node> spatial_nodes = databaseService.findNodes(GraphLabel.GRAPH_1);
			
			while ( spatial_nodes.hasNext())
			{
				Node node = spatial_nodes.next();
				long neo4jId = node.getId();
				int graphId = (Integer) node.getProperty("id");
				id_map.put(graphId, neo4jId);
			}
			
			tx.success();	tx.close();
			databaseService.shutdown();
			
			Util.Print("Write spatial node map to " + mapPath + "\n");
			Util.WriteMap(mapPath, true, id_map);
			
		} catch (java.lang.Exception e) {
			e.printStackTrace();	System.exit(-1);
		}
	}
	
	/**
	 * Load non-spatial graph vertices and write the map to file node_map.txt
	 */
	static void LoadNonSpatialEntity() 
	{
		try {
			Util.Print(String.format("LoadNonSpatialEntity\n from %s\n%s\n to %s", 
					entityPath, labelListPath, dbPath));
			
			if (!Util.pathExist(labelListPath))
				throw new java.lang.Exception(labelListPath + " does not exist");
			Util.Print("Read label list from: " + labelListPath);
			ArrayList<Integer> labelList = Util.readIntegerArray(labelListPath);
			
			Map<Object, Object> id_map = new TreeMap<Object, Object>();

			Map<String, String> config = new HashMap<String, String>();
			config.put("dbms.pagecache.memory", "80g");
			
			Util.Print("Batch insert into: " + dbPath);
			BatchInserter inserter = BatchInserters.inserter(new File(dbPath).getAbsoluteFile(), config);

			for (int i = 0; i < entities.size(); i++)
			{
				Entity entity = entities.get(i);
				if(entity.IsSpatial == false)
				{
					Map<String, Object> properties = new HashMap<String, Object>();
					properties.put("id", entity.id);
					int labelID = labelList.get(i);
					Label label = DynamicLabel.label(String.format("GRAPH_%d", labelID));
					Long pos_id = inserter.createNode(properties, label);
					id_map.put(entity.id, pos_id);
				}
			}
			inserter.shutdown();
			Util.Print("Write non-spatial node map to "+mapPath+"\n");
			Util.WriteMap(mapPath, false, id_map);
		} catch (java.lang.Exception e) {
			e.printStackTrace();	System.exit(-1);
		}
	}
	
//	public static void nonspatialLabelTest()
//	{
//		try {
//			BufferedReader reader = new BufferedReader(new FileReader(new File(labelListPath)));
//			ArrayList<Integer> statis = new ArrayList<Integer>(Collections.nCopies(nonspatial_label_count, 0));
//			String line = "";
//			while ( ( line = reader.readLine()) != null)
//			{
//				int label = Integer.parseInt(line);
//				//if the entity is a spatial one
//				if (label == 1)
//					continue;
//				int index = label - 2;
//				statis.set(index, statis.get(index) + 1);
//			}
//			reader.close();
//			for ( int count : statis)
//				Util.Print(count);
//		} catch (Exception e) {
//			e.printStackTrace();	System.exit(-1);
//		}
//	}
	
//	/**
//	 * generate new label.txt
//	 * for entities that non-spatial vertices
//	 */
//	public static void generateNonspatialLabel()
//	{
//		try {
//			FileWriter writer = new FileWriter(new File(labelListPath), true);
//			Random random = new Random();
//			
//			for ( Entity entity : entities)
//			{
//				if ( entity.IsSpatial)
//					writer.write("1\n");
//				else
//				{
//					int label = random.nextInt(nonspatial_label_count);
//					label += 2;
//					writer.write(label + "\n");
//				}
//			}
//			
//			writer.close();
//		} catch (IOException e) {
//			e.printStackTrace();
//		}
//	}
	
	/**
	 * Used if there is only one non-spatial label.
	 * Label will generated based on Entity.
	 * Spatial will be 1 and non-spatial will 0.
	 */
	public static void generateLabelList()
	{
		Util.Print("Generate the label list based on entity file\n");
		Util.getLabelListFromEntity(entityPath, labelListPath);
	}

	public static void batchRTreeInsert()
	{
		Util.Print("Batch insert RTree");
		try {
			String layerName = dataset;
			Util.Print("Connect to dbPath: " + dbPath);
			GraphDatabaseService databaseService = new GraphDatabaseFactory().newEmbeddedDatabase(new File(dbPath));
			Util.Print("dataset:" + dataset + "\ndatabase:" + dbPath + "\n");
			
			SpatialDatabaseService spatialDatabaseService = new SpatialDatabaseService(databaseService);
			
			Transaction tx = databaseService.beginTx();
			Util.Print(String.format("create point layer %s with lonName:%s and latName:%s", 
					layerName, lon_name, lat_name));
//			SimplePointLayer simplePointLayer = spatialDatabaseService.createSimplePointLayer(layerName);
			EditableLayer layer = spatialDatabaseService.getOrCreatePointLayer(layerName, lon_name, lat_name);
//			org.neo4j.gis.spatial.Layer layer = spatialDatabaseService.getLayer(layerName);
			
			Util.Print("add node to list");
			ArrayList<Node> geomNodes = new ArrayList<Node>(entities.size());
			for ( Entity entity : entities)
			{
				if ( entity.IsSpatial)
				{
					Node node = databaseService.createNode(GraphLabel.GRAPH_1);
					node.setProperty(lon_name, entity.lon);
					node.setProperty(lat_name, entity.lat);
					node.setProperty("id", entity.id);
					geomNodes.add(node);
				}
			}
			
			Util.Print("add node list to layer");
			layer.addAll(geomNodes);
			
			tx.success();
			tx.close();
			spatialDatabaseService.getDatabase().shutdown();
			
		} catch (java.lang.Exception e) {
			e.printStackTrace();	System.exit(-1);
		}
	}
	
	public long batchRTreeInsertTime()
	{
		Util.Print("Get Batch insert RTree time");
		String layerName = dataset;
		GraphDatabaseService databaseService = new GraphDatabaseFactory().newEmbeddedDatabase(new File(dbPath));
		Util.Print("dataset:" + dataset + "\ndatabase:" + dbPath + "\n");

		SpatialDatabaseService spatialDatabaseService = new SpatialDatabaseService(databaseService);

		Transaction tx = databaseService.beginTx();
		//			SimplePointLayer simplePointLayer = spatialDatabaseService.createSimplePointLayer(layerName);
		EditableLayer layer = spatialDatabaseService.getOrCreatePointLayer(layerName, lon_name, lat_name);
		//			org.neo4j.gis.spatial.Layer layer = spatialDatabaseService.getLayer(layerName);

		ArrayList<Entity> entities = Util.ReadEntity(entityPath); 
		ArrayList<Node> geomNodes = new ArrayList<Node>(entities.size());
		int spaCount = 0;
		long start =  System.currentTimeMillis();
		for ( Entity entity : entities)
		{
			if ( entity.IsSpatial)
			{
				Node node = databaseService.createNode(GraphLabel.GRAPH_1);
				node.setProperty(lon_name, entity.lon);
				node.setProperty(lat_name, entity.lat);
				node.setProperty("id", entity.id);
				geomNodes.add(node);
				spaCount++;
			}
		}
		long time = System.currentTimeMillis() - start;
		Util.Print("create node time:" + time);
		Util.Print("number of spatial objects:"+spaCount);

		start = System.currentTimeMillis();
		layer.addAll(geomNodes);
		long constructionTime = System.currentTimeMillis() - start;
		Util.Print("construct RTree time:" + constructionTime);

		start = System.currentTimeMillis();
		tx.success();
		tx.close();
		time = System.currentTimeMillis() - start;
		Util.Print("load into db time:" + time);
		spatialDatabaseService.getDatabase().shutdown();
		return constructionTime;
	}
}
