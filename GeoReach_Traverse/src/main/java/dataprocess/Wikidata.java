package dataprocess;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.TreeSet;

import commons.Entity;
import commons.GraphUtil;
import commons.ReadWriteUtil;
import commons.Util;

public class Wikidata {

	static String dir = "D:\\Project_Data\\wikidata-20180308-truthy-BETA.nt";
	static String fullfilePath = dir + "\\wikidata-20180308-truthy-BETA.nt";
	static String sliceDataPath = dir + "\\slice_100000.nt";
	static String logPath = dir + "\\extract.log";
	static String locationPath = dir + "\\locations.txt";
	static String entityMapPath = dir + "\\entity_map.txt";
	static String graphPath = dir + "\\graph.txt";
	static String entityPath = dir + "\\entity.txt";
	
	public static void main(String[] args) {
		// TODO Auto-generated method stub
//		extract();
		
//		extractEntityMap();
//		extractEntityToEntityRelation();
//		checkGraphVerticesCount();
//		generateEntityFile();
		
//		readGraphTest();
		
//		//test code
//		String string = "<http://www.wikidata.org/entity/Q26>";
//		Util.Print(isEntity(string));
//		Util.Print(getEntityID(string));
//		//test code
//		String string = "<https://www.wikidata.org/wiki/Property:P1151>";
//		Util.Print(isProperty(string));
//		Util.Print(getPropertyID(string));
		
		
//		getRange();
//		checkLocation();
//		findEntitiesNotOnEarth();
//		removeLocationOutOfEarth();
//		removeLocationOutOfBound();
//		getEdgeCount();
		
		extractPropertyID();
		
//		getLabelCount();
	}
	
	public static void checkPropertyEntityID()
	{
		HashMap<Long, Integer> idMap = readMap(entityMapPath);
		ArrayList<Integer> propertySet = ReadWriteUtil.readIntegerArray(dir + "\\propertyID.txt");
		int count = 0;
		for (int id : propertySet)
		{
			if (idMap.containsKey(id)) {
				Util.println(String.format("%d,%d", id, idMap.get(id)));
				count++;
			}
		}
		Util.println("count: " + count);
	}
	
	public static void extractPropertyID()
	{
		BufferedReader reader = null;
		String line = "";
		HashSet<Long> idSet = new HashSet<>();
		int lineIndex = 0;
		try {
			reader = new BufferedReader(new FileReader(new File(fullfilePath)));
			while ((line = reader.readLine())!=null)
			{
				lineIndex++;
				String[] strings = line.split(" ");
				String subject = strings[0];
				long id = getPropertyEntityID(subject);
				if (id != -1)
					idSet.add(id);
				
				String object = strings[2];
				id = getPropertyEntityID(object);
				if (id != -1)
					idSet.add(id);
				
				if (lineIndex % 10000000 == 0)
					Util.println(lineIndex);
			}
			reader.close();
			ArrayList<String> output = new ArrayList<>(idSet.size());
			for (long id : idSet)
				output.add(String.valueOf(id));
			
			ReadWriteUtil.WriteArray(dir + "\\propertyID.txt", output);
		} catch (Exception e) {
			// TODO: handle exception
			e.printStackTrace();
		}
	}
	
	public static void getLabelCount()
	{
		BufferedReader reader = null;
		String line = "";
		HashMap<Long, Integer> idMap = readMap(entityMapPath);
		TreeSet<Integer> hasLabelVertices = new TreeSet<>();
		HashMap<Integer, TreeSet<Integer>> labels = new HashMap<>();
		int count = 0;
		try {
			reader = new BufferedReader(new FileReader(new File(fullfilePath)));
			while ( (line = reader.readLine()) != null)
			{
				String[] strList = line.split(" ");
				String predicate = strList[1];
				if (predicate.equals("<http://www.wikidata.org/prop/direct/P31>"))
				{
					count++;
					String subject = strList[0];
					String object = strList[2];
					
					if (!isEntity(subject) || !isEntity(object))
						continue;
					
					int graphID = idMap.get(getEntityID(subject));
					hasLabelVertices.add(graphID);
					
					int labelID = idMap.get(getEntityID(object));
					if (!labels.containsKey(labelID))
						labels.put(labelID, new TreeSet<>());
					labels.get(labelID).add(graphID);
					
					if (count % 1000000 == 0)
						Util.println(count);
				}
			}
			
			String filePath = dir + "\\hasLabelVertices.txt";
			ArrayList<String> outputArray = new ArrayList<>(hasLabelVertices.size());
			for (int id : hasLabelVertices)
				outputArray.add(String.valueOf(id));
			ReadWriteUtil.WriteArray(filePath, outputArray);
			
			filePath = dir + "\\labels.txt";
			FileWriter writer = new FileWriter(new File(filePath));
			writer.write(labels.size() + "\n");
			for (int key : labels.keySet())
			{
				TreeSet<Integer> verticesID = labels.get(key);
				writer.write(String.format("%d,%d", key, verticesID.size()));
				for (int id : verticesID)
					writer.write(String.format(",%d", id));
				writer.write("\n");
			}
			writer.close();
			
		} catch (Exception e) {
			Util.println(line);
			e.printStackTrace();
		}
	}
	
	public static void getEdgeCount()
	{
		ArrayList<ArrayList<Integer>> graph = GraphUtil.ReadGraph(graphPath);
		int edgeCount = 0;
		for (ArrayList<Integer> neighbors : graph)
			edgeCount += neighbors.size();
		Util.println(edgeCount);
	}
	
	public static void removeLocationOutOfBound()
	{
		ArrayList<Entity> entities = GraphUtil.ReadEntity(entityPath);
		int count = 0;
		for (Entity entity : entities)
		{
			if (entity.IsSpatial)
			{
				if (entity.lon < -180 || entity.lon > 180 || entity.lat < -90 || entity.lat > 90)
				{
					count++;
					entity.IsSpatial = false;
					entity.lon = 0;
					entity.lat = 0;
				}
			}
		}
		Util.println(count);
		GraphUtil.writeEntityToFile(entities, entityPath);
	}
	
	public static void removeLocationOutOfEarth()
	{
		BufferedReader reader = null;
		HashMap<Long, Integer> idMap = readMap(entityMapPath);
		ArrayList<Entity> entities = GraphUtil.ReadEntity(entityPath);
		String outearthPath = dir + "\\outofearth_local.csv";
		String line = "";
		try {
			reader = new BufferedReader(new FileReader(new File(outearthPath)));
			while ((line = reader.readLine()) != null)
			{
//				String[] strList = line.split(",");
//				long wikiID = getEntityID(strList[0]);
				
				long wikiID = Long.parseLong(line); 

//				if (!idMap.containsKey(wikiID))
//					continue;

				int graphID = idMap.get(wikiID);
				Entity entity = entities.get(graphID);
				entity.IsSpatial = false;
				entity.lon = 0;
				entity.lat = 0;
			}
			GraphUtil.writeEntityToFile(entities, dir + "\\new_entity.txt");
		} catch (Exception e) {
			// TODO: handle exception
			Util.println(line);
			e.printStackTrace();
		}
	}
	
	public static void findEntitiesNotOnEarth()
	{
		String outputPath =  dir + "\\outofearth_local.csv";
		FileWriter writer = null;
		BufferedReader reader = null;
		String line = "";
		int predicateCount = 0;
		try {
			writer = new FileWriter(new File(outputPath));
			reader = new BufferedReader(new FileReader(new File(fullfilePath)));
			while ( (line = reader.readLine()) != null)
			{
				String[] strList = line.split(" ");
				String predicate = strList[1];
				if (predicate.matches("<http://www.wikidata.org/prop/direct/P\\d+>"))
				{
					predicateCount++;
					long propertyID = getPropertyID(predicate);
					if (propertyID == 376)
					{
						String object = strList[2];
						if (object.matches("<http://www.wikidata.org/entity/Q\\d+>"))
						{
							long planetID = getEntityID(object);
							if (planetID != 2)
							{
								String subject = strList[0];
								if (isEntity(subject))
								{
									long subjectWikiID = getEntityID(subject);
									writer.write(subjectWikiID + "\n");
								}
							}
						}
					}
					if (predicateCount % 10000000 == 0)
						Util.println(predicateCount);
				}
			}
			reader.close();
			writer.close();
		} catch (Exception e) {
			e.printStackTrace();
			System.exit(-1);
		}
	}
	
	/**
	 * Find some entities are not on earth.
	 */
	public static void checkLocation()
	{
		ArrayList<Entity> entities = GraphUtil.ReadEntity(entityPath);
		HashMap<String, String> map = ReadWriteUtil.ReadMap(entityMapPath);
		int count = 0;
		for (Entity entity : entities)
		{
			if (entity.IsSpatial)
			{
				if (entity.lon < -180 || entity.lon > 180 || entity.lat < -90 || entity.lat > 90)
				{
					Util.println(entity + " " + map.get("" + entity.id));
					count++;
				}
			}
		}
		Util.println(count);
	}
	
	public static void getRange()
	{
		ArrayList<Entity> entities = GraphUtil.ReadEntity(entityPath);
//		ArrayList<Entity> entities = Util.ReadEntity(dir + "\\new_entity.txt");
		Util.println(Util.GetEntityRange(entities));
	}
	
	public static void readGraphTest()
	{
		ArrayList<ArrayList<Integer>> graph = GraphUtil.ReadGraph(graphPath);
	}
	
	/**
	 * Check graph file first line and real number of vertices
	 */
	public static void checkGraphVerticesCount()
	{
		BufferedReader reader = null;
		try {
			reader = new BufferedReader(new FileReader(new File(graphPath)));
			String line = reader.readLine();
			int nodeCount = Integer.parseInt(line);
			Util.println(nodeCount);
			
			int index = 0;
			
			while ((line = reader.readLine()) != null)
			{
				index++;
			}
			reader.close();
			Util.println(index);
		}catch (Exception e) {
			// TODO: handle exception
			e.printStackTrace();
		}
	}

	public static void extractEntityMap()
	{
		BufferedReader reader;
		FileWriter writer;
		FileWriter logWriter;
		String line = "";
		long lineIndex = 0;
		
		HashSet<Long> startIdSet = new HashSet<>();
		ArrayList<Long> map = new ArrayList<>(95000000);
		HashSet<Long> leafVerticesSet = new HashSet<>();
		
		try {
			reader = new BufferedReader(new FileReader(new File(fullfilePath)));
			writer = new FileWriter(entityMapPath);
			logWriter = new FileWriter(logPath);
			
			long curWikiID = -1;
			
			while ((line = reader.readLine()) != null)
			{
				String[] strList = line.split(" ");
				String subject = strList[0];
				
				if (subject.matches("<http://www.wikidata.org/entity/Q\\d+>"))
				{
					long startID = getEntityID(subject);
					if (startID != curWikiID)
					{
						if (startIdSet.contains(startID))
						{
							throw new Exception(startID + "already exists before here!");
						}
						else
						{
							map.add(startID);
							startIdSet.add(startID);
							leafVerticesSet.remove(startID);
//							Util.Print(startID);
						}
						curWikiID = startID;
					}
				}

				String object = strList[2];
				if (object.matches("<http://www.wikidata.org/entity/Q\\d+>"))
				{
					long endID = getEntityID(object);
					if (startIdSet.contains(endID) == false)
						leafVerticesSet.add(endID);
				}
				
				lineIndex++;
				if (lineIndex % 10000000 == 0)
					Util.println(lineIndex);
				
				if (lineIndex == 10000000)
					break;
			}
			
			Util.println("leaf count: " + leafVerticesSet.size());
			for (long key : leafVerticesSet)
				map.add(key);
			
			lineIndex = 0;
			for ( long id : map)
			{
				writer.write(String.format("%d,%d\n", lineIndex, id));
				lineIndex++;
			}
			
			reader.close();
			writer.close();
			logWriter.close();
			
		} catch (Exception e) {
			Util.println(String.format("line %d:\n%s", lineIndex, line));
			e.printStackTrace();
		}
	}
	
	public static void extractEntityToEntityRelation()
	{
		BufferedReader reader;
		FileWriter writer;
		FileWriter logWriter;
		String line = "";
		long lineIndex = 0;
		
		try {
			HashMap<Long, Integer> idMap = readMap(entityMapPath);
			Util.println(idMap.size());
			
			reader = new BufferedReader(new FileReader(new File(fullfilePath)));
			writer = new FileWriter(graphPath);
			logWriter = new FileWriter(logPath);
			
			writer.write(idMap.size() + "\n");
			
			long curWikiID = 26;
			TreeSet<Integer> neighbors = new TreeSet<>();
			while ((line = reader.readLine()) != null)
			{
				String[] strList = line.split(" ");
				String subject = strList[0];
				
				if (subject.matches("<http://www.wikidata.org/entity/Q\\d+>"))
				{
					long startID = getEntityID(subject);
					if (curWikiID != startID)
					{
						writer.write(String.format("%d,%d", idMap.get(curWikiID), neighbors.size()));
						for( int neighbor : neighbors)
							writer.write("," + neighbor);
						writer.write("\n");
						neighbors = new TreeSet<>();
						curWikiID = startID;
					}
					
					String object = strList[2];
					if (object.matches("<http://www.wikidata.org/entity/Q\\d+>"))
					{
						long endID = getEntityID(object);
						int graphID = idMap.get(endID);
						neighbors.add(graphID);
					}
				}

				lineIndex++;
				if (lineIndex % 10000000 == 0)
					Util.println(lineIndex);
				
//				if (lineIndex == 10000000)
//					break;
			}
			reader.close();
			
			int leafID = idMap.get(curWikiID);
			leafID++;
			for (; leafID < idMap.size(); leafID++)
				writer.write(String.format("%d,0\n", leafID));
			
			writer.close();
			logWriter.close();
			
		} catch (Exception e) {
			Util.println(String.format("line %d:\n%s", lineIndex, line));
			e.printStackTrace();
		}
	}
	
	public static void generateEntityFile()
	{
		BufferedReader reader = null;
		FileWriter writer = null;
		int lineIndex = 0;
		String line = "";
		try {
			Util.println("read map from " + entityMapPath);
			HashMap<Long, Integer> idMap = readMap(entityMapPath);
			Util.println("initialize entities");
			ArrayList<Entity> entities = new ArrayList<>(idMap.size());
			for (int i = 0; i < idMap.size(); i++)
				entities.add(new Entity(i));
			
			Util.println("read locations from " + locationPath);
			reader = new BufferedReader(new FileReader(new File(locationPath)));
			while ((line = reader.readLine()) != null)
			{
				String[] strList = line.split(",");
				long wikiID = Long.parseLong(strList[0]);
				int graphID = idMap.get(wikiID);
				String location = strList[1];
				strList = location.split("Point\\(");
				location = strList[1];
				location = location.replace(")", "");
				strList = location.split(" ");
				double lon = Double.parseDouble(strList[0]);
				double lat = Double.parseDouble(strList[1]);
				Entity entity = entities.get(graphID);
				entity.IsSpatial = true;
				entity.lon = lon;
				entity.lat = lat;
				lineIndex++;
			}
			reader.close();
			
			GraphUtil.writeEntityToFile(entities, entityPath);
		} catch (Exception e) {
			Util.println(lineIndex);
			Util.println(line);
			e.printStackTrace();
		}
	}
	
	/**
	 * Extract entity full url, location map.
	 * Not used.
	 */
	public static void extract()
	{
		BufferedReader reader;
		FileWriter writer;
		FileWriter logWriter;
		String line = "";
		int p276Count = 0, p625Count = 0;
		long index = 0;
		try {
			reader = new BufferedReader(new FileReader(new File(fullfilePath)));
			writer = new FileWriter(locationPath);
			logWriter = new FileWriter(logPath);
			while ((line = reader.readLine()) != null)
			{
				String[] strList = line.split(" ");
				String predicate = strList[1];
				if (predicate.equals("<http://www.wikidata.org/prop/direct/P625>"))
				{
					p625Count++;
					if (line.contains("\""))
					{
						String subject = strList[0];
						Long wikiID = getEntityID(subject);
						strList = line.split("\"");
						String pointString = strList[1];
						writer.write(wikiID + "," + pointString + "\n");
					}
					else
						logWriter.write(line + "\n");
				}
				
				index++;
				
				if (index % 10000000 == 0)
					Util.println(index);
				
//				if (index == 100000)
//					break;
			}
			reader.close();
			writer.close();
			logWriter.close();
			
			Util.println("p625Count: " + p625Count);
		} catch (Exception e) {
			Util.println(String.format("line %d:\n%s", index, line));
			e.printStackTrace();
		}
	}

	public static HashMap<Long, Integer> readMap(String mapPath)
	{
		BufferedReader reader = null;
		String line = "";
		try {
			reader = new BufferedReader(new FileReader(new File(entityMapPath)));
			HashMap<Long, Integer> idMap = new HashMap<>();
			while ((line = reader.readLine()) != null)
			{
				String[] strList = line.split(",");
				int graphID = Integer.parseInt(strList[0]);
				long wikiID = Long.parseLong(strList[1]);
				idMap.put(wikiID, graphID);
			}
			reader.close();
			return idMap;
		} catch (Exception e) {
			if (reader != null)
				try {
					reader.close();
				} catch (IOException e1) {
					// TODO Auto-generated catch block
					e1.printStackTrace();
				}
			// TODO: handle exception
		}
		return null;
	}
	
	public static long getPropertyID(String string)
	{
		if (!string.contains("http://www.wikidata.org/prop/direct/P"))
		{
			Util.println(string + " does not match property format");
			System.exit(-1);
		}
		String tempString = string.replace("<", "").replace(">", "");
		String[] stringList = tempString.split("prop/direct/P");
		long id = Long.parseLong(stringList[1]);
		return id;
	}
	
	public static boolean isProperty(String string)
	{
		if (string.matches("<https://www.wikidata.org/wiki/Property:P\\d+>"))
			return true;
		else return false;
	}
	
	public static long getEntityID(String string)
	{
		if (!string.contains("http://www.wikidata.org/entity/"))
		{
			Util.println(string + " does not match entity format");
			System.exit(-1);
		}
		String tempString = string.replace("<", "").replace(">", "");
		String[] stringList = tempString.split("/entity/Q");
		long id = Long.parseLong(stringList[1]);
		return id;
	}
	

	public static long getPropertyEntityID(String string)
	{
		String tempString = string.replace("<", "").replace(">", "");
		if (tempString.matches("http://www.wikidata.org/entity/P\\d+"))
		{
			String[] stringList = tempString.split("/entity/P");
			long id = Long.parseLong(stringList[1]);
			return id;
		}
		else return -1;
	}
	
	public static boolean isPropertyEntity(String string)
	{
		if (string.matches("<http://www.wikidata.org/entity/P\\d+>"))
			return true;
		else return false;
	}
	
	public static boolean isEntity(String string)
	{
		if (string.matches("<http://www.wikidata.org/entity/Q\\d+>"))
			return true;
		else return false;
	}
}
