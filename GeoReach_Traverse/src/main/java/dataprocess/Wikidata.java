package dataprocess;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.TreeSet;

import org.apache.lucene.document.StringField;
import org.neo4j.cypher.internal.frontend.v2_3.ast.functions.E;

import commons.Entity;
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
		extractEntityToEntityRelation();
		checkGraphVerticesCount();
//		generateEntityFile();
		
		
		//test code
//		String string = "<http://www.wikidata.org/entity/Q26>";
//		Util.Print(isEntity(string));
//		Util.Print(getEntityID(string));
	}
	
	public static void checkGraphVerticesCount()
	{
		BufferedReader reader = null;
		try {
			reader = new BufferedReader(new FileReader(new File(graphPath)));
			String line = reader.readLine();
			int nodeCount = Integer.parseInt(line);
			Util.Print(nodeCount);
			
			int index = 0;
			
			while ((line = reader.readLine()) != null)
			{
				index++;
			}
			reader.close();
			Util.Print(index);
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
					Util.Print(lineIndex);
				
				if (lineIndex == 10000000)
					break;
			}
			
			Util.Print("leaf count: " + leafVerticesSet.size());
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
			Util.Print(String.format("line %d:\n%s", lineIndex, line));
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
			Util.Print(idMap.size());
			
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
					Util.Print(lineIndex);
				
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
			Util.Print(String.format("line %d:\n%s", lineIndex, line));
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
			Util.Print("read map from " + entityMapPath);
			HashMap<Long, Integer> idMap = readMap(entityMapPath);
			Util.Print("initialize entities");
			ArrayList<Entity> entities = new ArrayList<>(idMap.size());
			for (int i = 0; i < idMap.size(); i++)
				entities.add(new Entity(i));
			
			Util.Print("read locations from " + locationPath);
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
			
			writer = new FileWriter(entityPath);
			writer.write(entities.size() + "\n");
			for (Entity entity : entities)
			{
				writer.write(entity.id + ",");
				if (entity.IsSpatial)
					writer.write(String.format("1,%s,%s\n", 
							String.valueOf(entity.lon), String.valueOf(entity.lat)));
				else writer.write("0\n");
			}
			writer.close();
			
		} catch (Exception e) {
			Util.Print(lineIndex);
			Util.Print(line);
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
					Util.Print(index);
				
//				if (index == 100000)
//					break;
			}
			reader.close();
			writer.close();
			logWriter.close();
			
			Util.Print("p625Count: " + p625Count);
		} catch (Exception e) {
			Util.Print(String.format("line %d:\n%s", index, line));
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
	
	public static long getEntityID(String string)
	{
		String tempString = string.substring(1, string.length() - 1);
		String[] stringList = tempString.split("/entity/Q");
		long id = Long.parseLong(stringList[1]);
		return id;
	}
	
	public static boolean isEntity(String string)
	{
		if (string.matches("<http://www.wikidata.org/entity/Q\\d+>"))
			return true;
		else return false;
	}
}
