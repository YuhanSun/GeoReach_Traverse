package construction;

import java.io.Console;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.text.DecimalFormat;
import java.util.ArrayList;
import java.util.Base64;

import org.roaringbitmap.buffer.ImmutableRoaringBitmap;

import commons.MyRectangle;
import commons.Util;
import commons.VertexGeoReach;
import commons.VertexGeoReachList;

/**
 * Hello world!
 *
 */
public class App 
{
    public static void main( String[] args )
    {
//    	ArrayList<Integer> arrayList = new ArrayList<Integer>();
//    	arrayList.add(null);
//    	Integer i = arrayList.get(0);
//    	i = arrayList.get(1);
//    	
//    	Util.Print(i);
//        System.out.println( "Hello World!" );
    	
//    	String ser = "OjAAAAEAAAAAAAAAEAAAANwQ";
//    	ByteBuffer newbb = ByteBuffer.wrap(Base64.getDecoder().decode(ser));
//        ImmutableRoaringBitmap reachgrid = new ImmutableRoaringBitmap(newbb);
//        Util.Print(reachgrid);
    	
//    	String listStr = "[0, 1, 2]";
//    	Util.Print(listStr.substring(1, listStr.length()-1));
    	
//    	for (double x = 0.00001; x < 0.2; x *= 10)
//    	{
//    		Util.Print(x);
//    		DecimalFormat df = new DecimalFormat("0E0");
//    		String string = df.format(x);
//    		Util.Print(string);
//    	}
    	
//    	String teString = "Point(-5.84 54.590933333333)";
//    	Util.Print(args[0]);
//    	String teString = args[1];
//    	String[] strings = teString.split("3\\)");
//    	Util.Print(strings.length);
//    	for (String string : strings)
//    		Util.Print(string);
    	
//    	String dir = "D:\\Ubuntu_shared\\GeoReachHop\\data\\wikidata";
//    	String indexWholePath = dir + "\\128_128_2_whole.txt";
////    	ArrayList<VertexGeoReachList> index = Util.readGeoReachWhole(indexWholePath);
//    	
//    	ArrayList<ArrayList<ArrayList<Integer>>> reachgridsList = null;
//    	ArrayList<ArrayList<MyRectangle>> rmbrsList = null;
//    	Util.readGeoReachWhole(indexWholePath, reachgridsList, rmbrsList);
//    	
//    	try {
//			System.in.read();
//		} catch (IOException e) {
//			// TODO Auto-generated catch block
//			e.printStackTrace();
//		}
    	
    	String dir = "D:\\Ubuntu_shared\\GeoReachHop\\data\\Gowalla_10";
    	String file1 = dir + "\\128_128_3_whole.txt";
    	String file2 = dir + "\\128_128_3_whole_test.txt";
    	Util.Print(Util.compareFile(file1, file2));
    }
}
