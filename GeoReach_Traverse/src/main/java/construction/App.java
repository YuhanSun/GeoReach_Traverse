package construction;

import java.nio.ByteBuffer;
import java.text.DecimalFormat;
import java.util.ArrayList;
import java.util.Base64;

import org.roaringbitmap.buffer.ImmutableRoaringBitmap;

import commons.Util;

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
    	
    	for (double x = 0.00001; x < 0.2; x *= 10)
    	{
    		Util.Print(x);
    		DecimalFormat df = new DecimalFormat("0E0");
    		String string = df.format(x);
    		Util.Print(string);
    	}
    }
}
