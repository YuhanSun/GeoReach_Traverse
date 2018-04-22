package construction;

import java.nio.ByteBuffer;
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
    	
    	String ser = "OjAAAAEAAAAAAAAAEAAAANwQ";
    	ByteBuffer newbb = ByteBuffer.wrap(Base64.getDecoder().decode(ser));
        ImmutableRoaringBitmap reachgrid = new ImmutableRoaringBitmap(newbb);
        Util.Print(reachgrid);
    }
}
