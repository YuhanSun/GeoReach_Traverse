package construction;

import java.util.ArrayList;

import commons.Util;

/**
 * Hello world!
 *
 */
public class App 
{
    public static void main( String[] args )
    {
    	ArrayList<Integer> arrayList = new ArrayList<Integer>();
    	arrayList.add(null);
    	Integer i = arrayList.get(0);
    	i = arrayList.get(1);
    	
    	Util.Print(i);
        System.out.println( "Hello World!" );
    }
}
