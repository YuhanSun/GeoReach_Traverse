package kwaysort;

import java.util.ArrayList;
import java.util.List;
import java.util.PriorityQueue;

/*
 *
 * Given K Sorted List merge them into a sort list
 */

public class KWayMerging {
	PriorityQueue<Node> queue;

	public KWayMerging() {
		queue = new PriorityQueue<Node>(50, new NodeComparator());
	}
	
	public KWayMerging(int size)
	{
		queue = new PriorityQueue<Node>(size, new NodeComparator());
	}

	public ArrayList<Integer> mergeKList(ArrayList<ArrayList<Integer>> input) {
		ArrayList<Integer> output = new ArrayList<Integer>();
		if (input == null)
			return null;
		if (input.isEmpty())
			return new ArrayList<>();
		int[] index = new int[input.size()];
		makeHeap(input, index);
		int curVal = -1;
		while (!queue.isEmpty()) {
			Node node = queue.remove();
			int listIndex = node.getIndexList();
			int val = node.getData();
			if (curVal != val)
			{
				output.add(node.getData());
				curVal = val;
			}
			if (index[listIndex] < input.get(listIndex).size()) {
				queue.add(new Node(input.get(listIndex).get(index[listIndex]),
						listIndex));
				index[listIndex] = ++index[listIndex];
			}
		}
		return output;
		
	}
	
   /*
    * Creating an initial Heap.
    */

	private void makeHeap(ArrayList<ArrayList<Integer>> input, int[] index) {

		for (int i = 0; i < input.size(); i++) {
			if (!input.get(i).isEmpty()) {
				queue.add(new Node(input.get(i).get(0), i));
				index[i] = ++index[i];
			} else
				input.remove(i);
		}

	}

	public static void main(String[] args) {
		KWayMerging k = new KWayMerging();

		ArrayList<Integer> input1 = new ArrayList<Integer>();
		input1.add(1);
		input1.add(5);
		input1.add(9);

		ArrayList<Integer> input2 = new ArrayList<Integer>();
		input2.add(2);
		input2.add(4);
		input2.add(12);
		input2.add(14);

		ArrayList<Integer> input3 = new ArrayList<Integer>();
		input3.add(2);
		input3.add(4);
		input3.add(11);

		ArrayList<ArrayList<Integer>> inputA = new ArrayList<ArrayList<Integer>>();
		inputA.add(input1);
		inputA.add(input2);
		inputA.add(input3);

		System.out.println("Merged List:" + k.mergeKList(inputA));

	}

}