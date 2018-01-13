package com.hadoop;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Partitioner;

public class VCPartitioner extends Partitioner<Text, IntWritable>{

	@Override
	public int getPartition(Text key, IntWritable value, int numOfPartitions) {
		/*if (startsWithVowel(key))
			return numOfPartitions%2;
		return numOfPartitions%2 + 1;*/
		int iVal = startsWith(key); // NOPMD by administrator on 8/13/17 9:33 PM
		int iReturnVal = (iVal == 0) ? 0 : (iVal == 1 ? 1 : 2) ;
		System.out.println("numOfPartitions: "+numOfPartitions + " iVal: "+iVal+" iReturnVal: "+iReturnVal);
		return iReturnVal;
		
	}

	/*private boolean startsWithVowel(Text key) {
		if (key == null || key.toString().isEmpty()) return false;
		char c = key.toString().toUpperCase().charAt(0);
		if ((c=='A') || (c=='E') || (c=='I') || (c=='O') || (c=='U')) return true;
		return false;
	}*/
	
	private int startsWith(Text key) {
		if (key == null || key.toString().isEmpty()) return -1;
		char c = key.toString().toUpperCase().charAt(0);
		int iReturnVal = (c == 'T') ? 0 : (c == 'W' ? 1 : 2); 
		/*if ((c=='A') || (c=='E') || (c=='I') || (c=='O') || (c=='U')) return true;
		return false;*/
		return iReturnVal;
	}
}
