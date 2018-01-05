package org.cloudmerge.priorityqueue;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.FilenameFilter;
import java.io.IOException;
import java.io.PrintWriter;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Comparator;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import htsjdk.tribble.readers.TabixReader;

public class test {
	private static Logger logger = LoggerFactory.getLogger("test");
	public static void func(int x){
		System.out.println("inner x is "+x);
	}
	
	public static WorkloadSplit[] split_workload(int startchr,int endchr,int procnum,String prefix){
		WorkloadSplit[] splits = new WorkloadSplit[procnum];
		long total_pos_num = 0;
		for(int i=startchr-1;i<endchr;i++)
			total_pos_num += WorkloadSplit.CHRLENGTHS[i];
		long average_pos = (long)(total_pos_num / procnum);
		logger.debug("total pos "+total_pos_num+" average_pos "+average_pos);
		long start_pos = 1;
		long end_pos = average_pos;
		for(int i=0;i<startchr-1;i++){
			start_pos += WorkloadSplit.CHRLENGTHS[i];
			end_pos += WorkloadSplit.CHRLENGTHS[i];
		}
		for(int i=0;i<procnum;i++){
			logger.debug("process "+i+" startpos "+start_pos+" endpos "+end_pos);
			splits[i] = new WorkloadSplit(start_pos,end_pos,prefix);
			logger.debug("split "+i+" is "+splits[i]);
			start_pos += average_pos;
			end_pos += average_pos;
		}
		return splits;
	}
	public static class Reader implements Runnable{
		private TabixReader.Iterator iter;
		private Logger logger = LoggerFactory.getLogger(getClass());
		public Reader(TabixReader.Iterator iter){
			this.iter = iter;
		}
		@Override
		public void run(){
			String result = null;
			try{
			while(iter!= null  && (result = iter.next()) != null ){
				System.out.println("result "+result);
			}
			}catch(IOException ioe){
				System.err.println(ioe.getMessage());
			}
		}
	}
	public static void main(String[] args) {
		// TODO Auto-generated method stub
//		String path = "/Users/Xiaobo/Jobs/CloudMerge/data/tabixdata/1.vcf.gz";
//		String outpath = "/Users/Xiaobo/Desktop/test";
//		String query = "chr1:1";
//		String query1 = "chr4:82626091";
//		TabixReader.Iterator[] iters = new TabixReader.Iterator[2];
//		try{
//		TabixReader reader1 = new TabixReader(path);
//		TabixReader reader2 = new TabixReader(path);
//		
//		iters[0] = reader1.query(query);
//		iters[1] = reader2.query(query1);
////		String result1 = null;
////		String result2 = null;
//////		PrintWriter pw = new PrintWriter(new BufferedWriter(new FileWriter(outpath)));
////		while(iters[0]!= null && iters[1]!= null && (result1 = iters[0].next()) != null && (result2 = iters[0].next()) != null){
////			logger.debug("result1 "+result1);
////			logger.debug("result2 "+result2);
////		}
//
//		Thread thread1 = new Thread(new Reader(iters[0]));
//		thread1.start();
//		Thread thread2 = new Thread(new Reader(iters[1]));
//		thread2.start();
//		reader1.close();
//		reader2.close();
////		pw.close();
//		}catch(IOException ioe){
//			System.out.println(ioe.getMessage());
//		}
//		String input = "/Users/Xiaobo/Jobs/CloudMerge/data/tabixdata/";
//		File dir=new File(input);
//		 
//		String[] fileNames=dir.list(new FilenameFilter(){
//			@Override
//			public boolean accept(File dir,String name){
//				return name.toLowerCase().endsWith("vcf.gz");
//			}
//		});
//		ArrayList<String>inputFileNameList = new ArrayList<String>(Arrays.asList(fileNames));
//		System.out.println("before: "+inputFileNameList.toString());
//		Collections.sort(inputFileNameList,new Comparator<String>(){
//			@Override
//			public int compare(String name1,String name2){
//				int number1 = Integer.parseInt(name1.substring(0,name1.indexOf(".")));
//				int number2 = Integer.parseInt(name2.substring(0,name2.indexOf(".")));
//				return number1-number2;
//			}
//		});
//		System.out.println("after: "+inputFileNameList.toString());
//		
//		String[] a = new String[1];
//		a[0] = "s";
//		System.out.println("a0 "+a[0]);
//		int startchr=1;
//		int endchr=26;
//		int procnum=4;
//		String prefix = "chr";
//		WorkloadSplit[] splits = split_workload(startchr,endchr, procnum, prefix);
//		int i = 0, j=0;
//		for(WorkloadSplit split:splits){
//			
//			for(String query:split.get_query()){
//				
//				System.out.println("Split "+i+" query "+j+" is: "+query);
//				j++;
//			
//			}
//			i++;
//		}
		
		String output = "/Users/Xiaobo/Desktop/splits";
		String prefix = "chr";
		SplitData sd = new SplitData(output,1,26,4,prefix);
	}

}
