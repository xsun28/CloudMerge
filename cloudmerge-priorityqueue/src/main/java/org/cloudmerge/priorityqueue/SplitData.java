package org.cloudmerge.priorityqueue;

import java.io.BufferedWriter;
import java.io.FileWriter;
import java.io.IOException;
import java.io.PrintWriter;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class SplitData {
	Logger logger = LoggerFactory.getLogger(getClass());
	private WorkloadSplit[] splits;
	
	public SplitData(String output,String startchr,String endchr,int procnum,String prefix) {
		Pattern  pattern = Pattern.compile("[xym\\d]{1,2}",Pattern.CASE_INSENSITIVE);
		Matcher matcher = pattern.matcher(startchr);
		int schr = 0, echr = 0;
		if(matcher.find()){
			int start = matcher.start();
			int end = matcher.end();
			schr = Integer.parseInt(startchr.substring(start,end));
		}
		matcher = pattern.matcher(endchr);
		if(matcher.find()){
			int start = matcher.start();
			int end = matcher.end();
			echr = Integer.parseInt(endchr.substring(start,end));
		}
		this.splits = split_workload( schr,echr,procnum,prefix);
		writeToFile(splits,output);
	}
	
	public SplitData(String output,int startchr, int endchr,int procnum,String prefix) {
		this.splits = split_workload( startchr,endchr,procnum,prefix);
		writeToFile(splits,output);
	}
	
	public WorkloadSplit[] split_workload(int startchr,int endchr,int procnum,String prefix){
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
            start_pos += average_pos;
            end_pos += average_pos;
        }
        return splits;
    }
	
	public void writeToFile(WorkloadSplit[] splits,String output){
		try(PrintWriter pw = new PrintWriter(new BufferedWriter(new FileWriter(output)))){
			for(WorkloadSplit split: splits)
				pw.println(split.toString());
		}catch(IOException ioe){
			logger.error(ioe.getMessage());
		}
	}
	
	public static void main(String[] args) { //java -jar cloudmerge-pqsplitdata.jar Result/splits.csv 1 26 4 chr
		// TODO Auto-generated method stub
		String output = args[0];   //"/Users/Xiaobo/Desktop/splits";
		int start_chr = Integer.parseInt(args[1]);
		int end_chr =  Integer.parseInt(args[2]);
		int procnum = Integer.parseInt(args[3]);
		String prefix = args[4];  //"chr";
		SplitData sd = new SplitData(output,start_chr,end_chr,procnum,prefix);
	}

}
