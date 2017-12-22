/*using interval sampler for already sorted original files and transform directly into TPED format*/
package org.cloudmerge.mapreduce;
 
import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.OutputStreamWriter;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.*;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.commons.cli.CommandLine;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.SequenceFile.CompressionType;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.lib.output.MultipleOutputs;
import org.apache.hadoop.mapreduce.Counter;
import org.apache.hadoop.mapreduce.Counters;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Partitioner;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.LazyOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.mapreduce.lib.jobcontrol.ControlledJob;
import org.apache.hadoop.mapreduce.lib.jobcontrol.JobControl;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.compress.BZip2Codec;
import org.apache.hadoop.io.compress.CompressionCodec;
import org.apache.hadoop.io.compress.Lz4Codec;
import org.apache.hadoop.mapreduce.lib.partition.InputSampler;
import org.apache.hadoop.mapreduce.lib.partition.TotalOrderPartitioner;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.cloudmerge.mapreduce.common.Quality;

public class MRVCF2TPED extends Configured implements Tool {
	enum ChrNumCounter{
		chr1, chr2, chr3, chr4, chr5, chr6, chr7, chr8, chr9, chr10, chr11, chr12, chr13, chr14, chr15, chr16,
		chr17, chr18, chr19, chr20, chr21, chr22, chrx, chry, chrxy, chrm;
	}
	enum ChrSampleNumCounter{
		chr1, chr2, chr3, chr4, chr5, chr6, chr7, chr8, chr9, chr10, chr11, chr12, chr13, chr14, chr15, chr16,
		chr17, chr18, chr19, chr20, chr21, chr22, chrx, chry, chrxy, chrm;
	}
	
	public static class ChrBinningMapper extends Mapper<LongWritable, Text, LongWritable, IntWritable>	{//filtering out unwanted fields and bin records into chromosome folders under VoTECloud/output/chr1
		
		private String filename;
		private MultipleOutputs<LongWritable, Text> mos;
		private long kept = 1;    											//number of kept records for sampling
		private long records = 0; 											//number of records currently read in the input split
		private double freq;			
		private Quality qual_filter;
		private Random random;
		private boolean ordered;
		private int startchr, endchr;
		private int genotype_col;
		@Override
		public void setup(Context context) throws IOException, InterruptedException{ 		//called once at the beginning of a mapper with a single input split 
			
			Configuration conf = context.getConfiguration();
			freq = Double.parseDouble(conf.get("samplerate"));
			qual_filter = Enum.valueOf(Quality.class, conf.get("quality").trim().toUpperCase());
			random = new Random();
			ordered = conf.getBoolean("ordered", true);
			startchr = conf.getInt("startchr", 1);
			endchr = conf.getInt("endchr", 26);
			genotype_col = conf.getInt("genotype_col",9);
			FileSplit fileSplit = (FileSplit)context.getInputSplit();
			String fullname=fileSplit.getPath().getName();									//get the name of the file where the input split is from
			filename=fullname.substring(0, fullname.indexOf('.')); 							//the individual number			
			mos=new MultipleOutputs(context);
			
		}//end of setup
		

		
		@Override
		public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
			int chrm;
			LongWritable outkey = new LongWritable();
			StringBuilder outputResult = new StringBuilder();
			String line = value.toString();
			Quality qual = common.getQuality(line);
			if(null != qual && qual.compareTo(qual_filter) >= 0){
			
				String[] fields = line.split("\\s+");
				String chrnum = fields[0].substring(fields[0].indexOf("r")+1).trim();
				chrm = common.parseChrnum(chrnum);
				if(chrm >= startchr && chrm <= endchr){         //filter out chrm outside of the chrm range
					records++;
					context.getCounter(ChrNumCounter.values()[chrm-1]).increment(1);  //count the chr records num
					String outputFile;			
					outputFile = fields[0].trim()+"/part";				//output file for this record  
					String genotype = common.parseGenotype(line,genotype_col);
					outkey.set(Long.parseLong(fields[1]));
					boolean sample = false;                             //check if sample this record
					if(ordered){
						sample = ((double) kept / (double)records) < freq;
					}else{
						double random_num = random.nextDouble();
						sample = random_num*100 < freq*100;
					}
					if (sample) {
						kept++;
						context.write(outkey, new IntWritable(chrm));
					}
					outputResult.append(filename+"%").append(chrnum+"%").append(fields[2].trim()+"%").append(fields[3]+"%").append(genotype);
					mos.write("ChrBinningMos",outkey, new Text(outputResult.toString()), outputFile);
				}
			}
		}// end of map
		@Override
		protected void cleanup(Context context) throws IOException, InterruptedException {
			mos.close();
		}
	
	}// end of chrBinningMapper

	public static class PartitionlstPartitioner extends Partitioner <LongWritable, IntWritable>{
		@Override
		public int getPartition(LongWritable key, IntWritable value, int numPartitions){
			int chrmnum = value.get();
			return chrmnum-1;
		}
	}

	public static class PartitionReducer extends Reducer<LongWritable, IntWritable, LongWritable, NullWritable> {
		
		private int StepSize;
		private int samplenums = 0;
//		private int last = 0;
//		private int totalnums;
//		private String chrm = null;
//		private ArrayList<Long> samples = new ArrayList<Long> ();
//		private BufferedWriter writer;
//		private String sampleNumberFiles; 		//The file containing the number of sampled position
		private Configuration conf;
		private Counter sampleCounter;
		@Override
		public void setup(Context context) throws IOException, InterruptedException{
			conf = context.getConfiguration();
			StepSize = Integer.parseInt(conf.get("fileno"));
			int chrmnum = context.getTaskAttemptID().getTaskID().getId()+1;
//			if(chrmnum == 23) chrm = "X";
//			else if(chrmnum == 24) chrm = "Y";
//			else if(chrmnum == 25) chrm = "XY";
//			else if(chrmnum == 26) chrm = "M";
//			else chrm =""+chrmnum;
//			sampleNumberFiles = conf.get("outputpath")+"/chr"+chrm+"samplenum";
			sampleCounter = context.getCounter(ChrSampleNumCounter.values()[chrmnum-1]);
		}

		@Override
		public void reduce(LongWritable key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
			//outkey = key;
//			long longkey = key.get();
			for(IntWritable value: values){
				samplenums++;
				if(samplenums>=StepSize){ // take one sample every file_no
					sampleCounter.increment(1);
//					if(last!=0){
//						long lastkey = samples.get(last-1);
//						if(longkey != lastkey) {
//							samples.add(longkey);
//							last++;
//							context.write(key,NullWritable.get());
//							samplenums = 0;
//						}
//						else {
//							samplenums--;
//							break;
//						}	
//					}else {
//						samples.add(longkey);
//						last++;
						context.write(key,NullWritable.get());
						samplenums = 0;
//					}
				}
			}
		}//end of reduce
		
//		@Override
//		protected void cleanup(Context context) throws IOException, InterruptedException {
//			totalnums = samples.size();
//			FileSystem fs = FileSystem.get(URI.create(sampleNumberFiles), conf);
//	    	try{
//	    		writer = new BufferedWriter(new OutputStreamWriter(fs.create(new Path(sampleNumberFiles), true)));
//	    		System.out.println("total sample nums in "+chrm+" is "+totalnums);
//	    		writer.write(""+totalnums);
//	    	}finally{
//	    	writer.close();
//	    	}	
//		}//end of cleanup
		
	}//end of partitionReducer

	public static class ValueReducer extends Reducer<LongWritable, Text, LongWritable, Text> {

		private int total;
		private MultipleOutputs<NullWritable, Text> mos;
		@Override
		public void setup(Context context) throws IOException, InterruptedException{
			Configuration con = context.getConfiguration();
			total=Integer.parseInt(con.get("fileno"));
			mos=new MultipleOutputs(context);
		}
		@Override
		public void reduce(LongWritable key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
			StringBuilder outputStr = new StringBuilder();
			boolean [] notNull = new boolean[total];
			String ref = null;
			String [] genotypes=new String[total];
			String chrm = null;
			boolean first = true;
			for (Text t : values) {
				String line = t.toString();
				String[] temp = line.split("%");
				if(first){ 
					chrm = "chr"+temp[1];
					ref = temp[3].trim();
					outputStr.append(temp[1]+"\t").append(temp[2]).append("\t0\t").append(key.toString()+"\t");
					first = false;
				}
				int current = Integer.parseInt(temp[0]);
				genotypes[current-1] = temp[4];
				notNull[current-1] = true;
			}
	
			for(int count=1;count<=total;count++){
				if(!notNull[count-1]){
					outputStr.append(ref+" ").append(ref+" ");
				}
				else{				
					outputStr.append(genotypes[count-1]+" ");
				}
			}
			mos.write(NullWritable.get(),new Text(outputStr.toString()), chrm+".tped/part");

		}

		@Override
		protected void cleanup(Context context) throws IOException, InterruptedException {
			mos.close();
		}
	}


	public static Configuration getSecondJobConf( Tool tool, Configuration conf, int chr, Path inputPath, Path outputPath, String basePath, 
			double samplerate, int indno, long sampleNum)
		 throws IOException, ClassNotFoundException, InterruptedException,URISyntaxException{
//		conf.setBoolean("mapreduce.map.speculative", false);
		Job orderJob = Job.getInstance(conf,"Chr"+chr+" job");
		orderJob.setJarByClass(tool.getClass());
		orderJob.setReducerClass(ValueReducer.class);
		orderJob.getConfiguration().setBoolean("mapred.compress.map.output", true);
		orderJob.getConfiguration().setClass("mapred.map.output.compression.codec", Lz4Codec.class, CompressionCodec.class);
		FileInputFormat.setInputPaths(orderJob,  inputPath);
		orderJob.setInputFormatClass(SequenceFileReadCombiner.class);
		FileOutputFormat.setOutputPath(orderJob, outputPath);
//		if(compression){
//		FileOutputFormat.setCompressOutput(orderJob,true);
//		FileOutputFormat.setOutputCompressorClass(orderJob, BZip2Codec.class);
//		}
		orderJob.setOutputKeyClass(LongWritable.class);
		orderJob.setOutputValueClass(Text.class);
		LazyOutputFormat.setOutputFormatClass(orderJob, TextOutputFormat.class);
		//orderJob.getConfiguration().set( "mapred.textoutputformat.separator", "");
	
		String file = basePath+"/part-r-000";  //path of the partition list files eg. VoTECloud/output/part-r-00022
		if(chr > 10) file += (chr-1);
		else if(chr <= 10) file += "0"+(chr-1);

		Path partitionFile = new Path(file);
//		String samplenumFileStr = basePath + "/chr"+chrm+"samplenum";
//		Path samplenumFile = new Path(samplenumFileStr);
//		FileSystem fs = FileSystem.get(URI.create(samplenumFileStr),conf);
//		BufferedReader reader=new BufferedReader(new InputStreamReader(fs.open(samplenumFile)));
//		String samplenum = reader.readLine();
		System.out.println("retrieved samplenum for "+chr+" is "+sampleNum);
//		int reducernum = Integer.parseInt(samplenum.trim())+1; //reducer num = split point number +1
		int reducernum = (int)sampleNum+1; //reducer num = split point number +1
//		reader.close();	
		orderJob.setPartitionerClass(TotalOrderPartitioner.class);
		FileSystem fs = FileSystem.get(URI.create(file),conf);
		if(fs.exists(partitionFile)){
			
			TotalOrderPartitioner.setPartitionFile(orderJob.getConfiguration(), partitionFile);// set the partition file to the output file from partitionreducer
			orderJob.setNumReduceTasks(reducernum);
//			orderJob.addCacheFile(partitionFile.toUri());
		}else{
			
			Path regenerated_partitionFile = new Path(outputPath + "_partitions.lst"); //regenerate the partition list using the TotalOrderPartitioner for sparse chromsome which doesn't have a sampled-out list
			if(!fs.exists(regenerated_partitionFile)){ 
				int num = 500;
				System.out.println("partition file for "+chr+" not existed");
				orderJob.setNumReduceTasks((indno/93)*4);
				orderJob.setInputFormatClass(SequenceFileReadCombinerSmall.class);
				TotalOrderPartitioner.setPartitionFile(orderJob.getConfiguration(), regenerated_partitionFile);
				InputSampler.writePartitionFile(orderJob, new InputSampler.RandomSampler<LongWritable, Text>(samplerate*1000, num, 1));			
			}else{// if there is already re-generated partition file from previous run, use it
				orderJob.setNumReduceTasks((indno/93)*4);
				TotalOrderPartitioner.setPartitionFile(orderJob.getConfiguration(), regenerated_partitionFile);
			}
//			orderJob.addCacheFile(regenerated_partitionFile.toUri());
		}
		
		return orderJob.getConfiguration();
	}
	


	public int run(String [] args) throws Exception {
		
		int code = 0;
		Configuration conf = getConf();
		CommandLine cmd = commandParser.parseCommands(args, conf);
		String input = cmd.getOptionValue("i");			//plinkcloud/input/
		String output = cmd.getOptionValue("o");		//mapreduce/	
		Path inputPath = new Path(input); // 		VoTECloud/input
		Path outputPath = new Path(output); // 	VoTECloud/output
		int indno = Integer.parseInt(cmd.getOptionValue("n"));
		double sampleRate = 0.0001;
		if(cmd.hasOption("r"))
			sampleRate = Double.parseDouble(cmd.getOptionValue("r")); 
		String chrmrange = cmd.getOptionValue("c");
		String quality = cmd.getOptionValue("q");
		boolean ordered = true;
		if(cmd.hasOption("s"))
			ordered = Boolean.parseBoolean(cmd.getOptionValue("s"));
		int genotypeColumn = Integer.parseInt(cmd.getOptionValue("g"));
		//boolean compression = Boolean.parseBoolean(args[5]);
		int index = chrmrange.indexOf("-");
		String firstchrm = chrmrange.substring(0, index).trim();
		String lastchrm = chrmrange.substring(index+1).trim();		
		int first = common.parseChrnum(firstchrm);
		int last = common.parseChrnum(lastchrm);
		int chromsno = last-first+1;
		String defaultName = conf.get("fs.default.name");
		System.out.println("default "+defaultName);
		String []chroms = new String[chromsno];
		String []chrmFiles = new String[chromsno];
		String []chrmResults = new String[chromsno];
		for(int i = 0, j = first; i<chromsno; i++, j++){
			if(j == 23)chroms[i] = "chrX";
			else if(j==24) chroms[i] = "chrY";
			else if(j==25) chroms[i] = "chrXY";
			else if(j==26) chroms[i] = "chrM";
			else chroms[i] = "chr"+j;
			chrmFiles[i] = defaultName+output+"/"+chroms[i];
			chrmResults[i] = chrmFiles[i]+"_result";
		} 
		long [] chrSampleNum = new long[chromsno];  //sampled number of records for each chr
		long [] chrNum = new long[chromsno];   //number of records for each chr
		FileSystem fs = FileSystem.get(URI.create(chrmFiles[0]),conf);
		boolean exist = false;
		if(cmd.hasOption("e"))
			exist = true;
//		boolean exist = fs.exists(new Path(chrmFiles[0])); //test if chromosomes have been already binned
		conf.set("fileno", String.valueOf(indno));
		conf.set("outputpath",defaultName+output);
		conf.set("samplerate", ""+sampleRate);
		conf.setBoolean("ordered", ordered);
		conf.set("quality", quality);
		conf.setInt("startchr", first);
		conf.setInt("endchr", last);
		conf.setInt("genotype_col", genotypeColumn);
		conf.setDouble("mapreduce.reduce.input.buffer.percent", 1);   //because the reduce task is light, so we can reserve memory for input buffer after sort/merge phase before reduce phase to minimize spilled records.
		conf.setDouble("mapreduce.reduce.merge.inmem.threshold", 0);
		if(!exist){
//			conf.setBoolean("mapreduce.map.speculative", false);                  // set in the configuration file
			conf.setDouble("mapreduce.job.reduce.slowstart.completedmaps", 0.8);  //set the reduce slow start to 50% of completed map tasks
			Job chrJob = Job.getInstance(conf,"chromseparation");
			chrJob.setJarByClass(getClass());
			FileInputFormat.setInputPaths(chrJob, inputPath);
			FileOutputFormat.setOutputPath(chrJob, outputPath);
			chrJob.setMapperClass(ChrBinningMapper.class);
			chrJob.setMapOutputKeyClass(LongWritable.class);
			chrJob.setMapOutputValueClass(IntWritable.class);
			chrJob.setPartitionerClass(PartitionlstPartitioner.class);
			chrJob.setReducerClass(PartitionReducer.class );
			chrJob.setOutputKeyClass(LongWritable.class);
			chrJob.setOutputValueClass(NullWritable.class);
			chrJob.setNumReduceTasks(chromsno);
			chrJob.getConfiguration().setBoolean("mapred.compress.map.output", true);
			chrJob.getConfiguration().setClass("mapred.map.output.compression.codec", Lz4Codec.class, CompressionCodec.class);
			MultipleOutputs.addNamedOutput(chrJob, "ChrBinningMos",SequenceFileOutputFormat.class, LongWritable.class, Text.class);//multiple outputs for binning chromosomes				
			SequenceFileOutputFormat.setCompressOutput(chrJob, true);
			SequenceFileOutputFormat.setOutputCompressorClass(chrJob, Lz4Codec.class);
			SequenceFileOutputFormat.setOutputCompressionType(chrJob, CompressionType.BLOCK);
			LazyOutputFormat.setOutputFormatClass(chrJob, SequenceFileOutputFormat.class);
			code=chrJob.waitForCompletion(true)?0:1;
			Counters counters = chrJob.getCounters();
			for(int i = 0, j = first; i<chromsno; i++, j++){
				chrNum[i] = counters.findCounter(ChrNumCounter.values()[j-1]).getValue();
				chrSampleNum[i] = counters.findCounter(ChrSampleNumCounter.values()[j-1]).getValue();		
				System.out.println(" samplenum for chr"+j+" is "+chrSampleNum[i]);
				System.out.println(" total num for chr"+j+" is "+chrNum[i]);
			}
		}

		if(code==0){
			conf.setDouble("mapreduce.job.reduce.slowstart.completedmaps", 0.5);  //set the reduce slow start to 50% of completed map tasks
			int leftchrmno = chromsno;
			boolean [] exited = new boolean[leftchrmno];
			
			JobControl jc = new JobControl("paralleljobs");
			ControlledJob []jobs;
			for(int i=0; i<chromsno;i++){
				boolean existedResult = fs.exists(new Path(chrmResults[i]));
				if(existedResult){
					leftchrmno--;
					exited[i] = true;	
				}
			}
			
			if(leftchrmno==0) {
				System.err.println("all files existed");
				return 1;
			}else{
				jobs = new ControlledJob[leftchrmno];
				for(int i=0; i<chromsno; i++){
					
					if(exited[i]) continue;
					if(chrNum[i] == 0) continue; //no records for chr i
					jobs[i] = new ControlledJob(getSecondJobConf(this, conf,first+i,new Path(chrmFiles[i]),new Path(chrmResults[i]),defaultName+output, sampleRate, indno, chrSampleNum[i]));
					jc.addJob(jobs[i]);
				}
				JobRunner runner = new JobRunner(jc);
				Thread t = new Thread(runner);
				t.start();
				int i=1;
				while(!jc.allFinished()){
					System.out.println(i*5+"seconds elapsed...");
					i++;
					Thread.sleep(5000);
				}
				return 0;
			}
		}
		else return 2;

	}// end of run

	public static void main(String[] args) throws Exception{  //hadoop jar plinkcloud-mapreduce.jar org.cloudmerge.mapreduce.MRVCF2TPED -D mapreduce.task.io.sort.mb=600 -D mapreduce.reduce.merge.inmem.threshold=0 -D mapreduce.reduce.input.buffer.percent=1 -i /user/hadoop/plinkcloud/input/ -o /user/hadoop/mapreduce/output/ -n $1 -r 0.0001 -c 1-26 -s false -q PASS -g 9 -e //////-libjars lib/..jar,..jar
		//long start_time = System.currentTimeMillis();
		int code=ToolRunner.run(new MRVCF2TPED(), args);
		//long end_time = System.currentTimeMillis();
		//System.out.println("Running Plinkcloud-Mapreduce time elapsed: "+(end_time-start_time)%1000+" seconds");
		System.exit(code);
	}

}