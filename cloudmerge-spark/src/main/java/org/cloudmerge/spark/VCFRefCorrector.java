package org.cloudmerge.spark;

import java.util.Arrays;
import java.util.Iterator;
import java.util.StringTokenizer;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.commons.cli.CommandLine;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.compress.BZip2Codec;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.spark.Aggregator;
import org.apache.spark.Partitioner;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaNewHadoopRDD;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFlatMapFunction;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.storage.StorageLevel;

import com.google.common.base.Optional;

import scala.Tuple2;

public class VCFRefCorrector {
			
	
	public static class CorrectFunction implements PairFlatMapFunction<Iterator<Tuple2<String,String>>, String,String>{
		private double x;
		private long y;
		public CorrectFunction(double x, long y){
			this.x = x;
			this.y = y;
		}
		@Override
		public Iterator<Tuple2<String,String>> call(final Iterator<Tuple2<String,String>> input){
			
			return new Iterator<Tuple2<String,String>>(){
                private String pre_chr_pos = null;
                private String ref = "";
                
            	private String modifyLine(String line, String ref){
            		String[] fields = line.split("\\s+");		
            		long pos = Integer.parseInt(fields[1].trim());
            		long new_pos = (long)(pos*x)+y;
            		String new_rs = ".";
            		StringBuffer newline = new StringBuffer();
            		newline.append(fields[0]+"\t").append(new_pos+"\t").append(new_rs+"\t")
            			.append(ref+"\t");
            		for(int i=4;i<fields.length;i++)
            			newline.append(fields[i]+"\t");		
            		return newline.toString().trim();
            	}//end of modify line
                
                @Override
                public boolean hasNext() {
                    return input.hasNext();
                }
				
                @Override
                public Tuple2<String,String> next() {
                	
                	Tuple2<String, String> entry = input.next();
                	String[] chr_pos_id = entry._1.trim().split("-");//row_key[0] = chr, row_key[1] = pos, row_key[2] = id   
                	String chr_pos = chr_pos_id[0]+"-"+chr_pos_id[1];
            		String id_chr_pos = chr_pos_id[2]+"-"+chr_pos_id[0]+"-"+chr_pos_id[1];
                	String line = entry._2;
            		if(chr_pos_id[0].equals("00")) 
            			return new Tuple2<String,String>(id_chr_pos,line);      //return column header
            		if(null==pre_chr_pos || (!pre_chr_pos.equals(chr_pos))){
            			ref = line.split("\\s+")[3].trim();
            			pre_chr_pos = chr_pos;
            		}
            		String newline = modifyLine(line,ref);  
            		return new Tuple2<String,String>(id_chr_pos,newline);   	
                }
			};
		}
	}//end of correctfunction


	
	public static class ConvertMap implements Function2<InputSplit,  Iterator<Tuple2<LongWritable, Text>>, Iterator<Tuple2<String, String>>>{  //convert the raw row into chr-pos as row key, individual_num,rs,ref,genotypes as value
		
		private static final int POS_LENGTH = 10;
		private static final int CHR_LENGTH = 2;
		private static final int ID_LENGTH = 3;
		@Override
		public  Iterator<Tuple2<String, String>> call(InputSplit split, final Iterator<Tuple2<LongWritable, Text>> lines ){
			
			FileSplit fileSplit = (FileSplit) split;
			String fileName = fileSplit.getPath().getName();
			final String ind_id = fileName.substring(0,fileName.indexOf("."));
			return new Iterator<Tuple2<String, String>>() {
                @Override
                public boolean hasNext() {
                    return lines.hasNext();
                }
                @Override
                public Tuple2<String, String> next() {
                	while(lines.hasNext()){
                
                    Tuple2<LongWritable, Text> entry = lines.next();
                    String line = entry._2().toString(); 
                    if(line.length()==0) continue;
                    String rowKey="";
                    if(line.startsWith("#")){
                    	String fakeChr = "00";
                    	String fakePos = "0000000000";
                    	rowKey = getRowKey(fakeChr,fakePos,ind_id);
                    }else{
                    	String[] fields = line.split("\\s+");
                    	String chrm = fields[0].substring(fields[0].indexOf("r")+1).trim();
                    	int chr_num = parseChrnum(chrm);
                    	String pos = fields[1].trim();
                    	rowKey = getRowKey(String.valueOf(chr_num),pos,ind_id);
                    }
                    	return new Tuple2<String, String>(rowKey,line);
                	}
                	return null;
                }
            };		
		}  //end of call
		
		public int parseChrnum(String chr){
			int chrm;
			if(chr.equalsIgnoreCase("X")) chrm = 23;
			else if (chr.equalsIgnoreCase("Y")) chrm = 24;
			else if (chr.equalsIgnoreCase("XY")) chrm = 25;
			else if (chr.equalsIgnoreCase("M")) chrm = 26;
			else chrm = Integer.parseInt(chr);
			return chrm;
		}
		
		
		
		public String getRowKey(String chr, String pos,String ind_id){
			char[] pos_array = new char[POS_LENGTH];
			char[] chr_array = new char[CHR_LENGTH];
			char[] id_array = new char[ID_LENGTH];
			Arrays.fill(pos_array, '0');
			Arrays.fill(chr_array, '0');
			Arrays.fill(id_array, '0');
			int pos_start_offset = POS_LENGTH - pos.length();
			int chr_start_offset = CHR_LENGTH - chr.length();
			int id_start_offset = ID_LENGTH - ind_id.length();
			System.arraycopy(pos.toCharArray(), 0, pos_array, pos_start_offset, pos.length());
			System.arraycopy(chr.toCharArray(), 0, chr_array, chr_start_offset, chr.length());
			System.arraycopy(ind_id.toCharArray(), 0, id_array, id_start_offset, ind_id.length());
			return  String.valueOf(chr_array)+"-"+ String.valueOf(pos_array)+"-"+String.valueOf(id_array);			
		}
		
		
	} //end of ConvertMap
	
	public static class flatMap implements FlatMapFunction<Iterator<Tuple2<String,String>>,String>{
		@Override
		public Iterator<String> call(final Iterator<Tuple2<String,String>> input){
			
			return new Iterator<String>(){
				@Override
				public boolean hasNext(){
					return input.hasNext();
				}
				@Override
				public String next(){
					return input.next()._2.trim();
				}
			};		
		}
	}
	
	
	
	public static void main(String[] args) throws Exception {  //spark-submit --class org.cloudmerge.spark.VCFRefCorrector --master yarn --deploy-mode cluster --executor-cores 1 --executor-memory 1g --conf spark.network.timeout=10000000 --conf spark.yarn.executor.memoryOverhead=700 --conf spark.shuffle.memoryFraction=0.5 plinkcloud-spark.jar  plinkcloud/input/  Spark/output  $1 $2 $3
		
		String input_path = args[0];
		String output_path = args[1];
		int ind_num = Integer.parseInt(args[2]);
		double x = Double.parseDouble(args[3].trim());
		long y = Long.parseLong(args[4].trim());
		SparkConf conf = new SparkConf().setAppName("plinkcloud-spark");  //set master model on command line 
		conf.set("spark.serializer", "org.apache.spark.serializer.KryoSerializer");
		conf.set("spark.kyro.registrationRequired", "true");
		conf.set("spark.shuffle.memoryFraction", "0.5");
//		conf.registerKryoClasses(new Class<?>[]{FilterMap.class,CombineMap.class,Filter.class});
		JavaSparkContext sc = new JavaSparkContext(conf);
		JavaPairRDD<LongWritable, Text> javaPairRDD = sc.newAPIHadoopFile(
			    input_path, 
			    TextInputFormat.class, 
			    LongWritable.class, 
			    Text.class, 
			    new Configuration()
			);
		JavaNewHadoopRDD<LongWritable, Text> hadoopRDD = (JavaNewHadoopRDD) javaPairRDD;
		JavaPairRDD<String, String> paired_RDD = hadoopRDD.mapPartitionsWithInputSplit(new ConvertMap(), true)
				.mapToPair(   																	// .coalesce(ind_num/2) coalesce samll partitions after filter into larger partitions with number = filenum/2
						new PairFunction<Tuple2<String, String>,String,String>(){
							@Override
							public Tuple2<String, String> call(Tuple2<String, String> line){
								return new Tuple2(line._1,line._2);
								}
							}	
						);
//		JavaPairRDD<String,String>[] file_RDD_array = new JavaPairRDD[ind_num];
//		for(int i =0; i<ind_num; i++){
//			JavaRDD<String> input = sc.textFile(input_path+(i+1)+".bz2");
//			file_RDD_array[i] = input.filter(new Filter(start_chr, end_chr, quality)).mapToPair(new ConvertMap(String.valueOf(i+1)));
//		}
//		JavaPairRDD<String, String> union_RDD = sc.union(file_RDD_array).persist(StorageLevel.MEMORY_AND_DISK_SER()); 
//		JavaPairRDD<String,String> sample = union_RDD.sample(false, sampleRate);    //get the samples for partitioning the RDD into approximately equal partition
//		Map<String, String> sample_map = sample.collectAsMap();
//		List<String> boundaries = getPartitionBoundaries(sample_map, ind_num);
//		JavaPairRDD<String, String> partitioned_RDD = union_RDD.partitionBy(new ChrPosPartitioner(boundaries));
//		JavaRDD<String> result_RDD = partitioned_RDD.groupByKey(ind_num).sortByKey().map(new CombineMap(ind_num));  //first group then sort with ind_num parallel tasks assigned to executors. or group later will make data not sorted
//		union_RDD.cache();
		paired_RDD.cache().sortByKey().mapPartitionsToPair(new CorrectFunction(x,y), true).sortByKey(true, ind_num).mapPartitions(new flatMap(),true).saveAsTextFile(output_path);
		
		
//		Partitioner partitioner = sort_RDD.partitioner().get();				// get the partitioner from sortby on the RDDs
//		JavaPairRDD<String,String> reduced_RDD = sort_RDD.reduceByKey(partitioner,new ReduceFunction());	// avoid using groupByKey which is inefficient, and also keeps parallelism high to alleviate single executor memory pressure 
//		reduced_RDD.map(new CombineMap(ind_num)).saveAsTextFile(output_path);
//		JavaRDD<String> result_RDD = reduced_sorted_RDD.mapValues(new CombineMap(ind_num*2));  
//		result_RDD.saveAsTextFile(output_path);
//		result_RDD.saveAsTextFile(output_path,BZip2Codec.class);
		
		
	}

}
