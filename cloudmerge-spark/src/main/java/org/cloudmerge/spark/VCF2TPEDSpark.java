package org.cloudmerge.spark;

import java.util.Arrays;
import java.util.Iterator;
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
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaNewHadoopRDD;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.storage.StorageLevel;
import org.cloudmerge.spark.common.Quality;

import scala.Tuple2;

public class VCF2TPEDSpark {
			
//	public static class ChrPosPartitioner extends Partitioner{
//		private int num;
//		List<String> boundaries;                            //boundaries of the partition
//		
//		public ChrPosPartitioner(List<String> boundaries){
//			this.boundaries =  boundaries;
//			num = boundaries.size()+1;
//		}
//		
//		
//		@Override
//		public int numPartitions(){
//			return num;
//		}
//		
//		@Override
//		public int getPartition(Object key){
//			String chr_pos = (String) key;
//			for(int i = 0; i<boundaries.size();i++){
//				if(chr_pos.compareTo(boundaries.get(i))<0)
//					return i;
//			}
//			return num-1;			
//		}
//		
//		@Override
//		public boolean equals(Object other){
//			if(!(other instanceof ChrPosPartitioner))
//				return false;
//			ChrPosPartitioner p_other = (ChrPosPartitioner) other;
//			if(this.numPartitions() == p_other.numPartitions())
//				return true;
//			else return false;
//		}
//		
//	} //end of ChrPosPartitioner
	

	
//	public static class Filter implements Function<String, Boolean>{
//		
//		private enum Quality {
//			NULL,Q10,Q20,PASS;	
//		};
//		
//		Quality quality;
//		private int start_chr, end_chr;
//		
//		public Filter(String start, String end, String qual){
//			start_chr = parseChrnum(start);
//			end_chr = parseChrnum(end);
//			quality = getQuality(qual);
//		}
//		
//		private Quality getQuality(String line){
//			if(line.startsWith("#")) return null; // header
//			if(line.contains("PASS")) return Quality.PASS;
//			else if(line.contains("q20")) return Quality.Q10;
//			else if(line.contains("q10")) return Quality.Q20;
//			else return Quality.NULL;
//			
//		}
//		
//		public int parseChrnum(String chr){
//			int chrm;
//			if(chr.equalsIgnoreCase("X")) chrm = 23;
//			else if (chr.equalsIgnoreCase("Y")) chrm = 24;
//			else if (chr.equalsIgnoreCase("M")) chrm = 25;
//			else chrm = Integer.parseInt(chr);
//			return chrm;
//		}
//		
//		@Override
//		public Boolean call(String line){
//			if(line.startsWith("#")) 
//				return false;
//			Quality qual = getQuality(line);
//			if(null == qual || qual.compareTo(quality) < 0)
//				return false;
//			String[] fields =  line.split("\\s+");						
//			String chrm = fields[0].substring(fields[0].indexOf("r")+1).trim();
//			int chr_num = parseChrnum(chrm);
//			if(chr_num < start_chr || chr_num > end_chr)
//				return false;
//			return true;
//		}
//	}
	
//	public static class ConvertMap implements PairFunction<String, String, String>{  //convert the raw row into chr-pos as row key, individual_num,rs,ref,genotypes as value
//	
//	private static final int POS_LENGTH = 10;
//	private static final int CHR_LENGTH = 2;
//	private String ind_num;
//	public ConvertMap(String num){
//		ind_num = num;
//	}
//	
//	@Override
//	public Tuple2<String,String> call(String line){
//		String[] fields = line.split("\\s+");
//		String chrm = fields[0].substring(fields[0].indexOf("r")+1).trim();
//		int chr_num = parseChrnum(chrm);
//		String genotype = parseGenotype(line);
//		String pos = fields[1].trim();
//		String rs =  fields[2].trim();
//		String ref = fields[3].trim();
//		//String[] alts = fields[4].trim().split(",");
//		String rowKey = getRowKey(String.valueOf(chr_num),pos);
//		StringBuilder value = new StringBuilder().append(ind_num+",").append(rs+",")
//					.append(ref+",").append(genotype);
//		return new Tuple2(rowKey,value.toString());
//	}
//	
//	public int parseChrnum(String chr){
//		int chrm;
//		if(chr.equalsIgnoreCase("X")) chrm = 23;
//		else if (chr.equalsIgnoreCase("Y")) chrm = 24;
//		else if (chr.equalsIgnoreCase("M")) chrm = 25;
//		else chrm = Integer.parseInt(chr);
//		return chrm;
//	}
//	
//	private String parseGenotype(String line){
//		
//		StringBuilder genotype = new StringBuilder();
//		String numbered_genotype = null;//1/0, 1/1...
//		Pattern genotypePattern = Pattern.compile("[\\d]{1}([\\/\\|]{1}[\\d]{1})+");
//		String [] fields = line.split("\\s");
//		String genotype_field = fields[9].trim();
//		String [] alts = fields[4].trim().split(",");
//		String ref = fields[3].trim();
//		Matcher matcher = genotypePattern.matcher(genotype_field);
//		if(matcher.find())
//			numbered_genotype = genotype_field.substring(matcher.start(),matcher.end());
//		String [] genotype_numbers = numbered_genotype.split("[\\/\\|]");
//		for (int i=0;i<genotype_numbers.length;i++){
//			int number = Integer.parseInt(genotype_numbers[i].trim());
//			if(number==0)
//				genotype.append(ref).append(" ");
//			else
//				genotype.append(alts[number-1]).append(" ");	
//		}
//		return genotype.toString().trim();
//	}
//	
//	public String getRowKey(String chr, String pos){
//		char[] pos_array = new char[POS_LENGTH];
//		char[] chr_array = new char[CHR_LENGTH];
//		Arrays.fill(pos_array, '0');
//		Arrays.fill(chr_array, '0');
//		int pos_start_offset = POS_LENGTH - pos.length();
//		int chr_start_offset = CHR_LENGTH - chr.length();
//		System.arraycopy(pos.toCharArray(), 0, pos_array, pos_start_offset, pos.length());
//		System.arraycopy(chr.toCharArray(), 0, chr_array, chr_start_offset, chr.length());
//		return  String.valueOf(chr_array)+"-"+ String.valueOf(pos_array);			
//	}
//	
//	
//}//end of ConvertMap

//public static List<String> getPartitionBoundaries(Map<String, String> samples, int region_num){
//	int boundary_num = region_num -1;
//	List<String> boundaries = new ArrayList<>(boundary_num);
//	int step = (samples.size()+1)/region_num;
//	int i = 0;
//	for(String key: samples.keySet()){
//		if((++i)%step == 0) boundaries.add(key);
//	}
//	return boundaries;		
//}
	
	public static class Filter implements Function<Tuple2<String,String>, Boolean>{
		
		@Override
		public Boolean call(Tuple2<String,String> input){
			if(null == input) return false;
			else return true;
		}
	
	}
	public static class ReduceFunction implements Function2<String,String,String>{
		
		@Override
		public String call(String first,String second){
			StringBuilder result = new StringBuilder();
			String[] first_splits = first.split(",");
			String[] second_splits = second.split(",");
			result.append(first_splits[0]+",").append(first_splits[1]);
			for(int i = 2; i < first_splits.length; i++)
				result.append(","+first_splits[i]);
			for(int i = 2; i < second_splits.length; i++ )
				result.append(","+second_splits[i]);	
			return result.toString();
		}
		
	}
	public static class ConvertMap implements Function2<InputSplit,  Iterator<Tuple2<LongWritable, Text>>, Iterator<Tuple2<String, String>>>{  //convert the raw row into chr-pos as row key, individual_num,rs,ref,genotypes as value
		


		
		Quality quality;
		private int start_chr, end_chr;
		private int genotype_col;
		
		public ConvertMap(String start, String end, String qual, int genotype_col){
			start_chr = common.parseChrnum(start);
			end_chr = common.parseChrnum(end);
			quality = common.getQuality(qual);
			this.genotype_col = genotype_col;
		}

		
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
                    Tuple2<LongWritable, Text> entry = lines.next();
                    String line = entry._2().toString(); 
                    Quality qual = common.getQuality(line);
        			if(null == qual || qual.compareTo(quality) < 0) 
        				return null;
                    String[] fields = line.split("\\s+");
                    String chrm = fields[0].substring(fields[0].indexOf("r")+1).trim();
                    int chr_num = common.parseChrnum(chrm);
                    if(chr_num < start_chr || chr_num > end_chr)
        				return null;
                    String genotype = common.parseGenotype(line,genotype_col);
                    String pos = fields[1].trim();
                    String rs =  fields[2].trim();
                    String ref = fields[3].trim();
                    //String[] alts = fields[4].trim().split(",");
                    String rowKey = common.getRowKey(String.valueOf(chr_num),pos);
                    StringBuilder value = new StringBuilder().append(rs+",").append(ref+",")
                    		.append(ind_id+",").append(genotype);
                    return new Tuple2<String, String>(rowKey,value.toString());
                   
                }
            };		
		}  //end of call
		

		
		
	} //end of ConvertMap
	
	
	public static class CombineMap implements Function<Tuple2<String,String>, String >{  // the input row key is chr_pos and value is a collection of ind_id,rs,ref,genotype. Convert it into output row 
		private int total_num;            //total individual number
		public CombineMap(int num){
			total_num = num;
		}
		
		@Override
		public String call(Tuple2<String,String> input_row){
			StringBuilder result = new StringBuilder();
			String[] chr_pos = common.getChrPos(input_row._1);
			String ref = "", rs;
			String[] genotypes = new String[total_num];
			String[] fields = input_row._2.split(",");
			rs = fields[0];
			ref = fields[1];
			Arrays.fill(genotypes, ref+" "+ref);
			result.append(chr_pos[0]+"\t").append(rs).append("\t0\t").append(chr_pos[1]+"\t");
			for(int i = 2; i< fields.length; i+=2){
				int ind_id = Integer.parseInt(fields[i])-1;
				genotypes[ind_id] = fields[i+1];
			}			
			for(String genotype: genotypes)
					result.append(genotype+" ");			 
			return result.toString();
		}
		

	}// end of CombineMap
	
	public static void main(String[] args) throws Exception {  //spark-submit --class org.cloudmerge.spark.VCF2TPEDSpark --master yarn --deploy-mode cluster --executor-cores 1 --executor-memory 1g --conf spark.network.timeout=10000000 --conf spark.yarn.executor.memoryOverhead=700 --conf spark.shuffle.memoryFraction=0.5 plinkcloud-spark.jar -i plinkcloud/input/ -o Spark/output -n $1 -c 1-26 -q PASS -g 9
		CommandLine cmd = commandParser.parseCommands(args);
		String input_path = cmd.getOptionValue("i");
		String output_path = cmd.getOptionValue("o");
		int ind_num = Integer.parseInt(cmd.getOptionValue("n"));
		String chr_range = cmd.getOptionValue("c");
		String start_chr = chr_range.substring(0,chr_range.indexOf("-"));
		String end_chr = chr_range.substring(chr_range.indexOf("-")+1);
//		double sampleRate = Double.parseDouble(args[4]);  //0.0005
//		String quality = args[5];
//		SparkConf conf = new SparkConf().setMaster("yarn-client").setAppName("plinkcloud-spark");
		String quality = cmd.getOptionValue("q");
		int genotype_col = Integer.parseInt(cmd.getOptionValue("g"));
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
		JavaPairRDD<String, String> union_RDD = hadoopRDD.mapPartitionsWithInputSplit(new ConvertMap(start_chr,end_chr,quality,genotype_col), true).filter(new Filter())
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
		JavaPairRDD<String,String> reduced_sort_RDD = union_RDD.reduceByKey(new ReduceFunction(), ind_num*2).sortByKey();	// avoid using groupByKey which is inefficient, and also keeps parallelism high to alleviate single executor memory pressure 
		reduced_sort_RDD.map(new CombineMap(ind_num)).saveAsTextFile(output_path);
//		JavaRDD<String> result_RDD = reduced_sorted_RDD.mapValues(new CombineMap(ind_num*2));  
//		result_RDD.saveAsTextFile(output_path);
//		result_RDD.saveAsTextFile(output_path,BZip2Codec.class);
		
		
	}

}
