package org.cloudmerge.spark;

import java.util.Arrays;
import java.util.Iterator;

import org.apache.commons.cli.CommandLine;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaNewHadoopRDD;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.rdd.NewHadoopRDD;
import org.cloudmerge.spark.common.Quality;

import scala.Tuple2;

public class VCF2TPEDSparkOneShuffling {
			

	
	public static class ReduceFunction implements FlatMapFunction<Iterator<Tuple2<String,String>>, String>{
		private int total_num;            //total individual number
		public ReduceFunction(int num){
			total_num = num;
		}
		
		@Override
		public Iterator<String> call(final Iterator<Tuple2<String,String>> input){
			
			return new Iterator<String>(){
                private StringBuilder results = new StringBuilder();
                private String pre_chr_pos = null;
                private String genotypes[] = new String[total_num];
                private boolean first = true;
				
                @Override
                public boolean hasNext() {
                    return input.hasNext();
                }
				
                @Override
                public String next() {
                	String rs = "";
                	String ref = "";
                	while(input.hasNext()){
                		Tuple2<String, String> entry = input.next();
                		String chr_pos = entry._1.trim();
                		if(chr_pos.length()==0) continue;
                		String[] fields = entry._2.split(",");
        				String[] chrAndPos = common.getChrPos(chr_pos);
            			if(first){
            				rs = fields[0];
            				ref = fields[1];
            				Arrays.fill(genotypes, ref+" "+ref);
            				pre_chr_pos = chr_pos;
            				results.append(chrAndPos[0]+"\t").append(rs).append("\t0\t").append(chrAndPos[1]+"\t");
            				first = false;
            			}
            			
                    	if( !pre_chr_pos.equals(chr_pos)){
                    		for(String genotype:genotypes){
                    			results.append(genotype+"\t");	
                    		}
                    		String output = results.toString().trim();

                    		rs = fields[0];
            				ref = fields[1];
            				Arrays.fill(genotypes, ref+" "+ref);
                    		results.setLength(0);
                    		results.append(chrAndPos[0]+"\t").append(rs).append("\t0\t").append(chrAndPos[1]+"\t");
            				int ind_id = Integer.parseInt(fields[2])-1;
                			genotypes[ind_id] = fields[3]; 
            				pre_chr_pos = chr_pos;
                    		return output;
                    	}                     	
            			int ind_id = Integer.parseInt(fields[2])-1;
            			genotypes[ind_id] = fields[3];            				
                    
                	}
					return "";
                }
			};
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
                	while(lines.hasNext()){
                		Tuple2<LongWritable, Text> entry = lines.next();
                		String line = entry._2().toString(); 
                		Quality qual = common.getQuality(line);
                		if(null == qual || qual.compareTo(quality) < 0) 
                			continue;
                		String[] fields = line.split("\\s+");
                		String chrm = fields[0].substring(fields[0].indexOf("r")+1).trim();
                		int chr_num = common.parseChrnum(chrm);
                		if(chr_num < start_chr || chr_num > end_chr)
                			continue;
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
                	return new Tuple2<String, String>("","");
                }                
            };		
		}  //end of call		
	} //end of ConvertMap
	
	
	public static void main(String[] args) throws Exception {  //spark-submit --class org.cloudmerge.spark.VCF2TPEDSparkOneShuffling --master yarn --deploy-mode cluster --executor-cores 1 --executor-memory 1g --conf spark.network.timeout=10000000 --conf spark.yarn.executor.memoryOverhead=700 --conf spark.shuffle.memoryFraction=0.5 plinkcloud-spark.jar -i plinkcloud/input/ -o Spark/output -n $1 -c 1-26 -q PASS -g 9
		CommandLine cmd = commandParser.parseCommands(args);
		String input_path = cmd.getOptionValue("i");
		String output_path = cmd.getOptionValue("o");
		int ind_num = Integer.parseInt(cmd.getOptionValue("n"));
		String chr_range = cmd.getOptionValue("c");
		String start_chr = chr_range.substring(0,chr_range.indexOf("-"));
		String end_chr = chr_range.substring(chr_range.indexOf("-")+1);

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
		JavaNewHadoopRDD<LongWritable, Text> hadoopRDD = (JavaNewHadoopRDD<LongWritable, Text>) javaPairRDD;

		JavaPairRDD<String, String> union_RDD = hadoopRDD.mapPartitionsWithInputSplit(new ConvertMap(start_chr,end_chr,quality,genotype_col), true)//.filter(new Filter())
				.mapToPair(   																	// .coalesce(ind_num/2) coalesce samll partitions after filter into larger partitions with number = filenum/2
						new PairFunction<Tuple2<String, String>,String,String>(){
							@Override
							public Tuple2<String, String> call(Tuple2<String, String> line){
								return new Tuple2(line._1,line._2);
								}
							}	
						);

		union_RDD.sortByKey().mapPartitions(new ReduceFunction(ind_num), true).saveAsTextFile(output_path);		
	}
}
