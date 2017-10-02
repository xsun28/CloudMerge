package org.cloudmerge.spark;

import java.util.Arrays;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Objects;
import java.util.Set;

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
import org.cloudmerge.spark.common.Quality;

import scala.Tuple2;

public class VCFMergeSpark {
			

	public static class ReduceFunction implements FlatMapFunction<Iterator<Tuple2<String,String>>, String>{
		private int total_num;            //total individual number
		private int gt_col_num;


		
		public ReduceFunction(int num, int col_num){
			total_num = num;
			gt_col_num = col_num;
		}

		@Override
		public Iterator<String> call(final Iterator<Tuple2<String,String>> input){
			
			return new Iterator<String>(){
                private StringBuilder results = new StringBuilder();
                private String pre_chr_pos = null;
                private String format = "";                		
        		private String filter = "";
        		private String ref = "";
                private boolean first = true;
        		private StringBuffer all_info = new StringBuffer();
        		private Set<String> alts_set = new HashSet<>();
        		private double[] weights = new double[total_num];  //qual weights
        		private double[] quals = new double[total_num];   //all qualities
        		private int ptr = 0;
        		private String[] genotype_array = new String[total_num*gt_col_num];
        		     		
                @Override
                public boolean hasNext() {
                    return input.hasNext();
                }
				
                @Override
                public String next() {

                	while(input.hasNext()){
          		
                		Tuple2<String, String> entry = input.next();
                		if(entry._1.length()==0) continue;
                		String[] chr_pos_id = entry._1.trim().split("-");//row_key[0] = chr, row_key[1] = pos, row_key[2] = id   
                    	String chr_pos = chr_pos_id[0]+"-"+chr_pos_id[1];
                    	int id = Integer.parseInt(chr_pos_id[2]);
                		String[] fields = entry._2.split("\\s+");
            			if(first){
            				for(int i=0;i<4;i++)
            					results.append(fields[i]+"\t");
            				pre_chr_pos = chr_pos;
             				ref = fields[3];
            				Arrays.fill(weights, 0);
            				Arrays.fill(quals,0);
            				Arrays.fill(genotype_array, ".");
            				filter = fields[6];
            				format = fields[8];   
            				first = false;
            			}
            			String[] q_w = fields[5].trim().split("|"); 
            			
            			if( Objects.equals(pre_chr_pos, chr_pos)){
            				String[] alts = fields[4].trim().split(",");
            				for (String alt: alts){
            					if(!alts_set.contains(alt))
            						alts_set.add(alt);
            				} 
            				weights[ptr] = Integer.parseInt(q_w[1]);
            				quals[ptr] = Integer.parseInt(q_w[0]);
            				ptr++;
            				all_info.append(fields[7]+";");
            				for (int i=9;i<fields.length;i++){
            					genotype_array[(id-1)*gt_col_num+i-9] = fields[i];
            				}     			
            			}else{

                    		String[] alts_array = new String[alts_set.size()];
                    		alts_set.toArray(alts_array);
            				String total_alts = String.join(",", alts_array);
            				results.append(total_alts+"\t");                       //column 4
                    		double quality = common.calQual(quals,weights);			
                    		results.append(quality+"\t");							//column 5
                    		results.append(filter+"\t");                                //column 6
                    		results.append(all_info.substring(0, all_info.length()-1)+"\t");  //column 7
                    		results.append(format+"\t");                                        //column 8
                    		String genotypes = common.getGenotypes(alts_array,ref,genotype_array);
                    		results.append(genotypes);                                       //column 9 and more                    		
                    		String output = results.toString().trim();                   		
            				
                    		results.setLength(0);         //restart to next genomic location
            				for(int i=0;i<4;i++)
            					results.append(fields[i]+"\t");
            				pre_chr_pos = chr_pos;
            				ptr = 0;
             				ref = fields[3];
            				filter = fields[6];
            				format = fields[8];
            				alts_set.clear();
            				String[] alts = fields[4].trim().split(",");
            				for (String alt: alts){
            					if(!alts_set.contains(alt))
            						alts_set.add(alt);
            				}
            				Arrays.fill(weights, 0);
            				Arrays.fill(quals,0);
            				Arrays.fill(genotype_array, ".");
            				all_info.setLength(0);
            				
            				weights[ptr] = Integer.parseInt(q_w[1]);
            				quals[ptr] = Integer.parseInt(q_w[0]);
            				ptr++;
            				all_info.append(fields[7]+";");
            				for (int i=9;i<fields.length;i++){
            					genotype_array[(id-1)*gt_col_num+i-9] = fields[i];
            				} 
                    		return output;
                    	}                     	
            				                    
                	}
					return "";
                }
			};
		}
	}

	public static class ConvertMap implements Function2<InputSplit,  Iterator<Tuple2<LongWritable, Text>>, Iterator<Tuple2<String, String>>>{  //convert the raw row into chr-pos as row key, individual_num,rs,ref,genotypes as value
	
		Quality quality;
		private int start_chr, end_chr;
		private Set<Integer> genotype_cols = new HashSet<>();
		public ConvertMap(String start, String end, String qual,int[] gcols){
			start_chr = common.parseChrnum(start);
			end_chr = common.parseChrnum(end);
			quality = common.getQuality(qual);
			for(int i: gcols)
				genotype_cols.add(i);
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
                		StringBuffer value = new StringBuffer();
                		String pos = fields[1].trim();
                		String ref = fields[3].trim();
                		String alts = fields[4].trim();
                		int qual_weights = genotype_cols.size();                		
                		String rowKey = common.getRowKey(String.valueOf(chr_num),pos,ind_id);
                		for(int i=0;i<fields.length;i++){
                			if(genotype_cols.contains(i)){                				
                				String genotype = common.parseGenotype(ref,alts,fields[i]);
                				value.append(genotype+"\t");             				
                			}else if(i==5){
                				value.append(fields[i].trim()+"|");
                				value.append(qual_weights+"\t");
                			}
                			else value.append(fields[i].trim()+"\t");
                		}
//                		StringBuilder value = new StringBuilder().append(rs+",").append(ref+",");
                								
                		return new Tuple2<String, String>(rowKey,value.toString().trim());
                	}
                	return new Tuple2<String, String>("","");
                }                
            };		
		}  //end of call					
	} //end of ConvertMap
	

	
	public static void main(String[] args) throws Exception {  //spark-submit --class org.cloudmerge.spark.VCFMergeSpark --master yarn --deploy-mode cluster --executor-cores 1 --executor-memory 1g --conf spark.network.timeout=10000000 --conf spark.yarn.executor.memoryOverhead=700 --conf spark.shuffle.memoryFraction=0.5 plinkcloud-spark.jar -i plinkcloud/input/ -o Spark/output -n $1 -c 1-26 -q PASS -g 9,10
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
		String[] gt_cols = cmd.getOptionValue("g").trim().split(",");
		int [] genotype_col = new int[gt_cols.length];
		for(int i=0;i<gt_cols.length;i++)
			genotype_col[i] = Integer.parseInt(gt_cols[i]);
			
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
		JavaPairRDD<String, String> union_RDD = hadoopRDD.mapPartitionsWithInputSplit(new ConvertMap(start_chr,end_chr,quality,genotype_col), true)//.filter(new Filter())
				.mapToPair(   																	// .coalesce(ind_num/2) coalesce samll partitions after filter into larger partitions with number = filenum/2
						new PairFunction<Tuple2<String, String>,String,String>(){
							@Override
							public Tuple2<String, String> call(Tuple2<String, String> line){
								return new Tuple2(line._1,line._2);
								}
							}	
						);

		union_RDD.sortByKey().mapPartitions(new ReduceFunction(ind_num,genotype_col.length), true).saveAsTextFile(output_path);
		

		
	}

}
