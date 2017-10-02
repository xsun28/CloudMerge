/*This only sample selected chromosomes and output those chromosome records into a directory in the sampling mapper. 
 * These records are then used as the import to the bulkload mapper.
 * */

package org.plinkcloud.hbase;


import java.io.IOException;
import java.util.List;
import java.util.Random;

import org.apache.commons.cli.CommandLine;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.FsShell;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.RegionLocator;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.HFileOutputFormat2;
import org.apache.hadoop.hbase.mapreduce.LoadIncrementalHFiles;
import org.apache.hadoop.hbase.mapreduce.TableMapReduceUtil;
import org.apache.hadoop.hbase.mapreduce.TableMapper;
import org.apache.hadoop.hbase.mapreduce.TableSplit;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.SequenceFile.CompressionType;
import org.apache.hadoop.io.compress.CompressionCodec;
import org.apache.hadoop.io.compress.Lz4Codec;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.LazyOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.MultipleOutputs;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.plinkcloud.hbase.common.Quality;


public class HBaseVCF2TPEDSelectedChr extends Configured implements Tool{
	private static final String SELECTED_CHR_DIR = "selectedChr/";
	protected static class VCFSampleMapper extends Mapper<LongWritable,Text,Text,NullWritable>{  // First sampling from each individual file
		private Quality qual_filter;
		private double rate;         //sampling rate
		private long records = 0;
		private long kept = 1;      
		private TextParser parser;
		private int startchr;
		private int endchr;
		private boolean ordered;
		private Random random;
		private MultipleOutputs<Text, Text> mos;
		private String ind_id;
		private int genotype_col;
		@Override
		protected void setup(Context context){
			Configuration conf = context.getConfiguration();
			qual_filter =  Enum.valueOf(Quality.class, conf.get("quality").trim().toUpperCase());
			rate = Double.parseDouble(conf.get("samplerate").trim());
			parser = new TextParser();
			startchr = parser.parseChrnum(conf.get("startchr","1"));
			endchr = parser.parseChrnum(conf.get("endchr","26"));
			ordered = conf.getBoolean("ordered", true);
			random = new Random();
			mos =  new MultipleOutputs(context);
			FileSplit split = (FileSplit) context.getInputSplit();
			String fullname = split.getPath().getName();									//get the name of the file where the input split is from		
			ind_id = fullname.substring(0, fullname.indexOf('.')); 
			genotype_col = conf.getInt("genotype_col", 0);
		}
		
		@Override 
		public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException{
			TextParser parser = new TextParser(value.toString(),genotype_col);	
			Quality qual = parser.getQuality();
			if(null != qual && qual.compareTo(qual_filter) >= 0){
				int chrnum = parser.getChrNum();
				if(chrnum >= startchr && chrnum<=endchr ){
					StringBuilder output_value = new StringBuilder();
					records++;
					String rowKey = parser.getRowkey();
					String genotype = parser.getGenotype();
					String ref = parser.getRef();
					String rs_pos = parser.getRs();
					output_value.append(ind_id+"%").append(rs_pos+"%").append(ref+"%").append(genotype);
					boolean sample = false;
					if(ordered){
						sample = ((double) kept / (double)records) < rate;
					}else{
						double random_num = random.nextDouble();
						sample = random_num*100 < rate*100;
					}
					if (sample) {
						kept++;
						context.write(new Text(rowKey), NullWritable.get());
						}
					mos.write("ChrMos",new Text(rowKey), new Text(output_value.toString()),SELECTED_CHR_DIR+"/part");
				
				}
			}
		}
		
		@Override
		protected void cleanup(Context context) throws IOException, InterruptedException {
			mos.close();
		}
	}
	
	protected static class VCFSampleReducer extends Reducer<Text,NullWritable,Text,NullWritable>{
		private int file_no;
		private int samplenums = 0;
		@Override
		protected void setup(Context context){
			Configuration conf = context.getConfiguration();
			file_no = Integer.parseInt(conf.get("fileno"));
		}
		@Override
		public void reduce(Text key, Iterable<NullWritable> values, Context context)throws IOException, InterruptedException{
		
			for (NullWritable n : values) {
				samplenums++;
				if(samplenums>=file_no){ // take one sample every file_no
					context.write(key,NullWritable.get());
					samplenums = 0;
				}
			}
		}
	
	}
		
	protected static class BulkLoadingMapper extends Mapper<Text,Text,ImmutableBytesWritable,Put>{
		private byte[] family = null;
		private byte[] ref_qualifier = null;
		private byte[] rs_qualifier = null;
		
		@Override
		protected void setup(Context context) throws IOException, InterruptedException{ 		//called once at the beginning of a mapper with a single input split 
			family = Bytes.toBytes("individuals");							//qualifier is individual number
			ref_qualifier = Bytes.toBytes("ref");
			rs_qualifier =  Bytes.toBytes("rs");
		}//end of setup
	
		@Override
		public void map(Text key, Text value, Context context) throws IOException, InterruptedException{
				String[] input_values = value.toString().split("%");
				byte[] individual_qualifier = Bytes.toBytes(input_values[0]);
				byte [] rs_pos = Bytes.toBytes(input_values[1]);	 
				byte [] ref = Bytes.toBytes(input_values[2]);
				byte [] genotype = Bytes.toBytes(input_values[3]);     	// value is the genotype
				byte [] rowKey = Bytes.toBytes(key.toString()); //row key is the chrm-genomic pos
				Put p = new Put(rowKey);
				p.addColumn(family, individual_qualifier, genotype);
				p.addColumn(family, ref_qualifier, ref);
				p.addColumn(family, rs_qualifier, rs_pos);
				context.write(new ImmutableBytesWritable(rowKey), p);
			
		}
		
	}//end of bulk loading mapper
	
	protected static class AdditiveBulkLoadingMapper extends Mapper<LongWritable,Text,ImmutableBytesWritable,Put>{
		private byte[] family = null;
		private byte[] ref_qualifier = null;
		private byte[] rs_qualifier = null;
		private Quality qual_filter;     
		private TextParser parser;
		private int startchr;
		private int endchr;
		private String ind_id;
		private int genotype_col;
		@Override
		protected void setup(Context context) throws IOException, InterruptedException{ 		//called once at the beginning of a mapper with a single input split 
			family = Bytes.toBytes("individuals");							//qualifier is individual number
			ref_qualifier = Bytes.toBytes("ref");
			rs_qualifier =  Bytes.toBytes("rs");
			Configuration conf = context.getConfiguration();
			qual_filter =  Enum.valueOf(Quality.class, conf.get("quality").trim().toUpperCase());
			parser = new TextParser();
			startchr = parser.parseChrnum(conf.get("startchr","1"));
			endchr = parser.parseChrnum(conf.get("endchr","26"));
			FileSplit split = (FileSplit) context.getInputSplit();
			String fullname = split.getPath().getName();									//get the name of the file where the input split is from		
			ind_id = fullname.substring(0, fullname.indexOf('.')); 
			genotype_col = conf.getInt("genotype_col", 0);
		}//end of setup
	
		@Override
		public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException{
			TextParser parser = new TextParser(value.toString(),genotype_col);	
			Quality qual = parser.getQuality();
			if(null != qual && qual.compareTo(qual_filter) >= 0){
				int chrnum = parser.getChrNum();
				if(chrnum >= startchr && chrnum<=endchr ){
					byte[] individual_qualifier = Bytes.toBytes(ind_id);
					byte [] rs_pos = Bytes.toBytes(parser.getRs());	 
					byte [] ref = Bytes.toBytes(parser.getRef());
					byte [] genotype = Bytes.toBytes(parser.getGenotype());     	// value is the genotype
					byte [] rowKey = Bytes.toBytes(parser.getRowkey()); //row key is the chrm-genomic pos
					Put p = new Put(rowKey);
					p.addColumn(family, individual_qualifier, genotype);
					p.addColumn(family, ref_qualifier, ref);
					p.addColumn(family, rs_qualifier, rs_pos);
					context.write(new ImmutableBytesWritable(rowKey), p);
				}
			}
		}
		
	}//end of additive bulk loading mapper
	
	protected static class ExportMapper extends TableMapper<NullWritable,Text>{
		private int file_no;
		private byte[] family = null;
		private byte[] ref_qualifier = null;
		private byte[] rs_qualifier = null;
		private String startRow;
		private MultipleOutputs <NullWritable, Text> mos;
		@Override
		protected void setup(Context context){
			Configuration conf = context.getConfiguration();
			file_no = Integer.parseInt(conf.get("total_fileno"));
			family = Bytes.toBytes("individuals");							//qualifier is individual number
			ref_qualifier = Bytes.toBytes("ref");
			rs_qualifier =  Bytes.toBytes("rs");
			TableSplit split = (TableSplit) context.getInputSplit();
			startRow =  Bytes.toString(split.getStartRow());
			mos = new MultipleOutputs<>(context);
		}
		
		@Override
		public void map(ImmutableBytesWritable row, Result columns, Context context)throws IOException, InterruptedException{
			StringBuilder result = new StringBuilder();
			String ref = Bytes.toString(columns.getValue(family, ref_qualifier));
			String rs = Bytes.toString(columns.getValue(family, rs_qualifier));
			String[] chr_pos = TextParser.parseRowKey(Bytes.toStringBinary(row.get()));
			result.append(chr_pos[0]+"\t").append(rs).append("\t0\t").append(chr_pos[1]+"\t");
			for(int i = 1; i<=file_no; i++){
				if(columns.containsColumn(family, Bytes.toBytes(String.valueOf(i))))
					result.append(Bytes.toString(columns.getValue(family, Bytes.toBytes(String.valueOf(i))))+" ");
				else
					result.append(ref+" ").append(ref+" ");					
			}
			mos.write(NullWritable.get(), new Text(result.toString()),startRow);
		}
		
		@Override
		protected void cleanup(Context context) throws IOException, InterruptedException {
			mos.close();
		}
		
	}//end of export mapper
	



	
	@Override
	public int run(String[] args)throws Exception{
		int code = 0;
		Configuration conf = HBaseConfiguration.create(); //get configurations from configuration files on the classpath
		CommandLine cmd = commandParser.parseCommands(args, conf);
		String input = cmd.getOptionValue("i");			//plinkcloud/input/
		String outputPath = cmd.getOptionValue("o");  	// 	base output path  HBase
		Path inputPath = new Path(input); 	 						
		Path sample_outputPath = new Path(outputPath+"/sample"); 	// 	HBase/sample
		Path result_outputPath = new Path(outputPath+"/results");  	//	HBase/result
		Path selected_chr_dir = new Path(outputPath+"/sample/"+SELECTED_CHR_DIR); //HBASE/selectedChr
		double sampleRate = 0.0001;
		if(cmd.hasOption("r"))
			sampleRate = Double.parseDouble(cmd.getOptionValue("r")); 
		conf.set("samplerate", ""+sampleRate);
		String fileno = cmd.getOptionValue("n");
		conf.set("fileno", fileno);
		conf.set("quality", cmd.getOptionValue("q"));
		int genotype_col = Integer.parseInt(cmd.getOptionValue("g"));
		conf.setInt("genotype_col", genotype_col);
		boolean additive = false;
		if(cmd.hasOption("a")){     //if the merging is additive to the pre-existing table
			additive = true;
			conf.set("total_fileno", String.valueOf((Integer.parseInt(cmd.getOptionValue("a"))+Integer.parseInt(fileno))));
		}else conf.set("total_fileno", fileno);
		String chr_range = cmd.getOptionValue("c");
		String start_chr = chr_range.substring(0,chr_range.indexOf("-"));
		String end_chr = chr_range.substring(chr_range.indexOf("-")+1);
		boolean ordered = true;
		if(cmd.hasOption("s"))
			ordered = Boolean.parseBoolean(cmd.getOptionValue("s"));
		conf.set("startchr", start_chr);
		conf.set("endchr", end_chr);
		conf.setBoolean("ordered", ordered);
		String[] row_range = common.getRowRange(start_chr,end_chr);
		int region_num = Integer.parseInt(cmd.getOptionValue("n"))/2;  //keep each region to hold approximately 3 input file's size data. The region size should around 1G
		region_num = region_num > 1? region_num : 2;   //if region num 
		conf.setBoolean("mapreduce.map.speculative", false);
		conf.setBoolean("mapreduce.reduce.speculative", false);  //turn off the speculative execution
		conf.setDouble("mapreduce.job.reduce.slowstart.completedmaps", 0.8);  //set the reduce slow start to 50% of completed map tasks cause sampling reducer is very light weight
		Connection connection = ConnectionFactory.createConnection(conf);
		Admin admin = connection.getAdmin();
		
		if(!additive){ 						//sample job to get boundaries
			Job sample_job =  Job.getInstance(conf,"Sample Region Boundaries");
			sample_job.setJarByClass(getClass());
			TableMapReduceUtil.addDependencyJars(sample_job);
			FileInputFormat.addInputPath(sample_job, inputPath);
			FileOutputFormat.setOutputPath(sample_job,sample_outputPath);
			sample_job.setMapperClass(VCFSampleMapper.class);
			sample_job.setReducerClass(VCFSampleReducer.class);
			sample_job.setOutputKeyClass(Text.class);
			sample_job.setOutputValueClass(NullWritable.class);
			sample_job.setNumReduceTasks(1);
			sample_job.getConfiguration().setBoolean("mapred.compress.map.output", true);
			sample_job.getConfiguration().setClass("mapred.map.output.compression.codec", Lz4Codec.class, CompressionCodec.class);
			MultipleOutputs.addNamedOutput(sample_job, "ChrMos",SequenceFileOutputFormat.class, Text.class, Text.class);
			SequenceFileOutputFormat.setCompressOutput(sample_job, true);
			SequenceFileOutputFormat.setOutputCompressorClass(sample_job, Lz4Codec.class);
			SequenceFileOutputFormat.setOutputCompressionType(sample_job, CompressionType.BLOCK);
			LazyOutputFormat.setOutputFormatClass(sample_job, TextOutputFormat.class);   //if sampling doesn't have any sample, don't create the sample file.
			code = sample_job.waitForCompletion(true)?0:1;
		
			//Create TPED table with predefined boundaries
			String sample_file = new StringBuilder().append(outputPath)
					.append("/sample/part-r-00000.lz4").toString();
			System.out.println("sample_file "+sample_file);
			List<String> boundaries =  common.getRegionBoundaries(conf,sample_file,region_num);		
			common.createTable(admin,boundaries,"TPED");
		}
		
		//Bulk Load Job
		conf.setDouble("mapreduce.job.reduce.slowstart.completedmaps", 0.6);  
		Job bulk_load_job = Job.getInstance(conf,"Bulk_Load");		 
		bulk_load_job.setJarByClass(getClass());
		TableMapReduceUtil.addDependencyJars(bulk_load_job);//distribute the required dependency jars to the cluster nodes. Also need to add the plinkcloud-hbase.jar onto the HADOOP_CLASSPATH in  order for HBase's TableMapReduceUtil to access it. 
		if(!additive){
			FileInputFormat.addInputPath(bulk_load_job, selected_chr_dir);
			bulk_load_job.setInputFormatClass(SequenceFileReadCombiner.class); 
		    bulk_load_job.setMapperClass(BulkLoadingMapper.class);
		}
		else{
			FileInputFormat.addInputPath(bulk_load_job, inputPath);
			bulk_load_job.setMapperClass(AdditiveBulkLoadingMapper.class);
		}
		
	    Path tempPath = new Path("temp/bulk");	    
	    FileOutputFormat.setOutputPath(bulk_load_job, tempPath);
	    bulk_load_job.setMapOutputKeyClass(ImmutableBytesWritable.class);
	    bulk_load_job.setMapOutputValueClass(Put.class);
//	    bulk_load_job.getConfiguration().set(TableOutputFormat.OUTPUT_TABLE, "TPED");
//	    bulk_load_job.setNumReduceTasks(0);
//	    bulk_load_job.setOutputFormatClass(TableOutputFormat.class);
//	    bulk_load_job.waitForCompletion(true);
//	    connection.close();
	  
	    Table TPED_table = connection.getTable(TableName.valueOf("TPED"));
	    RegionLocator regionLocator = connection.getRegionLocator(TableName.valueOf("TPED"));
	    try{
	    	HFileOutputFormat2.configureIncrementalLoad(bulk_load_job, TPED_table, regionLocator);
	    	code = bulk_load_job.waitForCompletion(true)?0:2;  //map-reduce to generate the HFiles under the tempPath 
	    	FsShell shell=new FsShell(conf);
	        shell.run(new String[]{"-chown","-R","hbase:hbase","temp/"}); 
	    	LoadIncrementalHFiles loader = new LoadIncrementalHFiles(conf);
	    	loader.doBulkLoad(tempPath, admin, TPED_table, regionLocator);	
	    }finally{
	    	FileSystem.get(conf).delete(new Path("temp"), true);   //delete the temporary HFiles
	    	admin.close();
	    	regionLocator.close();
	    	TPED_table.close();
	    	connection.close();
	    }
	    
	    //Export to TPED files
	    Job export_job = Job.getInstance(conf,"Export to TPED");
	    export_job.setJarByClass(getClass());
	    TableMapReduceUtil.addDependencyJars(export_job);
	    Scan scan = new Scan(Bytes.toBytes(row_range[0]),Bytes.toBytes(row_range[1]));    // scan with start row and stop row
	    scan.setCacheBlocks(false);  //disable block caching on the regionserver side because mapreduce is sequential reading.
	    TableMapReduceUtil.initTableMapperJob("TPED", scan, ExportMapper.class, NullWritable.class,Text.class,export_job);
	    FileOutputFormat.setOutputPath(export_job, result_outputPath);
//	    FileOutputFormat.setCompressOutput(export_job, true);
//	    FileOutputFormat.setOutputCompressorClass(export_job, BZip2Codec.class);
	    LazyOutputFormat.setOutputFormatClass(export_job, TextOutputFormat.class);
	    export_job.setNumReduceTasks(0);							//no reduce task
		code = export_job.waitForCompletion(true)?0:3;	
	    return code;
	    
	}
	
	public static void main(String [] args)throws Exception{   //  hadoop jar plinkcloud-hbase.jar org.plinkcloud.hbase.HBaseVCF2TPEDSelectedChr -i plinkcloud/input/  -o HBase -r 0.0001 -n $1 -q PASS -c 1-26 -s true -g 9 -a 20 
		//long start_time = System.currentTimeMillis();
		int exit_code = ToolRunner.run(new HBaseVCF2TPEDSelectedChr(), args);
		//long end_time = System.currentTimeMillis();
		//System.out.println("plinkcloud-hbase running time is "+(end_time-start_time)%1000+" seconds");
		System.exit(exit_code);
	}

}
