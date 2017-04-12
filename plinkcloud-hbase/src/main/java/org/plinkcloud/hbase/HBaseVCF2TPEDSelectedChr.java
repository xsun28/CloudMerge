/*This only sample selected chromosomes and output those chromosome records into a directory in the sampling mapper. 
 * These records are then used as the import to the bulkload mapper.
 * */

package org.plinkcloud.hbase;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.URI;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.FsShell;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
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
import org.apache.hadoop.hbase.io.compress.Compression;
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
import org.apache.hadoop.io.compress.BZip2Codec;
import org.apache.hadoop.io.compress.CompressionCodec;
import org.apache.hadoop.io.compress.CompressionCodecFactory;
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
import org.plinkcloud.hbase.HBaseVCF2TPED.Quality;

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
		}
		
		@Override 
		public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException{
			TextParser parser = new TextParser(value.toString());	
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
			file_no = Integer.parseInt(conf.get("fileno"));
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
				if(columns.containsColumn(family, Bytes.toBytes(i)))
					result.append(Bytes.toString(columns.getValue(family, Bytes.toBytes(i)))+" ");
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
	
	
	private List<String> getRegionBoundaries(Configuration conf, String sample_path, int region_num) throws IOException{//the result region is 0 to first_boundary, ....., last boundary to maximum
		int boundary_num = region_num -1;
		List<String> boundaries = new ArrayList<>(boundary_num);
		List<String> temp = new ArrayList<>();
		FileSystem fs = FileSystem.get(URI.create(sample_path),conf);
		if(fs.exists(new Path(sample_path))){
			CompressionCodecFactory factory = new CompressionCodecFactory(conf);
			CompressionCodec codec = factory.getCodec(new Path(sample_path));
			BufferedReader reader = new BufferedReader(new InputStreamReader(codec.createInputStream(fs.open(new Path(sample_path)))));
			String line = "";
			while(null != ( line = reader.readLine()))
				{temp.add(line);
					System.out.println("Boundary lines: "+line);
				}
			int step = (temp.size()+1)/region_num;
			for(int i = 0; i<temp.size(); i++){
				if((i+1)%step == 0) {
					System.out.println("Selected boundary: "+temp.get(i));
					boundaries.add(temp.get(i));
				}
			}
		}else{
			TextParser parser = new TextParser();
			String boundary = parser.getRowKey("0", "0");    //if no sampleout boundary, use 00-000000000 as the boundary
			boundaries.add(boundary);
		}
		return boundaries;		
	}
	
	private void createTable(Admin admin,List<String> boundaries)throws IOException{
		TableName table_name = TableName.valueOf("TPED");
		if(admin.tableExists(table_name)){
			admin.disableTable(table_name);
			admin.deleteTable(table_name);
		}		
		byte[][] boundaries_array = new byte[boundaries.size()][];
		for(int i = 0; i< boundaries.size(); i++)
			boundaries_array[i] = Bytes.toBytesBinary(boundaries.get(i));
		
		HTableDescriptor desc = new HTableDescriptor(TableName.valueOf("TPED"));
		HColumnDescriptor col_desc = new HColumnDescriptor(Bytes.toBytes("individuals"));
//		col_desc.setCompressionType(Compression.Algorithm.BZIP2);
		desc.addFamily(col_desc);
		admin.createTable(desc,boundaries_array);
		admin.close();
	}
	
	private String[] getRowRange(String start, String end){
		TextParser parser = new TextParser();
		String start_row = parser.getRowKey(start, "0");
		int end_chr = parser.parseChrnum(end)+1;    //the end row is exclusive, so it is 1+end
		String end_row = parser.getRowKey(String.valueOf(end_chr), "0");
		return new String[]{start_row,end_row};
	}
	
	@Override
	public int run(String[] args)throws Exception{
		int code = 0;
		Configuration conf = HBaseConfiguration.create(); //get configurations from configuration files on the classpath	
		Path inputPath = new Path(args[0]); 						// 	VoTECloud/input
		String outputPath = args[1].trim(); 						// 	base output path  HBase
		Path sample_outputPath = new Path(outputPath+"/sample"); 	// 	HBase/sample
		Path result_outputPath = new Path(outputPath+"/results");  	//	HBase/result
		Path selected_chr_dir = new Path(outputPath+"/sample/"+SELECTED_CHR_DIR); //HBASE/selectedChr
		double sampleRate = Double.parseDouble(args[2]); //0.0001
		conf.set("samplerate", ""+sampleRate);	
		conf.set("fileno", args[3].trim());
		conf.set("quality", args[4].trim());
		String chr_range = args[5].trim();
		String start_chr = chr_range.substring(0,chr_range.indexOf("-"));
		String end_chr = chr_range.substring(chr_range.indexOf("-")+1);
		boolean ordered = Boolean.valueOf(args[6]);
		conf.set("startchr", start_chr);
		conf.set("endchr", end_chr);
		conf.setBoolean("ordered", ordered);
		String[] row_range = getRowRange(start_chr,end_chr);
		int region_num = Integer.parseInt(args[3].trim())/2;  //keep each region to hold approximately 3 input file's size data. The region size should around 1G
		region_num = region_num > 1? region_num : 2;   //if region num 
		conf.setBoolean("mapreduce.map.speculative", false);
		conf.setBoolean("mapreduce.reduce.speculative", false);  //turn off the speculative execution
		conf.setDouble("mapreduce.job.reduce.slowstart.completedmaps", 0.8);  //set the reduce slow start to 50% of completed map tasks cause sampling reducer is very light weight
		//sample job to get boundaries
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
		List<String> boundaries =  getRegionBoundaries(conf,sample_file,region_num);
		Connection connection = ConnectionFactory.createConnection(conf);
		Admin admin = connection.getAdmin();
		createTable(admin,boundaries);
		
		//Bulk Load Job
		conf.setDouble("mapreduce.job.reduce.slowstart.completedmaps", 0.6);  
		Job bulk_load_job = Job.getInstance(conf,"Bulk_Load");		 
		bulk_load_job.setJarByClass(getClass());
		TableMapReduceUtil.addDependencyJars(bulk_load_job);//distribute the required dependency jars to the cluster nodes. Also need to add the plinkcloud-hbase.jar onto the HADOOP_CLASSPATH in  order for HBase's TableMapReduceUtil to access it. 
		FileInputFormat.addInputPath(bulk_load_job, selected_chr_dir);
		bulk_load_job.setInputFormatClass(SequenceFileReadCombiner.class); 
	    Path tempPath = new Path("temp/bulk");	    
	    FileOutputFormat.setOutputPath(bulk_load_job, tempPath);
	    bulk_load_job.setMapperClass(BulkLoadingMapper.class);
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
	
	public static void main(String [] args)throws Exception{   //  hadoop jar plinkcloud-hbase.jar org.plinkcloud.hbase.HBaseVCF2TPEDSelectedChr VoTECloud/input/  HBase 0.0001 3 PASS 1-22 true 
		//long start_time = System.currentTimeMillis();
		int exit_code = ToolRunner.run(new HBaseVCF2TPEDSelectedChr(), args);
		//long end_time = System.currentTimeMillis();
		//System.out.println("plinkcloud-hbase running time is "+(end_time-start_time)%1000+" seconds");
		System.exit(exit_code);
	}

}
