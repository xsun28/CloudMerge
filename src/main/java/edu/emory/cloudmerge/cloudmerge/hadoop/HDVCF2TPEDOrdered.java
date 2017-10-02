/*using interval sampler for already sorted original files and transform directly into TPED format*/
package edu.emory.cloudmerge.cloudmerge.hadoop;
 
import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.OutputStreamWriter;

import java.net.URI;
import java.util.*;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.WritableComparator;
import org.apache.hadoop.io.SequenceFile.CompressionType;
import org.apache.hadoop.io.SequenceFile.Reader;
import org.apache.hadoop.io.Text;

import org.apache.hadoop.mapreduce.lib.output.MultipleOutputs;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Partitioner;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;

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

import org.apache.hadoop.util.GenericOptionsParser;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

public class HDVCF2TPEDOrdered extends Configured implements Tool {
	
public static class ChromMapper extends Mapper<LongWritable, Text, LongWritable, IntWritable>	{
	private LongWritable outkey = new LongWritable();
	private Text Result=new Text();
	private String[] temp;
	private boolean [] sex;
	private String filename;
	private MultipleOutputs<NullWritable, Text> mos;
	private long kept=0;
	private long records=0;
	private double freq;
	private int chrm;

	public void setup(Context context) throws IOException, InterruptedException{

Configuration conf=context.getConfiguration();
int fileNum=Integer.parseInt(conf.get("fileno"));
freq=Double.parseDouble(conf.get("samplerate"));
sex=new boolean[fileNum];
for(int i=0; i<fileNum; i++)
	sex[i]=false;

	FileSplit fileSplit = (FileSplit)context.getInputSplit();
	String fullname=fileSplit.getPath().getName();

	int i=fullname.indexOf('.');
	filename=fullname.substring(0, i);
mos=new MultipleOutputs(context);
	}
	
	public void map(LongWritable key, Text value, Context context)
			throws IOException, InterruptedException {
			String outputResult;
	
			String line=value.toString();
			if(line.contains("PASS")){
			temp=line.split("\\s+");
			String outputFile;
		 int x=temp[0].indexOf("r");
		 
		 String chrnum=temp[0].substring(x+1).trim();
		 
          /*if(chrnum.equals("Y")){
        	  int num=Integer.parseInt(filename);
        	 if(!sex[num-1]){
        		 mos.write("sex",new IntWritable(num), new Text("1"), "sex/part" );
        		 sex[num-1]=true;
        	 }
          }*/
			if(temp[6].equals("PASS")){
				
				if(chrnum.equalsIgnoreCase("X")) chrm=23;
				 else if (chrnum.equalsIgnoreCase("Y")) chrm=24;
				 else if (chrnum.equalsIgnoreCase("M")) chrm=25;
				 else chrm=Integer.parseInt(chrnum);
				
				records++;
				outputFile=temp[0].trim()+"/part";
				String maxgt="";
				if(temp[10].startsWith("1/0")||temp[10].startsWith("0/1"))
					maxgt="0/1";
				else if(temp[10].startsWith("0/0"))
					maxgt="0/0";
				else maxgt="1/1";
				outkey.set(Long.parseLong(temp[1]));
                
				if ((double) kept / (double)records < freq) {
					kept++;
					context.write(outkey, new IntWritable(chrm));}
					
				
                
			outputResult=(filename+","+chrnum+","+temp[3]+","+temp[4]+","+maxgt);

			Result.set(outputResult);

			
			mos.write(temp[0].trim(),outkey, Result, outputFile);

			}

			
			}
	}
	
	protected void cleanup(Context context) throws IOException, InterruptedException {
	    mos.close();
	}
	
}

public static class PartitionlstPartitioner extends Partitioner <LongWritable, IntWritable>{
	@Override
	public int getPartition(LongWritable key, IntWritable value, int numPartitions){
		int chrmnum=value.get();
		
			return chrmnum-1;
		
	}
}





	
public static class PartitionReducer extends Reducer<LongWritable, IntWritable, LongWritable, NullWritable> {
	private int StepSize;
	private int samplenums=0;
	private int last=0;
	private long longkey;
	private int totalnums;
	private String chrm=null;
	private LongWritable outkey=new LongWritable();
	private ArrayList<Long> samples=new ArrayList<Long> ();
	private BufferedWriter writer;
	private String outpath;
	private String outpath1;
	private Configuration conf;
	
	public void setup(Context context) throws IOException, InterruptedException{
	conf=context.getConfiguration();
	StepSize=Integer.parseInt(conf.get("fileno"));
	int chrmnum=context.getTaskAttemptID().getTaskID().getId()+1;
	outpath1=conf.get("outputpath");
	
	
	if(chrmnum==23) chrm="X";
	else if(chrmnum==24) chrm="Y";
	else if(chrmnum==25) chrm="M";
	else chrm=""+chrmnum;
	outpath=outpath1+"/chr"+chrm+"samplenum";
	
	}

	
	public void reduce(LongWritable key, Iterable<IntWritable> values, Context context)
		throws IOException, InterruptedException {
		outkey=key;
		longkey=outkey.get();
		for(IntWritable value: values){
		
			samplenums++;
			if(samplenums>=StepSize){
				
				if(last!=0){
					long lastkey=samples.get(last-1);
					if(longkey!=lastkey) {
						samples.add(longkey);
						last++;
						context.write(outkey,NullWritable.get());
					samplenums=0;
					}
					else {
						samplenums--;
						break;
					}
						
					
					
				}
				else {
					samples.add(longkey);
					last++;
					context.write(outkey,NullWritable.get());
			samplenums=0;
			}
			}
		}
				
			
		}

	protected void cleanup(Context context) throws IOException, InterruptedException {
		totalnums=samples.size();
		FileSystem fs=FileSystem.get(URI.create(outpath), conf);
if(totalnums==0){ 
	
	String file=outpath+"/part-r-000";
	if(chrm.equalsIgnoreCase("X")) file+="22";
	else if(chrm.equalsIgnoreCase("Y")) file+="23";
	else if(chrm.equalsIgnoreCase("M")) file+="24";
	else if(chrm.length()==2)file+=(Integer.parseInt(chrm)-1);
	else file+="0"+(Integer.parseInt(chrm)-1);
	FileSystem fs1=FileSystem.get(URI.create(file), conf);
	fs1.delete(new Path(file), true);
	System.out.println("deleted");
	
	
}
	    try{
		writer=new BufferedWriter(new OutputStreamWriter(fs.create(new Path(outpath), true)));
		System.out.println("total sample nums in "+chrm+" is "+totalnums);
		writer.write(""+totalnums);
		
	    
	    
	     
	     
	    
	    }finally{
	    	writer.close();
	    }
		
		
		
		
	}
		
	
	
}

public static class ValueReducer extends Reducer<LongWritable, Text, LongWritable, Text> {

private Text result=new Text();
private Text frequency=new Text();
private String[] temp;
private int total;
private boolean first=true;
private MultipleOutputs<NullWritable, Text> mos;
private String chrm;

public void setup(Context context) throws IOException, InterruptedException{
Configuration con=context.getConfiguration();
total=Integer.parseInt(con.get("fileno"));

mos=new MultipleOutputs(context);
}

public void reduce(LongWritable key, Iterable<Text> values, Context context)
throws IOException, InterruptedException {
String outputStr=null;
boolean [] notNull=new boolean[total];
String [] ref=new String [total];
String [] alt=new String [total];
String []maxgt=new String[total];
String al1=null;
String al2=null;
String al3=null;
int refnum=0;
int al1num=0;
int al2num=0;
int al3num=0;
String refnu=null;
boolean freq=true;
String outputfreq=null;
//String outputfreqrank=null;
/* For freqrank output
 * ArrayList<String> nuArray=new ArrayList<String> ();
nuArray.add("A");
nuArray.add("C");
nuArray.add("G");
nuArray.add("T");*/



for(int i=0;i<total;i++){
	notNull[i]=false;
	ref[i]="	";
	alt[i]="	";
}

for (Text t : values) {

String line=t.toString();
temp=line.split(",");

if(first) { 
chrm="chr"+temp[1];
outputStr=(temp[1]+" "+key.toString()+" 0 "+key.toString());
outputfreq=(temp[1]+"\t"+key.toString()+"\t");
//outputfreqrank=(temp[1]+"\t"+key.toString()+"\t");

first=false;}

int current=Integer.parseInt(temp[0]);
maxgt[current-1]=temp[4];
notNull[current-1]=true;
ref[current-1]=temp[2];

alt[current-1]=temp[3];
refnu=temp[2];
//nuArray.remove(refnu);

if(al1==null) {
	al1=temp[3];
	al1num++;
	//nuArray.remove(al1);
}
else if (al1.equals(temp[3]))
	al1num++;
else if(al2==null) {
	freq=false;
	al2=temp[3];
	al2num++;
	//nuArray.remove(al2);
}
else if(al2.equals(temp[3]))
	al2num++;
else if(al3==null) {
	
	al3=temp[3];
	al3num++;
	//nuArray.remove(al3);
}
else if(al3.equals(temp[3]))
	al3num++;
}

for(int count=1;count<=total;count++){
if(!notNull[count-1]){
	
outputStr+=(" "+refnu+" "+refnu);

}
else{
	if(maxgt[count-1].equals("0/1"))
		outputStr+=(" "+ref[count-1]+" "+alt[count-1]);
	else if(maxgt[count-1].equals("0/0"))
		outputStr+=(" "+refnu+" "+refnu);
	else
	outputStr+=(" "+alt[count-1]+" "+alt[count-1]);
	}
}

first=true;

result.set(outputStr);
refnum=total-al1num-al2num-al3num;
/*
if(al2==null){
	al2=nuArray.get(0);
	al3=nuArray.get(1);	
}
else if(al3==null)
	al3=nuArray.get(0);
String [] numlist=new String[]{refnum+"\t"+refnu, al1num+"\t"+al1, al2num+"\t"+al2, al3num+"\t"+al3};
Arrays.sort(numlist, new Comparator<String>(){
	@Override
	public int compare(String s1, String s2){
		String [] splits1= s1.trim().split("\t");
		String [] splits2= s2.trim().split("\t");
		return Integer.valueOf(splits1[0]).compareTo(Integer.valueOf(splits2[0]));
	}
});
outputfreqrank+=numlist[3]+"\t"+numlist[2]+"\t"+numlist[1]+"\t"+numlist[0];
mos.write(NullWritable.get(), new Text(outputfreqrank), "freqrank");
*/
if(freq){

mos.write(NullWritable.get(),result, chrm+".tped/part");

double reffreq=(double)refnum/total;
double al1freq=(double)al1num/total;
outputfreq+="ref "+refnu+"\tAL "+al1+"\tRefFreq "+reffreq+"\tMAF "+al1freq+"  ";
frequency.set(outputfreq);
mos.write(NullWritable.get(), frequency, chrm+".freq/part");
}
/*else{
	mos.write(NullWritable.get(),result, "multiresult");
	double al1freq=(double)al1num/total;
	double al2freq=(double)al2num/total;
	double al3freq=(double)al3num/total;
	double reffreq=(double)refnum/total;
	
	if(al3num==0) al3=" ";
outputfreq+="ref "+refnu+"\tAL1 "+al1+"\tAL2 "+al2+"\tAL3 "+al3+"\tRefFreq "+reffreq+"\tAL1F "+al1freq+"\tAL2F "+al2freq+"\tAL3F "+al3freq;
	
	frequency.set(outputfreq);
	mos.write(NullWritable.get(), frequency, "multifreq");
	
}*/

}

protected void cleanup(Context context) throws IOException, InterruptedException {
    mos.close();
}

}



public static Configuration getSecondJobConf( Tool tool, Configuration conf, String chrm, Path inputPath, Path outputPath, String partitionlst, double samplerate, int indno, boolean compression)
throws IOException, ClassNotFoundException, InterruptedException{
	
	int reducernum;
	Job orderJob=new Job(conf);
	orderJob.setJobName(chrm+" job");
	orderJob.setJarByClass(tool.getClass());
	orderJob.setReducerClass(ValueReducer.class);

	orderJob.getConfiguration().setBoolean("mapred.compress.map.output", true);
	orderJob.getConfiguration().setClass("mapred.map.output.compression.codec", Lz4Codec.class, CompressionCodec.class);
	FileInputFormat.setInputPaths(orderJob,  inputPath);
	orderJob.setInputFormatClass(SequenceFileReadCombiner.class);
	FileOutputFormat.setOutputPath(orderJob, outputPath);
	if(compression){
	FileOutputFormat.setCompressOutput(orderJob,true);
	FileOutputFormat.setOutputCompressorClass(orderJob, BZip2Codec.class);
	}
	
	orderJob.setOutputKeyClass(LongWritable.class);
	orderJob.setOutputValueClass(Text.class);
	LazyOutputFormat.setOutputFormatClass(orderJob, TextOutputFormat.class);
    
	orderJob.getConfiguration().set( "mapred.textoutputformat.separator", "");
	
	String file=partitionlst+"/part-r-000";
	if(chrm.equalsIgnoreCase("X")) file+="22";
	else if(chrm.equalsIgnoreCase("Y")) file+="23";
	else if(chrm.equalsIgnoreCase("M")) file+="24";
	else if(chrm.equals("10")) file+="09";
	else if(chrm.length()==2)file+=(Integer.parseInt(chrm)-1);
	else file+="0"+(Integer.parseInt(chrm)-1);
	Path partitionFile = new Path(file);
	String samplenumFileStr=partitionlst + "/chr"+chrm+"samplenum";
	Path samplenumFile = new Path(samplenumFileStr);
	FileSystem fs=FileSystem.get(URI.create(samplenumFileStr),conf);
	BufferedReader reader=new BufferedReader(new InputStreamReader(fs.open(samplenumFile)));
	String samplenum=reader.readLine();
	System.out.println("retrieved samplenum for "+chrm+" is "+samplenum);
	reducernum=Integer.parseInt(samplenum.trim())+1;
	
	reader.close();
	
   

	orderJob.setPartitionerClass(TotalOrderPartitioner.class);
	
	FileSystem fs1=FileSystem.get(URI.create(file),conf);
	
	if(fs1.exists(partitionFile)){
		TotalOrderPartitioner.setPartitionFile(orderJob.getConfiguration(), partitionFile);

		orderJob.setNumReduceTasks(reducernum);
	}
	else{
		Path partitionFile1 = new Path(outputPath + "_partitions.lst");
		FileSystem fs2=FileSystem.get(URI.create(outputPath + "_partitions.lst"),conf);
		if(!fs2.exists(partitionFile1)){
			int num=500;
			System.out.println("partition file for "+chrm+" not existed");
			orderJob.setNumReduceTasks((indno/93)*4);
orderJob.setInputFormatClass(SequenceFileReadCombinerSmall.class);
			TotalOrderPartitioner.setPartitionFile(orderJob.getConfiguration(), partitionFile1);
					InputSampler.writePartitionFile(orderJob, new InputSampler.RandomSampler<LongWritable, Text>(samplerate*1000, num, 1));
					
		}
		else{
			orderJob.setNumReduceTasks((indno/93)*4);
			TotalOrderPartitioner.setPartitionFile(orderJob.getConfiguration(), partitionFile1);
		}
		
	}
	
	return orderJob.getConfiguration();
}

public int run(String [] args) throws Exception {
	int code=0;
Path inputPath = new Path(args[0]);

Path outputOrder = new Path(args[1]);
int indno=Integer.parseInt(args[2].trim());
double sampleRate = Double.parseDouble(args[3]);


String chrmrange=args[4];
boolean compression=Boolean.parseBoolean(args[5]);
int index=chrmrange.indexOf("-");
String firstchrm=chrmrange.substring(0, index).trim();
String lastchrm=chrmrange.substring(index+1).trim();
int first, last;
if(firstchrm.equalsIgnoreCase("X"))
	first=23;
else if(firstchrm.equalsIgnoreCase("Y"))
	first=24;
else if(firstchrm.equalsIgnoreCase("M"))
	first=25;
else first=Integer.parseInt(firstchrm);

if(lastchrm.equalsIgnoreCase("X"))
	last=23;
else if(lastchrm.equalsIgnoreCase("Y"))
	last=24;
else if(lastchrm.equalsIgnoreCase("M"))
	last=25;
else last=Integer.parseInt(lastchrm);


int chromsno=last-first+1;

Configuration conf = new Configuration();
String defaultName=conf.get("fs.default.name");
System.out.println("default "+defaultName);

String []chroms=new String[chromsno];
String []chrmFiles=new String[chromsno];
String []chrmResults=new String[chromsno];
for(int i=0;i<chromsno;i++, first++){
	if(first==23)chroms[i]="chrX";
	else if(first==24) chroms[i]="chrY";
	else if(first==25) chroms[i]="chrM";
	else chroms[i]="chr"+first;
chrmFiles[i]=defaultName+args[1]+"/"+chroms[i];
chrmResults[i]=chrmFiles[i]+"_result";
}
FileSystem fs=FileSystem.get(URI.create(chrmFiles[0]),conf);
boolean exist=fs.exists(new Path(chrmFiles[0]));
conf.set("fileno", args[2]);
conf.set("outputpath",defaultName+args[1]);
conf.set("samplerate", ""+sampleRate);




if(!exist){



Job chrJob=new Job(conf);
chrJob.setJobName("chromseparation");
chrJob.setJarByClass(getClass());
FileInputFormat.setInputPaths(chrJob, inputPath);
FileOutputFormat.setOutputPath(chrJob, outputOrder);
chrJob.setMapperClass(ChromMapper.class);
chrJob.setMapOutputKeyClass(LongWritable.class);
chrJob.setMapOutputValueClass(IntWritable.class);

chrJob.setPartitionerClass(PartitionlstPartitioner.class);
chrJob.setReducerClass(PartitionReducer.class );
chrJob.setOutputKeyClass(LongWritable.class);
chrJob.setOutputValueClass(NullWritable.class);

chrJob.setNumReduceTasks(25);
chrJob.getConfiguration().setBoolean("mapred.compress.map.output", true);
chrJob.getConfiguration().setClass("mapred.map.output.compression.codec", Lz4Codec.class, CompressionCodec.class);
String fileName;
for(int i=1; i<=26; i++){
	
	if(i==23){
		fileName="chrX";
		MultipleOutputs.addNamedOutput(chrJob, fileName,SequenceFileOutputFormat.class, LongWritable.class, Text.class); 
		//MultipleOutputs.addNamedOutput(chrJob, fileName+"samplenums",SequenceFileOutputFormat.class, IntWritable.class, NullWritable.class);
	}
	else if(i==24){
		fileName="chrY";
		MultipleOutputs.addNamedOutput(chrJob, fileName,SequenceFileOutputFormat.class, LongWritable.class, Text.class);
		//MultipleOutputs.addNamedOutput(chrJob, fileName+"samplenums",SequenceFileOutputFormat.class, IntWritable.class, NullWritable.class);
	}
	else if(i==25){
		fileName="chrM";
		MultipleOutputs.addNamedOutput(chrJob, fileName,SequenceFileOutputFormat.class, LongWritable.class, Text.class);
		//MultipleOutputs.addNamedOutput(chrJob, fileName+"samplenums",SequenceFileOutputFormat.class, IntWritable.class, NullWritable.class);
	}
	else if(i==26){
		fileName="sex";
		MultipleOutputs.addNamedOutput(chrJob, fileName,SequenceFileOutputFormat.class, IntWritable.class, Text.class);
		//MultipleOutputs.addNamedOutput(chrJob, fileName+"samplenums",SequenceFileOutputFormat.class, IntWritable.class, NullWritable.class);
	}
	else{
		fileName="chr"+i;
		MultipleOutputs.addNamedOutput(chrJob, fileName,SequenceFileOutputFormat.class, LongWritable.class, Text.class);
		//MultipleOutputs.addNamedOutput(chrJob, fileName+"samplenums",SequenceFileOutputFormat.class, IntWritable.class, NullWritable.class);
	}
}

SequenceFileOutputFormat.setCompressOutput(chrJob, true);
SequenceFileOutputFormat.setOutputCompressorClass(chrJob, Lz4Codec.class);
SequenceFileOutputFormat.setOutputCompressionType(chrJob, CompressionType.BLOCK);

LazyOutputFormat.setOutputFormatClass(chrJob, SequenceFileOutputFormat.class);

 code=chrJob.waitForCompletion(true)?0:1;


}


if(code==0){
	
	
	int leftchrmno=chromsno;
	boolean [] exited=new boolean[leftchrmno];
	JobControl jc=new JobControl("paralleljobs");
   ControlledJob []jobs;
	FileSystem fs1;
	for(int i=0; i<chromsno;i++){
	fs1=FileSystem.get(URI.create(chrmResults[i]),conf);
	boolean existedResult=fs1.exists(new Path(chrmResults[i]));
	if(existedResult){
		leftchrmno--;
		exited[i]=true;	
	}
	else
		exited[i]=false;
	}
	if(leftchrmno==0) {
		System.err.println("all files existed");
		return 1;
	}
	else{
		jobs=new ControlledJob[leftchrmno];
		for(int i=0; i<chromsno; i++){
			if(exited[i]) continue;
			jobs[i]=new ControlledJob(getSecondJobConf(this, conf,chroms[i].substring(3),new Path(chrmFiles[i]),new Path(chrmResults[i]),defaultName+args[1], sampleRate, indno, compression ));
		jc.addJob(jobs[i]);
		}
		JobRunner runner = new JobRunner(jc);
		Thread t = new Thread(runner);
		t.start();

		
		int i=1;
	while(!jc.allFinished()){
		System.out.println(i+"th running...");
		i++;
		Thread.sleep(5000);
	}
	return 0;	
		
	}
	
}
else return 2;



}

public static void main(String[] args) throws Exception{
	int code=ToolRunner.run(new HDVCF2TPEDOrdered(), args);
	System.exit(code);
}

}