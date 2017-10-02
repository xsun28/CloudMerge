package org.cloudmerge.priorityqueue;


import java.io.BufferedInputStream;
import java.io.BufferedOutputStream;
import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.PrintWriter;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.Callable;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.PriorityBlockingQueue;
import java.util.concurrent.Semaphore;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.compress.compressors.CompressorException;
import org.apache.commons.compress.compressors.CompressorInputStream;
import org.apache.commons.compress.compressors.CompressorOutputStream;
import org.apache.commons.compress.compressors.CompressorStreamFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


public class MultiwayMergeVCF2TPED {
protected final Logger logger = LoggerFactory.getLogger(getClass());
private static final int READ_SUCCESS = 1;
private static final int READ_FAILURE = -1;
private static final int SORT_SUCCESS = 2;
private static final int SORT_FAILURE = -2;
private PriorityBlockingQueue<Pos> pqueue;
private String inputFileContext;
private String output;
private int file_no;
private int file_finished; 
private int startChr;
private int endChr;
private enum Quality {
	NULL,Q10,Q20,PASS
};
BufferedReader[] readers;
private Quality qual_filter;
private boolean sorted;
private int gcolumn;
//private String key;
class Pos implements Comparable<Pos>{
	private int chr;
	private String ref;
	private String genotype;
	private int seq;
	private String SNP_ID;
	private int file_no;
	private String chr_str;
	
	public Pos(String chr_str, int seq){	
		this.seq = seq;
		this.chr_str = chr_str;
		this.chr = chrToNum(chr_str);		
	}
	
	@Override
	public int compareTo(Pos second){
//		if(second.chr_str == null) return -1;
		if(Objects.equals(this.chr_str,second.chr_str))
			return this.seq-second.seq;
		else return this.chr_str.compareTo(second.chr_str);					
	}
	
	@Override 
	public boolean equals(Object second){
		if(this==second) return true;
		if(null==second) return false;
		if(!(second instanceof Pos)) return false;
		Pos second_pos = (Pos)second;
		return Objects.equals(chr_str,second_pos.chr_str) && seq==second_pos.seq;		
	}
	

	
	public void setFileNo(int num){
		file_no = num;
	}

	public int getFileNo(){
		return file_no;
	}
	
	public int getChr(){
		return chr;
	}
	
	public int getSeq(){
		return seq;
	}
	
	public String getRef(){
		return ref;
	}
	
	public void seRef(String ref){
		this.ref = ref;
	}
	
	public String getSNP_ID(){
		return SNP_ID;
	}
	
	public void setSNP_ID(String id){
		this.SNP_ID =  id;
	}
	
	public String getGeno_Type(){
		return genotype;
	}
	
	public void setGeno_Type(String type){
		genotype = type;
	}
}// end of Pos class

public String chrToStr(int num){
	String chr;
	switch(num){
	case 23: chr = "X"; break;
	case 24: chr = "Y"; break;
	case 25: chr = "XY"; break;
	case 26: chr = "M"; break;
	default: chr = String.valueOf(num);
	}
	return chr;
}


	
private boolean passQual(String line){
	Quality qual = getQuality(line);
	return !(null == qual || qual.compareTo(qual_filter) < 0);
}
	
private boolean chrInRange(String line){
	String[] fields = line.split("\\s");
	String chr =  parseChr(fields[0].trim()) ;	
	if(null == chr) return false; // in case some file has blank space at the end of the file
	int chrnum = chrToNum(chr);
	if(chrnum > endChr || chrnum < startChr) return false;
	return true;
}
	
private void readPos(int num) throws IOException, Exception{
	BufferedReader reader = readers[num];
	String line = reader.readLine();
	if(null == line){
		file_finished--;
		reader.close();
		return;
	}

	while((!passQual(line)) || (!chrInRange(line))){			
		line = reader.readLine();
		if(null == line){
			file_finished--;
			reader.close();
			return;
		}
	}	
	String[] fields = line.split("\\s");
	String chr =  parseChr(fields[0].trim()) ;		
	int seq = Integer.parseInt(fields[1].trim());
	String ref = fields[3].trim();
	String snp_id = fields[2].trim();
	String geno_type = parseGenotype(line);
	Pos pos = new Pos(chr, seq);
	pos.setFileNo(num);
	pos.seRef(ref);
	pos.setSNP_ID(snp_id);
	pos.setGeno_Type(geno_type);
	pqueue.put(pos);
}

private String parseChr(String input){
		
	Pattern  pattern = Pattern.compile("[xym\\d]{1,2}",Pattern.CASE_INSENSITIVE);
	Matcher matcher = pattern.matcher(input);
	if(matcher.find()){
		int start = matcher.start();
		int end = matcher.end();
		String chr = input.substring(start,end);
		return chr;
	}
	else{
		logger.error("chromosome {} can't be parsed",input);
		return null;
		//throw new Exception("Chromosome can't be parsed");
		}		
}
	
private String parseGenotype(String line){
	StringBuilder genotype = new StringBuilder();
	String numbered_genotype = null;//1/0, 1/1...
	Pattern genotypePattern = Pattern.compile("[\\d]{1}([\\/\\|]{1}[\\d]{1})+");
	String [] fields = line.split("\\s");
	String genotype_field = fields[gcolumn].trim();
	String [] alts = fields[4].trim().split(",");
	String ref = fields[3].trim();
	Matcher matcher = genotypePattern.matcher(genotype_field);
	if(matcher.find())
		numbered_genotype = genotype_field.substring(matcher.start(),matcher.end());
	String [] genotype_numbers = numbered_genotype.split("[\\/\\|]");
	for (int i=0;i<genotype_numbers.length;i++){
		int number = Integer.parseInt(genotype_numbers[i].trim());
		if(number==0)
			genotype.append(ref).append(" ");
		else
			genotype.append(alts[number-1]).append(" ");	
	}
	return genotype.toString().trim();
}
	
private Quality getQuality(String line){
	if(null == line) return null;
	if(line.startsWith("#")) return null; // header
	if(line.contains("PASS")) return Quality.PASS;
	else if(line.contains("q20")) return Quality.Q20;
	else if(line.contains("q10")) return Quality.Q10;
	else return null;
	
}
	

public MultiwayMergeVCF2TPED(String input, String output, String start_chr, String end_chr, String quality, boolean sorted, int genotypeColumn){
	File dir=new File(input);
	String[] fileNames=dir.list();
	file_no = fileNames.length;
	readers = new BufferedReader[file_no];
	file_finished = file_no;
	pqueue = new PriorityBlockingQueue<Pos>(file_no);
	inputFileContext = dir.getAbsolutePath()+"/";
	this.output = output;
	this.sorted = sorted;
	this.gcolumn = genotypeColumn;
	startChr = chrToNum(start_chr);
	endChr = chrToNum(end_chr);
	this.qual_filter = Enum.valueOf(Quality.class, quality.trim().toUpperCase()); 
	ArrayList<String>inputFileNameList = new ArrayList<String>(Arrays.asList(fileNames));
	Collections.sort(inputFileNameList,new Comparator<String>(){
		@Override
		public int compare(String name1,String name2){
			int number1 = Integer.parseInt(name1.substring(0,name1.indexOf(".")));
			int number2 = Integer.parseInt(name2.substring(0,name2.indexOf(".")));
			return number1-number2;
		}
	});
	try{
		if(!sorted)
			inputFileNameList = (ArrayList<String>) sortFiles(inputFileNameList);
		createReaders(inputFileNameList, sorted);
		
	}catch(Exception e){
		logger.error("Reader interrupting error");
		e.printStackTrace();
	}
	
}// end of PriorityQueueJoin constructor


public List<String> sortFiles(final ArrayList<String> nameList) throws Exception{
	
	ExecutorService threadPool = Executors.newCachedThreadPool();
	List<String> sorted_files = new ArrayList<>();
	List<Callable<Integer>> task_list = new ArrayList<>();
	for(final String fileName:nameList){
		final String sortedFile = fileName.replace(".bz2", "_sorted.VCF");
		final String unsortedFile = inputFileContext+fileName;
		sorted_files.add(sortedFile);
		task_list.add(new Callable<Integer>(){
			@Override
			public Integer call(){
				String outputSorted = inputFileContext+sortedFile;
				try{
					Process unzip = Runtime.getRuntime().exec("bunzip2 "+unsortedFile);
					unzip.waitFor();
					String unzipedFile = inputFileContext+fileName.substring(0, fileName.indexOf(".bz2"));
					Process p = Runtime.getRuntime().exec("sort --key=1b,1b --key=2bn,2bn "+unzipedFile+" -o "+outputSorted);
					p.waitFor();
					Process delete = Runtime.getRuntime().exec("rm -rf "+unzipedFile);
					delete.waitFor();
		        }catch(Exception e){
		        	logger.error("Sorting file "+unsortedFile+" errors ");
		        	logger.error(e.getMessage());
		        	return SORT_FAILURE;
		        }
				return SORT_SUCCESS;
			}
		});
	}
	long sort_start_time = System.currentTimeMillis();
	List<Future<Integer>> results = threadPool.invokeAll(task_list);
	System.out.println("Time elapsed on sorting is: "+(System.currentTimeMillis()-sort_start_time)/1000+" seconds");
	for(Future<Integer> result : results){
		if(result.get().intValue() == SORT_FAILURE)
			System.exit(0);
	}
	return sorted_files;
}

public int chrToNum(String chr){
	int chrnum;
	switch (chr.toLowerCase()) {
	case "m":
		chrnum = 26; break;
	case "xy":
		chrnum = 25; break;
	case "x":
		chrnum = 23; break;
	case "y":
		chrnum = 24; break;
	default:
		chrnum = Integer.parseInt(chr);
		break;
	}
	return chrnum;
}



private void createReaders(ArrayList<String> nameList, boolean sorted) {
	int i = 0;
	try{
	for(String fileName:nameList){
		CompressorInputStream cis =  new CompressorStreamFactory().createCompressorInputStream(
				new BufferedInputStream (new FileInputStream(inputFileContext+fileName)));
		readers[i] = new BufferedReader(new InputStreamReader(cis));
		i++;
	}
	}catch(IOException ie){
		logger.error("IOException when creating reader {}", i);
		ie.printStackTrace();
		
	}
	catch(CompressorException ce){
		logger.error("CompressorException when creating reader {}", i);
		ce.printStackTrace();
	}
}

private StringBuilder constructResult(StringBuilder sb, String[] genotypes, String ref){
	for(String genotype: genotypes){
		if(null == genotype){
			 sb.append(ref+" "+ref+" ");
		}else{
			sb.append(genotype).append(" ");
		}
	}
	return sb;
}

public void TPedMerge()  {
	String[] genotypes = null;
	StringBuilder outputLine = null;
	Pos prevPos = null;
	Pos currentPos = null;
	String ref = null;
	try(PrintWriter pw = new PrintWriter( new BufferedWriter(new FileWriter(output)))){
//	try(CompressorOutputStream cos =  new CompressorStreamFactory().createCompressorOutputStream(CompressorStreamFactory.BZIP2,new BufferedOutputStream (new FileOutputStream(output)));
//		PrintWriter pw = new PrintWriter(cos);	
//			){
		for(int i = 0;i < file_no; i++)
			readPos(i);
		
		
		while(file_finished>0){
//			if(pqueue.size() == file_no){
			currentPos = pqueue.take();
			int num = currentPos.getFileNo();
			readPos(num);
			if(!currentPos.equals(prevPos)){
				if(prevPos != null){
//					if(currentPos.chr == 19 && currentPos.getSeq() == 59118099){
//						System.err.println("current chr "+ currentPos.chr +"\t pos "+currentPos.getSeq());
//						System.err.println("prev chr "+ prevPos.chr +"\t pos "+ prevPos.getSeq());
//						System.err.println("equals: "+currentPos.equals(prevPos));
//					}
					StringBuilder result = constructResult(outputLine,genotypes,ref);
					pw.println(result.toString().trim());
				}
				outputLine = new StringBuilder();
				outputLine.append(currentPos.getChr()).append("\t").append(currentPos.getSNP_ID())
					.append("\t0\t").append(currentPos.getSeq()+"\t");			
				genotypes = new String[file_no];
				ref = currentPos.getRef();
				genotypes[currentPos.getFileNo()] = currentPos.getGeno_Type();
			}else{
				genotypes[currentPos.getFileNo()] = currentPos.getGeno_Type();
			}
			prevPos = currentPos;
			
		}
		StringBuilder result = constructResult(outputLine,genotypes,ref);
		pw.println(result.toString().trim());
	}catch(IOException ioe){
		logger.error("IOException of TPEDMerger");
		ioe.printStackTrace();
	}catch(InterruptedException ie){
		logger.error("InterrupttedException of TPEDMerger");
		ie.printStackTrace();
//	}catch(CompressorException ce){
//		logger.error("compressor exception of TPEDMerger");
//		ce.printStackTrace();
	}catch(Exception e){
		logger.error("Other Exception of TPEDMerger");
		e.printStackTrace();
	}
}//end of TPedMerge

public static void main(String[] args) throws Exception {  //java -jar plinkcloud-priorityqueue.jar -i VCF/ -o Result.tped -c 1-26 -q PASS -s true -g 9
	long startTime = System.currentTimeMillis();
	CommandLine cmd = commandParser.parseCommands(args);
	String input = cmd.getOptionValue("i");
	String output = cmd.getOptionValue("o");;
	String chr_range = cmd.getOptionValue("c");
	String start_chr = chr_range.substring(0,chr_range.indexOf("-"));
	String end_chr = chr_range.substring(chr_range.indexOf("-")+1);
	String quality = cmd.getOptionValue("q");
	boolean sorted = true;
	if(cmd.hasOption("s"))
		sorted = Boolean.parseBoolean(cmd.getOptionValue("s"));
	int genotypeColumn = Integer.parseInt(cmd.getOptionValue("g"));
	MultiwayMergeVCF2TPED pqj=new MultiwayMergeVCF2TPED(input,output,start_chr,end_chr, quality, sorted,genotypeColumn);
	pqj.TPedMerge();
	System.out.println("Join Execution Time: "+(System.currentTimeMillis()-startTime)/1000+" seconds");
}//end of main

}
