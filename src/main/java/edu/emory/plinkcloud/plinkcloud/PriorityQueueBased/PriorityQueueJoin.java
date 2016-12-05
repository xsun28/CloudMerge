package edu.emory.plinkcloud.plinkcloud.PriorityQueueBased;


import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.io.PrintWriter;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.PriorityBlockingQueue;
import java.util.concurrent.Semaphore;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


public class PriorityQueueJoin {
protected final Logger logger = LoggerFactory.getLogger(getClass());
private static final int READ_SUCCESS = 1;
private static final int READ_FAILURE = -1;
private PriorityBlockingQueue<Pos> pqueue;
private String inputFileContext;
private String output;
private ExecutorService threadPool;
private int file_no;

class Pos implements Comparable<Pos>{
	private String chr;
	private String ref;
	private String genotype;
	private int seq;
	private String SNP_ID;
	private Semaphore semaphore;
	private int file_no;
	
	public Pos(String chr, int seq, Semaphore sem){
		this.semaphore = sem;
		this.chr = chr;
		this.seq = seq;
	}
	
	@Override
	public int compareTo(Pos second){
		if(!this.chr.toLowerCase().equals(second.chr.toLowerCase())){
			int chrnum1, chrnum2;
			switch (chr.toLowerCase()) {
			case "m":
				chrnum1 = 25; break;
			case "x":
				chrnum1 = 23; break;
			case "y":
				chrnum1 = 24; break;

			default:
				chrnum1 = Integer.parseInt(chr);
				break;
			}
			
			switch (second.chr.toLowerCase()) {
			case "m":
				chrnum2 = 25; break;
			case "x":
				chrnum2 = 23; break;
			case "y":
				chrnum2 = 24; break;

			default:
				chrnum2 = Integer.parseInt(second.chr);
				break;
			}
			return chrnum1-chrnum2;
		}
		else{
			return this.seq-second.seq;
		}
	}
	
	@Override 
	public boolean equals(Object second){
		if(this==second) return true;
		if(null==second) return false;
		if(!(second instanceof Pos)) return false;
		Pos second_pos = (Pos)second;
		return Objects.equals(chr,second_pos.chr) && seq==second_pos.seq;		
	}
	

	public void Semaphore_Unlock(){
			semaphore.release();
	}
	public void setFileNo(int num){
		file_no = num;
	}
	
	public int getFileNo(){
		return file_no;
	}
	
	public String getChr(){
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

class VCFReader implements Callable<Integer>{
	BufferedReader reader;
	private Semaphore semaphore;
	private int num;
	public VCFReader(String filename, int num){
		String filePath = inputFileContext+filename;
		this.num = num;
		semaphore = new Semaphore(0);
		try{
		reader = new BufferedReader(new FileReader(filePath));
		}
		catch(IOException ioe){
			logger.error("Read VCF errors");
			ioe.printStackTrace();
		}
	}
	
	@Override
	public Integer call() throws IOException{
		try{
		logger.debug("Thread {} started",num);
		extractPosToSet();
		}catch(Exception e){
			logger.debug("IOE error reading VCF files");
			e.printStackTrace();
			return READ_FAILURE;
		}finally{
			reader.close();
		}
		return READ_SUCCESS;
	} 
	
	private void extractPosToSet() throws IOException, Exception{
		String line;
		boolean header=true;
		while ((line = reader.readLine()) != null){
			if(!line.toLowerCase().startsWith("#chrom")&&header)
				continue;
			else if (line.toLowerCase().startsWith("#chrom")){
				header=false;
				continue;
			}
			else{
				String[] fields = line.split("\\s");
				if(fields[0].length()==0) logger.debug("wrong input is {}",line);
				String chr =  parseChr(fields[0].trim()) ;
				int seq = Integer.parseInt(fields[1].trim());
				String ref = fields[3].trim();
				String snp_id = fields[2].trim();
				String geno_type = parseGenotype(line);
				Pos pos = new Pos(chr, seq,semaphore);
				pos.setFileNo(num);
				pos.seRef(ref);
				pos.setSNP_ID(snp_id);
				pos.setGeno_Type(geno_type);
				pqueue.put(pos);
				try{
					semaphore.acquire();
					}catch(InterruptedException ie){
						logger.error("Semaphore lock error");
						ie.printStackTrace();
					}
//				if(logger.isDebugEnabled()){
//					logger.debug("the first chr {}, the first pos {}",fields[0],fields[1]);
//				}					
			}		
		}
	}
	
	private String parseChr(String input) throws Exception{
		
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
			throw new Exception("Chromosome can't be parsed");
			}		
	}
	
	private String parseGenotype(String line){
		StringBuilder genotype = new StringBuilder();
		String numbered_genotype = null;//1/0, 1/1...
		Pattern genotypePattern = Pattern.compile("[\\d]{1}([\\/\\|]{1}[\\d]{1})+");
		String [] fields = line.split("\\s");
		String genotype_field = fields[9].trim();
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
	
}// end of VCFReader class

public PriorityQueueJoin(String input, String output){
	File dir=new File(input);
	String[] fileNames=dir.list();
	file_no = fileNames.length;
	pqueue = new PriorityBlockingQueue<Pos>(file_no);
	threadPool = Executors.newCachedThreadPool();
	inputFileContext = dir.getAbsolutePath()+"/";
	this.output = output;
	ArrayList<String>inputFileNameList = new ArrayList<String>(Arrays.asList(fileNames));
	Collections.sort(inputFileNameList,new Comparator<String>(){
		@Override
		public int compare(String name1,String name2){
			int number1 = Integer.parseInt(name1.substring(0,name1.indexOf(".")));
			int number2 = Integer.parseInt(name2.substring(0,name1.indexOf(".")));
			return number1-number2;
		}
	});
	try{
	readVCFs(inputFileNameList);
	}catch(InterruptedException ie){
		logger.error("Reader interrupting error");
		ie.printStackTrace();
	}
	
}// end of PriorityQueueJoin constructor

private void readVCFs(ArrayList<String> nameList) throws InterruptedException{
	int i = 0;
	for(String fileName:nameList)
		threadPool.submit(new VCFReader(fileName,i++));
}

private StringBuilder constructResult(StringBuilder sb, String[] genotypes, String ref){
	for(String genotype: genotypes){
		if(null == genotype){
			 sb.append("\t").append(ref+" "+ref);
		}else{
			sb.append("\t").append(genotype);
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
	
	while(!pqueue.isEmpty()|| prevPos==null){
		currentPos = pqueue.take();
		currentPos.Semaphore_Unlock();
		if(!currentPos.equals(prevPos)){
			
			if(null!=outputLine){
				outputLine = constructResult(outputLine,genotypes,ref);
				pw.println(outputLine.toString());
			}
			outputLine = new StringBuilder();
			outputLine.append(currentPos.getChr()).append("\t").append(currentPos.getSNP_ID())
			.append("\t").append("0\t").append(currentPos.getSeq());			
			genotypes = new String[file_no];
			ref = currentPos.getRef();
			genotypes[currentPos.getFileNo()] = currentPos.getGeno_Type();
		}else{
			genotypes[currentPos.getFileNo()] = currentPos.getGeno_Type();
		}
	}
	}catch(IOException ioe){
		logger.error("IOException of TPEDMerger");
		ioe.printStackTrace();
	}catch(InterruptedException ie){
		logger.error("InterrupttedException of TPEDMerger");
		ie.printStackTrace();
	}

}//end of TPedMerge

public static void main(String[] args) {
	
	PriorityQueueJoin pqj=new PriorityQueueJoin(args[0],args[1]);
	pqj.TPedMerge();
	
}//end of main

}
