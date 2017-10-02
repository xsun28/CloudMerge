package edu.emory.cloudmerge.cloudmerge.TabixBased;


import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.FilenameFilter;
import java.io.IOException;
import java.io.PrintWriter;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.NavigableSet;
import java.util.concurrent.Callable;
import java.util.concurrent.ConcurrentSkipListSet;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class TabixBasedJoin {
	
	public static final int READ_SUCCESS = 1;
	public static final int WRITE_SUCCESS = 2;
	protected final Logger logger = LoggerFactory.getLogger(getClass());  
	private ConcurrentSkipListSet<Pos> posSet;
	private ExecutorService threadPool;
	private String outputDir;
	private String inputFileContext;
	private ArrayList<String> inputFileNameList;
	class Pos implements Comparable<Pos>{
		private String chr;
		private String ref;
		private int seq;
		private String SNP_ID;
		public Pos(String chr, int seq){
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
	}// end of Pos class
	
	class paraReader implements Callable<Integer>{
		private TabixReader treader;
		public paraReader(TabixReader treader){
			this.treader=treader;
		}
		@Override
		public Integer call() {
			try{
			extractPosToSet();
			return READ_SUCCESS;
			}
			catch(IOException ioe){
				logger.error("IOE exception in paraReader {} ", ioe);
				return -2;
			}
			catch(Exception e){
				logger.error("Exception in paraReader {}",e);
				return -3;
			}
			
		}
		
		private void extractPosToSet() throws IOException, Exception{
			String line;
			boolean header=true;
			while ((line = treader.readLine()) != null){
				if(!line.toLowerCase().startsWith("#chrom")&&header)
					continue;
				else if (line.toLowerCase().startsWith("#chrom")){
					header=false;
					continue;
				}
				else{
					String[] fields = line.split("\\s");
					String chr =  parseChr(fields[0].trim()) ;
					int seq = Integer.parseInt(fields[1].trim());
					String ref = fields[3].trim();
					String snp_id = fields[2].trim();
					Pos pos = new Pos(chr, seq);
					pos.seRef(ref);
					pos.setSNP_ID(snp_id);
					posSet.add(pos);
//					if(logger.isDebugEnabled()){
//						logger.debug("the first chr {}, the first pos {}",fields[0],fields[1]);
//					}					
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
				throw new Exception("Chromosome can't be parsed");
				}		
		}
		
	}// end of paraReader
	
	private TabixReader[] createReaderArray() throws IOException{
		
			ArrayList<TabixReader> readerList=new ArrayList<TabixReader>();
			for(String name:inputFileNameList){
				readerList.add(new TabixReader(inputFileContext+name));
//				if(logger.isDebugEnabled()){
//					logger.debug("File name {}",context+name);
//				}
				}
			TabixReader[] readerArray=new TabixReader[readerList.size()];
			readerArray=readerList.toArray(readerArray);// Change to array for more efficient access
			return readerArray;	
	}
	
	public TabixBasedJoin(String input, String outputDir){
		posSet = new ConcurrentSkipListSet<Pos>();
		threadPool=Executors.newCachedThreadPool();
		this.outputDir=outputDir;
		File dir=new File(input);
		String[] fileNames=dir.list(new FilenameFilter(){
			@Override
			public boolean accept(File dir,String name){
				String regex = ".*gz$";
				return name.matches(regex);
			}
		});
		
		inputFileContext = dir.getAbsolutePath()+"/";

		inputFileNameList = new ArrayList<String>(Arrays.asList(fileNames));
		Collections.sort(inputFileNameList,new Comparator<String>(){
			@Override
			public int compare(String name1,String name2){
				int number1 = Integer.parseInt(name1.substring(0,name1.indexOf(".")));
				int number2 = Integer.parseInt(name2.substring(0,name1.indexOf(".")));
				return number1-number2;
			}
		});
		File out_dir = new File(outputDir);
		if(!out_dir.exists()) out_dir.mkdirs();
		
		
	}//end of the constructor of TabixBasedJoin
	
	public void readPosToSet() throws IOException, InterruptedException{
		TabixReader[] readerArray = createReaderArray();
		List<paraReader> taskList = new ArrayList<paraReader>();
		for (int i=0; i<readerArray.length;i++)
			taskList.add(new paraReader(readerArray[i]));
		threadPool.invokeAll(taskList);
		Pos[] posArray = new Pos[posSet.size()];
		posArray = posSet.toArray(posArray);
		
	}
	
	public void JoinToTPed() throws IOException, InterruptedException, Exception{
		ArrayList<writeToFileAsTPed> writeTaskList = new ArrayList<writeToFileAsTPed>();  
		for(int i=1;i<=25;i++)
           writeTaskList.add(new writeToFileAsTPed(i));
		
		threadPool.invokeAll(writeTaskList);
	}
	
	public void JoinToPED() throws IOException, InterruptedException, Exception{
	
	}
	
	private String convertToChr(int i){
        switch (i){
        case 23:
                return "X";
        case 24:
                return "Y";
        case 25:
                return "M";
       default:
               return Integer.toString(i);

        }
	}
	
	private NavigableSet<Pos> splitSet(int i){
		String floor_chr = convertToChr(i);
		String ceil_chr = convertToChr(i+1);
		return posSet.subSet(new Pos(floor_chr,0), new Pos(ceil_chr,0)); 
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
	
	class writeToFileAsTPed implements Callable<Integer>{
		private int chr;
		private TabixReader[] readerArray;
		public writeToFileAsTPed(int i) throws IOException{
			this.chr = i;
			readerArray = createReaderArray();
		}
		@Override
		public Integer call() {
			String chrString = convertToChr(chr);
		    String outputFile = outputDir+"/"+chrString;
		    NavigableSet<Pos> subset = splitSet(chr);
		    String chr_str;
		    String ref;
		    int seq;
		    String query;
		    String SNP_ID;
		   
			try(PrintWriter pw = new PrintWriter( new BufferedWriter(new FileWriter(outputFile)))){
				for(Pos pos: subset){
		               chr_str = pos.getChr();
		               seq = pos.getSeq();
		               ref = pos.getRef();
		               SNP_ID = pos.getSNP_ID();
		               StringBuilder query_builder = new StringBuilder();
		               StringBuilder result_builder = new StringBuilder();
		               query = query_builder.append("chr").append(chr_str).append(":").append(seq).append("-").append(seq).toString();
		              
		               result_builder.append(chr_str).append("\t").append(SNP_ID).append("\t").append("0\t").append(seq);
		              for (TabixReader reader: readerArray){
		            	  String result;
		            	  String genotype;
		            	  TabixReader.Iterator queryResultsIter = reader.query(query);
		            	  if(queryResultsIter != null && (result = queryResultsIter.next()) != null)
		            	  {
		            		 genotype = parseGenotype(result);
		            		 result_builder.append("\t").append(genotype);
		            	  }else{
		            		  result_builder.append("\t").append(ref+" "+ref);
		            	  }
		              }
		              //logger.debug("output result is {}",result_builder.toString());
		      	pw.println(result_builder.toString());
				}
		   
		    return WRITE_SUCCESS;
		}catch(Exception e){
			logger.error("Write Thread fail {}",e);
			return -1;
			}
			
		}
		
	}// end of writeToFileAsTPed class


	public static void main(String[] args) {
	
		TabixBasedJoin tbj=new TabixBasedJoin(args[0],args[1]);
	try{	
		tbj.readPosToSet();
		tbj.JoinToTPed();
		}catch(IOException ioe){
			ioe.printStackTrace();
		}catch(InterruptedException ie){
			ie.printStackTrace();
		}catch(Exception e){
			e.printStackTrace();
		}
	}//end of main

}
