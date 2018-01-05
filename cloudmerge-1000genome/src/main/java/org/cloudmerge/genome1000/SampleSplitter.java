package org.cloudmerge.genome1000;

import java.io.BufferedOutputStream;
import java.io.BufferedReader;
import java.io.FileOutputStream;
import java.io.FileReader;
import java.io.IOException;
import java.io.PrintWriter;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.FutureTask;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import htsjdk.tribble.readers.TabixReader;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.compress.compressors.CompressorException;
import org.apache.commons.compress.compressors.CompressorOutputStream;
import org.apache.commons.compress.compressors.CompressorStreamFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class SampleSplitter {
	protected final Logger logger = LoggerFactory.getLogger(getClass());
	private Map<Integer,List<String>> chrs_pos;
	private ExecutorService pool;
	private static boolean global_start = true;
	private static boolean stack = false;
	
	class split_chr implements Callable<Integer>{
		private static final String prefix = "ALL.chr";
		private static final String suffix = ".phase3_shapeit2_mvncall_integrated_v5a.20130502.genotypes.vcf.gz";
		private static final String suffixX = ".phase3_shapeit2_mvncall_integrated_v1b.20130502.genotypes.vcf.gz";
		private static final String suffixY = ".phase3_integrated_v2a.20130502.genotypes.vcf.gz";
		private static final String suffixM = ".phase3_callmom-v0_4.20130502.genotypes.vcf.gz";
		private static final String header = "#CHROM";
//		private BufferedReader bf;
		
		private TabixReader tr;
		private String output; 
		private int chr;
		private int num;
		private List<String> pos;
		private String datasets = null;
//		private String locker = getClass().getName().intern();
		public split_chr(String input,String output,int chr,List<String> pos,String datasets,int sample_num){
			try{
				String path = input+prefix+chr2str(chr);
				switch(chr){
				case 23:
					path += suffixX;
					break;
				case 24:
					path += suffixY;
					break;
				case 25:
					path += suffixM;
					break;
				default:
					path += suffix;
					break;
				}
//				this.bf = new BufferedReader(new FileReader(path));
				this.tr = new TabixReader(path);
				this.output = output;
				this.chr = chr;
				this.num = sample_num;
				this.pos = pos;
				this.datasets = datasets;
				}catch(IOException ioe){
					logger.debug(ioe.getMessage());
				}
		}
		
		
		public Integer call(){

			String [] samples;
			int [] sample_index = new int[num];
			System.out.println("sample num: "+num);
			List<String> sample_array = null;
			List<PrintWriter> writers = null;
			try{
				String head_line = null;
				
				while(null!=(head_line=tr.readLine())){
					
					if(!head_line.startsWith(header)){
						continue;
					}
					else{
						String[] splits = head_line.split("\\s+");
						if(datasets == null)
							samples = Arrays.copyOfRange(splits,9,this.num+9);
						else{
							samples = datasets.split(",");
							sample_array = new ArrayList<String>(Arrays.asList(samples));
						}
						writers = create_writers(samples);
						System.out.println("writers: "+writers.size());
						
						if(global_start){
							int j = 0;
							StringBuffer header_bf = new StringBuffer();
							if(datasets == null){
								for(int i=0;i<=8+this.num;i++){
									if(i<9){
										header_bf.append(splits[i]+"\t");
										continue;
									}
									writers.get(i-9).println(header_bf.toString()+splits[i]);
									sample_index[j++] = i;
								}
							}else{
								
								for(int i=0;i<splits.length;i++){
									if(i<9){
										header_bf.append(splits[i]+"\t");
										continue;
									}

									if(sample_array.contains(splits[i])){
										sample_index[j++] = i;
										writers.get(j-1).println(header_bf.toString()+splits[i]);
										
									}
								}
							}
							if(SampleSplitter.stack)
								global_start = false;							
						}
						
						break;
					}
				}
				System.out.println("here1");
				Integer k = 0;
				if(pos.size()>0)
					read_write_chr_pos(writers,sample_index,k);
				else
					read_write_chr(writers,sample_index,k);

			}catch(IOException ioe){
				logger.debug(ioe.getMessage());
				System.err.println(ioe.getMessage());
			}finally{
				for(PrintWriter writer:writers){
					writer.flush();
					writer.close();
				}
				tr.close();
			}
			return 0;
		}

		private void read_write_chr_pos(List<PrintWriter> writers,int[] sample_index,Integer k) throws IOException{
			String line = null;
			for(String start_end: pos){
				TabixReader.Iterator iter = tr.query(start_end);					
				StringBuffer sb = new StringBuffer();
				while(iter != null && (line = iter.next()) != null){
					++k;
					if(k%10000==0)
						System.out.println("Read line "+k.intValue());
					String[] splits = line.split("\\s+");
					if(splits[3].length()>1) continue;
					sb.setLength(0);
					for(int i=0;i<=8;i++){
						if(i==7)
							sb.append("N/A\t");
						else
							sb.append(splits[i]+"\t");
						}
					for(int i=9;i<9+num;i++){
						int idx = sample_index[i-9];
						if(splits[idx].equals("0|0")) continue;
						writers.get(i-9).println(sb.toString()+splits[idx]);
					}
				}
			}
		}
		
		private void read_write_chr(List<PrintWriter> writers,int[] sample_index,Integer k) throws IOException{
			String line = null;					
			StringBuffer sb = new StringBuffer();
			while((line = tr.readLine()) != null){
				++k;
				if(k%10000==0)
					System.out.println("Read line "+k.intValue());
				String[] splits = line.split("\\s+");
				if(splits[3].length()>1) continue;
				sb.setLength(0);
				for(int i=0;i<=8;i++){
					if(i==7)
						sb.append("N/A\t");
					else
						sb.append(splits[i]+"\t");
					}
				for(int i=9;i<9+num;i++){
					int idx = sample_index[i-9];
					if(splits[idx].equals("0|0")) continue;
					writers.get(i-9).println(sb.toString()+splits[idx]);
				}
			}
		}
		
		private List<PrintWriter> create_writers(String[]samples){
			List<PrintWriter> writers = new ArrayList<>();
			try{
				for(String sample: samples){
					String file = null;
					if(SampleSplitter.stack)
						file = output+sample+".bz2";
					else
						file = output+chr2str(chr)+"_"+sample+".bz2";
					CompressorOutputStream cos =  new CompressorStreamFactory().createCompressorOutputStream(
							CompressorStreamFactory.BZIP2,new BufferedOutputStream (new FileOutputStream(file,true)));
					writers.add(new PrintWriter(cos));
					}
							
				}catch(IOException ioe){
					System.err.println(ioe.getMessage());
				}catch(CompressorException ce){
					System.err.println(ce.getMessage());
				}catch(Exception e){
					System.err.println(e.getMessage());
				}
			return writers;
			}			
	}
	
	public SampleSplitter(String input, String output,String chrs,String pos,int sample_num,boolean stack){
		this(input, output,null, chrs,pos,sample_num, stack);
	}
	
	
	public SampleSplitter(String input, String output,String datasets, String chrs,String pos,int sample_num,boolean stack){
		try{
			this.chrs_pos = parse_chr(chrs,pos);

			SampleSplitter.stack = stack;
			pool = Executors.newCachedThreadPool();		
			if(!stack){
				List<Callable<Integer>> tasks = new ArrayList<>();
				for(Map.Entry<Integer, List<String>> entry:this.chrs_pos.entrySet())
					tasks.add(new split_chr(input,output,entry.getKey(),entry.getValue(),datasets,sample_num));
				pool.invokeAll(tasks);
				System.out.println("going to end");
			}else{
				for(Map.Entry<Integer, List<String>> entry:this.chrs_pos.entrySet()){
					FutureTask <Integer> task = new FutureTask<>(new split_chr(input,output,entry.getKey(),entry.getValue(),datasets,sample_num));
					pool.submit(task);
					task.get();
				}
			}						
		}catch(InterruptedException ie){
			logger.debug(ie.getMessage());
		}catch(ExecutionException ee){
			logger.debug(ee.getMessage());
		}
		finally{
		pool.shutdown();
		}
	}

	private int chr2int(String chr){
		String chr1 = null;
		
		Pattern  pattern = Pattern.compile("[xym\\d]{1,2}",Pattern.CASE_INSENSITIVE);
		Matcher matcher = pattern.matcher(chr);
		if(matcher.find()){
			int start = matcher.start();
			int end = matcher.end();
			chr1 = chr.substring(start,end);
		}
		int chrint = 0;
		switch(chr1.toUpperCase()){
		case "X":
			chrint = 23;
			break;
		case "Y":
			chrint = 24;
			break;
		case "MT":
			chrint = 25;
			break;
		default:
			chrint = Integer.parseInt(chr1);
			break;
		}
		return chrint;
	}
	
	private String chr2str(int chr){
		String chrstr = "";
		switch(chr){
		case 23:
			chrstr = "X";
			break;
		case 24:
			chrstr = "Y";
			break;
		case 25:
			chrstr = "MT";
			break;
		default:
			chrstr = ""+chr;
			break;
		}
		return chrstr;
	}
	
	private Map<Integer,List<String>> parse_chr(String chrs, String pos){
		Map<Integer,List<String>> chr_pos_map = new HashMap<>();

		int hyphen = chrs.indexOf("-");
		int start = hyphen==-1 ? chr2int(chrs) : chr2int(chrs.substring(0,chrs.indexOf("-")));
		int end = hyphen==-1 ? start : chr2int(chrs.substring(chrs.indexOf("-")+1));
		for(int i=start;i<=end;i++)
			chr_pos_map.put(i, new ArrayList<String>());	
		System.out.println("chromosome from "+start+" to "+end);

		if(pos!=null){		
			for(String p: pos.trim().split(",")){
				int chr_index = p.indexOf(":");
				int chr = chr_index == -1?chr2int(p):chr2int(p.substring(0,chr_index));
				System.out.println("chr "+chr);
	//			int start_end[] = new int[2];
	//			int pos_index = p.indexOf("-");
	//			int start_pos = pos_index ==-1? Integer.parseInt(p.substring(chr_index+1)):Integer.parseInt(chrs.substring(chr_index+1,pos_index));
	//			int end_pos = pos_index == -1? -1: Integer.parseInt(chrs.substring(pos_index+1));
	//			start_end[0] = start_pos;
	//			start_end[1] = end_pos;
				if(chr_pos_map.containsKey(chr))
					chr_pos_map.get(chr).add(p);
			}
		}
		return chr_pos_map;
	}
	
	public static void main(String[] args) {  //java -jar cloudmerge-1000genome.jar -i 1000genome/  -o samples/ -c 1-22 -d HG00096,HG00097 -p chr1:1,chr2:1 -n 300 -s
		try{
			CommandLine cmd = commandParser.parseCommands(args);
			String input = cmd.getOptionValue("i");
			String output = cmd.getOptionValue("o");
			String chr = cmd.getOptionValue("c");
			String pos = null;
			if(cmd.hasOption("p"))
				pos = cmd.getOptionValue("p");
			String datasets = null;
			if(cmd.hasOption("d"))
				datasets = cmd.getOptionValue("d");
			int sample_num = Integer.parseInt(cmd.getOptionValue("n"));
			boolean stack = false;
			if (cmd.hasOption("s"))
				stack = true;
			
			SampleSplitter splitter = null==datasets?new SampleSplitter(input,output,chr,pos,sample_num,stack):new SampleSplitter(input,output,datasets,chr,pos,sample_num,stack);
		}catch(Exception e){
			System.err.println(e.getMessage());
		}
	}

}


