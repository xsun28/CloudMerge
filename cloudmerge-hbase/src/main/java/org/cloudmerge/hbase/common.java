package org.cloudmerge.hbase;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.URI;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Objects;


import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.compress.CompressionCodec;
import org.apache.hadoop.io.compress.CompressionCodecFactory;


public class common {
	
	public enum Quality {
		NULL,Q10,Q20,PASS
	};
	
	public static double calQual(double[]quals, double[] weights){
		double total_weight = 0;
		double quality = 0;
		for(double weight:weights)
			total_weight += weight;
		for(int i=0; i<weights.length; i++){
			double weight = weights[i]/total_weight;
			quality += weight*quals[i]; 
		}      			
		return Math.round(quality*100.0)/100.0;
	}
	
	public static String getGenotypes(String[] alts_array,String ref, String[] genotype_array){
		StringBuffer genotypes = new StringBuffer();
		ArrayList<String> alts_list = new ArrayList<>(Arrays.asList(alts_array));
		for(String str: genotype_array){
			if(Objects.equals(str, ".")){
				genotypes.append(str+"\t");
				continue;
			}
			String[] strs = str.split(":");
			String[] letters = strs[0].split("/");
			String[] numbers = new String[letters.length];
			for(int i=0;i<letters.length;i++){
				int num;
				if(Objects.equals(letters[i], ref))
					num = 0;
				else
					num = alts_list.indexOf(letters[i])+1;
				numbers[i] = String.valueOf(num);
			}
			String num_genotype = String.join("/", numbers);
			genotypes.append(num_genotype+":"+strs[1]+"\t");
		}
		return genotypes.toString().trim();
	}
	
	public static String[] getRowRange(String start, String end){
		TextParser parser = new TextParser();
		String start_row = parser.getRowKey(start, "0");
		int end_chr = parser.parseChrnum(end)+1;    //the end row is exclusive, so it is 1+end
		String end_row = parser.getRowKey(String.valueOf(end_chr), "0");
		return new String[]{start_row,end_row};
	}
	
	
	public static void createTable(Admin admin,List<String> boundaries,String name)throws IOException{
		TableName table_name = TableName.valueOf(name);
		if(admin.tableExists(table_name)){
			admin.disableTable(table_name);
			admin.deleteTable(table_name);
		}		
		byte[][] boundaries_array = new byte[boundaries.size()][];
		for(int i = 0; i< boundaries.size(); i++)
			boundaries_array[i] = Bytes.toBytesBinary(boundaries.get(i));
		
		HTableDescriptor desc = new HTableDescriptor(TableName.valueOf(name));
		HColumnDescriptor col_desc = new HColumnDescriptor(Bytes.toBytes("individuals"));
//		col_desc.setCompressionType(Compression.Algorithm.BZIP2);
		desc.addFamily(col_desc);
		admin.createTable(desc,boundaries_array);
		admin.close();
	}
	
	
	public static List<String> getRegionBoundaries(Configuration conf, String sample_path, int region_num) throws IOException{//the result region is 0 to first_boundary, ....., last boundary to maximum
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
	
	
}