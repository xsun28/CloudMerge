package org.cloudmerge.spark;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Objects;
import java.util.regex.Matcher;
import java.util.regex.Pattern;



public class common {
	private static final int POS_LENGTH = 10;
	private static final int CHR_LENGTH = 2;
	private static final int ID_LENGTH = 3;
	
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
	
	public static String parseGenotype(String ref, String alt_str, String field){
		String[] alts = alt_str.trim().split(",");
		StringBuffer genotype = new StringBuffer();
		String numbered_genotype = null;//1/0, 1/1...
		Pattern genotypePattern = Pattern.compile("[\\d]{1}([\\/\\|]{1}[\\d]{1})+");
//		String genotype_field = fields[genotype_col].trim();
//		String ref = fields[3].trim();
		Matcher matcher = genotypePattern.matcher(field);
		String left="";
		if(matcher.find()){
			numbered_genotype = field.substring(matcher.start(),matcher.end());
			left = field.substring(matcher.end());
		}
		
		String [] genotype_numbers = numbered_genotype.split("[\\/\\|]");
		for (int i=0;i<genotype_numbers.length;i++){
			int number = Integer.parseInt(genotype_numbers[i].trim());
			if(number==0)
				genotype.append(ref).append("/");
			else
				genotype.append(alts[number-1]).append("/");	
		}
		String temp = genotype.substring(0, genotype.length()-1);
		genotype.setLength(0);
		genotype.append(temp+left);			
		return genotype.toString().trim();
	}
	
	public enum Quality {
		NULL,Q10,Q20,PASS
	};
	
	public static Quality getQuality(String line){
		if(line.startsWith("#")) return null;
		if(line.contains("PASS")) return Quality.PASS;
		else if(line.contains("q20")) return Quality.Q10;
		else if(line.contains("q10")) return Quality.Q20;
		else return null;
		
	}
	
	public static int parseChrnum(String chr){
		int chrm;
		if(chr.equalsIgnoreCase("X")) chrm = 23;
		else if (chr.equalsIgnoreCase("Y")) chrm = 24;
		else if (chr.equalsIgnoreCase("XY")) chrm = 25;
		else if (chr.equalsIgnoreCase("M")) chrm = 26;
		else chrm = Integer.parseInt(chr);
		return chrm;
	}
	
	
	public static String parseGenotype(String line, int genotype_col){
		
		StringBuilder genotype = new StringBuilder();
		String numbered_genotype = null;//1/0, 1/1...
		Pattern genotypePattern = Pattern.compile("[\\d]{1}([\\/\\|]{1}[\\d]{1})+");
		String [] fields = line.split("\\s");
		String genotype_field = fields[genotype_col].trim();
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
	
	
	public static String[] getChrPos(String line){
		String chr_pos[] = new String[2];  //row_key[0] = chr, row_key[1] = pos
		String tmp1 = line.substring(0,line.indexOf("-"));
		String tmp2 = line.substring(line.indexOf("-")+1);
		int chr = Integer.parseInt(tmp1);
		long pos = Long.parseLong(tmp2);
		chr_pos[0] = String.valueOf(chr);
		chr_pos[1] = String.valueOf(pos);
		return chr_pos;
	}// end of getChrPOs
	
	
	
	public static String getRowKey(String chr, String pos){
		char[] pos_array = new char[POS_LENGTH];
		char[] chr_array = new char[CHR_LENGTH];
		Arrays.fill(pos_array, '0');
		Arrays.fill(chr_array, '0');
		int pos_start_offset = POS_LENGTH - pos.length();
		int chr_start_offset = CHR_LENGTH - chr.length();
		System.arraycopy(pos.toCharArray(), 0, pos_array, pos_start_offset, pos.length());
		System.arraycopy(chr.toCharArray(), 0, chr_array, chr_start_offset, chr.length());
		return  String.valueOf(chr_array)+"-"+ String.valueOf(pos_array);			
	}
	
	public static String getRowKey(String chr, String pos,String ind_id){
		char[] pos_array = new char[POS_LENGTH];
		char[] chr_array = new char[CHR_LENGTH];
		char[] id_array = new char[ID_LENGTH];
		Arrays.fill(pos_array, '0');
		Arrays.fill(chr_array, '0');
		Arrays.fill(id_array, '0');
		int pos_start_offset = POS_LENGTH - pos.length();
		int chr_start_offset = CHR_LENGTH - chr.length();
		int id_start_offset = ID_LENGTH - ind_id.length();
		System.arraycopy(pos.toCharArray(), 0, pos_array, pos_start_offset, pos.length());
		System.arraycopy(chr.toCharArray(), 0, chr_array, chr_start_offset, chr.length());
		System.arraycopy(ind_id.toCharArray(), 0, id_array, id_start_offset, ind_id.length());
		return  String.valueOf(chr_array)+"-"+ String.valueOf(pos_array)+"-"+String.valueOf(id_array);			
	}
	
}