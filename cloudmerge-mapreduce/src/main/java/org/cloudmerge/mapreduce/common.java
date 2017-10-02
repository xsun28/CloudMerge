package org.cloudmerge.mapreduce;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Objects;
import java.util.regex.Matcher;
import java.util.regex.Pattern;



public class common {

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
	
	
}
