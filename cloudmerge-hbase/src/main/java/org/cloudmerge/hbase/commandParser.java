package org.cloudmerge.hbase;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;
import org.apache.commons.cli.PosixParser;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.util.GenericOptionsParser;

public class commandParser {
	private static CommandLine parseCommands(String[] args) throws ParseException{
		Options options = new Options();
		Option input = new Option("i","input",true,"input directory");
		input.setRequired(true);
		options.addOption(input);
		Option output = new Option("o","output",true,"output directory");
		output.setRequired(true);
		options.addOption(output);
		Option id_num = new Option("n","number",true,"individual number");
		id_num.setRequired(true);
		options.addOption(id_num);
		Option chr_range = new Option("c","chr",true,"chromosome range");
		chr_range.setRequired(true);
		options.addOption(chr_range);
		Option qual = new Option("q","qual",true,"quality");
		qual.setRequired(true);
		options.addOption(qual);
		Option sorted = new Option("s","sorted",true,"sorted");
		sorted.setRequired(false);
		options.addOption(sorted);
		Option rate = new Option("r","rate",true,"Sampling rate");
		rate.setRequired(false);
		options.addOption(rate);
		Option genotype_col = new Option("g","genotype",true,"genotype column");
		genotype_col.setRequired(true);
		options.addOption(genotype_col);
		Option additive = new Option("a","additive",true,"incremental merging");
		additive.setRequired(false);
		options.addOption(additive);
		CommandLineParser parser = new PosixParser();
		CommandLine cmd = null;
		try{
			cmd = parser.parse(options,args);
		}catch(Exception e){
			System.out.println(e.getMessage()+"\n");
			HelpFormatter formatter = new HelpFormatter();
			formatter.printHelp("Required arguments", options, true);
			System.exit(1);
		}
		return cmd;
	}
	public static CommandLine parseCommands(String[] args, Configuration conf) throws Exception{
		String[] otherArgs = new GenericOptionsParser(conf,args).getRemainingArgs();
		return parseCommands(otherArgs);
	}
	
//	public static void main(String[] args) throws Exception{
//		Configuration conf = new Configuration();
//		String[] otherArgs = new GenericOptionsParser(conf,args).getRemainingArgs(); 
//		CommandLine cmd = commandParser.parseCommands(otherArgs);
//		String input = cmd.getOptionValue("i");
//		String output = cmd.getOptionValue("o");;
//		String chr_range = cmd.getOptionValue("c");
//		String quality = cmd.getOptionValue("q");
//		boolean sorted = true;
//		if(cmd.hasOption("s"))
//			sorted = Boolean.parseBoolean(cmd.getOptionValue("s"));
//		int genotypeColumn = Integer.parseInt(cmd.getOptionValue("g"));
//		System.out.println("input is "+input);
//		System.out.println("output is "+output);
//		System.out.println("chr_range is "+chr_range);
//		System.out.println("quality is "+quality);
//		System.out.println("sorted is "+sorted);
//		System.out.println("column is "+genotypeColumn);
//	}
	
}
