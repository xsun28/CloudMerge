package org.plinkcloud.hbase;

import java.util.Arrays;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.hadoop.hbase.util.Bytes;

public class test {
	private static final int POS_LENGTH = 10;
	private static final int CHR_LENGTH = 2;
enum Size{
	SMALL("S"), LARGE("L");
	private String desc;
	private Size(String s){
		this.desc = s;
	}
	public String getDesc(){
		return desc;
	}
	
};


private static String getRowKey(String chr, String pos){
	char[] pos_array = new char[POS_LENGTH];
	char[] chr_array = new char[CHR_LENGTH];
	Arrays.fill(pos_array, '0');
	Arrays.fill(chr_array, '0');
	int pos_start_offset = POS_LENGTH - pos.length();
	int chr_start_offset = CHR_LENGTH - chr.length();
	System.arraycopy(pos.toCharArray(), 0, pos_array, pos_start_offset, pos.length());
	System.arraycopy(chr.toCharArray(), 0, chr_array, chr_start_offset, chr.length());
	String Key =  String.valueOf(chr_array)+"-"+ String.valueOf(pos_array);	
	return Key;	
}

public static String[] parseRowKey(String rowkey){
	String row_key[] = new String[2];  //row_key[0] = chr, row_key[1] = pos
	String tmp1 = rowkey.substring(0,rowkey.indexOf("-"));
	String tmp2 = rowkey.substring(rowkey.indexOf("-")+1);
	int chr = Integer.parseInt(tmp1);
	long pos = Long.parseLong(tmp2);
	row_key[0] = String.valueOf(chr);
	row_key[1] = String.valueOf(pos);
	return row_key;
}

public static void main(String args[]){
	Size test = Enum.valueOf(Size.class, "SMALL");
	Size test1 = Size.LARGE;
//	Pattern  pattern = Pattern.compile("[xym\\d]{1,2}",Pattern.CASE_INSENSITIVE);
//	String input = "abxd";
//	Matcher matcher = pattern.matcher(input);
//	if(matcher.find()){
//		int start = matcher.start();
//		int end = matcher.end();
//		String chr = input.substring(start,end);
//		System.out.println(chr);
//	}
	String rowKey = getRowKey("2","123");
	byte[] row = Bytes.toBytes(rowKey);
	String rowKey1 = Bytes.toStringBinary(row);
	String rowKey2 = Bytes.toString(row);
	System.out.println("rowkey1 "+rowKey1);
	System.out.println("rowkey2 "+rowKey2);
	String[] chr_pos = parseRowKey(rowKey);
	System.out.println("chr: "+ chr_pos[0]);
	System.out.println("pos: "+ chr_pos[1]);
}
}
