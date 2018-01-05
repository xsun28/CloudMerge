package org.cloudmerge.priorityqueue;


public class WorkloadSplit{
	public final static long[] CHRLENGTHS = new long[]{248956422,242193529,198295559,190214555,181538259,170805979,159345973,
			145138636,138394717,133797422,135086622,133275309,114364328,107043718,101991189,90338345,83257441,80373285,
			58617616,64444167,46709983,50818468,156040895,57227415,0,16569}; 
	private String prefix;
	private int start_chr;
	private int end_chr;
	private String sstart_chr;
	private String send_chr;
	private long start_pos;
	private long end_pos;
	private String[] queries;
		
	private String chrStr(int chr){
		String str = this.prefix;
		switch (chr){
			case 23:
				str += "X";
				break;
			case 24:
				str += "Y";
				break;
			case 25:
				str += "XY";
				break;
			case 26:
				str += "M";
				break;
			default:
				str += chr;				
		}
		return str;
	}
	
	public WorkloadSplit(long startpos, long endpos,String prefix){
		find_chr_pos(startpos,endpos);
		this.prefix = prefix;
		this.sstart_chr = chrStr(start_chr);
		this.send_chr = chrStr(end_chr);
		this.queries = generate_query();
	}
	
	public int get_startChr(){
		return this.start_chr;
	}
	
	public int get_endChr(){
		return this.end_chr;
	}
	
	public long get_startPos(){
		return this.start_pos;
	}
	
	public long get_endPos(){
		return this.end_pos;
	}
	
	public String[] get_query(){
		return this.queries;
	}
	
	private String[] generate_query(){
		int num = this.end_chr-this.start_chr+1;
		String[] queries = new String[this.end_chr-this.start_chr+1];
		queries[0] = chrStr(this.start_chr)+":"+this.start_pos;
		for(int i=1; i<num-1;i++ ){
			queries[i] = chrStr(start_chr+i);
		}
		queries[num-1] = chrStr(end_chr)+":1-"+this.end_pos;
		return queries;
	}
	
	private void find_chr_pos(long startpos, long endpos){
		int i = 0;
		long sum = 0;
		long start = startpos;
		long end = endpos;
		boolean startnotfound = true;
		for(; i<CHRLENGTHS.length;i++){
			sum += CHRLENGTHS[i];
			if(startpos<=sum && startnotfound){				
				this.start_chr = i+1;
				this.start_pos = start;
				startnotfound = false;
			}
			if(endpos<=sum){
				this.end_chr = i+1;
				this.end_pos = end;
				break;
			}
			start -= CHRLENGTHS[i];
			end -= CHRLENGTHS[i];
		}
	}
	
	@Override
	public String toString(){
		StringBuilder sb = new StringBuilder();
		for(String query:this.queries)
			sb.append(query).append(",");
		return sb.toString().substring(0,sb.length()-1);
	}
}
