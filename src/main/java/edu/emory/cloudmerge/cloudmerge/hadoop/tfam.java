package edu.emory.cloudmerge.cloudmerge.hadoop;

import java.io.*;
import java.util.*;


public class tfam extends Thread{
	
	
	
	private int chr;
	private File infoFile;
	private File nameFile;
	private File inputFile;
	private File posFile;
	private File tfamFile;

public tfam(){
	
}
public tfam(String infoFile, String nameFile, int chr){
	

	this.chr=chr;
	this.infoFile=new File(infoFile);//patient info file
	this.nameFile=new File(nameFile);//file_ID file
	this.tfamFile=new File("chr"+chr+".tfam");

	super.start();
}
	
public  void run()  {
		long starttime=System.currentTimeMillis();		
		HashMap<String,Info> map=new HashMap<String,Info>();
		long pos;		
		long snpno=0;
		
		String patientID;
		String gender;
		String pheno;
		try{
			
			PrintWriter tfamWriter=new PrintWriter(new BufferedWriter(new FileWriter(tfamFile)));
			
			BufferedReader br=new BufferedReader(new FileReader(infoFile));
			String line;
			String temp[];
			while((line=br.readLine())!=null){
				temp=line.split("\\s+");
				Info i=new Info();
			
				i.setSampleID(temp[1].trim());
				i.setGender(temp[3].trim());
				i.setPheno(temp[5].trim());
				map.put(temp[0].trim(), i);
			}
			Scanner sc1=new Scanner(nameFile);
			Info i1;
			
			while(sc1.hasNext()){
				String tfamLine="0 ";
				patientID=sc1.next().trim();
			i1=map.get(patientID);
			
			if(i1.getGender().equals("M")) gender="1";
		    else if (i1.getGender().equals("F")) gender="2";
		    else gender="0";
		    if(i1.getPheno().equals("Case")) pheno="2";
		    else if (i1.getPheno().equals("Control")) pheno="1";
		    else pheno="0";
			tfamLine+=patientID+" 0 0 "+gender+" "+pheno;
			
			//System.out.println("id "+ patientID);
			tfamWriter.println(tfamLine);
			
			
		
			
					
			}
			tfamWriter.close();
		
		
	
	
		
		long endtime=System.currentTimeMillis();
		System.out.println("TFILE"+chr+" elapsed time is "+ (endtime-starttime)/1000+" seconds");
		}
		catch (FileNotFoundException fe){
			System.out.println(fe);
		}
		catch (IOException ie){
			System.out.println(ie);
		}

	}

public static void main(String args[]){
	long starttime=System.currentTimeMillis();
	
	String infofile=args[0].trim();
	String namefile=args[1].trim();

	
	
	String chrmrange=args[2].trim();
	
	int index=chrmrange.indexOf("-");
	String firstchrm=chrmrange.substring(0, index).trim();
	String lastchrm=chrmrange.substring(index+1).trim();
	int first, last;
	if(firstchrm.equalsIgnoreCase("X"))
		first=23;
	else if(firstchrm.equalsIgnoreCase("Y"))
		first=24;
	else if(firstchrm.equalsIgnoreCase("M"))
		first=25;
	else first=Integer.parseInt(firstchrm);

	if(lastchrm.equalsIgnoreCase("X"))
		last=23;
	else if(lastchrm.equalsIgnoreCase("Y"))
		last=24;
	else if(lastchrm.equalsIgnoreCase("M"))
		last=25;
	else last=Integer.parseInt(lastchrm);


	int chromsno=last-first+1;
	Thread threads[]=new Thread[chromsno];
	for(int i=0;i<chromsno;i++)
		threads[first+i-1]=new tfam(infofile,namefile,first+i);
	try{
		//mp1.join();
	//mp2.join();
	//mp3.join();
		for(int i=0;i<chromsno;i++){
			threads[i].join();
		}
	}
	catch(Exception e){
		System.err.println(e.toString());
		
	}
	long endtime=System.currentTimeMillis();
	//System.out.println("PedMap elapsed time is "+ (endtime-starttime)/1000+" seconds");
	System.out.println(" Elapsed time is "+ (endtime-starttime)/1000+" seconds");
	
}

}
