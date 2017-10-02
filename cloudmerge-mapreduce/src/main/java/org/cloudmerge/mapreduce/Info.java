package org.cloudmerge.mapreduce;

public class Info{
			
			String sample_id;
			String gender;
			String pheno;
		
			public void setSampleID(String sample_id){
				this.sample_id=sample_id;
			}
			public String getSampleID(){
				return sample_id;
			}
			public void setGender(String gender){
				this.gender=gender;
			}
			public String getGender(){
				return gender;
			}
			public void setPheno(String pheno){
				this.pheno=pheno;
			}
			public String getPheno(){
				return pheno;
			}
			
		}
