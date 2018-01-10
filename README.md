# High-efficient Schemas of Distributed Systems for Sorted Merging of Omics Data

Schemas implemented in Java 8 for merging multiple VCF files into one VCF file or one TPED file using Apache big-data platforms---MapReduce,HBase and Spark respectively. Source codes can be slightly modified to fit into other types of sorted merging of Omics data.

<br>

---

## Prerequsites

### 1. System Requiremnts
Recommended hardware configurations:  
 	
Node Type | CPU | Memory | Disk  
---|---|---|---
**Master**| 2.5G Hz| 15 GB| 200 GB|
**Slave**| 2.5G Hz | 15 GB | 500 GB



 
### 2. Platform Installations 

* Software Version  
	The version of platforms we used are:
	
	Platform|Version
	---|---
	Hadoop | Hadoop-2.7.3
	HBase | HBase-1.3.0
	Spark | Spark-2.1.0
	StarCluster | StarCluster-0.95.6
	
	Note: If you are using different versions of software, please update dependencies in the pom files of corresponding project, and recompile the project.  


  
	
* Separate installation   
    * [Install Hadoop MapReduce](https://hadoop.apache.org/docs/stable/hadoop-project-dist/hadoop-common/ClusterSetup.html)
    * [Install HBase](http://hbase.apache.org/book.html#getting_started)
    * [Install Spark](https://spark.apache.org/docs/latest/spark-standalone.html) 
    * [Install StarCluster](http://mpitutorial.com/tutorials/launching-an-amazon-ec2-mpi-cluster/)
      		
    	We also provide a StarCluster [configuration file](https://s3.amazonaws.com/xsun316/StarClusterConfig/config) and a [bootstrap file](https://s3.amazonaws.com/xsun316/StarClusterConfig/bootstrap.sh) used in our test.     
* Bundled installation
    * [Install Cloudera Manager](http://www.cloudera.com/documentation/manager/5-1-x/Cloudera-Manager-Installation-Guide/Cloudera-Manager-Installation-Guide.html)
* Launch an pre-installed AWS Elastic-MapReduce cluster
    * [How to launch an EMR cluster](https://aws.amazon.com/emr/getting-started/)
    * [Configure EMR cluster](http://docs.aws.amazon.com/emr/latest/ReleaseGuide/emr-configure-apps.html)  
    	We also provide an [EMR configuration file](https://s3.amazonaws.com/xsun316/plinkcloud/EMRConfig/EMRConfiguration.json)	 used in our test.   
 <br>
 
### 3. VCFTools installation  
 
 * [Install VCFTools](http://vcftools.sourceforge.net/examples.html)
 * [Install Tabix](http://www.htslib.org/download/)  
 	Note: You need to have gcc, zlib-devel, bzip2-devel and liblzma for installing Tabix
    	
### 4. [Maven Installation](https://maven.apache.org/install.html)

### 5. [Git Installation](https://git-scm.com/book/en/v2/Getting-Started-Installing-Git)

### 6. Data Preparation  

* We provide a test dataset of 93 VCF files with encrypted genomic locations. Click [here](https://s3.amazonaws.com/xsun316/encrypted/encrypted.tar.gz) to download.  
* A sample merged result data can be downloaded [here](https://s3.amazonaws.com/xsun316/sample_results/result.tar.gz).  
* Type the following command to unzip downloaded files into 93 bzipped VCF files.  
   		
		$ tar xzf encrypted.tar.gz  
* For much larger scale of data, we suggest to use individual VCF files by splitting VCF files from 1000Genome project, which can be downloaded [here](ftp://ftp.1000genomes.ebi.ac.uk/vol1/ftp/release/20130502/).  Note: A 1000Genome VCF file is already a merged file of all the studying subjects. To run the test, you will need to split the file into individual VCF files using [Data Slicer](http://www.internationalgenome.org/data-slicer), [a combination of tabix and VCFtools](http://www.internationalgenome.org/faq/can-i-get-genotypes-specific-individualpopulation-your-vcf-files/), or our much faster and more convenient **[cloudmerge-1000genome](#1000genome)** software.	
	
<br>

---

## Build Project

1. Download the project
	
	```
	$ git clone https://github.com/xsun28/CloudMerge.git
	```
2. If you do not want to rebuild the project and your Java version is 7 or higher, you can directly using the jar files from the following locations in your project home directory :
		
	Schema	| Location
	---|---
	**1000Genome Splitter** | _cloudmerge-1000genome/target/cloudmerge-1000genome.jar_
	**HBase** | _cloudmerge-hbase/target/cloudmerge-hbase.jar_
	**HPC** | _cloudmerge-hpc/*.py_
	**MapReduce** | _cloudmerge-mapreduce/target/cloudmerge-mapreduce.jar_
	**Parallel priority-queue** | _cloudmerge-priorityqueue/target/cloudmerge-parallelpriorityqueue.jar_, _cloudmerge-priorityqueue/target/cloudmerge-ppqsplitdata.jar_
	**Priority-queue** |  _cloudmerge-priorityqueue/target/cloudmerge-priorityqueue.jar_
	**Spark** | _cloudmerge-spark/target/cloudmerge-spark.jar_

3. If step 2 is not chosen, you can compile the project from scratch
	
	```
	$ cd project_home
	
	$ mvn package -Dmaven.test.skip=True
	```
	Note: Maven must be version 3.1 or higher for success compilation. Compiled jars can be found in the same locations as step 2.
	<br>
	<br>
	<br>

---	

## Usage  
[1. Split 1000Genome VCF](#1000genome)  
[2. Load Data to HDFS](#loading)  
[3. Common options to all commands](#options)  
[4. Merge VCF files into one VCF file](#vcf-merge)  
[5. Merge VCF files into one TPED file](#tped-merge)  
[6. Retrieve results from HDFS](#results)
### <a name="1000genome"> </a> Split 1000Genome VCF

* Options  
	
	Option|Meaning|Mandatory
	---|---|---
	**-i** | Input directory | Yes
	**-o** | Output directory | Yes 
	**-c** | Chromosomes range | Yes
	**-d** | ID of samples to be extracted | No
	**-p** | Genomic position range to be extractd | No
	**-n**	| Number of samples to be extracted | Yes
	**-s**	| If all chromosomes of a sample are stacked in one file? | No
* Example  
  We provide a script for indexing 1000Genome VCF files using tabix and splitting 1000Genome VCFs into individual VCF files of interested samples and genomic regions. To run the script:   
	
		$ inputDir=1000Genome/
		$ outputDir=Splits/
		$ startChr=1
		$ endChr=24  ## X=23,Y=24,M=25
		$ samples=HG00096,HG00097
		$ regions=chr1:1,chr2:1,chr3:1,chr4:1,chrX:1,ChrM:1
		$ samplenum=2
		$ cp -p  $project_home_dir/cloudmerge-1000genome/src/main/resources/run-splitter.sh .
		$ chmod 755 run-splitter.sh
		$ nohup ./run-splitter.sh  $inputDir $outputDir $startChr $endChr $samples $regions $samplenum > split.log 2>&1 		 
	Note: The script must under the same directory as the jar files.
	
### <a name="loading"> </a> Loading Data to HDFS
	$ cd $data_dir/
	$ hdfs dfs -mkdir -p $input_dir
	$ hdfs dfs -copyFromLocal *bz2 $input_dir

<br>  

### <a name="options"></a> Common Command Options  
  
The options common to all command scripts (except vcftools-merge.sh):   

Option|Meaning|Mandatory
---|---|---
**-c** | Chromosomes range | Yes
**-g** | Genotype colunm number in input data | Yes
**-i** | Input directory on HDFS | Yes
**-n** | Input file number | Yes
**-o** | Results directory on HDFS | Yes
**-q** | Filter symbol | Yes

<br>  

### <a name="vcf-merge"></a> Merge VCF files into one VCF file
  
<br>  

#### 1. VCFTools (benchmark)  
We provide a Linux script for running VCFTools. You can simply follow the instruction below
	
	$ cd $data_dir/
	
	$ cp -p  $project_home_dir/vcftools-merge.sh .
	
	$ chmod 755 vcftools-merge.sh
	
	$ ./vcftools-merge.sh
		
	 
<br>  
	
#### 2. MapReduce schema
* Command example
		
		$ hadoop jar cloudmerge-mapreduce.jar org.cloudmerge.mapreduce.MRVCF2TPED 
		-D mapreduce.task.io.sort.mb=600 
		-D mapreduce.reduce.merge.inmem.threshold=0 
		-D mapreduce.reduce.input.buffer.percent=1 
		-i /user/hadoop/input/ 
		-o /user/hadoop/output/ 
		-n 93 
		-r 0.0001 
		-c 1-25 
		-s false 
		-q PASS 
		-g 9
		-e false
***Note: Include the complete path to input and output directories on HDFS*** 
* <a name="mapred-config"></a> Suggested configurations  
	
	Name|Value
	--|--
	**mapreduce.task.io.sort.mb**|600
	**mapreduce.reduce.merge.inmem.threshold**|0
	**mapreduce.reduce.input.buffer.percent**|1
	
* Options
	
	Option|Meaning|Mandatory
	---|---|---
	**-e** | If first phase already run? | Yes
	**-r** | First sampling rate | No
	**-s**	| If input data already sorted? | No
	

	
#### 3. HBase schema

* Command example

		$ export HBASE_CONF_DIR=/etc/hbase/conf/
		
		$ export HADOOP_CLASSPATH=./cloudmerge-hbase.jar:$HBASE_CONF_DIR:$(hbase classpath):$HADOOP_CLASSPATH
			
		$ hadoop jar cloudmerge-hbase.jar  org.cloudmerge.hbase.HBaseVCF2TPEDSelectedChr 			-i input/  
		-o output/ 
		-r 0.0001 
		-n 93 
		-q PASS 
		-c 1-25 
		-s true 
		-g 9   
		-a false

* <a name="hbase-config"></a> Suggested configurations:  
	See HBase part in [EMR Configuration](https://s3.amazonaws.com/xsun316/cloudmerge/EMRConfig/EMRConfiguration.json)	

	
* Options
	
	Option|Meaning|Mandatory
	---|---|---
	**-a**	| If incremental merging | No
	
	

#### 4. Spark schema	
	 	  
 * Command example
		
		$ spark-submit --class  org.cloudmerge.spark.VCF2TPEDSparkOneShuffling 
		--master yarn 
		--deploy-mode cluster 
		--executor-cores 1 
		--executor-memory 1g 
		--conf spark.network.timeout=10000000 
		--conf spark.yarn.executor.memoryOverhead=700 
		--conf spark.shuffle.memoryFraction=0.5 		cloudmerge-spark.jar 
		-i input/ 
		-o output/ 
		-n 93 
		-c 1-25 
		-q PASS 
		-g 9

* <a name="spark-config"></a>Suggested configurations:  
	
	Name|Value
	--|--
	**master**|yarn
	**deploy-mode**|cluster
	**executor-cores**|1
	**executor-memory**|1g
	**spark.network.timeout**|10000000
	**spark.yarn.executor.memoryOverhead**|700
	**spark.shuffle.memoryFraction**|0.5
	


 <br>
 <br>  
 	
### <a name="tped-merge"></a> Merge VCF files into one TPED file 
 
#### 1. Multiway-merge implementation (benchmark) 
* Single multiway-merge:
	
	* Command example
		
			$ java -jar cloudmerge-priorityqueue.jar 
				-i input/ 
				-o result.tped 
				-c 1-25 
				-q PASS 
				-s true 
				-g 9	 	
* Parallel multiway-merge:
	* This implementation is based on Tabix and requires VCF files compressed as .gz file and tabix-indexed. We provide a script for running it and a command example as follows:
	
			$ cp -p $project_home_dir/cloudmerge-priorityqueue/src/main/resources/run-parapq.sh .
			$ inputDir=VCF/
			$ outputDir=Result/
  			$ startChr=1
  			$ endChr=26
  			$ procnum=4	
  			$ chr_prefix=chr
  			$ nohup ./run-parapq.sh $inputDir $outputDir $startChr $endChr $procnum $chr_prefix > parapq.log 2>&1 
  		Note: The script must under the same directory as the jar files.	 
	  
#### 2. MapReduce schema	  
Note: all recommend platform configurations and platform-specific options are same (except for -g) as above.   

* Command example
 
		$ hadoop jar cloudmerge-mapreduce.jar org.cloudmerge.mapreduce.VCFMergeMR 
		-D mapreduce.task.io.sort.mb=600 
		-D mapreduce.reduce.merge.inmem.threshold=0 
		-D mapreduce.reduce.input.buffer.percent=1 
		-i /users/hadoop/input/ 
		-o /usrs/hadoop/output/ 
		-n 93 
		-r 0.0001 
		-c 1-25 
		-s false 
		-q PASS 
		-g 9,10 
		-e false
	Note: the -g option here refers to all genotype columns which might be more than one. ***And include the complete path to input and output directories on HDFS*** 


#### 3. HBase schema
* Command example
		
		$ export HBASE_CONF_DIR=/etc/hbase/conf/
		
		$ export HADOOP_CLASSPATH=./cloudmerge-hbase.jar:$HBASE_CONF_DIR:$(hbase classpath):$HADOOP_CLASSPATH
		
		$ hadoop jar cloudmerge-hbase.jar org.cloudmerge.hbase.VCFMergeHBase 
		-i input/  
		-o output/ 
		-r 0.0001 
		-n 93 
		-q PASS 
		-c 1-25 
		-s true 
		-g 9,10   
		-a false
	
#### 4. Spark schema
* Command example  
 		
  		$ spark-submit 
  		--class org.cloudmerge.spark.VCFMergeSpark 
  		--master yarn 
  		--deploy-mode cluster 
  		--executor-cores 1 
  		--executor-memory 1g 
  		--conf spark.network.timeout=10000000 
  		--conf spark.yarn.executor.memoryOverhead=700 
  		--conf spark.shuffle.memoryFraction=0.5 		cloudmerge-spark.jar 
  		-i input/ 
  		-o output/ 
  		-n 93 
  		-c 1-25 
  		-q PASS 
  		-g 9,10

#### 5. HPC implementation
The HPC schema is implemented using Python and mpi4py module. It is running on AWS Virtual Private Cloud (VPC)created by StarCluster toolkit. Please follow the steps below to initiate a VPC cluster and run our HPC implementation:		  

1.	Add your _AWS_ACCESS_KEY_ID_, _AWS_SECRET_ACCESS_KEY_, _AWS_USER_ID_, in the _[aws info]_ section in the StarCluster's config file.

2.	Add _userdata_scripts = bootstrap.sh_ in the _[cluster smallcluster]_ section in the StarCluster's config file

3.	Creating an EBS volume (a NFS file system):  
			
		$ starcluster createvolume --name=cloudmerge-hpc --shutdown-volume-host 500 us-east-1a
4. Get the volume id of the new created EBS volume:  

		$ starcluster listvolumes --name cloudmerge-hpc
5. Add the _volume_id_ in the _[volume cloudmerge]_ section in the StarCluster's config file. The volume will be mounted under the path _/home/ubuntu/cloudmerge/_

6. Start, copy python scripts and connect to the cluster:  
		
		$ starcluster start mpicluster
 	 	$ starcluster put mpicluster $project_home_dir/cloudmerge-hpc/*.py /home/ubuntu/cloudmerge/
 	 	$ starcluster sshmaster mpicluster
 	 	$ cd /home/ubuntu/cloudmerge
7. Create a host file listing all node names in separate lines:   
		
		$ cat << EOF > /home/ubuntu/cloudmerge/hosts
		  master  
		  node001  
		  node002 
		  EOF  
		
8. Run python scripts (an example):  
	
		$ procnum=12
		$ inputDir=/home/ubuntu/cloudmerge/input/
		$ outputDir=/home/ubuntu/cloudmerge/output/
		$ filenum=30
		$ startChr=1
		$ endChr=22
		$ genotype_col=9
		$ filter=PASS
		$ mpiexec -n $procnum --hostfile /home/ubuntu/cloudmerge/hosts python -m mpi4py /home/ubuntu/cloudmerge/mpi_merge.py -i $inputDir -o $outputDir -n $filenum -l $startChr -u $endChr -g $genotype_col -f $filter
	Note: Input and output directory must on the created EBS volume to be shared with all nodes in the cluster.
	

<br>
<br>  

### <a name="results"></a> Retrieve results from HDFS

#### 1. MapReduce schema
For merging to a TPED file  	  

	$hdfs dfs -getmerge ${mapred_output_dir}/chr${i}_result/chr${i}.tped/part* mapredResult.tped 

For merging to a VCF file

	$hdfs dfs -getmerge ${mapred_output_dir}/chr${i}_result/chr${i}.vcf/part* mapredResult.vcf
		
#### 2. HBase schema
	$hdfs dfs -getmerge ${hbase_output_dir}/results/*m* hbaseResult.vcf/tped
#### 3. Spark schema
	$hdfs dfs -getmerge ${spark_output_dir}/part* sparkResult.vcf/tped
	 	
## Contact  

* _xsun28@emory.edu_	 	
	 	
	 	













