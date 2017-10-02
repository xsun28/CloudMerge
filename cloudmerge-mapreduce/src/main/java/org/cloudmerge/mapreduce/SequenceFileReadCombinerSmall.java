package org.cloudmerge.mapreduce;

import java.io.IOException;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.CombineFileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.CombineFileRecordReader;
import org.apache.hadoop.mapreduce.lib.input.CombineFileSplit;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import java.util.List;

public class SequenceFileReadCombinerSmall extends CombineFileInputFormat<LongWritable, Text> {
  

	public SequenceFileReadCombinerSmall(){
    super();
    setMaxSplitSize(40000); 
  }
  
  public RecordReader<LongWritable, Text> createRecordReader(InputSplit split, TaskAttemptContext context) throws IOException{
	 return new CombineFileRecordReader<LongWritable, Text>((CombineFileSplit)split, context, CFRecordReader.class);
  }
  @Override
  protected boolean isSplitable(JobContext context, Path file){
    return false;
  }
  @Override
  protected List<FileStatus> listStatus(JobContext job
                                        )throws IOException {

    List<FileStatus> files = super.listStatus(job);
    
    return files;
  }
  
}
