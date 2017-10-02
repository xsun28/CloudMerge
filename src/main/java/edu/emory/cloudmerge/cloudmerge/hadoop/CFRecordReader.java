package edu.emory.cloudmerge.cloudmerge.hadoop;

import java.io.IOException;
import java.io.InterruptedIOException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.CombineFileSplit;

public class CFRecordReader extends RecordReader<LongWritable, Text> {
	private LongWritable key;
	private Text value;
	private Path path;
	private Configuration conf;
	private SequenceFile.Reader reader;
	private FileSystem fs;
	private long startoffsite;
	private long pos;
	private long end;

	public CFRecordReader(CombineFileSplit split, TaskAttemptContext context, Integer index) throws IOException{
	this.path=split.getPath(index);
	this.conf=context.getConfiguration();
	this.fs=path.getFileSystem(conf);
	this.reader=new SequenceFile.Reader(fs, path, conf);
	this.startoffsite=split.getOffset(index);
	this.end=startoffsite+split.getLength(index);
	this.pos=startoffsite;
	this.value=new Text();
	this.key=new LongWritable();

	}
@Override
public void initialize(InputSplit split, TaskAttemptContext context)throws IOException, InterruptedIOException{

}
@Override
public boolean nextKeyValue() throws IOException, InterruptedIOException{
	if(reader.next(this.key, this.value)){
	pos=reader.getPosition();
		return true;
	}
	else {
		key=null;
		value=null;
		return false;}
}
@Override
public LongWritable getCurrentKey()throws IOException, InterruptedIOException{
	return key;
}
@Override
public Text getCurrentValue()throws IOException{
	return value;
}
@Override
public float getProgress()throws IOException{
	return Math.min(1.0f,(float)(pos-startoffsite)/(float)(end-pos));
}

@Override
public void close(){
	IOUtils.closeStream(reader);
}

}