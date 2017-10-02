package edu.emory.cloudmerge.cloudmerge.hadoop;

import org.apache.hadoop.mapreduce.lib.jobcontrol.JobControl;


public class JobRunner implements Runnable {
	  private JobControl control;

	  public JobRunner(JobControl _control) {
	    this.control = _control;
	  }

	  public void run() {
	    this.control.run();
	  }
	

}
