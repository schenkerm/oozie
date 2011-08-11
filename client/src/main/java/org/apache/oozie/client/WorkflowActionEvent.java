package org.apache.oozie.client;

import java.util.Date;

public interface WorkflowActionEvent {

	public static final String TYPE_OTHER = "other";
	public static final String TYPE_HADOOP_JOB_ID = "hadoop-job-id";
	
	public String getActionId();
	
	public Date getTimestamp();
	
	public String getMessage();
	
	public String getType();
	
}
