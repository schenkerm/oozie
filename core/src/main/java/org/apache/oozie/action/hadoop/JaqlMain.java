package org.apache.oozie.action.hadoop;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileOutputStream;
import java.io.FileReader;
import java.io.IOException;
import java.io.OutputStream;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;

import com.ibm.jaql.util.shell.JaqlShell;

public class JaqlMain extends LauncherMain {
	
	public static final String JAQL_EVAL      = "oozie.jaql.eval";
	public static final String JAQL_PATH      = "oozie.jaql.path";
	public static final String JAQL_SCRIPT    = "oozie.jaql.script";
	
	public final static String JAQL_L4J_PROPS = "log4j.properties";
	
    public static void setJaqlScript(Configuration conf, String script, String eval, String[] jaqlPath) {
        conf.set(JAQL_SCRIPT, script);
        if (eval != null) {
           conf.set(JAQL_EVAL, eval);
        }
        MapReduceMain.setStrings(conf, JAQL_PATH, jaqlPath);
    }
    
    public static void main(String[] args) throws Exception {
        run(JaqlMain.class, args);
    }

	@Override
	protected void run(String[] args) throws Exception {		    	
        System.out.println();
        System.out.println("Oozie Jaql action configuration");
        System.out.println("=================================================================");

        // loading action conf prepared by Oozie
        Configuration actionConf = new Configuration(false);

        String actionXml = System.getProperty("oozie.action.conf.xml");

        if (actionXml == null) {
            throw new RuntimeException("Missing Java System Property [oozie.action.conf.xml]");
        }
        if (!new File(actionXml).exists()) {
            throw new RuntimeException("Action Configuration XML file [" + actionXml + "] does not exist");
        }
        
        actionConf.addResource(new Path("file:///", actionXml));

// TODO: There is no Jaql runtime properties (or jaql-site.xml) file right now - so, there is no way to pass the Kerberos 
//       Information to the Jaql runtime - also the jobConf properties specified under <configuration> will not be used 
        
//        Properties jaqlProperties = new Properties();
//        for (Map.Entry<String, String> entry : actionConf) {
//            jaqlProperties.setProperty(entry.getKey(), entry.getValue());
//        }
//        
//        //propagate delegation related props from launcher job to Jaql job
//        if (System.getenv("HADOOP_TOKEN_FILE_LOCATION") != null) {
//        	jaqlProperties.setProperty("mapreduce.job.credentials.binary", System.getenv("HADOOP_TOKEN_FILE_LOCATION"));
//            System.out.println("------------------------");
//            System.out.println("Setting env property for mapreduce.job.credentials.binary to:"
//                    + System.getenv("HADOOP_TOKEN_FILE_LOCATION"));
//            System.out.println("------------------------");
//            System.setProperty("mapreduce.job.credentials.binary", System.getenv("HADOOP_TOKEN_FILE_LOCATION"));
//        }
//        else {
//            System.out.println("Non-kerberoes execution");
//        }


        // determine the Jaql log file
        // Note: Right now Jaql puts it's log messages into the generic Hadoop sysout log 
        // TODO: We need to figure out, if there is a way to separate the Jaql log from the Hadoop log by providing a separate log4j.properties file
        //       but Jaql does not provide this capability (yet)
        String logFile = System.getProperty("hadoop.log.dir") + File.separatorChar + "userlogs" + File.separatorChar + System.getProperty("hadoop.tasklog.taskid") + File.separatorChar + "syslog";
        System.out.println("log file: " + logFile);
        
        // retrieve the Jaql script to be run
        List<String> arguments = new ArrayList<String>();
        String scriptPath = actionConf.get("oozie.jaql.script");

        if (scriptPath == null) {
            throw new RuntimeException("Action Configuration does not have [oozie.jaql.script] property");
        }

       	if (!new File(scriptPath).exists()) {
       		throw new RuntimeException("Error: Jaql script file [" + scriptPath + "] does not exist");
       	}
       	
       	System.out.println("Jaql script [" + scriptPath + "] content: ");
       	System.out.println("------------------------");
       	BufferedReader br = new BufferedReader(new FileReader(scriptPath));
       	String line = br.readLine();
       	while (line != null) {
       		System.out.println(line);
       		line = br.readLine();
       	}
       	br.close();
       	System.out.println("------------------------");
       	System.out.println();
       
       	// assemble JaqlShell arguments
       	
        // we want to run in batch mode
        arguments.add("-b");

        // assemble Jaql search path where Jaql searches for modules and jars
        String jpString = "";
        
        // add workflow's user module paths to search path
        String[] jaqlPaths = MapReduceMain.getStrings(actionConf, JAQL_PATH);
        if (jaqlPaths != null && jaqlPaths.length > 0) {
            for (String p : jaqlPaths) {
            	File f = new File(p);
            	if (!f.exists()) {
            		throw new RuntimeException("Jaql search path [" + p + "] does not exist!");
            	}

            	if (jpString.length() > 0) {
            	   jpString += File.pathSeparatorChar;
            	}
            	jpString += p;
            }
        }

        // add jaql system module directory to search path
        // modules/ should have been registered into distributed cache
        // and made locally available;
        File jaqlSystemModulePath = new File(".", "modules");
        if (jaqlSystemModulePath.exists() && jaqlSystemModulePath.isDirectory()) {
            if (jpString.length() > 0) {
                jpString += File.pathSeparatorChar;
            }
            jpString += "." + File.separatorChar + "modules";
        }

        if (jpString.length() > 0) {
            arguments.add("-jp");
        	arguments.add('\"' + jpString + '\"');
        }

        // substitute parameters in Jaql script
        String eval = actionConf.get(JAQL_EVAL);
        if (eval != null && eval.length() > 0) {
           arguments.add("-e");
           arguments.add(eval);
        }
        
        // add the jaql script as the last argument
        arguments.add(scriptPath);

        System.out.println("Jaql command arguments :");
        for (String arg : arguments) {
            System.out.println("             " + arg);
        }

        System.out.println("=================================================================");
        System.out.println();
        System.out.println(">>> Invoking Jaql command line now >>>");
        System.out.println();
        System.out.flush();

        try {
            runJaqlJob(arguments.toArray(new String[arguments.size()]));
        }
        catch (SecurityException ex) {
            if (LauncherSecurityManager.getExitInvoked()) {
                if (LauncherSecurityManager.getExitCode() != 0) {
                    throw ex;
                }
            }
        }

        System.out.println();
        System.out.println("<<< Invocation of Jaql command completed <<<");
        System.out.println();

        // harvesting and recording Hadoop Job IDs
        Properties jobIds = getHadoopJobIds(logFile, JOB_ID_LOG_PREFIX);
        File file = new File(System.getProperty("oozie.action.output.properties"));
        OutputStream os = new FileOutputStream(file);
        jobIds.store(os, "");
        os.close();
        System.out.println(" Hadoop Job IDs executed by Jaql: " + jobIds.getProperty("hadoopJobs"));
        System.out.println();        
	}
	
	
   //TODO: Jaql should provide a programmatic way of spitting out Hadoop jobs
   private static final String JOB_ID_LOG_PREFIX = "Running job: ";

   public static Properties getHadoopJobIds(String logFile, String prefix) throws IOException {
       Properties props = new Properties();
       StringBuffer sb = new StringBuffer(100);
       if (!new File(logFile).exists()) {
           System.err.println("jaql log file: " + logFile + " not present. Therefore no Hadoop jobids found");
           props.setProperty("hadoopJobs", "");
       }
       else {
           BufferedReader br = new BufferedReader(new FileReader(logFile));
           String line = br.readLine();
           String separator = "";
           while (line != null) {
               if (line.contains(prefix)) {
                   int jobIdStarts = line.indexOf(prefix) + prefix.length();
                   String jobId = line.substring(jobIdStarts).trim();

                   if (jobId.startsWith("job_")) {
                       sb.append(separator).append(jobId);
                       separator = ",";
                   }
               }
               line = br.readLine();
           }
           br.close();
           props.setProperty("hadoopJobs", sb.toString());
       }
       return props;
   }
	
    protected void runJaqlJob(String[] args) throws Exception {
      JaqlShell.main(args);	    	
    }
}
