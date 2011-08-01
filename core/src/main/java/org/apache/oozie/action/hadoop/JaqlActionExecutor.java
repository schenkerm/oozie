package org.apache.oozie.action.hadoop;

import java.io.IOException;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.oozie.action.ActionExecutorException;
import org.apache.oozie.action.hadoop.JavaActionExecutor;
import org.apache.oozie.action.hadoop.LauncherMain;
import org.apache.oozie.action.hadoop.LauncherMapper;
import org.apache.oozie.action.hadoop.MapReduceMain;
import org.apache.oozie.client.WorkflowAction;
import org.apache.oozie.util.XLog;
import org.jdom.Element;
import org.jdom.JDOMException;
import org.jdom.Namespace;

/**
 * @author mschenk
 *
 */

public class JaqlActionExecutor extends JavaActionExecutor {

	public JaqlActionExecutor() {
		super("jaql");
	}
	
    @SuppressWarnings("unchecked")
	protected List<Class> getLauncherClasses() {
        List<Class> classes = super.getLauncherClasses();
        classes.add(LauncherMain.class);
        classes.add(MapReduceMain.class);
        classes.add(JaqlMain.class);
        return classes;
    }
    
    
    /**
     * we have to override this one since Jaql itself already has a jar with the name jaql-launcher.jar
     */
    @Override
	protected String getLauncherJarName() {
		return "oozie-" + getType() + "-launcher.jar";
	}

	protected String getLauncherMain(Configuration launcherConf, Element actionXml) {
        return launcherConf.get(LauncherMapper.CONF_OOZIE_ACTION_MAIN_CLASS, JaqlMain.class.getName());
    }
    
    void injectActionCallback(Context context, Configuration launcherConf) {
    }
    
   @Override
   @SuppressWarnings("unchecked")
   Configuration setupLauncherConf(Configuration conf, Element actionXml, Path appPath, Context context)
            throws ActionExecutorException {
        super.setupLauncherConf(conf, actionXml, appPath, context);
        Namespace ns = actionXml.getNamespace();
        String script = actionXml.getChild("script", ns).getTextTrim();
        String jaqlName = new Path(script).getName();
        String jaqlScriptContent = context.getProtoActionConf().get("oozie.jaql.script");

        Path jaqlScriptFile = null;
        if (jaqlScriptContent != null) { 
         // TODO: needs to be tested
        	// Create jaql script on hdfs if this is
            // an http submission jaql job;
            FSDataOutputStream dos = null;
            try {
                Path actionPath = context.getActionDir();
                jaqlScriptFile = new Path(actionPath, script);
                FileSystem fs = context.getAppFileSystem();
                dos = fs.create(jaqlScriptFile);
                dos.writeBytes(jaqlScriptContent);

                addToCache(conf, actionPath, script + "#" + jaqlName, false);
            }
            catch (Exception ex) {
                throw new ActionExecutorException(ActionExecutorException.ErrorType.ERROR, "FAILED_OPERATION", XLog
                        .format("Not able to write jaql script file {0} on hdfs", jaqlScriptFile), ex);
            }
            finally {
                try {
                    if (dos != null) {
                        dos.close();
                    }
                }
                catch (IOException ex) {
                    XLog.getLog(getClass()).error("Error: " + ex.getMessage());
                }
            }
        }
        else {
            addToCache(conf, appPath, script + "#" + jaqlName, false);
        }

        // add the Jaql search path directories to the distributed cache - so that they can be picked-up when running the launcher
        List<Element> jaqlPath = (List<Element>) actionXml.getChildren("jaql-path", ns);
        for (int i = 0; i < jaqlPath.size(); i++) {
            addToCache(conf, appPath, jaqlPath.get(i).getTextTrim() + "#" + jaqlPath.get(i).getTextTrim(), false);
        }

        return conf;
    }
    
    @SuppressWarnings("unchecked")
    Configuration setupActionConf(Configuration actionConf, Context context, Element actionXml, Path appPath)
            throws ActionExecutorException {
        super.setupActionConf(actionConf, context, actionXml, appPath);
        Namespace ns = actionXml.getNamespace();
        
        String eval = null;
        
        String script = actionXml.getChild("script", ns).getTextTrim();
        String jaqlName = new Path(script).getName();

        // get the Jaql evaluation String (used to pass-in parameter values as Jaql variables)
        // check for NULL - since this is an optional element
        Element evalEl = actionXml.getChild("eval", ns);
        if (evalEl != null) {
           eval = evalEl.getTextTrim();
        } 
        
        List<Element> jaqlPath = (List<Element>) actionXml.getChildren("jaql-path", ns);
        String[] strJaqlPath = new String[jaqlPath.size()];
        for (int i = 0; i < jaqlPath.size(); i++) {
           strJaqlPath[i] = jaqlPath.get(i).getTextTrim();
        }

        
        JaqlMain.setJaqlScript(actionConf, jaqlName, eval, strJaqlPath);
        return actionConf;
    }
    
    protected boolean getCaptureOutput(WorkflowAction action) throws JDOMException {
        return true;
    }

}
