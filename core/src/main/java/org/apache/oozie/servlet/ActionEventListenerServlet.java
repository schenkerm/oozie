package org.apache.oozie.servlet;

import java.io.IOException;
import java.io.Reader;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Iterator;

import javax.servlet.ServletException;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import org.apache.oozie.DagEngine;
import org.apache.oozie.WorkflowActionEventBean;
import org.apache.oozie.service.DagEngineService;
import org.apache.oozie.service.Services;
import org.json.simple.JSONArray;
import org.json.simple.JSONObject;
import org.json.simple.JSONValue;

/**
 * Servlet listening for Action events
 * 
 * A remote/asynchronous action may send events back to the Oozie server while it is running.
 * This servlet is listening for such events from an Oozie action launcher job and inserts them
 * into the persistent store.
 * 
 * This servlet is the counter part to the ActionEventHandler class.
 *  
 */
public class ActionEventListenerServlet extends JsonRestServlet {

	private static final String INSTRUMENTATION_NAME = "actionEventListener";
    private static final ResourceInfo RESOURCE_INFO = new ResourceInfo("", Arrays.asList("POST"), Collections.EMPTY_LIST);

	private static final long serialVersionUID = 1L;

	
	public ActionEventListenerServlet() {
		super(INSTRUMENTATION_NAME, RESOURCE_INFO);
	}

	@Override
	protected void doPost(HttpServletRequest request, HttpServletResponse resp)
			throws ServletException, IOException {
		String contentType = request.getContentType();
		if (contentType == null || !contentType.startsWith("application/json")) {
			resp.sendError(HttpServletResponse.SC_UNSUPPORTED_MEDIA_TYPE, "Unexpected content type: " + contentType);
		
		} else {
		
			Reader rd = request.getReader();
			JSONArray eventListJson = (JSONArray)JSONValue.parse(rd);
			rd.close();
		
			if (eventListJson != null) {
				ArrayList<WorkflowActionEventBean> eventList = new ArrayList<WorkflowActionEventBean>();
		
				for (Iterator<JSONObject> it = eventListJson.iterator(); it.hasNext();) {
					WorkflowActionEventBean wab = new WorkflowActionEventBean(it.next());
					eventList.add(wab);
				}
		
				if (eventList.size() > 0) {
					DagEngine dagEngine = Services.get().get(DagEngineService.class).getSystemDagEngine();	
					dagEngine.processActionEventList(eventList);			
				}
			} else {
				resp.sendError(HttpServletResponse.SC_BAD_REQUEST, "No events could be prased from request");
			}
		}
	}
}
