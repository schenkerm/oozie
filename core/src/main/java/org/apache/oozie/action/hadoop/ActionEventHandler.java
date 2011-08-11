package org.apache.oozie.action.hadoop;

import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.OutputStream;
import java.net.HttpURLConnection;
import java.net.URL;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.Locale;
import java.util.TimeZone;
import java.util.concurrent.ConcurrentLinkedQueue;

import javax.net.ssl.HttpsURLConnection;

/**
 * Utility class to send action events to the Oozie server
 * 
 * An ActionEventHander can be used with any action to report interim status updates back to the Oozie server.
 * Status updates are treated as events which are asynchronously handled. Events will be kept in a sending 
 * queue until they were successfully sent to the server, or the sender thread is stopped via finish().
 * 
 * The HTTP message sent to the Oozie server may contain more than one event. Events are sent using a 
 * serialized JSON array. 
 *
 */
public class ActionEventHandler {
	
	private String actionId;
	private ConcurrentLinkedQueue<LiteWorkflowActionEvent> eventQueue = null;
	private EventSender sender = null;
	
	public ActionEventHandler() {
		this.actionId                 = System.getProperty("oozie.action.id");
		String oozieActionCallbackUrl = System.getProperty("oozie.action.event.callback.url");
		int    oozieActionSendInterval = 10;
		
		if (oozieActionCallbackUrl != null) { 
			String intervalString = System.getProperty("oozie.action.event.send.interval");
			if (intervalString != null) {
				oozieActionSendInterval = Integer.parseInt(intervalString);
			}
		
			System.out.println("event handling is enabled:");
			System.out.println("==========================");
			System.out.println("Oozie event callback URL: " + oozieActionCallbackUrl);
			System.out.println("Event send interval     : " + oozieActionSendInterval);
			System.out.println("==========================");
			
			this.eventQueue = new ConcurrentLinkedQueue<LiteWorkflowActionEvent>();
			this.sender = new EventSender(eventQueue, oozieActionCallbackUrl, oozieActionSendInterval * 1000);
			sender.start();
		} else {
			System.out.println("event handling is disabled");
		}
	}
	
	public void sendEvent(LiteWorkflowActionEvent event) {
		if (eventQueue != null) {
			eventQueue.add(event);
		} else {
			System.out.println("no events will be sent because event handling is disabled");
		}
	}
	
	public void finish() {
		if (sender != null) {
			sender.stopSending();
			try {
				sender.join();
			} catch (InterruptedException e) {
				e.printStackTrace();
			}
		}
	}
	
	public static boolean isEventHandlingEnabled() {
		return System.getProperty("oozie.action.event.callback.url") != null;
	}
	
    public LiteWorkflowActionEvent createWorkflowActionEvent() {
		return new LiteWorkflowActionEvent(actionId);
	}
    
    /**
     * Handles the communication with the Oozie server
     * 
     * The EventSender is a separate thread which attempts to send action events back to the 
     * Oozie server in specified intervals. The events are taken off the send queue; serialized
     * as a JSON array and sent via a HTTP POST request to the Oozie server.
     * 
     * In case the messages could not be sent successfully they're put back into the send queue 
     * for later attempts. The EventSender continues to send events until stopSending() is called. 
     *
     */
    private static class EventSender extends Thread {

    	private ConcurrentLinkedQueue<LiteWorkflowActionEvent> eventQueue = null;
    	private int interval = -1;
    	private volatile boolean stopSending = false;
    	private String oozieActionCallbackUrl;
    	
    	public EventSender(
    			ConcurrentLinkedQueue<LiteWorkflowActionEvent> eventQueue, 
    			String oozieActionCallbackUrl,
    			int interval) {
    		this.eventQueue = eventQueue;
    		this.interval = interval;
    		this.oozieActionCallbackUrl = oozieActionCallbackUrl;
    	}
    	
		@Override
		public void run() {
			System.out.println("EventSender thread started");
			while (!stopSending) {
				try {
					
					if (eventQueue.size() > 0) {
						// remove all available events from the queue into an ArrayList
						ArrayList<LiteWorkflowActionEvent> eventList = new ArrayList<LiteWorkflowActionEvent>();
						LiteWorkflowActionEvent event = null;					
						while ((event = eventQueue.poll()) != null) {
							eventList.add(event);
						}
						// try to send the event list back to the Oozie server
						try {
							sendEvents(eventList);
						} catch (Exception e) {
							e.printStackTrace();
							// an error occurred - add events back to list
							// to try send them later again
							// Note: event order does not matter since the Oozie server will 
							//       sort by timestamp later on later on when serving the data with the action
							for (LiteWorkflowActionEvent ev : eventList) {
								eventQueue.add(ev);
							}
						}
					}
					Thread.sleep(interval);
				} catch (Exception e) {
					e.printStackTrace();
				}
			}
			
			// make a last attempt to send events to the Oozie server before we die
			if (eventQueue.size() > 0) {
				// remove all available events from the queue into an ArrayList
				ArrayList<LiteWorkflowActionEvent> eventList = new ArrayList<LiteWorkflowActionEvent>();
				LiteWorkflowActionEvent event = null;					
				while ((event = eventQueue.poll()) != null) {
					eventList.add(event);
				}			
				try {
					sendEvents(eventList);
				} catch (Exception e) {
					e.printStackTrace();
					String events = serializeEventListAsString(eventList);
					System.err.println("the follwing event could not be sent to the Oozie server:\n" + events);
				}
			}
			
			System.out.println("EventSender thread stopped");
		}
		
		public void stopSending() {
			stopSending = true;
		}

		private void sendEvents(ArrayList<LiteWorkflowActionEvent> eventList) throws IOException {
			HttpURLConnection con = null;
			StringBuffer errorStrBuf = null; 
			
			// get serialized events as JsonArray List
			String jsonEventList = serializeEventListAsString(eventList);
			
			System.out.println("about to send the following events: \n" + jsonEventList);
			
			// convert JSON string into a utf-8 byte array so that we can calculate the HTTP message length 
			byte[] messageContent = jsonEventList.getBytes("utf-8");
			
			char[] cBuf = new char[1024];
			int charsRead = -1;
			
			URL url = new java.net.URL(oozieActionCallbackUrl);
			
			// open a regular or encrypted (SSL) connection
			if (url.getProtocol().equalsIgnoreCase("https")) {
				con = (HttpsURLConnection)url.openConnection();
			} else {
				con = (HttpURLConnection)url.openConnection();
			}
			
			// set the HTTP method to be used
			con.setRequestMethod("POST");

			//set authorization header if user credentials are provided
			if (url.getUserInfo() != null) {
				byte[] credentials = url.getUserInfo().getBytes("utf-8");
				con.setRequestProperty("Authorization", "Basic " + org.apache.commons.codec.binary.Base64.encodeBase64(credentials));
			}
			
			// set other request properties
			con.setRequestProperty("Content-type", "application/json; charset=utf-8");
			con.setRequestProperty("Content-Length", Integer.toString(messageContent.length));
			con.setDoOutput(true);
			con.setDoInput(true);
			
			con.connect();
			
			// write request message
			OutputStream os = con.getOutputStream();
			os.write(messageContent);
			os.flush();
			os.close();
			
			// check response code
			int responseCode = con.getResponseCode();
			
			// receive response message, in case we did not get HTTP 200 as a response
			if (responseCode != HttpURLConnection.HTTP_OK) { 
				InputStream is = con.getInputStream();
				InputStreamReader isr = new InputStreamReader(is, "utf-8");
				errorStrBuf = new StringBuffer();
				// TODO: handle error message
				while ((charsRead = isr.read(cBuf)) > -1) {
					errorStrBuf.append(cBuf, 0, charsRead);
				}
				isr.close();
			}
			con.disconnect();

			// we got an unexpected HTTP response code indicating that events were not saved on the server
			if (responseCode != HttpURLConnection.HTTP_OK) {
				String errorMessage = errorStrBuf != null ? errorStrBuf.toString() : "";
				throw new IOException("unexpected HTTP response code: " + Integer.toString(responseCode) + "\n" + errorMessage);
			}
		}

		private static void serializeEventAsString(StringBuffer strBuf, LiteWorkflowActionEvent event) {
			strBuf.append("{\"type\":\"");
			writeJSONSafe(strBuf, event.getType());
			strBuf.append("\",\"actionId\":\"");
			writeJSONSafe(strBuf, event.getActionId());
			strBuf.append("\",\"timestamp\":\"");
			strBuf.append(formatDateRfc822(event.getTimestamp()));
			strBuf.append("\",\"message\":\"");
			writeJSONSafe(strBuf, event.getMessage());
			strBuf.append("\"}");
		}
		
		private static String serializeEventListAsString(ArrayList<LiteWorkflowActionEvent> eventList) {
			StringBuffer strBuf = new StringBuffer();
			boolean first = true;
			strBuf.append("[");
			for (LiteWorkflowActionEvent e : eventList) {
				if (!first) {strBuf.append(',');} else {first = false;}
				serializeEventAsString(strBuf, e);
			}
			strBuf.append("]");
			return strBuf.toString();
		}
		
	    private static void writeJSONSafe(StringBuffer buf, String value) {
	        if (value != null) {
	            int valueLength = value.length();
	            for (int i = 0; i < valueLength; i++) {
	                char c = value.charAt(i);
	                switch (c) {
	                case '\\':
	                    buf.append("\\\\");
	                    break;
	                case '"':
	                	buf.append("\\\"");
	                    break;
	                case '/':
	                	buf.append("\\/");
	                    break;
	                case 0x08:
	                	buf.append("\\b");
	                    break;
	                case 0xC:
	                	buf.append("\\f");
	                    break;
	                case 0xA:
	                	buf.append("\\n");
	                    break;
	                case 0xD:
	                	buf.append("\\r");
	                    break;
	                case 0x9:
	                	buf.append("\\t");
	                    break;
	                default:
	                	buf.append(c);
	                    break;
	                }
	            }
	        }
	    }
		
	    private static String formatDateRfc822(Date date) {
	        if (date != null) {
	            SimpleDateFormat dateFormater = new SimpleDateFormat("EEE, dd MMM yyyy HH:mm:ss 'GMT'", Locale.US);
	            dateFormater.setTimeZone(TimeZone.getTimeZone("GMT"));
	            return dateFormater.format(date);
	        }
	        return null;
	    }

    	
    }
	
	/**
	 * Note: this class should in theory implement the org.apache.oozie.client.WorkflowActionEvent interface
	 *       but since we want to be as light-weight as possible we don't want to package that interface class with 
	 *       the launcher job - therefore we just don't inherit
	 */
	public static class LiteWorkflowActionEvent {

		public static final String TYPE_OTHER = "other";
		public static final String TYPE_HADOOP_JOB_ID = "hadoop-job-id";
		
	    private String actionId = null;
	    private String type = TYPE_OTHER;
	    private String message = null;
	    private Date timestamp;
		
	    private LiteWorkflowActionEvent(String actionId) {
	    	this.actionId = actionId;
	    	this.timestamp = new java.util.Date();
	    }
	    
		public String getActionId() {
			return this.actionId;
		}

		protected void setActionId(String actionId) {
			this.actionId = actionId;
		}
		
		public String getMessage() {
			return this.message;
		}
		
		public void setMessage(String message) {
			this.message = message;
		}

		public Date getTimestamp() {
			return this.timestamp;
		}

		protected void setTimestamp(Date timestamp) {
			this.timestamp = timestamp;
		}
		
		public String getType() {
			return this.type;
		}
		
		public void setType(String type) {
			this.type = type;
		}
	}
	
}
