package org.apache.oozie.client.rest;

import java.util.ArrayList;
import java.util.Date;
import java.util.List;

import javax.persistence.Basic;
import javax.persistence.Column;
import javax.persistence.DiscriminatorColumn;
import javax.persistence.DiscriminatorType;
import javax.persistence.Entity;
import javax.persistence.Inheritance;
import javax.persistence.InheritanceType;
import javax.persistence.Table;

import org.apache.oozie.client.WorkflowActionEvent;
import org.json.simple.JSONArray;
import org.json.simple.JSONObject;

@Entity
@Table(name = "WF_ACTION_EVENTS")
@Inheritance(strategy = InheritanceType.SINGLE_TABLE)
@DiscriminatorColumn(name = "bean_type", discriminatorType = DiscriminatorType.STRING)
public class JsonWorkflowActionEvent  implements WorkflowActionEvent, JsonBean {
	
    @Basic
    @Column(name = "actionId")
    private String actionId = null;
    
    @Basic
    @Column(name = "type")
    private String type = null;

    @Basic
    @Column(name = "message")
    private String message = null;

    @Basic
    @Column(name = "timestamp")
    private Date timestamp;
    
	public JsonWorkflowActionEvent() {};
    
    public JsonWorkflowActionEvent(JSONObject jsonObject) {
    	type = (String) jsonObject.get(JsonTags.WORKFLOW_ACTION_EVENT_TYPE);
    	message = (String) jsonObject.get(JsonTags.WORKFLOW_ACTION_EVENT_MESSAGE);
    	actionId = (String) jsonObject.get(JsonTags.WORKFLOW_ACTION_EVENT_ACTION_ID);
    	timestamp = JsonUtils.parseDateRfc822((String)jsonObject.get(JsonTags.WORKFLOW_ACTION_EVENT_TIMESTAMP));
    }

	@Override
	@SuppressWarnings("unchecked")
	public JSONObject toJSONObject() {
        JSONObject json = new JSONObject();
        json.put(JsonTags.WORKFLOW_ACTION_EVENT_TYPE, type);
        json.put(JsonTags.WORKFLOW_ACTION_EVENT_MESSAGE, message);
        json.put(JsonTags.WORKFLOW_ACTION_EVENT_ACTION_ID, actionId);
        json.put(JsonTags.WORKFLOW_ACTION_EVENT_TIMESTAMP, JsonUtils.formatDateRfc822(timestamp));
		return json;
	}
    
	@Override
	public String getActionId() {
		return actionId;
	}
	
	public void setActionId(String actionId) {
		this.actionId = actionId;
	}

	@Override
	public String getMessage() {
		return message;
	}

	public void setMessage(String message) {
		this.message = message;
	}
	
	@Override
	public Date getTimestamp() {
		return timestamp;
	}

	public void setTimestamp(Date timestamp) {
		this.timestamp = timestamp;
	}

	@Override
	public String getType() {
		return type;
	}

	public void setType(String type) {
		this.type = type;
	}

	
    @SuppressWarnings("unchecked")
    public static JSONArray toJSONArray(List<? extends JsonWorkflowActionEvent> events) {
        JSONArray array = new JSONArray();
        if (events != null) {
            for (JsonWorkflowActionEvent node : events) {
                array.add(node.toJSONObject());
            }
        }
        return array;
    }
	
    /**
     * Convert a JSONArray into a nodes list.
     *
     * @param array JSON array.
     * @return the corresponding nodes list.
     */
    @SuppressWarnings("unchecked")
    public static List<JsonWorkflowActionEvent> fromJSONArray(JSONArray array) {
        List<JsonWorkflowActionEvent> list = new ArrayList<JsonWorkflowActionEvent>();
        for (Object obj : array) {
            list.add(new JsonWorkflowActionEvent((JSONObject) obj));
        }
        return list;
    }


}
