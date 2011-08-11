package org.apache.oozie;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.Date;

import javax.persistence.Entity;
import javax.persistence.NamedQueries;
import javax.persistence.NamedQuery;

import org.apache.hadoop.io.Writable;
import org.apache.oozie.client.rest.JsonWorkflowActionEvent;
import org.apache.oozie.util.WritableUtils;
import org.json.simple.JSONObject;


@Entity
@NamedQueries({

    @NamedQuery(name = "DELETE_EVENTS_FOR_ACTION", query = "delete from WorkflowActionEventBean w where w.actionId = :actionId"),

    @NamedQuery(name = "GET_EVENTS_FOR_ACTION", query = "select OBJECT(w) from WorkflowActionEventBean w where w.actionId = :actionId order by w.timestamp desc"),

    @NamedQuery(name = "GET_EVENT_COLUMNS", query = "select w.actionId, w.timestamp, w.type, w.message from WorkflowActionEventBean w order by w.timestamp desc"),

    @NamedQuery(name = "GET_EVENT_COUNT_FOR_ACTION", query = "select count(w) from WorkflowActionEventBean w where w.actionId = :actionId")

        })
public class WorkflowActionEventBean extends JsonWorkflowActionEvent implements Writable {

	public WorkflowActionEventBean() {
	}

	
	public WorkflowActionEventBean(JSONObject obj) {
		super(obj);
	}
	
	@Override
	public void readFields(DataInput dataInput) throws IOException {
		setActionId(WritableUtils.readStr(dataInput));
        long d = dataInput.readLong();
        if (d != -1) {
            setTimestamp(new Date(d));
        }
        setType(WritableUtils.readStr(dataInput));
		setMessage(WritableUtils.readStr(dataInput));
	}

	@Override
	public void write(DataOutput dataOutput) throws IOException {
		WritableUtils.writeStr(dataOutput, getActionId());
		dataOutput.writeLong((getTimestamp() != null) ? getTimestamp().getTime() : -1);
		WritableUtils.writeStr(dataOutput, getType());
		WritableUtils.writeStr(dataOutput, getMessage());
	}

}
