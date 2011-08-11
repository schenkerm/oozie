package org.apache.oozie.command.wf;

import java.util.ArrayList;

import org.apache.oozie.WorkflowActionEventBean;
import org.apache.oozie.command.CommandException;
import org.apache.oozie.store.StoreException;
import org.apache.oozie.store.WorkflowStore;

/**
 * Command to persist a list of action events 
 *
 */
public class ActionEventListCommand extends WorkflowCommand<Void> {

	private ArrayList<WorkflowActionEventBean> eventList;
	
	public ActionEventListCommand(ArrayList<WorkflowActionEventBean> eventList, int priority) {
		super("action.event", "action.event", priority, 0);
		this.eventList = eventList;
	}

	@Override
	protected Void call(WorkflowStore store) throws StoreException, CommandException {
			store.insertActionEventList(eventList);
		return null;
	}

}
