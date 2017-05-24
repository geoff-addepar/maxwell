package com.zendesk.maxwell.producer;

import com.zendesk.maxwell.MaxwellContext;
import com.zendesk.maxwell.row.RowMap;
import com.zendesk.maxwell.util.StoppableTask;

public abstract class AbstractProducer {
	protected MaxwellContext context;
	protected MaxwellOutputConfig outputConfig;

	public void setContext(MaxwellContext context) {
		this.context = context;
		this.outputConfig = context.getConfig().outputConfig;
	}

	abstract public void push(RowMap r) throws Exception;

	public StoppableTask getStoppableTask() {
		return null;
	}
}
