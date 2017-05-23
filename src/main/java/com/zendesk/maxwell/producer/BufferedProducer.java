package com.zendesk.maxwell.producer;

import com.zendesk.maxwell.row.RowMap;

import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;

public class BufferedProducer extends AbstractProducer {
	private final LinkedBlockingQueue<RowMap> queue;

	public BufferedProducer(int maxSize) {
		this.queue = new LinkedBlockingQueue<>(maxSize);
	}

	@Override
	public void push(RowMap r) throws Exception {
		try {
			this.queue.put(r);
		} catch ( InterruptedException e ) {}
	}

	public RowMap poll(long timeout, TimeUnit unit) throws InterruptedException {
		RowMap r = this.queue.poll(timeout, unit);
		if (r != null) {
			this.context.setPosition(r);
		}
		return r;
	}
}
