/**
 * 
 */
package org.one.remote.common;

import java.util.List;
import java.io.Serializable;
import java.util.ArrayList;

/**消息队列列表
 * @author yangkunguo
 *
 */
public class OneQueue implements Serializable{

	/**
	 * 
	 */
	private static final long serialVersionUID = -6522223769070827703L;
	
	
	private String queueName;//队列名（topic）
	
	private List<OneQueue> queues = new ArrayList<>(10);//子队列

	private List<OneBroker> brokers = new ArrayList<>(10);//队列所在Broker服务
	
	
	public OneQueue() {
		super();
	}

	public OneQueue(String queueName) {
		super();
		this.queueName = queueName;
	}

	public String getQueueName() {
		return queueName;
	}
	
	public void setQueueName(String queueName) {
		this.queueName = queueName;
	}
	
	public List<OneQueue> getQueues() {
		return queues;
	}
	
	public void setQueues(List<OneQueue> queues) {
		this.queues = queues;
	}

	public List<OneBroker> getBrokers() {
		return brokers;
	}

	public void setBrokers(List<OneBroker> brokers) {
		this.brokers = brokers;
	}

	@Override
	public int hashCode() {
		final int prime = 31;
		int result = 1;
		result = prime * result + ((queueName == null) ? 0 : queueName.hashCode());
		return result;
	}

	@Override
	public boolean equals(Object obj) {
		if (this == obj)
			return true;
		if (obj == null)
			return false;
		if (getClass() != obj.getClass())
			return false;
		OneQueue other = (OneQueue) obj;
		if (queueName == null) {
			if (other.queueName != null)
				return false;
		} else if (!queueName.equals(other.queueName))
			return false;
		return true;
	}
	
}
