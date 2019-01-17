/**
 * 
 */
package org.one.remote.common;

/**客户端对象
 * @author yangkunguo
 *
 */
public class OneClient {

	private String clientName;
	private String clientUrl;//地址
	private String queueName;//绑定队列
	private String tags;//标签
	
	
	public String getClientName() {
		return clientName;
	}
	public void setClientName(String clientName) {
		this.clientName = clientName;
	}
	public String getClientUrl() {
		return clientUrl;
	}
	public void setClientUrl(String clientUrl) {
		this.clientUrl = clientUrl;
	}
	public String getQueueName() {
		return queueName;
	}
	public void setQueueName(String queueName) {
		this.queueName = queueName;
	}
	public String getTags() {
		return tags;
	}
	public void setTags(String tags) {
		this.tags = tags;
	}
	@Override
	public String toString() {
		return "OneClient [clientName=" + clientName + ", clientUrl=" + clientUrl + ", queueName=" + queueName
				+ ", tags=" + tags + "]";
	}
	@Override
	public int hashCode() {
		final int prime = 31;
		int result = 1;
		result = prime * result + ((clientName == null) ? 0 : clientName.hashCode());
		result = prime * result + ((clientUrl == null) ? 0 : clientUrl.hashCode());
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
		OneClient other = (OneClient) obj;
		if (clientName == null) {
			if (other.clientName != null)
				return false;
		} else if (!clientName.equals(other.clientName))
			return false;
		if (clientUrl == null) {
			if (other.clientUrl != null)
				return false;
		} else if (!clientUrl.equals(other.clientUrl))
			return false;
		if (queueName == null) {
			if (other.queueName != null)
				return false;
		} else if (!queueName.equals(other.queueName))
			return false;
		return true;
	}
	
	
}
