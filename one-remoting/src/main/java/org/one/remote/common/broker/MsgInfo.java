/**
 * 
 */
package org.one.remote.common.broker;

/**
 * @author yangkunguo
 *
 */
public class MsgInfo {

	private String id;
	private String topic;
//	private Set<String> ids=new HashSet<>(10000);
	
	public MsgInfo() {
		super();
	}

	
	public MsgInfo(String id, String topic) {
		super();
		this.id = id;
		this.topic = topic;
	}
	public String getId() {
		return id;
	}
	public void setId(String id) {
		this.id = id;
	}
	public String getTopic() {
		return topic;
	}
	public void setTopic(String topic) {
		this.topic = topic;
	}


	@Override
	public int hashCode() {
		final int prime = 31;
		int result = 1;
		result = prime * result + ((id == null) ? 0 : id.hashCode());
		result = prime * result + ((topic == null) ? 0 : topic.hashCode());
		return result;
	}


	@Override
	public boolean equals(Object obj) {
		if (this == obj) return true;
		if (obj == null) return false;
		if (getClass() != obj.getClass()) return false;
		MsgInfo other = (MsgInfo) obj;
		if (id == null) {
			if (other.id != null) return false;
		} else if (!id.equals(other.id)) return false;
		if (topic == null) {
			if (other.topic != null) return false;
		} else if (!topic.equals(other.topic)) return false;
		return true;
	}


	@Override
	public String toString() {
		return "MsgInfo [id=" + id + ", topic=" + topic + "]";
	}
	
	
}
