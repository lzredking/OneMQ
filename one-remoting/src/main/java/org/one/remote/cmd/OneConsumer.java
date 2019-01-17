/**
 * 
 */
package org.one.remote.cmd;

import java.io.Serializable;

/**
 * @author yangkunguo
 *
 */
public class OneConsumer implements Serializable{

	/**
	 * 
	 */
	private static final long serialVersionUID = 1L;
	
	private String _id;//消息ID唯一
	private String topic;//消息主题
	private String tags;//消息标签
	private Integer readSize;//一次消费数量，默认为1
	
	public OneConsumer() {
		super();
	}
	
	public OneConsumer(String topic, String tags) {
		super();
		this.topic = topic;
		this.tags = tags;
	}
	public OneConsumer(String topic) {
		super();
		this.topic = topic;
	}
	public OneConsumer(String _id, String topic, String tags) {
		super();
		this._id = _id;
		this.topic = topic;
		this.tags = tags;
	}
	public String get_id() {
		return _id;
	}
	public void set_id(String _id) {
		this._id = _id;
	}
	public String getTopic() {
		return topic;
	}
	public void setTopic(String topic) {
		this.topic = topic;
	}
	public String getTags() {
		return tags;
	}
	public void setTags(String tags) {
		this.tags = tags;
	}
	
	@Override
	public String toString() {
		return "OneConsumer [_id=" + _id + ", topic=" + topic + ", tags=" + tags + ", readSize=" + readSize + "]";
	}
	@Override
	public int hashCode() {
		final int prime = 31;
		int result = 1;
		result = prime * result + ((_id == null) ? 0 : _id.hashCode());
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
		OneConsumer other = (OneConsumer) obj;
		if (_id == null) {
			if (other._id != null)
				return false;
		} else if (!_id.equals(other._id))
			return false;
		return true;
	}
	public Integer getReadSize() {
		return readSize;
	}
	public void setReadSize(Integer readSize) {
		this.readSize = readSize;
	}
	
	
}
