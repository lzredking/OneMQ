/**
 * 
 */
package org.one.remote.cmd;

import java.io.Serializable;
import java.util.UUID;

/**消息体
 * @author yangkunguo
 *
 */
public class OneMessage implements Serializable{

	/**
	 * 
	 */
	private static final long serialVersionUID = 9172049860087895066L;

	private String _id;//消息ID唯一
	private String topic;//消息主题
	private Object body;//消息内容
	private int readNumber;//读取次数
	private int confirm;//是否要确认收到
	private int transcation;//是否要事务
	
	public OneMessage(String _id, String topic, Object body) {
		super();
		this._id = _id;
		this.topic = topic;
		this.body = body;
	}
	
	public OneMessage() {
		super();
	}

	public OneMessage(String topic, Object body) {
		super();
		this._id=UUID.randomUUID().toString().replaceAll("-", "");
		this.topic = topic;
		this.body = body;
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
	public Object getBody() {
		return body;
	}
	public void setBody(Object body) {
		this.body = body;
	}

	public int getReadNumber() {
		return readNumber;
	}

	public void setReadNumber(int readNumber) {
		this.readNumber = readNumber;
	}


	public int getConfirm() {
		return confirm;
	}

	public void setConfirm(int confirm) {
		this.confirm = confirm;
	}


	public int getTranscation() {
		return transcation;
	}

	public void setTranscation(int transcation) {
		this.transcation = transcation;
	}

	@Override
	public String toString() {
		return "OneMessage [_id=" + _id + ", topic=" + topic + ", body=" + body + ", readNumber=" + readNumber
				+ ", confirm=" + confirm + ", transcation=" + transcation + "]";
	}
}
