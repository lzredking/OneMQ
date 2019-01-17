/**
 * 
 */
package org.one.remote.common;

/**Broker信息类，同组的消息保持相同。一个队列只在一个组中注册
 * @author yangkunguo
 *
 */
public class OneBroker {

	private String groupName;//同组的Broker,groupName相同，一组2个Broker,最多3个。
	private String brokerName;
	private String role;//MASTER,SLAVE
	private String brokerUrl;//borker地址
//	private String flushDiskType;//ASYNC_FLUSH,SYNC_FLUSH
	private String queueName;//
	private Integer isLive;//1表示存活 0表示下线
	private long lastTime;//最后上线时间
	private boolean isMaster;
	
	public OneBroker() {}
	
	/**
	 * @param groupName 组名
	 * @param brokerName 
	 * @param role MASTER,SLAVE
	 * @param brokerUrl
	 * @param isLive 1表示存活 0表示下线
	 */
	public OneBroker(String groupName,String brokerName, String role, String brokerUrl,Integer isLive) {
		super();
		this.groupName=groupName;
		this.brokerName = brokerName;
		this.role = role;
		this.brokerUrl = brokerUrl;
		this.isLive=isLive;
	}
	
	public String getBrokerName() {
		return brokerName;
	}

	public void setBrokerName(String brokerName) {
		this.brokerName = brokerName;
	}
	public boolean isMaster() {
		return role.equals("MASTER")?true:false;
	}
//	public void setMaster(boolean isMaster) {
//		this.isMaster = isMaster;
//	}
	public String getRole() {
		return role;
	}
	public void setRole(String role) {
		this.role = role;
	}
	public String getBrokerUrl() {
		return brokerUrl;
	}
	public void setBrokerUrl(String brokerUrl) {
		this.brokerUrl = brokerUrl;
	}
//	public String getFlushDiskType() {
//		return flushDiskType;
//	}
//	public void setFlushDiskType(String flushDiskType) {
//		this.flushDiskType = flushDiskType;
//	}
	

	public String getQueueName() {
		return queueName;
	}

	public void setQueueName(String queueName) {
		this.queueName = queueName;
	}

	public Integer getIsLive() {
		return isLive;
	}

	public void setIsLive(Integer isLive) {
		this.isLive = isLive;
	}

	public long getLastTime() {
		return lastTime;
	}

	public void setLastTime(long lastTime) {
		this.lastTime = lastTime;
	}

	public String getGroupName() {
		return groupName;
	}

	public void setGroupName(String groupName) {
		this.groupName = groupName;
	}

	@Override
	public int hashCode() {
		final int prime = 31;
		int result = 1;
		result = prime * result + ((brokerName == null) ? 0 : brokerName.hashCode());
		result = prime * result + ((groupName == null) ? 0 : groupName.hashCode());
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
		OneBroker other = (OneBroker) obj;
		if (brokerName == null) {
			if (other.brokerName != null)
				return false;
		} else if (!brokerName.equals(other.brokerName))
			return false;
		if (groupName == null) {
			if (other.groupName != null)
				return false;
		} else if (!groupName.equals(other.groupName))
			return false;
		return true;
	}

	@Override
	public String toString() {
		return "OneBroker [groupName=" + groupName + ", brokerName=" + brokerName + ", role=" + role + ", brokerUrl="
				+ brokerUrl + ", queueName=" + queueName + ", isLive=" + isLive + ", lastTime=" + lastTime + "]";
	}

	
}
