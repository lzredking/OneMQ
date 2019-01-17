/**
 * 
 */
package org.one.client.producer;

import java.util.List;

import org.one.remote.common.broker.MsgInfo;

/**如果没有收到返回消息，说明消息发送失败
 * @author yangkunguo
 *
 */
public interface MsgTanscationListener {

	
	/**消息消费成功回调
	 * @param _id
	 */
	public void tanscation(List<MsgInfo> _ids) ;

}
