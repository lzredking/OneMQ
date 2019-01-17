/**
 * 
 */
package org.one.client.consumer;

import java.util.List;

import org.one.remote.cmd.OneMessage;
import org.tio.core.ChannelContext;

/**
 * @author yangkunguo
 *
 */
public interface ConsumerMsgListener {

	/**消费回调
	 * @param channelContext
	 * @param msg
	 * @return true 成功 false 失败
	 */
	public Boolean onMessage(ChannelContext channelContext,List<OneMessage> msgs) ;

}
