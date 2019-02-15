/**
 * 
 */
package org.one.broker.handler;

import org.tio.core.ChannelContext;
import org.tio.core.intf.Packet;

/**
 * @author yangkunguo
 *
 */
public interface BrokerHandler {

	
	
	public void handler(Packet packet, ChannelContext channelContext) throws Exception;
	
}
