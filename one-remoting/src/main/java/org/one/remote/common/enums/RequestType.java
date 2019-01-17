/**
 * 
 */
package org.one.remote.common.enums;

/**消息类型
 * @author yangkunguo
 *
 */
public interface RequestType {

	byte REGISTER =0;//注册
	byte BROKER=1;//Broker
	byte PRODUCER=2;//生产端
	byte CONSUMMER=3;//消费端Consummer
	byte MESSAGE=4;//消息
	byte HEART_BEAT=5;//心跳
	byte ASKOK=6;//消费确认
	byte ASKERR=7;//消费确认
	byte COMSUMER_MSG=8;//消费消息
	byte MSG_CONFIRM=9;//消息收到确认，Broker返回
	byte MSG_TANSCATION=10;//消息消费确认，Consummer返回给Broker,Broker返回发送人
}
