# OneMQ
用tio开发的试验版本消息队列，只是想验证Tio能不能做这方面的应用。
目前纯内存版，有消息发送确认，消息消费成功确认。
感觉Tio用在这方面不是很稳定，消息发送和接受不实时。

参照Rocketmq的理念，但是消息框架不同实现差异大。
register接受Broker注册
producer和consumer到register拉取Broker信息
Broker负责接收到发送消息。

