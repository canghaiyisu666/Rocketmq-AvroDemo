package com.sisctor.mqtest;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericArray;
import org.apache.avro.generic.GenericDatumReader;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.io.DatumReader;
import org.apache.avro.io.Decoder;
import org.apache.avro.io.DecoderFactory;

import com.alibaba.rocketmq.client.consumer.DefaultMQPushConsumer;
import com.alibaba.rocketmq.client.consumer.listener.ConsumeConcurrentlyContext;
import com.alibaba.rocketmq.client.consumer.listener.ConsumeConcurrentlyStatus;
import com.alibaba.rocketmq.client.consumer.listener.MessageListenerConcurrently;
import com.alibaba.rocketmq.common.consumer.ConsumeFromWhere;
import com.alibaba.rocketmq.common.message.Message;
import com.alibaba.rocketmq.common.message.MessageExt;

public class ConsumerAvro {
	private transient static DatumReader reader;

	public static void main(String[] args) {
		if (reader == null) {
			Schema schema = new Schema.Parser()
					.parse("{\"type\": \"record\", \"name\": \"Test\", \"fields\" : [{\"name\": \"col1\", \"type\": \"int\"}, {\"name\": \"col2\", \"type\": \"long\"}]}");
			reader = new GenericDatumReader<GenericRecord>(schema);
		}

		DefaultMQPushConsumer consumer = new DefaultMQPushConsumer(
				"PushConsumer");
		consumer.setNamesrvAddr("172.16.8.103:9876");
		try {
			// 订阅TopicTest2下Tag为*的消息
			consumer.subscribe("TopicTest2", "*");
			// 程序第一次启动从消息队列头取数据
			consumer.setConsumeFromWhere(ConsumeFromWhere.CONSUME_FROM_FIRST_OFFSET);
			consumer.registerMessageListener(new MessageListenerConcurrently() {
				public ConsumeConcurrentlyStatus consumeMessage(
						List<MessageExt> list,
						ConsumeConcurrentlyContext Context) {
					Message msg = list.get(0);
					System.out.println("=======");
					System.out.println(msg.toString());

					Decoder decoder = DecoderFactory.get().binaryDecoder(
							msg.getBody(), null);

					try {
						GenericRecord record = (GenericRecord) reader.read(
								null, decoder);
						System.out.println(record.get(0));
						System.out.println(record.get(1));
					} catch (IOException e) {
						// TODO Auto-generated catch block
						e.printStackTrace();
					}

					return ConsumeConcurrentlyStatus.CONSUME_SUCCESS;
				}
			});
			consumer.start();
		} catch (Exception e) {
			e.printStackTrace();
		}

	}
}
