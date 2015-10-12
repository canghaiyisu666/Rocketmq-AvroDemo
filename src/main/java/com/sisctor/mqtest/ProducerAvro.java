package com.sisctor.mqtest;

import java.io.ByteArrayOutputStream;
import java.io.IOException;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericDatumWriter;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.io.DatumWriter;
import org.apache.avro.io.Encoder;
import org.apache.avro.io.EncoderFactory;

import com.alibaba.rocketmq.client.producer.DefaultMQProducer;
import com.alibaba.rocketmq.client.producer.SendResult;
import com.alibaba.rocketmq.common.message.Message;



public class ProducerAvro {
	public static void main(String[] args) throws Exception {
		
		Schema schema = new Schema.Parser().parse("{\"type\": \"record\", \"name\": \"Test\", \"fields\" : [{\"name\": \"col1\", \"type\": \"int\"}, {\"name\": \"col2\", \"type\": \"long\"}]}");
		GenericRecord record = new GenericData.Record(schema);
		record.put("col1", 6);
		record.put("col2", 7L);
		DatumWriter<GenericRecord> writer = new GenericDatumWriter<>(schema);
		ByteArrayOutputStream out = new ByteArrayOutputStream();
		Encoder encoder = EncoderFactory.get().binaryEncoder(out, null);
		writer.write(record, encoder);//将record写到out中
		encoder.flush();//结束本次序列化操作
		
		
		DefaultMQProducer producer = new DefaultMQProducer("Producer");
		producer.setNamesrvAddr("172.16.8.103:9876");
		try {
			producer.start();

			for (int i = 0; i < 110; i++) {
				Message msg = new Message("TopicTest2", "push", (i + 1) + "", out.toByteArray());

				SendResult result = producer.send(msg);
				System.out.println("id:" + result.getMsgId() + " result:" + result.getSendStatus());
			}

		} catch (Exception e) {
			e.printStackTrace();
		} finally {
			producer.shutdown();
		}
	}
}