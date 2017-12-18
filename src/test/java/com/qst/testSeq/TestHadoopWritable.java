package com.qst.testSeq;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;

import org.apache.hadoop.io.ByteWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.junit.Test;

public class TestHadoopWritable {

	
	/**
	 * 进行java的串行化操作100
	 * @throws Exception 
	 */
	@Test
	public void testJavaSerial() throws Exception{
		Integer i = new Integer(100);
		
		ByteArrayOutputStream baos = new ByteArrayOutputStream();
		
		ObjectOutputStream oos = new ObjectOutputStream(baos);
		
		oos.writeObject(i);
		
		//打印出串行化后的结果
		System.out.println(baos.toString());
		//打印出串行化之后的长度
		System.out.println(baos.toByteArray().length);
		
		
		//测试java反串行化
		ObjectInputStream ois = new ObjectInputStream(new ByteArrayInputStream(baos.toByteArray()));
		Integer i2 = (Integer) ois.readObject();
		System.out.println(i2);
	}
	
	/**
	 * 进行hadoop的串行化操作
	 * @throws Exception 
	 */
	@Test
	public void testHadoopSerial() throws Exception{
		
		IntWritable iw = new IntWritable(100);
		
		ByteArrayOutputStream baos = new ByteArrayOutputStream();
		
		DataOutputStream dos = new DataOutputStream(baos);
		
		iw.write(dos);
		
		//打印出串行化后的结果
		System.out.println(baos.toString());
		//打印出串行化之后的长度
		System.out.println(baos.toByteArray().length);
		
		//测试hadoop反串行化
		DataInputStream dis = new DataInputStream(new ByteArrayInputStream(baos.toByteArray()));
		iw.readFields(dis);
		System.out.println(iw.get());
		
		
	}
	
	
	
	
	
	
	
	
	
	
	
}
