package com.qst.codec;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.io.compress.BZip2Codec;
import org.apache.hadoop.io.compress.CompressionCodec;
import org.apache.hadoop.io.compress.CompressionInputStream;
import org.apache.hadoop.io.compress.CompressionOutputStream;
import org.apache.hadoop.io.compress.Decompressor;
import org.apache.hadoop.io.compress.DefaultCodec;
import org.apache.hadoop.io.compress.DeflateCodec;
import org.apache.hadoop.io.compress.GzipCodec;
import org.apache.hadoop.io.compress.Lz4Codec;
import org.apache.hadoop.io.compress.SnappyCodec;
import org.apache.hadoop.util.ReflectionUtils;

import com.hadoop.compression.lzo.LzoCodec;
import com.hadoop.compression.lzo.LzoIndexer;


public class TestCodec {
	
	/**
	 * 综合测试压缩编解码器
	 */
	@SuppressWarnings("rawtypes")
	public static void main(String[] args) {
		
		if(args.length < 0 ||  args == null){
			
			System.out.println("need path");
			System.exit(-1);
			
		}
		
		Class[] clazzes = {
				DefaultCodec.class,
				DeflateCodec.class,
				GzipCodec.class,
				BZip2Codec.class,
				Lz4Codec.class,
				LzoCodec.class,
				SnappyCodec.class
		};
		
		String path = args[0];
		
		for(Class clazz : clazzes){
			new TestCodec().testCompress(clazz, path);
			new TestCodec().testDecompress(clazz, path);
		}
		
		
	}
	
	/**
	 * 测试codec的压缩
	 */
	@SuppressWarnings({ "unchecked", "rawtypes" })
	public void testCompress(Class clazz, String path) {
		
		try {
			//配置文件
			Configuration conf = new Configuration();
			//通过hadoop反射类实例化对象，并使用多态取得CompressionCodec
			CompressionCodec codec = (CompressionCodec)ReflectionUtils.newInstance(clazz, conf);
			//取得默认扩展名(.gz)
			String ext = codec.getDefaultExtension();
			
			long start= System.currentTimeMillis();
			
			
			//设置输出流
			FileOutputStream fos = new FileOutputStream(path + ext);
			//使用createOutputStream包装输出流
			CompressionOutputStream cos = codec.createOutputStream(fos);
			//设置输入流
			FileInputStream fis = new FileInputStream(path);
			//开始写入
			IOUtils.copyBytes(fis, cos, 1024);
			
			fis.close();
			
			cos.close();
			
			System.out.print("文件扩展名："+ext+ "\t");
			
			//打印压缩时间
			System.out.print("压缩时间：" + (System.currentTimeMillis() - start) + "\t");
			
		} catch (Exception e) {
			e.printStackTrace();
		}
	}
	
	/**
	 * 测试codec解压
	 */
	@SuppressWarnings({ "unchecked", "rawtypes" })
	public void testDecompress(Class clazz, String path) {
		
		try {
			//配置文件
			Configuration conf = new Configuration();
			//通过hadoop反射类实例化对象，并使用多态取得CompressionCodec
			CompressionCodec codec = (CompressionCodec)ReflectionUtils.newInstance(clazz, conf);
			//取得默认扩展名(.gz)
			String ext = codec.getDefaultExtension();
			//得到解压器
			Decompressor decompressor = codec.createDecompressor();
			
			long start= System.currentTimeMillis();
			
			//得到输入流
			CompressionInputStream cis =  codec.createInputStream(new FileInputStream(path+ext), decompressor);
			//创建文件输出流
			FileOutputStream fos = new FileOutputStream(path);
			
			//开始写入
			IOUtils.copyBytes(cis, fos, 1024);
			
			cis.close();
			
			fos.close();
			
			//打印解压时间
			System.out.print("解压时间：" + (System.currentTimeMillis() - start) + "\t");
			
			File f = new File(path+ext);
			System.out.println("压缩后大小："+ f.length());
			
		} catch (Exception e) {
			e.printStackTrace();
		}
		
	}

}
