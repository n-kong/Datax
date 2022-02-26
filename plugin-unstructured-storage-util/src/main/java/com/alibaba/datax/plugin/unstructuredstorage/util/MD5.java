package com.alibaba.datax.plugin.unstructuredstorage.util;

import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class MD5 {

	private static final String MD5 = "MD5";
	private static final String ENCODING = "UTF-8";
	private static final Logger log = LoggerFactory.getLogger(MessageDigest.class);

	private final static String[] strDigits = { "0", "1", "2", "3", "4", "5",
			"6", "7", "8", "9", "a", "b", "c", "d", "e", "f" };

	public static void main(String[] args) {
		String md5 = encrypt2("330104201700095913");
		String md52 = encrypt("330104201700095913");
		System.out.println(md5);
		System.out.println(md52);
	}
	// return Hexadecimal
	private static String byteToArrayString(byte bByte) {
		int iRet = bByte;
		if (iRet < 0) {
			iRet += 256;
		}
		int iD1 = iRet / 16;
		int iD2 = iRet % 16;
		return strDigits[iD1] + strDigits[iD2];
	}

	// 转换字节数组�?16进制字串
	private static String byteToString(byte[] bByte) {
		StringBuffer sBuffer = new StringBuffer();
		for (int i = 0; i < bByte.length; i++) {
			sBuffer.append(byteToArrayString(bByte[i]));
		}
		//log.info("16进制转换传入"+bByte.toString()+"输出"+sBuffer.toString().toUpperCase());
		return sBuffer.toString().toUpperCase();
	}

	public static String encrypt(String strObj) {
		//log.info("MD5传入值"+strObj);
		String resultString = "";
		if(strObj == null){
			strObj = "";
		}
		resultString = new String(strObj);
		try {
			MessageDigest md = MessageDigest.getInstance("MD5");
			// md.digest() 该函数返回�?�为存放哈希值结果的byte数组
			resultString = byteToString(md.digest(strObj.getBytes()));
			//log.info("MD5被计算值"+strObj+"结果"+resultString+"初次计算结果"+md.digest(strObj.getBytes()).toString());
		} catch (NoSuchAlgorithmException ex) {
			ex.printStackTrace();
		}
		return resultString;
	}

	/**
	 *
	 * add by jji
	 *
	 * 计算MD5�?
	 *
	 * @param str
	 * 				待计算MD5值的字符�?
	 *
	 * @return String
	 * 				MD5值字符串
	 *
	 */
	public static String encrypt2(String str) {

		if (str == null || str.length() == 0) {
			return "";
		}


		byte[] bytes = null;

		try {
			bytes = MessageDigest.getInstance(MD5).digest(str.getBytes(ENCODING));
		} catch (Exception e) {
			return "";
		}

		if (bytes == null || bytes.length == 0) {
			return "";
		}

		StringBuilder sb = new StringBuilder();
		for (byte b : bytes) {

			int val = b;
			if (val < 0) {
				val += 256;
			}
			if (val < 16) {
				sb.append("0");
			}
			sb.append(Integer.toHexString(val));
		}

		return sb.toString();

	}
}