package com.controller;

import java.io.IOException;

import javax.servlet.ServletException;
import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.mapred.JobConf;

import com.model.HdfsDAO;
import com.sun.security.ntlm.Server;

/**
 * Servlet implementation class DeleteFileServlet
 */
public class DeleteFileServlet extends HttpServlet {

	/**
	 * @see HttpServlet#doGet(HttpServletRequest request, HttpServletResponse
	 *      response)
	 */
	protected void doGet(HttpServletRequest request, HttpServletResponse response)
			throws ServletException, IOException {
		// ��request�л�ȡҪɾ�� ���ļ���
		String filePath = new String(request.getParameter("filePath").getBytes("ISO-8859-1"), "GB2312");
		//��ȡ����������
		JobConf conf = HdfsDAO.config();
		//��ȡ���ݽӿ�
		HdfsDAO hdfs = new HdfsDAO(conf);
		//ִ��hdfs����
		hdfs.rmr(filePath);
		System.out.println("====" + filePath + "====");
		//����request�е��ļ��б�
		FileStatus[] list = hdfs.ls("/user/root/");
		request.setAttribute("list", list);
		//������ת
		request.getRequestDispatcher("index.jsp").forward(request, response);
	}

	/**
	 * @see HttpServlet#doPost(HttpServletRequest request, HttpServletResponse
	 *      response)
	 */
	protected void doPost(HttpServletRequest request, HttpServletResponse response)
			throws ServletException, IOException {
		this.doGet(request, response);
	}

}
