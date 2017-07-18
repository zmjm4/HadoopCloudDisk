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
		// 从request中获取要删除 的文件名
		String filePath = new String(request.getParameter("filePath").getBytes("ISO-8859-1"), "GB2312");
		//获取连接配配置
		JobConf conf = HdfsDAO.config();
		//获取数据接口
		HdfsDAO hdfs = new HdfsDAO(conf);
		//执行hdfs命令
		hdfs.rmr(filePath);
		System.out.println("====" + filePath + "====");
		//更新request中的文件列表
		FileStatus[] list = hdfs.ls("/user/root/");
		request.setAttribute("list", list);
		//重新跳转
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
