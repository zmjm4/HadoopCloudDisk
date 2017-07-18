package com.controller;

import java.io.IOException;

import javax.servlet.ServletException;
import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.mapred.JobConf;

import com.model.HdfsDAO;

/**
 * Servlet implementation class DocumentServlet
 */
public class DocumentServlet extends HttpServlet {
	private static final long serialVersionUID = 1L;
       
 

	/**
	 * @see HttpServlet#doGet(HttpServletRequest request, HttpServletResponse response)
	 */
	protected void doGet(HttpServletRequest request, HttpServletResponse response) throws ServletException, IOException {
		 String filePath = new String(request.getParameter("filePath").getBytes("ISO-8859-1"),"GB2312");
		 JobConf conf = HdfsDAO.config();
	     HdfsDAO hdfs = new HdfsDAO(conf);
	     FileStatus[] documentList = hdfs.ls(filePath);
	     request.setAttribute("documentList",documentList);
		 request.getRequestDispatcher("document.jsp").forward(request,response);
	}

	/**
	 * @see HttpServlet#doPost(HttpServletRequest request, HttpServletResponse response)
	 */
	protected void doPost(HttpServletRequest request, HttpServletResponse response) throws ServletException, IOException {
		this.doGet(request, response);
	}

}
