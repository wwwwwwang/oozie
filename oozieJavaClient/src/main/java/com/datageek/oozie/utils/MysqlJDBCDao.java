package com.datageek.oozie.utils;

import org.apache.log4j.Logger;

import java.io.IOException;
import java.sql.*;
import java.util.HashMap;

public class MysqlJDBCDao {
    private ResultSet res;
    private String driverName  = "";
    private String dbUrl = "";
    private String dbUser  = "";
    private String dbPassword  = "";
    private Connection conn = null;
    private Statement stmt = null;

    private static Logger log = Logger.getLogger(MysqlJDBCDao.class);

    public MysqlJDBCDao() {
        try {
            PropertiesReader.loadPropertiesByClassPath("conn.properties");
        } catch (IOException e) {
            e.printStackTrace();
        }
        driverName = "com.mysql.jdbc.Driver";
        dbUrl = PropertiesReader.getProperty("db.mysql.url");
        log.info("db.mysql.url:" + dbUrl);
        dbUser = PropertiesReader.getProperty("db.mysql.user");
        log.info("db.mysql.user:" + dbUser);
        dbPassword = PropertiesReader.getProperty("db.mysql.password");

        try {
            conn = getConn();
            stmt = conn.createStatement(ResultSet.TYPE_SCROLL_INSENSITIVE, ResultSet.CONCUR_READ_ONLY);
        } catch (ClassNotFoundException e) {
            e.printStackTrace();
        } catch (SQLException e) {
            e.printStackTrace();
        }
        log.info("one MysqlJDBCDao object has been created...");
    }

    private Connection getConn() throws ClassNotFoundException, SQLException {
        Class.forName(driverName);
        return DriverManager.getConnection(dbUrl, dbUser, dbPassword);
    }

    public void closeConn() throws SQLException {
        if(res != null)
            res.close();
        if(stmt != null)
            stmt.close();
        if(conn != null)
            conn.close();
    }

    public void save(String jobId, String appId, String appName, String timeStamp) {
        String query = " insert into applicationID(jobId, appId, appName, timestamp)"
                + " values (?, ?, ?, ?)";
        PreparedStatement preparedStmt = null;
        try {
            preparedStmt = conn.prepareStatement(query);
            preparedStmt.setString(1, jobId);
            preparedStmt.setString(2, appId);
            preparedStmt.setString(3, appName);
            preparedStmt.setString(4, timeStamp);
            preparedStmt.executeUpdate();
        } catch (SQLException e) {
            e.printStackTrace();
        }
    }

    public void saveStatus(String jobId, String status, String appName, String timeStamp) {
        String query = " insert into jobstatus(jobId, status, appName, timestamp)"
                + " values (?, ?, ?, ?)";
        PreparedStatement preparedStmt = null;
        try {
            preparedStmt = conn.prepareStatement(query);
            preparedStmt.setString(1, jobId);
            preparedStmt.setString(2, status);
            preparedStmt.setString(3, appName);
            preparedStmt.setString(4, timeStamp);
            preparedStmt.executeUpdate();
        } catch (SQLException e) {
            e.printStackTrace();
        }
    }

    public void saveSubStatus(String jobId, String ooziejobid, String oozieid, String externalid, String status,
                              String actionnumber, String createtime, String nominaltime, String timestamp) {
        String query = " insert into subjobstatus(jobId, ooziejobid, oozieid, externalid, " +
                "status, actionnumber, createtime, nominaltime, timestamp)"
                + " values (?, ?, ?, ?, ?, ?, ?, ?, ?)";
        PreparedStatement preparedStmt = null;
        try {
            preparedStmt = conn.prepareStatement(query);
            preparedStmt.setString(1, jobId);
            preparedStmt.setString(2, ooziejobid);
            preparedStmt.setString(3, oozieid);
            preparedStmt.setString(4, externalid);
            preparedStmt.setString(5, status);
            preparedStmt.setString(6, actionnumber);
            preparedStmt.setString(7, createtime);
            preparedStmt.setString(8, nominaltime);
            preparedStmt.setString(9, timestamp);
            preparedStmt.executeUpdate();
        } catch (SQLException e) {
            e.printStackTrace();
        }
    }

    public String getAppId(String jobId) throws SQLException{
        //HashMap<String, String> map = new HashMap<String, String>();
        String sql = "SELECT a.APPID from applicationID a " +
                " where a.jobId = '"+ jobId+"'";
        log.info("========sql========"+sql);
        res = stmt.executeQuery(sql);
       if (res.next()) {
            return res.getString(1).trim();
        }else{
           return "";
       }
    }
}
