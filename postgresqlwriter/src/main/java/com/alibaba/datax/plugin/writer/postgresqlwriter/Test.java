package com.alibaba.datax.plugin.writer.postgresqlwriter;

import com.google.common.collect.Lists;
import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.List;
import org.apache.commons.collections.ListUtils;
import org.apache.commons.lang3.StringUtils;

/**
 * @author nkong
 * @version V1.0
 * @date 2021/4/8
 */
public class Test {


    private static final String dbDriver = "org.postgresql.Driver";
    private static final String dbUrl = "jdbc:postgresql://139.64.0.240:3433/postgres";
    private static final String user = "hzdt_hikpg_sc001";
    private static final String password = "hik@12345+";


    public static void main(String args[]) {

        Connection conn = getConnection();

        try {

            System.out.println(conn.getMetaData().getDatabaseMajorVersion());
            System.out.println(conn.getMetaData().getDatabaseMinorVersion());
            String version = conn.getMetaData().getDatabaseProductVersion();
            System.out.println(Double.parseDouble(version) > 10.14);
            System.out.println(conn.getMetaData().getDatabaseProductVersion());

            //ResultSet catalogs = conn.getMetaData().getCatalogs();
            //while (catalogs.next()) {
            //    System.out.println(catalogs.getString(1));
            //    DatabaseMetaData metaData = conn.getMetaData();
            //    ResultSet test = metaData.getPrimaryKeys(null, null, "ods_gakx_gazhk_hjxx_czrkjbxxb");
            //
            //    while (test.next()){
            //        System.out.println("pk:" + test.getArray("column_name"));
            //        System.out.println("seq:" + test.getString("key_seq"));
            //    }
            //}

            //DatabaseMetaData metaData = connection.getMetaData();
            //ResultSet catalogs = metaData.getCatalogs();
            //while (catalogs.next()) {
            //    dataBaseName = catalogs.getString(1);
            //}
            //ResultSet primaryKeys = metaData.getPrimaryKeys(dataBaseName, null, table);
            //while (primaryKeys.next()) {
            //    primaryKeys.getString("column_name");
            //}
            //string = conn.getMetaData().getCatalogs().getString(1);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }



    private static Connection getConnection() {
        Connection conn = null;
        try {
            Class.forName(dbDriver);
            conn = DriverManager.getConnection(dbUrl, user, password);
        } catch (Exception e) {
            e.printStackTrace();
        }
        return conn;
    }

    private static void close(Connection conn, PreparedStatement ps, Statement stmt, ResultSet rs) {
        try {
            if (rs != null) { rs.close(); }
            if (ps != null) { ps.close(); }
            if (stmt != null) { stmt.close(); }
            if (conn != null) { conn.close(); }
        } catch (SQLException e) {
            e.printStackTrace();
        }
    }
}
