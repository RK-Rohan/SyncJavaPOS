/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package syncjavapos;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Statement;

/**
 *
 * @author kazi
 */
public class HO2BZ1 {
    private static String dbOrigin = "jdbc:postgresql://localhost:5432/HO";
    private static String dbDest = "jdbc:postgresql://192.168.0.109:5432/babyzone1";
    //private static String tableToMerge = "SELECT tablename from PG_TABLES WHERE SCHEMANAME='public' ";
    private static String tableToMerge = "";
    private static String tableToUpdate = "STOCKCURRENT";
    private static String lastcreated = "";
    private static String lastupdated = "";
    private static String recordId = "";
    private static String recordIdR = "";

    public static void main(String[] args) throws Exception {

         //update StockCurrent Table
    
            Connection dbConnOriginR = DriverManager.getConnection(dbDest, "postgres", "syspass");
            Statement dbOriginStatR = dbConnOriginR.createStatement();

            System.out.println("Data updating to : " + tableToUpdate + " table");
            
            String sqlSelectR = "SELECT * FROM " + tableToUpdate;
            ResultSet assetsR = dbOriginStatR.executeQuery(sqlSelectR);
            ResultSetMetaData rsMetaR = assetsR.getMetaData();
            
            Connection dbConnDestR = DriverManager.getConnection(dbOrigin, "postgres", "syspass");
            Statement dbDestStatR = dbConnDestR.createStatement();
            
            while (assetsR.next()) {

                String sqlUpdateR = "UPDATE " + tableToUpdate + " SET ";

                for (int i = 1; i <= rsMetaR.getColumnCount(); i++) {
                    String value = assetsR.getString(i);
                    
                    if (i == 2) {
                        //System.out.println(value);
                        recordIdR = "  WHERE PRODUCT='" + value + "' AND LOCATION='1'"; //for pos
                        //recordId ="  where "+ tableToMerge+"_id='"+ value +"'"; //for erp
                        System.out.println(recordIdR);
                    }
                    if (i > 3) {

                        if (assetsR.wasNull()) {
                            sqlUpdateR += rsMetaR.getColumnName(i) + "=NULL,";
                        } else {
                            sqlUpdateR += rsMetaR.getColumnName(i) + "='" + value + "',";
                        }
                    }
                }

                sqlUpdateR = sqlUpdateR.substring(0, sqlUpdateR.length() - 1) + recordIdR;
                System.out.println("sqlUpdate is : " + sqlUpdateR);
                try {
                    dbDestStatR.executeUpdate(sqlUpdateR);
                } catch (SQLException e) {
                    //TODO: attempt to update the row in the event of duplicate key
                }
            }
    // Insert Data
        Connection dbConnOrigin = DriverManager.getConnection(dbOrigin, "postgres", "syspass");
        Statement dbOriginStat = dbConnOrigin.createStatement();
        Statement dbOriginStatT = dbConnOrigin.createStatement();
        

        String tablesSqlToExecute = "SELECT * FROM privatesync.productsync order by orderby";
        ResultSet rsa100 = dbOriginStatT.executeQuery(tablesSqlToExecute);
        System.out.println("Print1 : " + tableToMerge);
        while (rsa100.next()) {
            tableToMerge = rsa100.getString("tablename");
            lastcreated = rsa100.getString("lastcreated");
            lastupdated = rsa100.getString("lastupdated");
            System.out.println("Data inserting to : " + tableToMerge + " table");

            Connection dbConnDest = DriverManager.getConnection(dbDest, "postgres", "syspass");
            Statement dbDestStat = dbConnDest.createStatement();

            String sqlToExecute = "SELECT * FROM " + tableToMerge+ " where created > '"+ lastcreated+"'";
            ResultSet assets = dbOriginStat.executeQuery(sqlToExecute);
            ResultSetMetaData rsMeta = assets.getMetaData();

            while (assets.next()) {
                String insertSQL = "INSERT INTO " + tableToMerge + " VALUES(";

                for (int i = 1; i <= rsMeta.getColumnCount(); i++) {
                    String value = assets.getString(i);
                    if (assets.wasNull()) {
                        insertSQL += "NULL,";
                    } else {
                        insertSQL += "'" + value + "',";
                    }
                }
                insertSQL = insertSQL.substring(0, insertSQL.length() - 1) + ")";
                System.out.println(insertSQL);
                try {
                    dbDestStat.executeUpdate(insertSQL);
                } catch (SQLException e) {
                    //TODO: attempt to update the row in the event of duplicate key
                }
            }
    //update StockCurrent Table
    
            Connection dbConnOriginU = DriverManager.getConnection(dbOrigin, "postgres", "syspass");
            Statement dbOriginStatU = dbConnOriginU.createStatement();

            System.out.println("Data updating to : " + tableToUpdate + " table");

            String sqlSelect = "SELECT * FROM " + tableToUpdate;
            ResultSet assetsU = dbOriginStatU.executeQuery(sqlSelect);
            ResultSetMetaData rsMetaU = assetsU.getMetaData();
            
            Connection dbConnDestU = DriverManager.getConnection(dbDest, "postgres", "syspass");
            Statement dbDestStatU = dbConnDestU.createStatement();
            
            while (assetsU.next()) {

                String sqlUpdate = "UPDATE " + tableToUpdate + " SET ";

                for (int i = 1; i <= rsMetaU.getColumnCount(); i++) {
                    String value = assetsU.getString(i);
                    
                    if (i == 2) {
                        //System.out.println(value);
                        recordId = "  WHERE PRODUCT='" + value + "' AND LOCATION='1'"; //for pos
                        //recordId ="  where "+ tableToMerge+"_id='"+ value +"'"; //for erp
                        System.out.println(recordId);
                    }
                    if (i > 3) {

                        if (assetsU.wasNull()) {
                            sqlUpdate += rsMetaU.getColumnName(i) + "=NULL,";
                        } else {
                            sqlUpdate += rsMetaU.getColumnName(i) + "='" + value + "',";
                        }
                    }
                }

                sqlUpdate = sqlUpdate.substring(0, sqlUpdate.length() - 1) + recordId;
                System.out.println("sqlUpdate is : " + sqlUpdate);
                try {
                    dbDestStatU.executeUpdate(sqlUpdate);
                } catch (SQLException e) {
                    //TODO: attempt to update the row in the event of duplicate key
                }
            }

        }

        //return;
    }
}
