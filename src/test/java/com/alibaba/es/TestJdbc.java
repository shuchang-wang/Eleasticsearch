package com.alibaba.es;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.Statement;

//es sql功能
public class TestJdbc {

    public static void main(String[] args) {
        try {
            //1.创建连接
            Connection connection = DriverManager.getConnection("jdbc:es://http://localhost:9200");
            //2.获取statement
            Statement statement = connection.createStatement();
            //3.执行SQL语句
            ResultSet results = statement.executeQuery("select * from tvs");
            //4.获取结果
            while (results.next()) {
                System.out.println(results.getString(1));
                System.out.println(results.getString(2));
                System.out.println(results.getString(3));
                System.out.println(results.getString(4));
                System.out.println("============================");
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}