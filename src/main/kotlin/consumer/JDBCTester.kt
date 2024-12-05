package com.example.consumer

import java.sql.*

class JDBCTester {
    fun test(){
        var con: Connection? = null
        var stmt: Statement? = null
        var rs: ResultSet? = null

        val url = "jdbc:postgresql://localhost:5432/postgres"
        val user = "root"
        val password = "1234"
        try {
            con = DriverManager.getConnection(url,user,password)
            stmt = con!!.createStatement()
            rs = stmt!!.executeQuery("SELECT 'postgresql is connected'")
            if (rs.next()) {
                println(rs.getString(1))
            }
        }catch (e: SQLException){
            e.printStackTrace()
        }finally {
            try {
            rs?.close()
            stmt?.close()
            con?.close()
            }catch (e: SQLException){
                e.printStackTrace()
            }
        }
    }
}

fun main() {
    JDBCTester().test()
}
