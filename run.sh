#!/bin/bash
export CLASSPATH=:mysql-connector-java-8.0.16.jar:.
export L7_JDBC_URL=jdbc:mysql://db.labthreesixfive.com/gwayne?autoReconnect=true\&useSSL=false
export L7_JDBC_USER=gwayne
export L7_JDBC_PW=S19_CSC-365-012591536
javac TableGenerator.java && javac Lab7.java && java Lab7
