//package com.hnii
//
//import org.apache.hadoop.hbase.HBaseConfiguration
//import org.apache.hadoop.hbase.client.{HBaseAdmin, HTable, Result, Scan}
//import org.apache.hadoop.hbase.util.Bytes
//
//
//object HbaseDemo {
//  def main(args: Array[String]): Unit = {
//    //查询是否存在表格table1
//    println("----------------------查询表是否存在---------------------")
//    println(HbaseAPI.isExists("test2"))
//    println("---------------------列出所有表------------------------")
//
//    //列出所有表
//    HbaseAPI.listTables().foreach(println(_))
//    println("---------------------查询表数据------------------------")
//    //查询表数据
//    HbaseAPI.queryData("test2")
//    println("------------------------------------------------------")
//
//  }
//}
//
//object HbaseAPI{
//  //创建配置文件
//  val conf = HBaseConfiguration.create()
//  //获取Hbase操作类
//  val admin = new HBaseAdmin(conf)
//
//
//
//  //表格是否存在
//  def isExists(tableName:String)=admin.tableExists(tableName)
//
//  //列出表
//  def listTables()=admin.listTableNames()
//
//  //获取表数据
//  def queryData(tableName:String){
//      val scan = new Scan()
//      val htable = new HTable(conf,tableName)
//      val result= htable.getScanner(scan)
//      val it = result.iterator()
//      while(it.hasNext){
//
//          val next:Result = it.next()
//          for(kv <- next.raw()){
//             println("Row:"+new String(kv.getRow),"utf-8")
//             println("Family:"+new String(kv.getFamily,"utf-8"))
//             print("KV:"+new String(kv.getKey,"utf-8"))
//             println(":"+new String(kv.getValue,"utf-8"))
//             println("Qualifier:"+new String(kv.getQualifierArray,"utf-8"))
//          }
//      }
//  }
//
//  //获取列簇数据
//  def queryData(tableName:String,family:String): Unit ={
//      val scan = new Scan()
//
//  }
//
//
//  }
//
//
