package com.hnii

import org.apache.hadoop.hdfs.DFSUtil
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.jdbc.{JdbcDialect, JdbcDialects, JdbcType}
import org.apache.spark.sql.types._

object Employee {
  def main(args: Array[String]): Unit = {

    //重写数据类型
//    val OracleDialect = new JdbcDialect {
//      override def canHandle(url: String): Boolean = url.startsWith("jdbc:oracle") || url.contains("oracle")
//
//      override def getJDBCType(dt: DataType): Option[JdbcType] = dt match {
//        case StringType => Some(JdbcType("VARCHAR2(255)", java.sql.Types.VARCHAR))
//        case BooleanType => Some(JdbcType("NUMBER(1)", java.sql.Types.NUMERIC))
//        case IntegerType => Some(JdbcType("NUMBER(16)", java.sql.Types.NUMERIC))
//        case LongType => Some(JdbcType("NUMBER(16)", java.sql.Types.NUMERIC))
//        case DoubleType => Some(JdbcType("NUMBER(16,4)", java.sql.Types.NUMERIC))
//        case FloatType => Some(JdbcType("NUMBER(16,4)", java.sql.Types.NUMERIC))
//        case ShortType => Some(JdbcType("NUMBER(5)", java.sql.Types.NUMERIC))
//        case ByteType => Some(JdbcType("NUMBER(3)", java.sql.Types.NUMERIC))
//        case BinaryType => Some(JdbcType("BLOB", java.sql.Types.BLOB))
//        case TimestampType => Some(JdbcType("DATE", java.sql.Types.DATE))
//        case DateType => Some(JdbcType("DATE", java.sql.Types.DATE))
//        //        case DecimalType => Some(JdbcType("NUMBER(38,4)", java.sql.Types.NUMERIC))
//        case _ => None
//      }
//
//    }
//    //注册数据类型转换驱动
//    JdbcDialects.registerDialect(OracleDialect)

    //开启Cross Join (笛卡儿积)配置，
    val sparkConf = new SparkConf()
    sparkConf.set("spark.sql.crossJoin.enabled","true")
//    sparkConf.setMaster("spark://192.168.139.101:7077")
    sparkConf.setMaster("local")
    //创建SParkSession
    val ss = SparkSession.builder()
      .appName("Employees Application")
      .config(sparkConf)
      .enableHiveSupport()
      .getOrCreate()

    //数据库地址
//    val URL = "jdbc:oracle:thin:@localhost:1521:orcl"
      val URL = "jdbc:oracle:thin:@192.168.11.9:1521:scpt"

    //配置参数
    val prop = new java.util.Properties()
    prop.put("user", "scott")
    prop.put("password", "tiger")
    prop.put("Driver", "jdbc.oracle.OracleDriver")

    //获取DataFrame，创建SparkSQL表
//    val EMP = ss.read.jdbc(URL, "EMP", prop)
//    EMP.createOrReplaceTempView("emp")
//    val DEPT = ss.read.jdbc(URL, "DEPT", prop)
//    DEPT.createOrReplaceTempView("dept")
//    val SAL = ss.read.jdbc(URL, "SALGRADE", prop)
//    SAL.createOrReplaceTempView("sal_grade")

    //执行计算逻辑
    //求出每个员工的领导、所在部门、以及工资水平等级
//    ss.sql(
//      """
//        |SELECT
//        |e1.EMPNO id,e1.ENAME name,e1.JOB job,e2.ENAME manager,e1.HIREDATE hire_date,
//        |(e1.SAL+(case when isnull(e1.COMM) then 0 else e1.COMM end)) as salary,
//        |d.DNAME department,d.LOC location,cast(s.GRADE as int)
//        |FROM
//        |emp e1
//        |LEFT JOIN
//        |emp e2 ON e1.MGR=e2.EMPNO
//        |LEFT JOIN
//        |dept d ON e1.DEPTNO=d.DEPTNO
//        |LEFT JOIN
//        |sal_grade s ON
//        |(e1.SAL+(case when isnull(e1.COMM) then 0 else e1.COMM end)) between s.LOSAL and s.HISAL
//      """.stripMargin).repartition(1)
////      .write.jdbc(URL,"RESULT",prop)
//      .show()


      ss.sql("show tables").show()
  }
}