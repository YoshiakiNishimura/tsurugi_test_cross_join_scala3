package org.example
import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.{Future, ExecutionContext}
import scala.concurrent.{Await, Future, Promise}
import scala.concurrent.duration.Duration
import scala.util.{Using, Try, Success, Failure}
import scala.util.control.Exception
import com.tsurugidb.iceaxe.session.TsurugiSession
import com.tsurugidb.tsubakuro.common.{Session, SessionBuilder}
import com.tsurugidb.tsubakuro.sql.{SqlClient, Transaction}
import com.tsurugidb.tsubakuro.kvs.{KvsClient, RecordBuffer, TransactionHandle}
import com.tsurugidb.iceaxe.{TsurugiConnector}
import com.tsurugidb.iceaxe.transaction.manager.{
  TgTmSetting,
  TsurugiTransactionManager
}
import com.tsurugidb.iceaxe.transaction.option.TgTxOption
import java.net.URI
import java.io.IOException
class Setting(val tg: TgTmSetting, val name: String) {
  def getName: String = name
  def getTgTmSetting: TgTmSetting = tg
}

class Table(
    val tableName: String,
    val format: String,
    val rowCount: Int,
    val columnCount: Int
) {
  def getTableName: String = tableName
  def getFormat: String = format
  def getRowCount: Int = rowCount
  def getColumnCount: Int = columnCount

  def createRecordBuffer(id: Int): RecordBuffer = {
    val record = new RecordBuffer()
    if (rowCount == 3) {
      record.add("id", id)
      record.add("name", 1)
      record.add("note", 1)
    } else {
      record.add("id", id)
      record.add("name", 1)
    }
    record
  }
}

def dropCreate(sql: SqlClient, t: Table)(implicit
    ec: ExecutionContext
): Unit = {
  val drop = s"DROP TABLE ${t.getTableName}"
  val create = s"CREATE TABLE ${t.getTableName} ${t.getFormat}"

  println(s"drop ${t.getTableName}")
  val dropFuture = Future {
    val transaction = sql.createTransaction().await
    transaction.executeStatement(drop).await
    transaction.commit().await
    transaction.close()
  }
  dropFuture.recover { case e: Exception =>
    println(e.getMessage)
  }

  println(s"create ${t.getTableName}")
  val createFuture = Future {
    val transaction = sql.createTransaction().await
    transaction.executeStatement(create).await
    transaction.commit().await
    transaction.close()
  }
  createFuture.recover { case e: Exception =>
    println(e.getMessage)
  }
}

def insert(kvs: KvsClient, table: Table)(implicit
    ec: ExecutionContext
): Unit = {
  println(s"insert ${table.getTableName}")
  val insertFuture = Future {
    val tx = kvs.beginTransaction().await
    (0 until table.getColumnCount).foreach { i =>
      val record = table.createRecordBuffer(i)
      kvs.put(tx, table.getTableName, record).await
    }
    kvs.commit(tx).await
    tx.close()
  }
  insertFuture.recover { case e: Exception =>
    println(e.getMessage)
  }
}

def sqlExecute(session: Session, sql: SqlClient, kvs: KvsClient): Unit = {
  val columnCount = 1000
  val list = List(
    new Table(
      "test_table",
      "(id int primary key, name int, note int)",
      3,
      columnCount
    ),
    new Table("test_table_2", "(id int primary key, name int)", 2, columnCount)
  )

  val createAndInsertTime = System.nanoTime()
  list.foreach { table =>
    dropCreate(sql, table)
    insert(kvs, table)
  }
  val createAndInsertEndTime = System.nanoTime()
  println(
    s"createAndInsert ${(createAndInsertEndTime - createAndInsertTime) / 1_000_000} ms"
  )
}

def executeSelect(session: TsurugiSession, setting: Setting): Unit = {
  println(setting.getName)
  val tm = session.createTransactionManager(setting.getTgTmSetting)
  val sql = "SELECT * FROM test_table CROSS JOIN test_table_2"
  val start = System.nanoTime()
  tm.executeAndForEach(
    sql,
    _ => {
      // do nothing
    }
  )
  val end = System.nanoTime()
  println(s"executeAndForEach do nothing ${(end - start) / 1_000_000} ms")
}

def main(args: Array[String]): Unit = {
  val endpoint = URI.create("tcp://localhost:12345")
  Using.Manager { use =>
    implicit val ec: ExecutionContext = ExecutionContext.global
    val session = use(SessionBuilder.connect(endpoint).create())
    val sql = use(SqlClient.attach(session))
    val kvs = use(KvsClient.attach(session))

    sqlExecute(session, sql, kvs)
  } match {
    case Success(_)         =>
    case Failure(exception) => println(s"error : ${exception.getMessage}")
  }

  val connector = TsurugiConnector.of(endpoint)
  val list = List(
    new Setting(TgTmSetting.ofAlways(TgTxOption.ofRTX()), "RTX"),
    new Setting(TgTmSetting.ofAlways(TgTxOption.ofOCC()), "OCC"),
    new Setting(TgTmSetting.ofAlways(TgTxOption.ofLTX()), "LTX")
  )

  Using.Manager { use =>
    val session = connector.createSession()
    list.foreach { setting =>
      Try {
        executeSelect(session, setting)
      } recover {
        case e: IOException          => e.printStackTrace()
        case e: InterruptedException => e.printStackTrace()
      }
    }
  } match {
    case Success(_)         =>
    case Failure(exception) => println(s"error : ${exception.getMessage}")
  }
}
