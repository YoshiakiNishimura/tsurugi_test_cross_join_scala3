package org.example
import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.{Await, ExecutionContext, Promise}
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
// private val Connect ="tcp://localhost:12345"
private val Connect = "ipc://tsurugi"
private val TableName = ("test_table", "test_table_2")
private val ColumnList = List(1_000, 2_000, 3_000)
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

  println(s"${drop}")
  Try {
    val transaction = sql.createTransaction().await
    transaction.executeStatement(drop).await
    transaction.commit().await
    transaction.close()
  } recover { case e: Exception =>
    println(e.getMessage)
  }

  println(s"${create}")
  Try {
    val transaction = sql.createTransaction().await
    transaction.executeStatement(create).await
    transaction.commit().await
    transaction.close()
  } recover { case e: Exception =>
    println(e.getMessage)
  }
}

def insert(kvs: KvsClient, table: Table)(implicit
    ec: ExecutionContext
): Unit = {
  println(s"insert ${table.getTableName} column ${table.getColumnCount}")
  Try {
    val tx = kvs.beginTransaction().await
    (0 until table.getColumnCount).foreach { i =>
      val record = table.createRecordBuffer(i)
      kvs.put(tx, table.getTableName, record).await
    }
    kvs.commit(tx).await
    tx.close()
  } recover { case e: Exception =>
    println(e.getMessage)
  }
}

def sqlExecute(
    session: Session,
    sql: SqlClient,
    kvs: KvsClient,
    table: Table
): Unit = {
  val createAndInsertTime = System.nanoTime()
  dropCreate(sql, table)
  insert(kvs, table)
  val createAndInsertEndTime = System.nanoTime()
  println(
    s"createAndInsert ${(createAndInsertEndTime - createAndInsertTime) / 1_000_000} ms"
  )
}

def executeSelect(session: TsurugiSession, setting: Setting): Unit = {
  println(setting.getName)
  val tm = session.createTransactionManager(setting.getTgTmSetting)
  val sql = s"SELECT * FROM ${TableName(0)} CROSS JOIN ${TableName(1)}"
  println(s"${sql}")
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
  val endpoint = URI.create(Connect)
  ColumnList.foreach { columncount =>
    val list_table = List(
      new Table(
        TableName(0),
        "(id int primary key, name int, note int)",
        3,
        columncount
      ),
      new Table(TableName(1), "(id int primary key, name int)", 2, columncount)
    )
    Using.Manager { use =>
      implicit val ec: ExecutionContext = ExecutionContext.global
      val session = use(SessionBuilder.connect(endpoint).create())
      val sql = use(SqlClient.attach(session))
      val kvs = use(KvsClient.attach(session))
      list_table.foreach { table =>
        sqlExecute(session, sql, kvs, table)
      }
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

}
