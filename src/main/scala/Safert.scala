import org.apache.flink.api.common.typeinfo.{TypeInformation, Types}
import org.apache.flink.api.java.io.jdbc.JDBCAppendTableSink
import org.apache.flink.api.scala._
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.table.api.Table
import org.apache.flink.table.api.Types._
import org.apache.flink.table.api.scala.{StreamTableEnvironment, _}
import org.apache.flink.table.descriptors.{Json, Kafka, Rowtime, Schema}
import org.apache.flink.types.Row

object Safert {
  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    // env.setParallelism(1)
    env.enableCheckpointing(18000)
    //      val backend = new FsStateBackend("hdfs://ns1/tmp/flink_stats/", false)
    //      val backend = new FsStateBackend("file:///c:/tmp/", true)
    //      env.setStateBackend(backend.asInstanceOf[StateBackend])
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)
    val tableEnv = StreamTableEnvironment.create(env)
    val tableEnv2 = tableEnv.connect(
      new Kafka()
        .version("0.11")
        .topic("CepIhrTopic")
        // .startFromEarliest()
        //   .property("bootstrap.servers", "192.168.217.132:9092").
        .property("bootstrap.servers", "192.168.217.113:9092, 192.168.217.114:9092, 192.168.217.115:9092").
        startFromLatest()
    ).withFormat(
      new Json().schema(ROW(Array[String]("createdate", "importid", "ip", "logid", "moduledesc", "moduleid", "modulename", "moduletype", "orgid", "url", "userid", "username", "usertype")
        , Array[TypeInformation[_]](SQL_TIMESTAMP, INT, STRING, LONG, STRING, INT, STRING, INT, LONG, STRING, LONG, STRING, INT)))
        .failOnMissingField(false)

        .jsonSchema(
          "{" +
            "  type: 'object'," +
            "  properties: {" +
            "    createdate: {" +
            "      type: 'number'," +
            "      format: 'date-time'" +
            "    }," +
            "    importid: {" +
            "      type: 'integer'" +
            "    }" +
            "    ip: {" +
            "      type: 'string'," +

            "    }" +
            "    logid: {" +
            "      type: 'number'," +

            "    }" +
            "    moduledesc: {" +
            "      type: 'string'," +

            "    }" +
            "    moduleid: {" +
            "      type: 'number'," +

            "    }" +
            "    modulename: {" +
            "      type: 'string'," +

            "    }" +
            "    moduletype: {" +
            "      type: 'number'," +

            "    }" +
            "    orgid: {" +
            "      type: 'number'," +

            "    }" +
            "    url: {" +
            "      type: 'string'," +

            "    }" +
            "    userid: {" +
            "      type: 'number'," +

            "    }" +
            "    username: {" +
            "      type: 'string'," +

            "    }" +
            "    usertype: {" +
            "      type: 'string'," +

            "    }" +
            "  }" +
            "}"
        ).deriveSchema()


    ).withSchema(new Schema()
      .field("createdate1", Types.SQL_TIMESTAMP).rowtime(new Rowtime()
      .timestampsFromField("createdate")
      .watermarksPeriodicBounded(600000)
    ).field("importid", Types.INT)
      .field("ip", Types.STRING)
      .field("logid", Types.LONG)
      .field("moduledesc", Types.STRING)


      .field("moduleid", Types.INT)
      .field("modulename", Types.STRING)
      .field("moduletype", Types.INT)
      .field("orgid", Types.LONG)
      .field("url", Types.STRING)
      .field("userid", Types.LONG).field("username", Types.STRING).field("usertype", Types.INT)
    )
      .inAppendMode()
      .registerTableSource("Ticker")


    // val table: Table =  tableEnv.sqlQuery("select TUMBLE_START(createdate1, INTERVAL '1' MINUTE) AS tumble_start ,count(*) AS amt  from Ticker where modulename = '注册'  GROUP BY  TUMBLE(createdate1, INTERVAL '1' MINUTE) ")

    val table: Table = tableEnv.sqlQuery("SELECT  createdate1 ,  importid ,  ip , logid , moduledesc , moduleid , modulename , moduletype , orgid , url , userid , username as staffname, usertype FROM Ticker where modulename = '注册' ")
    table.toRetractStream[Row].print() //输出打印结果


    val sink: JDBCAppendTableSink = JDBCAppendTableSink.builder()
      .setDrivername("org.postgresql.Driver")
      .setDBUrl("jdbc:postgresql://192.168.9.222/safe_base").setUsername("secu_man").setPassword("secu_man")
      .setQuery("INSERT INTO ihr_register (createdate ,  importid ,  ip , logid , moduledesc , moduleid , modulename , moduletype , orgid , url , userid , staffname , usertype) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?)")
      .setParameterTypes(SQL_TIMESTAMP, INT, STRING, LONG, STRING, INT, STRING, INT, LONG, STRING, LONG, STRING, INT)
      .build()

    tableEnv.registerTableSink(
      "ihr_register",
      // specify table schema
      Array[String]("createdate", "importid", "ip", "logid", "moduledesc", "moduleid", "modulename", "moduletype", "orgid", "url", "userid", "staffname", "usertype"),
      Array[TypeInformation[_]](SQL_TIMESTAMP, INT, STRING, LONG, STRING, INT, STRING, INT, LONG, STRING, LONG, STRING, INT),
      sink)

    table.insertInto("ihr_register")

    env.execute()

  }


}
