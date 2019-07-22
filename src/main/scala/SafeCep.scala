import org.apache.flink.api.common.typeinfo.{TypeInformation, Types}
import org.apache.flink.runtime.state.StateBackend
import org.apache.flink.runtime.state.filesystem.FsStateBackend
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.table.api.Table
import org.apache.flink.table.api.Types._
import org.apache.flink.table.api.scala.StreamTableEnvironment
import org.apache.flink.table.descriptors.{Json, Kafka, Rowtime, Schema}
import org.apache.flink.types.Row
import org.apache.flink.table.api.scala._
import org.apache.flink.api.scala._

object SafeCep {
  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    // env.setParallelism(1)
    env.enableCheckpointing(300000)
    //      val backend = new FsStateBackend("hdfs://ns1/tmp/flink_stats/", false)
    //  val backend = new FsStateBackend("file:///c:/tmp/", true)
    //   env.setStateBackend(backend.asInstanceOf[StateBackend])
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)
    val tableEnv = StreamTableEnvironment.create(env)
    val tableEnv2 = tableEnv.connect(
      new Kafka()
        .version("0.11")
        .topic("order_sql")
        .startFromEarliest()
        //.property("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer")
        //.property("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer")
        //  .property("zookeeper.connect", "192.168.217.132:2181")
        .property("bootstrap.servers", "192.168.21.128:9092") //.startFromLatest()
    ).withFormat(
      new Json().schema(ROW(Array[String]("createdate", "importid", "ip", "logid", "moduledesc", "moduleid", "modulename", "moduletype", "orgid", "url", "userid", "username", "usertype")
        , Array[TypeInformation[_]](SQL_TIMESTAMP, INT, STRING, LONG, STRING, STRING, STRING, INT, LONG, STRING, LONG, STRING, STRING)))
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
      .watermarksPeriodicBounded(60000)
    ).field("importid", Types.INT).field("ip", Types.STRING)
      .field("logid", Types.LONG).field("moduledesc", Types.STRING).field("moduleid", Types.INT).field("modulename", Types.STRING).field("moduletype", Types.LONG).field("orgid", Types.LONG)
      .field("url", Types.STRING).field("userid", Types.LONG).field("username", Types.STRING).field("usertype", Types.INT)
    )
      .inAppendMode()
      .registerTableSource("Ticker")


    val table: Table = tableEnv.sqlQuery("SELECT * " +
      "FROM Ticker    MATCH_RECOGNIZE(        PARTITION BY userid      ORDER BY createdate1" +
      "      MEASURES            " +
      "     A.modulename AS modulename" +
      "       ONE ROW PER MATCH" +
      "        AFTER MATCH SKIP PAST LAST ROW" +
      "        PATTERN (A B+ C) WITHIN INTERVAL '60' MINUTE" +
      "        DEFINE" +
      "           A AS A.modulename = 'PC登录'," +
      "           B AS B.modulename = '简历处理'," +
      "           C AS C.modulename <> '简历处理'    )")

    //// val table:Table = tableEnv.sqlQuery("SELECT * FROM Ticker")
    table.toAppendStream[Row].print()

    //      val sink: JDBCAppendTableSink = JDBCAppendTableSink.builder()
    //        .setDrivername("org.postgresql.Driver")
    //        .setDBUrl("jdbc:postgresql://192.168.9.222/safe_base").setUsername("secu_man").setPassword("secu_man")
    //        .setQuery("INSERT INTO book (symbol,lastprice) VALUES (?,?)")
    //        .setParameterTypes(Types.STRING,Types.LONG)
    //        .build()

    //      tableEnv.registerTableSink(
    //        "book",
    //        // specify table schema
    //        Array[String]("symbol","lastPrice"),
    //        Array[TypeInformation[_]](Types.STRING,Types.LONG),
    //        sink)
    //
    //
    //      table.insertInto("book")

    env.execute()

  }


}
