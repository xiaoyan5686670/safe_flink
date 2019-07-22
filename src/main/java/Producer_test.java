import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;

import java.io.*;
import java.util.Properties;

/**
 * Created by xiaoyan on 2018/6/1.
 */
public class Producer_test {
    public static void main(String[] args) throws IOException {


        Properties props = new Properties();
        props.put("bootstrap.servers", "192.168.21.128:9092");
        props.put("acks", "all");
        props.put("retries", 0);
        props.put("batch.size", 16384);
        props.put("linger.ms", 1);
        props.put("buffer.memory", 33554432);
        props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");

        Producer<String, String> producer = new KafkaProducer<String, String>(props);
//        String s = "{\"at\":\"ae5168db3d944521a5d7014849064cad\",\"send_date\":,\"loginDto\":{\"access_Token\":\"ae5168db3d944521a5d7014849064cad\",\"businessSystem\":1,\"clientTypes\":\"\",\"csid\":0,\"device_info\":{\"deviceid\":\"\",\"ip\":\"171.214.138.209\",\"userAgent\":\"Mozilla/5.0 (Windows NT 6.1; WOW64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/55.0.2883.87 Safari/537.36\"},\"email\":\"\",\"expires_In\":43199,\"lastLoginIP\":\"110.184.46.5\",\"lastLoginTime\":1527409557000,\"orgId\":44502888,\"passportName\":\"chaojikeai01\",\"refresh_Token\":\"ae5168db3d944521a5d7014849064cad\",\"userId\":718432673,\"userType\":153},\"eventType\":\"120\",\"orgDto\":{\"businesslicenceurl\":\"https://fileihr.zhaopin.com/044/502/044502888/license/1fdd5828-065e-4976-a5ac-68deaa5498c2.jpg\",\"businesslicenceurlname\":\"济南英蓓克机械有限公司\",\"cityid\":702,\"companyaddress\":\"章丘市明水道办事处柳沟村柳岗路\",\"companyid\":44502888,\"companyname\":\"济南英蓓克机械有限公司\",\"companysize\":1,\"companytype\":5,\"contactor\":\"邢金星\",\"countryid\":0,\"cqid\":0,\"createstaffid\":705562456,\"email\":\"\",\"fax\":\"\",\"hostid\":0,\"industryid\":\"990000\",\"industryid2\":\"\",\"introurl\":\"http://company.zhaopin.com/CZ445028880.htm\",\"iscompany\":\"y\",\"latitude\":\"\",\"legalpersonname\":\"\",\"logourl\":\"\",\"longitude\":\"\",\"mobile\":\"15677507651\",\"modifystaffid\":10201,\"orgVipInfoDto\":{\"endtime\":0,\"limitcount\":5,\"orgid\":44502888,\"rdorgid\":0,\"staffid\":0,\"starttime\":0,\"total\":5,\"type\":0,\"userid\":0,\"vipDescription\":\"您当前的会员等级及有效期是根据您与智联招聘的合作金额得出。\",\"vipEndtime\":0,\"vipName\":\"非会员\"},\"orgdescription\":\"\",\"orgid\":44502888,\"orgname\":\"济南英蓓克机械有限公司\",\"orgnumber\":\"CZ445028880\",\"orgnumber2\":\"\",\"orgshortname\":\"\",\"parentid\":44502888,\"platsource\":0,\"postalcode\":\"\",\"provinceid\":544,\"rddepid\":0,\"rdorgid\":0,\"sourceid\":3,\"status\":30,\"telephone\":\"--\",\"url\":\"\",\"zoomlevel\":0},\"taskId\":\"7a78dd6074f14993b9e309c466d94c39\",\"userDto\":{\"accountnumber\":\"\",\"authenticationsubtype\":10,\"authenticationtype\":10,\"businessid\":\"\",\"businesslicenceapplydate\":\"2018-04-05 12:56:35.637\",\"businesslicenceurl\":\"https://fileihr.zhaopin.com/044/502/044502888/license/1fdd5828-065e-4976-a5ac-68deaa5498c2.jpg\",\"clientip\":\"\",\"companyid\":44502888,\"createdate\":1522901794514,\"createuserid\":0,\"ctimestamp\":0,\"customaudittime\":1523152221564,\"customcommittime\":1522904281096,\"email\":\"1278034346@qq.com\",\"email2\":\"\",\"fax\":\"\",\"idcard\":\"412725199208156990\",\"idcardcommittime\":1522904281096,\"idcardname\":\"周亚\",\"invmode\":0,\"invtp\":0,\"invuserid\":0,\"jobposition\":\"\",\"lastlogindate\":1527674397507,\"legalpersonname\":\"邢金星\",\"mobile\":\"13162011121\",\"modifieddate\":1527585265516,\"modifiedusertid\":0,\"nickName\":\"\",\"nopasscode\":0,\"nopassmessage\":\"\",\"orghostid\":0,\"orgid\":44502888,\"passportname\":\"chaojikeai01\",\"passportuserhostid\":0,\"passportuserid\":718432673,\"password\":\"\",\"platsource\":0,\"sex\":0,\"sourcecompanyid\":0,\"sourcecompanytype\":0,\"sourceid\":3,\"staffappealid\":0,\"staffaudittime\":1523151692722,\"staffcode\":\"\",\"staffcommittime\":1522904195624,\"staffid\":718432673,\"staffname\":\"王先生\",\"staffrole\":90,\"staffsource\":0,\"status\":20,\"substatus\":0,\"telephone\":\"--\",\"userid\":0}}\n";
//        for(int x = 1; x <= 20; x = x+1) {
//            producer.send(new ProducerRecord<String, String>("qxy_test4", "qxy", s));
//            System.out.println(x);
//        }
        BufferedReader inputStream = null;
        PrintWriter outputStream = null;

        try {
            inputStream = new BufferedReader(new FileReader("d:\\2019061919"));

            //   outputStream = new PrintWriter(new FileWriter("d:\\characteroutput.txt"));

            String l;
            while ((l = inputStream.readLine()) != null) {

                producer.send(new ProducerRecord<>("order_sql", l));

            }

        } catch (FileNotFoundException e) {
            e.printStackTrace();
        } catch (IOException e) {
            e.printStackTrace();
        } finally {
            if (inputStream != null) {
                inputStream.close();
            }
//            if (outputStream != null) {
//                outputStream.close();
//            }
        }

        producer.close();
    }
}
