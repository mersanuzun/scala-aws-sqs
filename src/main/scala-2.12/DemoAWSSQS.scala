import org.slf4j.LoggerFactory

import scala.concurrent.ExecutionContext

/**
  * Created by mersanuzun on 1/23/17.
  */
object DemoAWSSQS {
  def main(args: Array[String]): Unit = {
    val queueName = "mersanuzun"
    val ec = ExecutionContext.Implicits.global
    L.info("Selam")
    L.error("Hİ")
    System.setProperty("conf-file", "conf/application.conf")
    SqsClient.init()
    SqsClient.sendMessage(queueName, "Hello, it's Mehmet Ersan Uzun").foreach(println)(ec)
    //SqsClient.getMessage(queueName).foreach(a => SqsClient.deleteMessage(queueName, a.get))(ec)
    SqsClient.getMessage(queueName).foreach(println)(ec)
    SqsClient.getSizeOfQueue(queueName).foreach(println)(ec)
  }
}
