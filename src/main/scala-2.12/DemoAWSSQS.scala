import scala.concurrent.{ExecutionContext}

/**
  * Created by mersanuzun on 1/23/17.
  */
object DemoAWSSQS {
  def main(args: Array[String]): Unit = {
    val queueName = "mersanuzun"
    val ec = ExecutionContext.Implicits.global
    //SqsClient.sendMessage(queueName, "Hello, it's Mehmet Ersan Uzun").foreach(println)(ec)
    SqsClient.getMessage(queueName).foreach(a => SqsClient.deleteMessage(queueName, a.get.receiptHandle).foreach(println)(ec))(ec)
    SqsClient.getSizeOfQueue(queueName).foreach(println)(ec)
    println("bitti")
  }
}
