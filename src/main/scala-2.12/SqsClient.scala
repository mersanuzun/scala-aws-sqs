import java.util

import com.amazonaws.auth.BasicAWSCredentials
import com.amazonaws.handlers.AsyncHandler
import com.amazonaws.services.sqs.AmazonSQSAsyncClient
import com.amazonaws.services.sqs.model._
import scala.collection.mutable.{Map => MMap}
import scala.concurrent.{ExecutionContext, Future, Promise}
import scala.util.control.NonFatal

/**
  * Created by mersanuzun on 1/24/17.
  */
object SqsClient {
  private val accessKey: String = AwsConfig.getOptionString("aws.sqs.access-key").getOrElse("x")
  private val secretKey: String = AwsConfig.getOptionString("aws.sqs.secret-key").getOrElse("x")
  private val defaultEndPoint: String = AwsConfig.getOptionString("aws.sqs.end-point").getOrElse("http://localhost:9324")
  private val defaultQueueName: String = AwsConfig.getOptionString("aws.sqs.queue-name").getOrElse("mersanuzun") // fixed
  private val awsClient: AmazonSQSAsyncClient = new AmazonSQSAsyncClient(new BasicAWSCredentials(accessKey, secretKey))
  awsClient.setEndpoint(defaultEndPoint)
  private var queueNamesAndUrls: MMap[String, String] = MMap.empty[String, String]
  getQueueUrls()
  private val ec: ExecutionContext = ExecutionContext.Implicits.global

  def sendMessage(queueName: String, messageBody: String): Future[Option[String]] = {
    runWithQueueName(queueName, (url: String) => {
      val response = asScalaFuture[SendMessageRequest, SendMessageResult]{ h =>
        awsClient.sendMessageAsync(new SendMessageRequest(url, messageBody), h)
      }
      response.map(r => {
        if (r == null) None
        else{
          Some(r.getMessageId)
        }
      })(ec).recover{
        case NonFatal(e) =>
          println(e.getMessage)
          None
      }(ec)
    })
  }

  def getMessage(queueName: String): Future[Option[SqsMessage]] = {
    runWithQueueName(queueName, (url: String) => {
      val response = asScalaFuture[ReceiveMessageRequest, ReceiveMessageResult]{ h =>
        awsClient.receiveMessageAsync(url, h)
      }
      response.map(r => {
        if (r == null) None
        else {
          val messages = r.getMessages
          if (messages.isEmpty) None
          else{
            val message = messages.get(0)
            Some(new SqsMessage(message.getMessageId, message.getBody, message.getReceiptHandle))
          }
        }
      })(ec).recover{
        case NonFatal(e) => {
          println(e.getMessage)
          None
        }
      }(ec)
    })
  }

  def deleteMessage(queueName: String, receiptHandle: String): Future[Option[String]] = {
    runWithQueueName(queueName, (url: String) => {
      val response = asScalaFuture[DeleteMessageRequest, DeleteMessageResult]{ h =>
        awsClient.deleteMessageAsync(new DeleteMessageRequest(url, receiptHandle), h)
      }
      response.map(_ => {
        Some(receiptHandle)
      })(ec).recover{
        case NonFatal(e) => {
          println(e)
          None
        }
      }(ec)
    })
  }

  private def getQueueUrls(): Unit = {
    /*val response: Future[ListQueuesResult] = asScalaFuture[ListQueuesRequest, ListQueuesResult]{ h =>
      awsClient.listQueuesAsync(new ListQueuesRequest(), h)
    }*/
    val listQueuesResult = awsClient.listQueues()
    val urls: util.List[String] = listQueuesResult.getQueueUrls
    if (urls.isEmpty) throw new QueueListEmpty("Queue list is empty.")
    urls.forEach((url: String) => {
      if (url != null){
        val splitted: Array[String] = url.split("/")
        queueNamesAndUrls += splitted(splitted.length - 1) -> url
      }
    })
    if (queueNamesAndUrls.isEmpty) throw new QueueListCannotFetched("Queue list cannot be fetched from AWS.")
  }

  private def runWithQueueName[B](queueName: String, f: String => Future[Option[B]]): Future[Option[B]] = {
    queueNamesAndUrls.get(queueName) match {
      case Some(url) => f(url)
      case None =>
        queueNamesAndUrls.get(defaultQueueName) match {
          case Some(url) => f(url)
          case None => Future.successful(None)
        }
    }
  }

  private def asScalaFuture[T <: com.amazonaws.AmazonWebServiceRequest, R](f: AsyncHandler[T, R] => Unit): Future[R] = {
    val p = Promise[R]()
    try {
      f(new AsyncHandler[T, R] {
        def onError(e: Exception): Unit = p failure e

        def onSuccess(request: T, response: R): Unit = p success response
      })
    } catch {
      case NonFatal(t) => p failure t
    }
    p.future
  }

  /*
 private def toScalaFuture[T](javaFuture: java.util.concurrent.Future[T]): Future[T] = {
   Future {
     javaFuture.get()
   }(ec)
 }
 */

}

class QueueListCannotFetched(m: String) extends RuntimeException(m)
class QueueListEmpty(m: String) extends RuntimeException(m)