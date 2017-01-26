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
  private val defaultQueueName: String = AwsConfig.getOptionString("aws.sqs.queue-name").getOrElse("mersanuzun")
  private val awsClient: AmazonSQSAsyncClient = new AmazonSQSAsyncClient(new BasicAWSCredentials(accessKey, secretKey))
  awsClient.setEndpoint(defaultEndPoint)
  private var queueNamesAndUrls: MMap[String, String] = MMap.empty[String, String]
  getQueueUrls()
  private val ec: ExecutionContext = ExecutionContext.Implicits.global

  def sendMessage(queueName: String, messageBody: String): Future[Option[String]] = {
    runWithQueueName(queueName, (url: String) => {
      val response: Future[SendMessageResult] = asScalaFuture[SendMessageRequest, SendMessageResult]{ h =>
        awsClient.sendMessageAsync(new SendMessageRequest(url, messageBody), h)
      }
      handleResponse(response, (r: SendMessageResult) => {
        if (r == null) None
        else{
          Some(r.getMessageId)
        }
      })
    })
  }

  def getMessage(queueName: String): Future[Option[SqsMessage]] = {
    runWithQueueName(queueName, (url: String) => {
      val response: Future[ReceiveMessageResult] = asScalaFuture[ReceiveMessageRequest, ReceiveMessageResult]{ h =>
        awsClient.receiveMessageAsync(url, h)
      }
      handleResponse(response, (r: ReceiveMessageResult) => {
        if (r == null) None
        else {
          val messages = r.getMessages
          if (messages.isEmpty) None
          else{
            val message = messages.get(0)
            Some(new SqsMessage(message.getMessageId, message.getBody, message.getReceiptHandle))
          }
        }
      })
    })
  }

  def deleteMessage(queueName: String, receiptHandle: String): Future[Option[String]] = {
    runWithQueueName(queueName, (url: String) => {
      val response: Future[DeleteMessageResult] = asScalaFuture[DeleteMessageRequest, DeleteMessageResult]{ h =>
        awsClient.deleteMessageAsync(new DeleteMessageRequest(url, receiptHandle), h)
      }
      handleResponse(response, (r: DeleteMessageResult) => {
        Some(receiptHandle)
      })
    })
  }

  def getSizeOfQueue(queueName: String): Future[Option[Int]] = {
    runWithQueueName(queueName, (url: String) => {
      val response: Future[GetQueueAttributesResult] = asScalaFuture[GetQueueAttributesRequest, GetQueueAttributesResult]{ h =>
        awsClient.getQueueAttributesAsync(
          new GetQueueAttributesRequest(url, util.Arrays.asList("ApproximateNumberOfMessages")), h)
      }
      handleResponse(response, (attributesResult: GetQueueAttributesResult) => {
        if (attributesResult == null) None
        else {
          val attributes: util.Map[String, String] = attributesResult.getAttributes
          val sizeOfMessages: String = attributes.get("ApproximateNumberOfMessages")
          if (sizeOfMessages == null) None
          else Some(sizeOfMessages.toInt)
        }
      })
    })
  }

  private def handleResponse[T, R](response: Future[T], responseFunction: T => Option[R]): Future[Option[R]] = {
    response.map(responseFunction)(ec)
      .recover{
        case NonFatal(e) => {
          println(e)
          None
        }
      }(ec)
  }

  private def getQueueUrls(): Unit = {
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

}

class QueueListCannotFetched(m: String) extends RuntimeException(m)
class QueueListEmpty(m: String) extends RuntimeException(m)