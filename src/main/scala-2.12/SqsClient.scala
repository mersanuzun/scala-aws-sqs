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
  private lazy val accessKey: String = Config.getString("aws.sqs.access-key")
  private lazy val secretKey: String = Config.getString("aws.sqs.secret-key")
  private lazy val defaultEndPoint: String = Config.getString("aws.sqs.end-point")
  private lazy val queuesNames: List[String] = Config.getStringList("aws.sqs.queue-names")
  private lazy val awsClient: AmazonSQSAsyncClient = new AmazonSQSAsyncClient(new BasicAWSCredentials(accessKey, secretKey))
  private val queueNamesAndUrls: MMap[String, String] = MMap.empty[String, String]
  private val ec: ExecutionContext = ExecutionContext.Implicits.global

  def init(): Unit ={
    accessKey
    secretKey
    defaultEndPoint
    queuesNames
    awsClient
    awsClient.setEndpoint(defaultEndPoint)
    getQueueUrls()
  }

  def sendMessage(queueName: String, messageBody: String): Future[Option[String]] = {
    withQueueName(queueName){ (url: String) => {
      try{
        val response: Future[SendMessageResult] = asScalaFuture[SendMessageRequest, SendMessageResult]{ h =>
          awsClient.sendMessageAsync(new SendMessageRequest(url, messageBody), h)
        }
        response.map((sendMessageResult: SendMessageResult) => {
          if (sendMessageResult == null) None
          else{
            Some(sendMessageResult.getMessageId)
          }
        })(ec).recover{
          case NonFatal(e) => {
            L.errorE("An error occurred while processing SendMessageResult which come from " + queueName + ".", e)
            None
          }
        }(ec)
      }catch {
        case NonFatal(e) => {
          L.errorE("An error occurred while sending message that contains '" + messageBody + "' to " + queueName + ".", e)
          Future.successful(None)
        }
      }
      }
    }
  }

  def getMessage(queueName: String): Future[Option[SqsMessage]] = {
    withQueueName(queueName)((url: String) => {
      try{
        val response: Future[ReceiveMessageResult] = asScalaFuture[ReceiveMessageRequest, ReceiveMessageResult] { h =>
          awsClient.receiveMessageAsync(url, h)
        }
        response.map((receiveMessageResult: ReceiveMessageResult) => {
          val messages = receiveMessageResult.getMessages
          if (messages.isEmpty) None
          else {
            val message = messages.get(0)
            Some(new SqsMessage(message.getMessageId, message.getBody, message.getReceiptHandle))
          }
        })(ec).recover{
          case NonFatal(e) => {
            L.errorE("An error occured while processing ReceiveMessageResult which come from " + queueName + " queue.", e)
            None
          }
        }(ec)
      }catch {
        case NonFatal(e) =>
          L.errorE("An error occurred while sending ReceiveMessageRequest to " + queueName + " queue.", e)
          Future.successful(None)
      }
    })
  }

  def deleteMessage(queueName: String, message: SqsMessage): Future[Option[Boolean]] = {
    deleteMessage(queueName, message.receiptHandle)
  }

  def deleteMessage(queueName: String, receiptHandle: String): Future[Option[Boolean]] = {
    withQueueName(queueName)((url: String) => {
      try{
        val response: Future[DeleteMessageResult] = asScalaFuture[DeleteMessageRequest, DeleteMessageResult] { h =>
          awsClient.deleteMessageAsync(new DeleteMessageRequest(url, receiptHandle), h)
        }
        response.map((_: DeleteMessageResult) => {
          Some(true)
        })(ec).recover{
          case NonFatal(e) => {
            L.errorE("An error occurred while processing DeleteMessageResult which come from " + queueName + " queue.", e)
            None
          }
        }(ec)
      }catch {
        case NonFatal(e) => {
          L.errorE("An error occurred while sending DeleteMessageRequest to " + queueName + " queue.", e)
          Future.successful(None)
        }
      }
    })
  }

  def getSizeOfQueue(queueName: String): Future[Option[Int]] = {
    withQueueName(queueName)((url: String) => {
      try{
        val response: Future[GetQueueAttributesResult] = asScalaFuture[GetQueueAttributesRequest, GetQueueAttributesResult] { h =>
          awsClient.getQueueAttributesAsync(
            new GetQueueAttributesRequest(url, util.Arrays.asList("ApproximateNumberOfMessages")), h)
        }
        response.map((attributesResult: GetQueueAttributesResult) => {
          val attributes: util.Map[String, String] = attributesResult.getAttributes
          val sizeOfMessages: String = attributes.get("ApproximateNumberOfMessages")
          if (sizeOfMessages == null) None
          else Some(sizeOfMessages.toInt)
        })(ec).recover{
          case NonFatal(e) => {
            L.errorE("An error occurred while processing GetAttibutesResult which come from " + queueName + " queue.", e)
            None
          }
        }(ec)
      }catch {
        case NonFatal(e) => {
          L.errorE("An error occured while sending GetQueueAttributesRequest to " + queueName + " queue.", e)
          Future.successful(None)
        }
      }
    })
  }

  private def getQueueUrls(): Unit = {
    queuesNames.foreach((queueName: String) => {
      val listQueuesResult: GetQueueUrlResult = awsClient.getQueueUrl(queueName)
      val url: String = listQueuesResult.getQueueUrl()
      if (url != null){
        queueNamesAndUrls += (queueName -> url)
      }
    })
    if (queueNamesAndUrls.isEmpty) throw new QueueListCannotFetched("Queue list cannot be fetched from AWS.")
  }

  private def withQueueName[B](queueName: String)(f: String => Future[Option[B]]): Future[Option[B]] = {
    queueNamesAndUrls.get(queueName) match {
      case Some(url) => f(url)
      case None =>
        L.error(queueName + " could not be found in " + queueNamesAndUrls)
        Future.successful(None)
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