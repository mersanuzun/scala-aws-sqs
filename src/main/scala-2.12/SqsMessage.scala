/**
  * Created by mersanuzun on 1/24/17.
  */
class SqsMessage(val id: String, val body: String, val receiptHandle: String){
  override def toString: String = "ID: " + id + " Body: " + body + " ReceiptHandle: " + receiptHandle
}
