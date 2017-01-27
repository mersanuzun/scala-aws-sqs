import org.slf4j.{Logger, LoggerFactory}

/**
  * Created by mersanuzun on 1/26/17.
  */
object L {
  private val logger: Logger = LoggerFactory.getLogger("com.mersanuzun")

  def errorE(m: => String, e: Throwable): Unit = logger.error(m, e)
  def error(m: => String): Unit = logger.error(m)
  def info(m: => String): Unit = logger.info(m)
}
