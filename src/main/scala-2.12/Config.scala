import java.io.File
import scala.collection.JavaConverters._
import com.typesafe.config.{Config, ConfigFactory}

import scala.util.control.NonFatal

/**
  * Created by mersanuzun on 1/24/17.
  */
object Config{
  private val confFile: String = System.getProperty("conf-file")
  private val conf: Config = ConfigFactory.parseFile(new File(confFile))

  def getString(path: String): String = {
    conf.getString(path)
  }

  def getOptionString(path: String): Option[String] = {
    try{
      Some(getString(path))
    }catch {
      case NonFatal(_) => None
    }
  }

  def getStringList(path: String): List[String] = {
    val list = conf.getStringList(path)
    list.asScala.toList
  }

  def getOptionStringList(path: String): Option[List[String]] = {
    try{
      Some(getStringList(path))
    }catch {
      case NonFatal(e) => None
    }
  }
}
