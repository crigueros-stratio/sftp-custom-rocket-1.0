package com.stratio.sparta

import java.io.{File, InputStream}
import java.util.Properties
import com.jcraft.jsch.{ChannelSftp, JSch, Session}
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs._
import org.apache.hadoop.io.IOUtils
//import org.slf4j.{Logger, LoggerFactory}

import scala.util.{Failure, Success, Try}

object SftpToHdfsServiceImpl {

  //  val log: Logger = LoggerFactory.getLogger(HdfsToFtpServiceImpl.getClass)
  val nameClass: String = getClass.getName
  val conf: Configuration = new Configuration
  val fs: FileSystem = FileSystem.get(conf)


  def start(properties: Map[String, String]) = {
    var channelSftp: ChannelSftp = null
    var session: Session = null

    connectSFTP(properties) match {
      case Success(success) =>
        channelSftp = success._1
        session = success._2
        val successDirActualSftp: String = success._3
        val localDirectory = properties("localDirectoryWs")

        downloadDirectory(channelSftp, successDirActualSftp, "", localDirectory) match {
          case Success(_) =>
            println(s"Process [SftpToHdFs] completed successfully")
            channelSftp.exit()
            session.disconnect()
          case Failure(fail) =>
            //            GenericMethods.printError(log, fail, nameClass, "save.[connectSFTP].[downloadDirectory]")
            channelSftp.exit()
            session.disconnect()
        }
      case Failure(fail) =>
        println("save.[connectSFTP]" + fail.getMessage)
      //        GenericMethods.printError(log, fail, nameClass, "save.[connectSFTP]")
    }
  }

  private def connectSFTP(properties: Map[String, String]): Try[(ChannelSftp, Session, String)] = Try {
    val serverAddress = properties("serverWs")
    val userId = properties("userWs")
    val password = properties("passwordWs")
    val remoteDirectory = properties("remoteDirectoryWs")
    val port = properties("portWs")

    val jsch: JSch = new JSch
    val session: Session = jsch.getSession(userId, serverAddress, port.toInt)
    session.setPassword(password)
    val config = new Properties
    config.put("StrictHostKeyChecking", "no")
    session.setConfig(config)
    session.connect()

    val channel = session.openChannel("sftp")
    channel.connect()

    val channelSftp = channel.asInstanceOf[ChannelSftp]
    val workingDirectory: String = if (remoteDirectory.isEmpty) channelSftp.getHome else remoteDirectory
    channelSftp.cd(workingDirectory)

    (channelSftp, session, workingDirectory)
  }

  def downloadSingleFile(channelSftp: ChannelSftp, remoteFilePath: String, savePath: String): Try[Unit] = Try {


    val filePathInFs = new Path(savePath)
    fs.delete(filePathInFs, true)

    val inpStr: InputStream = channelSftp.get(remoteFilePath)
    val out = fs.create(filePathInFs)

    IOUtils.copyBytes(inpStr, out, conf)
    IOUtils.closeStream(out)
    IOUtils.closeStream(inpStr)

  }

  def downloadDirectory(channelSftp: ChannelSftp, parentDir: String, currentDir: String, saveDir: String): Try[Unit] = Try {
    var dirToList = parentDir
    if (!(currentDir == "")) dirToList += "/" + currentDir
    val subFiles = channelSftp.ls(dirToList)
    if (subFiles != null && !subFiles.isEmpty)
      subFiles.toArray.foreach {
        case aFile: ChannelSftp#LsEntry =>
          val currentFileName = aFile.getFilename
          if (!currentFileName.equals(".") && !currentFileName.equals("..")) {
            var filePath = parentDir + "/" + currentDir + "/" + currentFileName
            if (currentDir == "") filePath = parentDir + "/" + currentFileName
            var newDirPath = saveDir + parentDir + File.separator + currentDir + File.separator + currentFileName
            if (currentDir == "") newDirPath = saveDir + parentDir + File.separator + currentFileName
            if (aFile.getAttrs.isDir) {
              println(" DIR: " + filePath)
              downloadDirectory(channelSftp, dirToList, currentFileName, saveDir)
            }
            else {
              println("filePath: " + filePath + " newDirPath:" + newDirPath)
              downloadSingleFile(channelSftp, filePath, newDirPath)
              println("Archivo " + filePath + " descargado")
            }
          }
        case _ =>
      }
  }
}