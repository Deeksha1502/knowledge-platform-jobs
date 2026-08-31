package org.sunbird.job.publish.helpers

import org.apache.commons.lang3.StringUtils
import org.slf4j.LoggerFactory
import org.sunbird.job.domain.`object`.DefinitionCache
import org.sunbird.job.publish.config.PublishConfig
import org.sunbird.job.publish.core.{DefinitionConfig, ObjectData}
import org.sunbird.job.util.{CloudStorageUtil, FileUtils, JanusGraphUtil}

import java.io.File
import java.nio.file.Files
import java.security.MessageDigest
import scala.concurrent.duration.Duration
import scala.concurrent.{Await, ExecutionContext, Future}

trait EcarGenerator extends ObjectBundle {

	private[this] val logger = LoggerFactory.getLogger(classOf[EcarGenerator])

	def generateEcar(obj: ObjectData, pkgType: List[String])(implicit ec: ExecutionContext, janusGraphUtil: JanusGraphUtil, cloudStorageUtil: CloudStorageUtil, config: PublishConfig, defCache: DefinitionCache, defConfig: DefinitionConfig): Map[String, String] = {
		logger.info("Generating Ecar For : " + obj.identifier)
		val enObjects: List[Map[String, AnyRef]] = getDataForEcar(obj).getOrElse(List())
		pkgType.flatMap(pkg => Map(pkg -> generateEcar(obj, enObjects, pkg))).toMap
	}

	def getDataForEcar(obj: ObjectData): Option[List[Map[String, AnyRef]]]

	// this method returns only cloud url for given pkg
	def generateEcar(obj: ObjectData, objList: List[Map[String, AnyRef]], pkgType: String)(implicit ec: ExecutionContext, janusGraphUtil: JanusGraphUtil, cloudStorageUtil: CloudStorageUtil, config: PublishConfig, defCache: DefinitionCache, defConfig: DefinitionConfig): String = {
		logger.info(s"Generating ${pkgType} Ecar For : " + obj.identifier)
		val bundle: File = getObjectBundle(obj, objList, pkgType)
		uploadFile(Some(bundle), obj.identifier, obj.dbObjType.replaceAll("Image", "")).getOrElse("")
	}

	private def uploadFile(fileOption: Option[File], identifier: String, objectType: String)(implicit cloudStorageUtil: CloudStorageUtil): Option[String] = {
		fileOption match {
			case Some(file: File) => {
				logger.info("bundle file path ::: "+file.getAbsolutePath)
				val folder = objectType.toLowerCase + File.separator + identifier
				val urlArray: Array[String] = cloudStorageUtil.uploadFile(folder, file, Some(false))
				logger.info(s"EcarGenerator ::: uploadFile ::: ecar url for $identifier is : ${urlArray(1)}")
				Some(urlArray(1))
			}
			case _ => None
		}
	}
	// Downloads its own copy of the artifact independently of getObjectBundle/ecar generation above,
	// so it must bound that download itself the same way getObjectBundle bounds its downloads — via
	// media_download_duration — otherwise a stalled artifact host would block this Flink task
	// thread indefinitely (FileUtils.downloadFile sets no socket timeout of its own).
	def computeArtifactHash(obj: ObjectData)(implicit ec: ExecutionContext, janusGraphUtil: JanusGraphUtil, config: PublishConfig): Option[(String, Option[String])] = {
		val artifactUrl = obj.getString("artifactUrl", "")
		if (StringUtils.isBlank(artifactUrl)) None
		else {
			val scratchDir = new File("/tmp" + File.separator + "artifact_hash_" + obj.identifier + "_" + System.currentTimeMillis)
			try {
				val duration = Duration.apply(config.getString("media_download_duration", "300 seconds"))
				val artifactFile = Await.result(Future(FileUtils.downloadFile(artifactUrl, scratchDir.getAbsolutePath)), duration)
				val newHash = sha256Hex(artifactFile)
				Some((newHash, readPrevArtifactHash(obj)))
			} catch {
				case e: Exception =>
					logger.error(s"EcarGenerator ::: Unable to compute artifactHash for ${obj.identifier}: ${e.getMessage}", e)
					None
			} finally {
				FileUtils.deleteDirectory(scratchDir)
			}
		}
	}
	def hashMeta(hashInfo: Option[(String, Option[String])]): Map[String, AnyRef] =
		hashInfo.map { case (hash, prevHash) =>
			Map[String, AnyRef]("artifactHash" -> hash) ++ prevHash.map(prev => Map[String, AnyRef]("prevArtifactHash" -> prev)).getOrElse(Map.empty)
		}.getOrElse(Map.empty)

	protected def readPrevArtifactHash(obj: ObjectData)(implicit janusGraphUtil: JanusGraphUtil): Option[String] = {
		try {
			Option(janusGraphUtil.getNodeProperties(obj.identifier)).flatMap(props => Option(props.get("artifactHash"))).map(_.toString)
		} catch {
			case e: Exception =>
				logger.error(s"EcarGenerator ::: Unable to read previous artifactHash for ${obj.identifier}: ${e.getMessage}", e)
				None
		}
	}

	private def sha256Hex(file: File): String = {
		val digest = MessageDigest.getInstance("SHA-256")
		val buffer = new Array[Byte](8192)
		val input = Files.newInputStream(file.toPath)
		try {
			var bytesRead = input.read(buffer)
			while (bytesRead != -1) {
				digest.update(buffer, 0, bytesRead)
				bytesRead = input.read(buffer)
			}
		} finally {
			input.close()
		}
		digest.digest().map("%02x".format(_)).mkString
	}
}
