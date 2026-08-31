package org.sunbird.job.publish.spec

import com.sun.net.httpserver.{HttpExchange, HttpHandler, HttpServer}
import com.typesafe.config.{Config, ConfigFactory}
import org.mockito.Mockito
import org.mockito.ArgumentMatchers.anyString
import org.scalatest.{BeforeAndAfterAll, FlatSpec, Matchers}
import org.scalatestplus.mockito.MockitoSugar.mock
import org.sunbird.job.domain.`object`.DefinitionCache
import org.sunbird.job.util.{CloudStorageUtil, JanusGraphUtil, ScalaJsonUtil}
import org.sunbird.job.publish.config.PublishConfig
import org.sunbird.job.publish.core.{DefinitionConfig, ObjectData}
import org.sunbird.job.publish.helpers.EcarGenerator

import java.net.InetSocketAddress
import java.security.MessageDigest
import scala.collection.JavaConverters._
import scala.concurrent.ExecutionContext.Implicits.global

class EcarGeneratorSpec extends FlatSpec with BeforeAndAfterAll with Matchers {

  override protected def beforeAll(): Unit = {
    super.beforeAll()
  }

  override protected def afterAll(): Unit = {
    super.afterAll()
  }

  val config: Config = ConfigFactory.load("test.conf").withFallback(ConfigFactory.systemEnvironment())
  implicit val publishConfig: PublishConfig = new PublishConfig(config, "")
  implicit val cloudStorageUtil: CloudStorageUtil = new CloudStorageUtil(publishConfig)
  implicit val mockJanusGraphUtil: JanusGraphUtil = mock[JanusGraphUtil](Mockito.withSettings().serializable())
  val definitionBasePath: String = if (config.hasPath("schema.basePath")) config.getString("schema.basePath") else "https://sunbirddev.blob.core.windows.net/sunbird-content-dev/schemas/local"
  val schemaSupportVersionMap = if (config.hasPath("schema.supportedVersion")) config.getObject("schema.supportedVersion").unwrapped().asScala.toMap else Map[String, AnyRef]()
  implicit val defCache = new DefinitionCache()
  implicit val defConfig = DefinitionConfig(schemaSupportVersionMap, definitionBasePath)

  "Object Ecar Generator generateEcar" should "return a Map containing Packaging Type and its url after uploading it to cloud" in {

    val hierarchy = Map("identifier" -> "do_123", "children" -> List(Map("identifier" -> "do_234", "name" -> "Children-1", "objectType" -> "Question"), Map("identifier" -> "do_345", "name" -> "Children-2", "objectType" -> "Question")))
    val metadata = Map("identifier" -> "do_123", "appIcon" -> "https://dev.sunbirded.org/content/preview/assets/icons/avatar_anonymous.png", "identifier" -> "do_123", "objectType" -> "QuestionSet", "name" -> "Test QuestionSet", "status" -> "Live")
    val objData = new ObjectData("do_123", metadata, None, Some(hierarchy))
    val obj = new TestEcarGenerator()
    val result = obj.generateEcar(objData,List("SPINE"))
    result.isEmpty should be(false)
  }

  "readPrevArtifactHash" should "return None when the node has no prior artifactHash" in {
    val mockJanusGraphUtil: JanusGraphUtil = mock[JanusGraphUtil](Mockito.withSettings().serializable())
    Mockito.when(mockJanusGraphUtil.getNodeProperties(anyString())).thenReturn(null)
    val obj = new TestEcarGenerator()
    val objData = new ObjectData("do_123", Map("identifier" -> "do_123"), None, None)

    obj.readPrevArtifactHash(objData)(mockJanusGraphUtil) should be(None)
  }

  it should "return the existing artifactHash as prevArtifactHash on republish" in {
    val mockJanusGraphUtil: JanusGraphUtil = mock[JanusGraphUtil](Mockito.withSettings().serializable())
    val existingProps: java.util.Map[String, AnyRef] = new java.util.HashMap[String, AnyRef]()
    existingProps.put("artifactHash", "oldhash123")
    Mockito.when(mockJanusGraphUtil.getNodeProperties(anyString())).thenReturn(existingProps)
    val obj = new TestEcarGenerator()
    val objData = new ObjectData("do_123", Map("identifier" -> "do_123"), None, None)

    obj.readPrevArtifactHash(objData)(mockJanusGraphUtil) should be(Some("oldhash123"))
  }

  it should "log and swallow the error instead of throwing when reading current node properties fails" in {
    val mockJanusGraphUtil: JanusGraphUtil = mock[JanusGraphUtil](Mockito.withSettings().serializable())
    Mockito.when(mockJanusGraphUtil.getNodeProperties(anyString())).thenThrow(new RuntimeException("read failed"))
    val obj = new TestEcarGenerator()
    val objData = new ObjectData("do_123", Map("identifier" -> "do_123"), None, None)

    noException should be thrownBy obj.readPrevArtifactHash(objData)(mockJanusGraphUtil)
  }

  "hashMeta" should "include both artifactHash and prevArtifactHash when both are present" in {
    val obj = new TestEcarGenerator()
    obj.hashMeta(Some(("newhash", Some("oldhash")))) should be(Map("artifactHash" -> "newhash", "prevArtifactHash" -> "oldhash"))
  }

  it should "omit prevArtifactHash entirely rather than writing it as null on first publish" in {
    val obj = new TestEcarGenerator()
    obj.hashMeta(Some(("newhash", None))) should be(Map("artifactHash" -> "newhash"))
  }

  it should "be empty when no artifact was hashed this round" in {
    val obj = new TestEcarGenerator()
    obj.hashMeta(None) should be(Map.empty)
  }

  "computeArtifactHash" should "return None when the object has no artifactUrl" in {
    val mockJanusGraphUtil: JanusGraphUtil = mock[JanusGraphUtil](Mockito.withSettings().serializable())
    val obj = new TestEcarGenerator()
    val objData = new ObjectData("do_123", Map("identifier" -> "do_123"), None, None)

    obj.computeArtifactHash(objData)(global, mockJanusGraphUtil, publishConfig) should be(None)
  }

  it should "download the artifact and return its hash with no prevArtifactHash on first publish" in {
    val content = "first-publish-artifact-bytes"
    withHttpServer(content) { url =>
      val mockJanusGraphUtil: JanusGraphUtil = mock[JanusGraphUtil](Mockito.withSettings().serializable())
      Mockito.when(mockJanusGraphUtil.getNodeProperties(anyString())).thenReturn(null)
      val obj = new TestEcarGenerator()
      val objData = new ObjectData("do_123", Map("identifier" -> "do_123", "artifactUrl" -> url), None, None)

      obj.computeArtifactHash(objData)(global, mockJanusGraphUtil, publishConfig) should be(Some((sha256Hex(content), None)))
    }
  }

  it should "carry forward the existing artifactHash as prevArtifactHash on republish" in {
    val content = "republish-artifact-bytes"
    withHttpServer(content) { url =>
      val mockJanusGraphUtil: JanusGraphUtil = mock[JanusGraphUtil](Mockito.withSettings().serializable())
      val existingProps: java.util.Map[String, AnyRef] = new java.util.HashMap[String, AnyRef]()
      existingProps.put("artifactHash", "oldhash123")
      Mockito.when(mockJanusGraphUtil.getNodeProperties(anyString())).thenReturn(existingProps)
      val obj = new TestEcarGenerator()
      val objData = new ObjectData("do_123", Map("identifier" -> "do_123", "artifactUrl" -> url), None, None)

      obj.computeArtifactHash(objData)(global, mockJanusGraphUtil, publishConfig) should be(Some((sha256Hex(content), Some("oldhash123"))))
    }
  }

  it should "time out and return None instead of blocking indefinitely when the artifact host stalls" in {
    withSlowHttpServer(delayMillis = 3000) { url =>
      val mockJanusGraphUtil: JanusGraphUtil = mock[JanusGraphUtil](Mockito.withSettings().serializable())
      val obj = new TestEcarGenerator()
      val objData = new ObjectData("do_123", Map("identifier" -> "do_123", "artifactUrl" -> url), None, None)
      val shortTimeoutConfig: PublishConfig = new PublishConfig(ConfigFactory.parseString("media_download_duration = \"1 second\"").withFallback(config), "")

      val start = System.currentTimeMillis()
      val result = obj.computeArtifactHash(objData)(global, mockJanusGraphUtil, shortTimeoutConfig)
      val elapsedMs = System.currentTimeMillis() - start

      result should be(None)
      (elapsedMs < 3000) should be(true)
    }
  }

  private def sha256Hex(content: String): String =
    MessageDigest.getInstance("SHA-256").digest(content.getBytes).map("%02x".format(_)).mkString

  private def withHttpServer(responseBody: String)(test: String => Unit): Unit = {
    val server = HttpServer.create(new InetSocketAddress(0), 0)
    server.createContext("/artifact", new HttpHandler {
      override def handle(exchange: HttpExchange): Unit = {
        val bytes = responseBody.getBytes
        exchange.sendResponseHeaders(200, bytes.length)
        val os = exchange.getResponseBody
        os.write(bytes)
        os.close()
      }
    })
    server.start()
    try {
      test(s"http://localhost:${server.getAddress.getPort}/artifact")
    } finally {
      server.stop(0)
    }
  }

  private def withSlowHttpServer(delayMillis: Long)(test: String => Unit): Unit = {
    val server = HttpServer.create(new InetSocketAddress(0), 0)
    server.createContext("/artifact", new HttpHandler {
      override def handle(exchange: HttpExchange): Unit = {
        Thread.sleep(delayMillis)
        val bytes = "slow-artifact".getBytes
        exchange.sendResponseHeaders(200, bytes.length)
        val os = exchange.getResponseBody
        os.write(bytes)
        os.close()
      }
    })
    server.start()
    try {
      test(s"http://localhost:${server.getAddress.getPort}/artifact")
    } finally {
      server.stop(0)
    }
  }
}

class TestEcarGenerator extends EcarGenerator {
  override def readPrevArtifactHash(obj: ObjectData)(implicit janusGraphUtil: JanusGraphUtil): Option[String] =
    super.readPrevArtifactHash(obj)
  val media = Map(
    "id" -> "do_1127129497561497601326",
    "type" -> "image",
    "src" -> "somepath/sunbird_1551961194254.jpeg",
    "baseUrl" -> "some_base_url"
  )
  val testObj = List(Map("children" -> List(Map("identifier" -> "do_234", "name" -> "Children-1", "objectType" -> "Question"), Map("identifier" -> "do_345", "name" -> "Children-2", "objectType" -> "Question")), "name" -> "Test QuestionSet", "appIcon" -> "https://dev.sunbirded.org/content/preview/assets/icons/avatar_anonymous.png", "objectType" -> "QuestionSet", "identifier" -> "do_123", "status" -> "Live", "identifier" -> "do_123"), Map("identifier" -> "do_234", "name" -> "Children-1", "objectType" -> "Question", "media" -> ScalaJsonUtil.serialize(List(media))), Map("identifier" -> "do_345", "name" -> "Children-2", "objectType" -> "Question"))
  override def getDataForEcar(obj: ObjectData): Option[List[Map[String, AnyRef]]] = Some(testObj)
}
