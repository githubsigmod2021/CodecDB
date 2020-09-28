package codecdb.app

import java.io.File
import java.net.URI
import java.nio.file.Files
import java.nio.file.Paths

import codecdb.dataset.DataCollector
import codecdb.dataset.persist.DummyPersistence

import scala.collection.mutable.ArrayBuffer
import scala.io.Source
import org.junit.Assert.assertEquals
import org.junit.Assert.assertFalse
import org.junit.Assert.assertTrue
import org.junit.Test
import org.junit.BeforeClass

class DataCollectorForTest extends DataCollector {

  def isDoneForTest(source: URI) = {
    isDone(source)
  }
  def markDoneForTest(source: URI) = {
    markDone(source)
  }
  override def collect(source: URI) = {
    this.synchronized {
      scanned += source
    }
  }

  var scanned = new ArrayBuffer[URI]()
}

object DataCollectorTest {
  @BeforeClass
  def deleteFile(): Unit = {
    Files.deleteIfExists(Paths.get(new File("src/test/resource/filefmt/content.csv.done").toURI))
  }
}

class DataCollectorTest {

  @Test
  def testDone(): Unit = {
    val srcpath = new File("src/test/resource/filefmt/test_columner.csv").toPath
    val srcdone = new File("src/test/resource/filefmt/test_columner.csv.done").toPath
    val srcuri = srcpath.toUri
    Files.deleteIfExists(srcdone)

    val dc = new DataCollectorForTest()
    assertFalse(dc.isDoneForTest(srcuri))
    dc.markDoneForTest(srcuri)
    assertTrue(dc.isDoneForTest(srcuri))
  }

  @Test
  def testCollect(): Unit = {
    val dc = new DataCollector
    val dp = new DummyPersistence
    dc.persistence = dp

    //    Files.deleteIfExists(Paths.get(new File("src/test/resource/content.csv.done").toURI))

    dc.collect(new File("src/test/resource/filefmt/content.csv").toURI)
    //    assertTrue(Files.exists(Paths.get(new File("src/test/resource/content.csv.done").toURI)))

    val columns = dp.load()
    assertEquals(5, columns.size)
    columns.foreach { col =>
      {
        assertEquals(7, Source.fromFile(col.colFile).getLines().size)
      }
    }
  }

  @Test
  def testScan(): Unit = {
    val dc = new DataCollectorForTest

    dc.scan(new File("src/test/resource/scan_folder").toURI)

    assertEquals(5, dc.scanned.size)
    val fileNames = dc.scanned.map {
      Paths.get(_).getFileName.toString
    }

    assertTrue(fileNames.contains("c.json"))
    assertTrue(fileNames.contains("a.csv"))
    assertTrue(fileNames.contains("b.csv"))
    assertTrue(fileNames.contains("w.tsv"))
    assertTrue(fileNames.contains("n.txt"))
  }
}