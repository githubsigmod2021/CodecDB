package codecdb.dataset.persist.file

import java.io.File
import java.nio.file.Files

import codecdb.dataset.column.Column
import codecdb.model.DataType

import scala.collection.mutable.ArrayBuffer
import org.junit.Assert.assertEquals
import org.junit.Before
import org.junit.Test

class FilePersistenceTest {

  @Before
  def removeDataFile = {
    val path = new File("storage.dat").toPath
    if (Files.exists(path))
      Files.delete(path)
  }

  @Test
  def testSave(): Unit = {
    val fp = new FilePersistence

    var dl = new ArrayBuffer[Column]()
    dl += new Column(null, 0, "", DataType.STRING)
    dl += new Column(null, 1, "", DataType.STRING)

    fp.save(dl)
    
    fp.save(dl)

    val res = fp.load()
    assertEquals(2, res.size)
  }

  @Test
  def testLoad(): Unit = {
    val fp = new FilePersistence

    fp.load()
  }

  @Test
  def testClean(): Unit = {
    val fp = new FilePersistence

    var dl = new ArrayBuffer[Column]()
    dl += new Column(null, 0, "", DataType.STRING)
    dl += new Column(null, 1, "", DataType.STRING)

    fp.save(dl)

    var dl2 = fp.load()
    assertEquals(2, dl2.size)

    fp.clean()

    dl2 = fp.load()
    assertEquals(0, dl2.size)
  }
}