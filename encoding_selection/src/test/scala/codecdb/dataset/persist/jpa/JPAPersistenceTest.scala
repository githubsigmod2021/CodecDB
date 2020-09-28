package codecdb.dataset.persist.jpa

import java.io.File

import codecdb.dataset.column.Column
import codecdb.dataset.feature.Feature
import codecdb.model.DataType
import org.junit.Assert._
import org.junit.{Before, Test}

import scala.collection.JavaConversions._

class JPAPersistenceTest {

  @Before
  def cleanSchema: Unit = {
    val em = JPAPersistence.emf.createEntityManager()

    em.getTransaction.begin()
    em.createNativeQuery("DELETE FROM col_info;").executeUpdate()
    em.createNativeQuery("DELETE FROM col_pattern;").executeUpdate()
    em.createNativeQuery("DELETE FROM feature;").executeUpdate()
    em.createNativeQuery("DELETE FROM col_data;").executeUpdate()

    em.createNativeQuery("INSERT INTO col_data (id, file_uri, idx, name, data_type, origin_uri, parent_id) VALUES (?, ?, ?, ?, ?, ?, ?);")
      .setParameter(1, 20)
      .setParameter(2, "file:/home/harper/IdeaProjects/enc-selector/aab")
      .setParameter(3, 5)
      .setParameter(4, "a")
      .setParameter(5, "STRING")
      .setParameter(6, "file:/home/harper/IdeaProjects/enc-selector/ccd")
      .setParameter(7, null).executeUpdate()
    em.createNativeQuery("INSERT INTO feature(col_id,type, name, value) VALUES(?, ?, ?, ?);").setParameter(1, 20)
      .setParameter(2, "P").setParameter(3, "M").setParameter(4, 2.4).executeUpdate()

    em.getTransaction.commit()
    em.getEntityManagerFactory.getCache.evictAll()
    em.close()
  }

  @Test
  def testSaveNew: Unit = {
    val jpa = new JPAPersistence

    val col0 = new Column(new File("xmp").toURI, 9, "tpq", DataType.INTEGER)
    col0.colFile = new File("wpt").toURI
    col0.parent = jpa.find(20)

    val col1 = new Column(new File("dd").toURI, 3, "m", DataType.INTEGER)
    col1.colFile = new File("tt").toURI
    col1.parent = col0

    col1.features = new java.util.HashSet[Feature]

    val fea1 = new Feature("W", "A", 3.5)

    col1.features ++= Array(fea1)

    jpa.save(Array(col1))

    val cols = jpa.load()

    assertEquals(3, cols.size)

    cols.foreach(col => {
      col.colIndex match {
        case 3 => {
          assertEquals(DataType.INTEGER, col.dataType)
          assertEquals("m", col.colName)
          assertEquals(9, col.parent.colIndex)
          val feature = col.features.iterator.next
          assertEquals("W", feature.featureType)
          assertEquals("A", feature.name)
          assertEquals(3.5, feature.value, 0.01)
        }
        case 5 => {
          assertEquals(DataType.STRING, col.dataType)
          assertEquals("a", col.colName)
          val feature = col.features.iterator.next
          assertEquals("P", feature.featureType)
          assertEquals("M", feature.name)
          assertEquals(2.4, feature.value, 0.01)
        }
        case 9 => {
          assertEquals(DataType.INTEGER, col.dataType)
          assertEquals("tpq", col.colName)
          assertEquals(20, col.parent.colIndex)
        }
      }
    })
  }

  @Test
  def testSaveMerge: Unit = {
    val jpa = new JPAPersistence
    val cols = jpa.load().toArray
    assertEquals(1, cols.length)

    cols(0).features += new Feature("T", "PP", 3.25)
    jpa.save(cols)

    assertEquals(1, cols.length)
    val features = cols(0).features.toList
    assertEquals(2, features.size)
    assertEquals("PP", features(0).name)
    assertEquals("M", features(1).name)
  }

  @Test
  def testUpdate: Unit = {
    val jpa = new JPAPersistence

    val col1 = new ColumnWrapper()
    col1.origin = new File("dd").toURI
    col1.colIndex = 3
    col1.colName = "m"
    col1.dataType = DataType.INTEGER
    col1.id = 20
    col1.colFile = new File("tt").toURI

    col1.features = new java.util.HashSet[Feature]

    val fea1 = new Feature("W", "A", 3.5)

    col1.features ++= Array(fea1)

    jpa.save(Array[Column](col1))

    val cols = jpa.load().toArray

    assertEquals(1, cols.length)
    val col = cols(0)
    assertEquals(1, col.features.size())
  }

  @Test
  def testLoad: Unit = {
    val jpa = new JPAPersistence
    val cols = jpa.load().toArray

    assertEquals(1, cols.length)
    val col = cols(0)
    assertEquals(DataType.STRING, col.dataType)
    assertEquals(5, col.colIndex)
    assertEquals("a", col.colName)
  }

  @Test
  def testGetInfo: Unit = {
    val jpa = new JPAPersistence
    val col = jpa.find(20)
    col.putInfo("abc", 12312.52)

    jpa.save(Seq(col))

    val col2 = jpa.find(20)
    assertEquals(12312.52, col.getInfo("abc"), 0.01);
  }

}