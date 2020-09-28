package codecdb.report

import codecdb.dataset.column.Column
import codecdb.dataset.feature.compress.ParquetCompressFileSize
import codecdb.dataset.persist.jpa.{ColumnWrapper, JPAPersistence}
import codecdb.model.DataType
import edu.uchicago.cs.encsel.dataset.persist.jpa.{ColumnWrapper, JPAPersistence}

import scala.collection.JavaConverters._

object RunCompressOnSubAttrs extends App {

  val sql = "SELECT c FROM Column c WHERE c.parentWrapper IS NULL AND c.dataType = :dt AND c.id >= :id ORDER BY c.id ASC"
  val childSql = "SELECT c FROM Column c WHERE c.parentWrapper = :parent"

  val persist = new JPAPersistence
  val em = persist.em
  val start = args(0).toInt
  var counter = 0
  em.createQuery(sql, classOf[ColumnWrapper])
    .setParameter("id", start).setParameter("dt", DataType.STRING).getResultList.asScala.foreach(col => {
    val ben = col.getInfo("subattr_benefit")
    if (ben > 0 && ben < 0.95) {
      counter += 1
      println("%d:%d".format(counter, col.id))
      val childrenCols = getChildren(col)
      childrenCols.foreach(child => {
        val features = ParquetCompressFileSize.extract(child)
        child.replaceFeatures(features)
        persist.save(Seq(child))
      })
    }
  })

  def getChildren(col: Column): Seq[Column] = {
    em.createQuery(childSql, classOf[ColumnWrapper]).setParameter("parent", col).getResultList.asScala
  }
}
