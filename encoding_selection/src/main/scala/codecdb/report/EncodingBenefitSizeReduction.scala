package codecdb.report

import codecdb.classify.AbadiDecisionTree
import codecdb.dataset.column.Column
import codecdb.dataset.feature.classify.{AvgRunLength, Distinct}
import codecdb.dataset.feature.resource.ParquetEncFileSize
import codecdb.dataset.persist.jpa.{ColumnWrapper, JPAPersistence}
import codecdb.model.DataType
import edu.uchicago.cs.encsel.dataset.feature.classify.Distinct
import edu.uchicago.cs.encsel.dataset.persist.jpa.JPAPersistence

import scala.collection.JavaConverters._
import scala.io.Source

object EncodingBenefitSizeReduction extends App {

  val sql = "SELECT c FROM Column c WHERE c.dataType = :dt and c.parentWrapper IS NULL"
  val em = new JPAPersistence().em
  val types = Array(DataType.STRING, DataType.INTEGER)

  var plainInt = 0L
  // Abadi, Parquet, KNN, DTree, Our
  val intResult = Array.fill(5)(0L)

  val intmap =
    Source.fromFile("src/main/matlab/classification/int_class.csv").getLines
      .map(_.split(",")).map(p => (p(0).toInt, (p(1).toInt, p(2).toInt))).toMap
  val intTypeMap = Array((0 -> "PLAIN"), (1 -> "DICT"), (2 -> "BP"), (3 -> "RLE"), (4 -> "DELTABP")).toMap

  val strmap =
    Source.fromFile("src/main/matlab/classification/str_class.csv").getLines
      .map(_.split(",")).map(p => (p(0).toInt, (p(1).toInt, p(2).toInt))).toMap

  val strTypeMap = Array((0 -> "PLAIN"), (1 -> "DICT"), (2 -> "DELTA"), (3 -> "DELTAL"), (4 -> "BITVECTOR")).toMap

  val abadi = new AbadiDecisionTree
  // Integer
  em.createQuery(sql, classOf[ColumnWrapper]).setParameter("dt", DataType.INTEGER).getResultList.asScala.foreach(col => {

    if (intmap.contains(col.id)) {

      val abadiDecision = abadi.classifyInt(
        col.findFeature(AvgRunLength.featureType, "value").get,
        col.findFeature(Distinct.featureType, "count").get,
        null
      ).name()
      intResult(0) += getSize(col, abadiDecision)

      var parquet = getSize(col, "DICT")
      if (parquet <= 0)
        parquet = getSize(col, "DELTABP")
      intResult(1) += parquet

      val knn = getSize(col, intTypeMap.get(intmap.get(col.id).get._1).get)
      intResult(2) += knn
      val tree = getSize(col, intTypeMap.get(intmap.get(col.id).get._2).get)
      intResult(3) += tree

      val best = getBest(col)
      intResult(4) += best

      plainInt += getSize(col, "PLAIN")
    }
  })

  println(intResult.mkString(","))
  println(plainInt)
  // String
  val strResult = Array.fill(5)(0L)
  var plainStr = 0L

  em.createQuery(sql, classOf[ColumnWrapper]).setParameter("dt", DataType.STRING).getResultList.asScala.foreach(col => {

    if (strmap.contains(col.id)) {

      val abadiDecision = abadi.classifyString(
        col.findFeature(AvgRunLength.featureType, "value").get,
        col.findFeature(Distinct.featureType, "count").get,
        null
      ).name()
      strResult(0) += getSize(col, abadiDecision)

      var parquet = getSize(col, "DICT")
      if (parquet <= 0)
        parquet = getSize(col, "DELTA")
      strResult(1) += parquet

      val knn = getSize(col, strTypeMap.get(strmap.get(col.id).get._1).get)
      strResult(2) += knn
      val tree = getSize(col, strTypeMap.get(strmap.get(col.id).get._2).get)
      strResult(3) += tree

      val best = getBest(col)
      strResult(4) += best

      plainStr += getSize(col, "PLAIN")
    }
  })

  println(strResult.mkString(","))
  println(plainStr)

  def getSize(col: Column, name: String): Long = {
    col.findFeature(ParquetEncFileSize.featureType, "%s_file_size".format(name)).get.value.toLong
  }

  def getBest(col: Column): Long = {
    col.findFeatures(ParquetEncFileSize.featureType).filter(_.value > 0).minBy(_.value).value.toLong
  }
}
