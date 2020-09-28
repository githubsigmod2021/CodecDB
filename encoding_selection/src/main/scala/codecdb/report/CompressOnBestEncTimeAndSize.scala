package codecdb.report

import java.io.{FileOutputStream, PrintWriter}

import codecdb.dataset.feature.classify.{Distinct, Entropy, Length, Sortness, Sparsity}
import codecdb.dataset.feature.compress.{ParquetCompressFileSize, ParquetCompressTimeUsage, ScanCompressedTimeUsage}
import codecdb.dataset.feature.resource.{EncTimeUsage, ParquetEncFileSize, ScanTimeUsage}
import codecdb.dataset.persist.jpa.{ColumnWrapper, JPAPersistence}
import codecdb.model.DataType
import edu.uchicago.cs.encsel.dataset.feature._
import edu.uchicago.cs.encsel.dataset.feature.classify._
import edu.uchicago.cs.encsel.dataset.feature.compress.ScanCompressedTimeUsage
import edu.uchicago.cs.encsel.dataset.feature.resource.ParquetEncFileSize
import edu.uchicago.cs.encsel.dataset.persist.jpa.JPAPersistence

import scala.collection.JavaConverters._


object CompressOnBestEncTimeAndSize extends App {

  val sql = "SELECT c FROM Column c where c.parentWrapper IS NULL AND c.dataType = :dt"

  val em = new JPAPersistence().em
  val dts = Array(DataType.INTEGER, DataType.STRING)

  val fTypes = Array(
    EncTimeUsage.featureType, EncTimeUsage.featureType,
    ScanTimeUsage.featureType, ScanTimeUsage.featureType,
    ParquetCompressFileSize.featureType,
    ParquetCompressTimeUsage.featureType, ParquetCompressTimeUsage.featureType,
    ScanCompressedTimeUsage.featureType, ScanCompressedTimeUsage.featureType,
    ParquetCompressFileSize.featureType,
    ParquetCompressTimeUsage.featureType, ParquetCompressTimeUsage.featureType,
    ScanCompressedTimeUsage.featureType, ScanCompressedTimeUsage.featureType)

  val names = Array("_wctime", "_cputime",
    "_wallclock", "_cpu",
    "_GZIP_file_size",
    "_GZIP_wctime", "_GZIP_cputime",
    "_GZIP_wallclock", "_GZIP_cpu",
    "_LZO_file_size",
    "_LZO_wctime", "_LZO_cputime",
    "_LZO_wallclock", "_LZO_cpu"
  )

  val classifyFeatures = Array(
    // Add features to predict whether compression can benefit more
    Distinct.featureType,
    Length.featureType, Length.featureType,
    Sparsity.featureType,
    Entropy.featureType, Entropy.featureType, Entropy.featureType,
    Sortness.featureType, Sortness.featureType, Sortness.featureType,
    Sortness.featureType, Sortness.featureType, Sortness.featureType
  )

  val classifyNames = Array(
    "ratio",
    "mean", "variance",
    "valid_ratio",
    "line_mean", "line_var", "total",
    "ivpair_50", "kendalltau_50", "spearmanrho_50", "ivpair_100", "kendalltau_100", "spearmanrho_100"
  )

  val colheaders = Array("plain_size", "enc_size",
    "gen_wc", "gen_cpu", "scan_wc", "scan_cpu",
    "gzip_size", "gzip_gen_wc", "gzip_gen_cpu", "gzip_scan_wc", "gzip_scan_cpu",
    "lzo_size", "lzo_gen_wc", "lzo_gen_cpu", "lzo_scan_wc", "lzo_scan_cpu",
    "f1", "f2", "f3", "f4", "f5", "f6", "f7", "f8", "f9", "f10", "f11", "f12", "f13").mkString(",")

  for (dt <- dts) {
    val output = new PrintWriter(new FileOutputStream("%s_compress_on_enc.csv".format(dt.name())))
    output.println(colheaders)

    em.createQuery(sql, classOf[ColumnWrapper]).setParameter("dt", dt).getResultList.asScala.foreach(col => {
      val plain = col.findFeature(ParquetEncFileSize.featureType, "PLAIN_file_size")
      val encs = col.findFeatures(ParquetEncFileSize.featureType).filter(f => f.name != "BITVECTOR_file_size" && f.value > 0)
      if (plain.nonEmpty && encs.nonEmpty) {
        val bestEnc = encs.minBy(_.value)

        val firsttwo = Array(plain.get.value, bestEnc.value)

        val values = fTypes.zip(names).flatMap(pair => {
          val ft = pair._1
          val n = pair._2
          val rn = bestEnc.name.replace("_file_size", n)
          col.findFeature(ft, rn)
        }).map(_.value)

        val features = classifyFeatures.zip(classifyNames).flatMap(pair => {
          col.findFeature(pair._1, pair._2)
        }).map(_.value)

        if (values.length == names.length && features.length == classifyFeatures.length) {
          output.println((firsttwo ++ values ++ features).mkString(","))
        }
      }
    })

    output.close()
  }
}
