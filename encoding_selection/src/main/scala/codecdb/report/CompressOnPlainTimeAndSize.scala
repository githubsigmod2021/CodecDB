package codecdb.report

import java.io.{FileOutputStream, PrintWriter}

import codecdb.dataset.feature.compress.{ParquetCompressFileSize, ParquetCompressTimeUsage, ScanCompressedTimeUsage}
import codecdb.dataset.feature.resource.{EncTimeUsage, ParquetEncFileSize, ScanTimeUsage}
import codecdb.dataset.persist.jpa.{ColumnWrapper, JPAPersistence}
import codecdb.model.DataType
import edu.uchicago.cs.encsel.dataset.feature.compress.ScanCompressedTimeUsage
import edu.uchicago.cs.encsel.dataset.feature.resource.ParquetEncFileSize
import edu.uchicago.cs.encsel.dataset.persist.jpa.JPAPersistence

import scala.collection.JavaConverters._


object CompressOnPlainTimeAndSize extends App {

  val sql = "SELECT c FROM Column c where c.parentWrapper IS NULL AND c.dataType = :dt"

  val em = new JPAPersistence().em
  val dts = Array(DataType.INTEGER, DataType.STRING)

  val fTypes = Array(
    EncTimeUsage.featureType, EncTimeUsage.featureType,
    ScanTimeUsage.featureType, ScanTimeUsage.featureType
  )
  val fixedFtypes = Array(
    ParquetCompressFileSize.featureType,
    ParquetCompressTimeUsage.featureType, ParquetCompressTimeUsage.featureType,
    ScanCompressedTimeUsage.featureType, ScanCompressedTimeUsage.featureType,
    ParquetCompressFileSize.featureType,
    ParquetCompressTimeUsage.featureType, ParquetCompressTimeUsage.featureType,
    ScanCompressedTimeUsage.featureType, ScanCompressedTimeUsage.featureType)

  val names = Array("_wctime", "_cputime",
    "_wallclock", "_cpu")

  val fixedNames = Array(
    "PLAIN_GZIP_file_size",
    "PLAIN_GZIP_wctime", "PLAIN_GZIP_cputime",
    "PLAIN_GZIP_wallclock", "PLAIN_GZIP_cpu",
    "PLAIN_LZO_file_size",
    "PLAIN_LZO_wctime", "PLAIN_LZO_cputime",
    "PLAIN_LZO_wallclock", "PLAIN_LZO_cpu"
  )

  val colheaders = Array("plain_size", "enc_size",
    "gen_wc", "gen_cpu", "scan_wc", "scan_cpu",
    "gzip_size", "gzip_gen_wc", "gzip_gen_cpu", "gzip_scan_wc", "gzip_scan_cpu",
    "lzo_size", "lzo_gen_wc", "lzo_gen_cpu", "lzo_scan_wc", "lzo_scan_cpu",
  ).mkString(",")

  for (dt <- dts) {
    val output = new PrintWriter(new FileOutputStream("%s_compress_on_plain.csv".format(dt.name())))
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

        val fixedValues = fixedFtypes.zip(fixedNames).flatMap(pair => {
          col.findFeature(pair._1, pair._2)
        }).map(_.value)

        if (values.length == names.length && fixedValues.length == fixedNames.length) {
          output.println((firsttwo ++ values ++ fixedValues).mkString(","))
        }
      }
    })

    output.close()
  }
}
