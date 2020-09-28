package codecdb.report

import java.io.{FileOutputStream, PrintWriter}

import codecdb.dataset.feature.resource.{EncTimeUsage, ParquetEncFileSize, ScanTimeUsage}
import codecdb.dataset.persist.jpa.{ColumnWrapper, JPAPersistence}
import codecdb.model.DataType
import edu.uchicago.cs.encsel.dataset.feature.resource.ParquetEncFileSize
import edu.uchicago.cs.encsel.dataset.persist.jpa.JPAPersistence

import scala.collection.JavaConverters._


object BestEncTimeAndSize extends App {

  val sql = "SELECT c FROM Column c where c.parentWrapper IS NULL AND c.dataType = :dt"

  val em = new JPAPersistence().em
  val dts = Array(DataType.INTEGER, DataType.STRING)

  val fTypes = Array(
    EncTimeUsage.featureType, EncTimeUsage.featureType,
    ScanTimeUsage.featureType, ScanTimeUsage.featureType/*,
    ParquetCompressFileSize.featureType,
    ParquetCompressTimeUsage.featureType, ParquetCompressTimeUsage.featureType,
    ScanCompressedTimeUsage.featureType, ScanCompressedTimeUsage.featureType,
    ParquetCompressFileSize.featureType,
    ParquetCompressTimeUsage.featureType, ParquetCompressTimeUsage.featureType,
    ScanCompressedTimeUsage.featureType, ScanCompressedTimeUsage.featureType*/
  )
  val names = Array("_wctime", "_cputime",
    "_wallclock", "_cpu"/*,
    "_GZIP_file_size",
    "_GZIP_wctime", "_GZIP_cputime",
    "_GZIP_wallclock", "_GZIP_cpu",
    "_LZO_file_size",
    "_LZO_wctime", "_LZO_cputime",
    "_LZO_wallclock", "_LZO_cpu"*/
  )

  val colheaders = Array("plain_size", "enc_size",
    "gen_wc", "gen_cpu", "scan_wc", "scan_cpu"/*,
    "gzip_size", "gzip_gen_wc", "gzip_gen_cpu", "gzip_scan_wc", "gzip_scan_cpu",
    "lzo_size", "lzo_gen_wc", "lzo_gen_cpu", "lzo_scan_wc", "lzo_scan_cpu"*/).mkString(",")

  for (dt <- dts) {
    val output = new PrintWriter(new FileOutputStream("%s_enc_scan.csv".format(dt.name())))
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
        if (values.length == names.length) {
          output.println((firsttwo ++ values).mkString(","))
        }
      }
    })

    output.close()
  }
}
