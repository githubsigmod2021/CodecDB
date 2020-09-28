package codecdb.report

import java.io.{FileOutputStream, PrintWriter}

import codecdb.dataset.feature.compress.ParquetCompressFileSize
import codecdb.dataset.feature.resource.{ParquetEncFileSize, ScanTimeUsage}
import codecdb.dataset.persist.jpa.{ColumnWrapper, JPAPersistence}
import codecdb.model.DataType
import edu.uchicago.cs.encsel.dataset.feature.resource.ParquetEncFileSize
import edu.uchicago.cs.encsel.dataset.persist.jpa.JPAPersistence

import scala.collection.JavaConverters._

object ScanOnPlainTimeAndSize extends App {

  val sql = "SELECT c FROM Column c where c.parentWrapper IS NULL AND c.dataType = :dt"

  val em = new JPAPersistence().em

  val intEncs = Array("PLAIN", "BP", "RLE", "DICT", "DELTABP")
  val strEncs = Array("PLAIN", "DICT", "DELTA", "DELTAL")
  val dblEncs = Array("PLAIN", "DICT")

  val dts = Array((DataType.INTEGER, intEncs), (DataType.STRING, strEncs), (DataType.DOUBLE, dblEncs)).toMap


  dts.foreach(dtpair => {
    val enc = dtpair._2
    val output = new PrintWriter(new FileOutputStream("%s_scan.csv".format(dtpair._1.name())))

    val header = enc.flatMap(e => Seq("%s_size".format(e), "%s_cpu".format(e)))
    output.println(header.mkString(","))

    em.createQuery(sql, classOf[ColumnWrapper])
      .setParameter("dt", dtpair._1).getResultList.asScala.foreach(col => {
      if(col.hasFeature(ParquetCompressFileSize.featureType)) {
        val values = enc.flatMap(e => {
          val fs = col.findFeature(ParquetEncFileSize.featureType, "%s_file_size".format(e))
          val cpu = col.findFeature(ScanTimeUsage.featureType, "%s_cpu".format(e))
          if (fs.nonEmpty && cpu.nonEmpty) {
            Iterable(
              fs.get, cpu.get
            )
          } else {
            Iterable()
          }
        }).map(_.value)
        if (values.length == enc.length * 2) {
          output.println(values.mkString(","))
        }
      }
    })
    output.close()
  })
}
