package codecdb.report

import java.io.{FileOutputStream, PrintWriter}

import codecdb.dataset.persist.jpa.JPAPersistence
import codecdb.model.{DataType, FloatEncoding, IntEncoding, LongEncoding, StringEncoding}
import edu.uchicago.cs.encsel.model._

import scala.collection.JavaConverters._

object CompressTimeAndSize extends App {

  val scan_time_sql = "select\n  file_size.value fs,\n  scan_time.value wc,\n  scan_cpu.value cput,\n  scan_user.value ut\n    " +
    "from\n  col_data cd\n    join\n  feature file_size " +
    "ON file_size.col_id = cd.id\n  and file_size.type = 'CompressEncFileSize'\n  and file_size.name = ?\n  " +
    "join\n  feature scan_time ON scan_time.col_id = cd.id\n  and scan_time.type = 'ScanTimeUsage'\n  and scan_time.name = ?\n  " +
    "join\n  feature scan_cpu ON scan_cpu.col_id = cd.id\n  and scan_cpu.type = 'ScanTimeUsage'\n  and scan_cpu.name = ?\n  " +
    "join\n  feature scan_user ON scan_user.col_id = cd.id\n  and scan_user.type = 'ScanTimeUsage'\n  and scan_user.name = ?\n" +
    "where cd.data_type = ?"

  val gen_time_sql = "select\n  file_size.value fs,\n  scan_time.value wc,\n  scan_cpu.value cput,\n  scan_user.value ut\n    " +
    "from\n  col_data cd\n    join\n  feature file_size " +
    "ON file_size.col_id = cd.id\n  and file_size.type = 'CompressEncFileSize'\n  and file_size.name = ?\n  " +
    "join\n  feature scan_time ON scan_time.col_id = cd.id\n  and scan_time.type = 'CompressTimeUsage'\n  and scan_time.name = ?\n  " +
    "join\n  feature scan_cpu ON scan_cpu.col_id = cd.id\n  and scan_cpu.type = 'CompressTimeUsage'\n  and scan_cpu.name = ?\n  " +
    "join\n  feature scan_user ON scan_user.col_id = cd.id\n  and scan_user.type = 'CompressTimeUsage'\n  and scan_user.name = ?\n" +
    "where cd.data_type = ?"

  def scanTimeData: Unit = {
    val dts = Array((DataType.STRING, classOf[StringEncoding]),
      (DataType.INTEGER, classOf[IntEncoding]),
      (DataType.DOUBLE, classOf[FloatEncoding]),
      (DataType.LONG, classOf[LongEncoding]))
    val codecs = Array("GZIP", "LZO")

    val em = new JPAPersistence().em

    for (dt <- dts; codec <- codecs) {
      val encodings = dt._2.getEnumConstants
      for (enc <- encodings) {

        val fsname = "%s_%s_file_size".format(enc.name(), codec)
        val wcname = "%s_%s_wallclock".format(enc.name(), codec)
        val cputname = "%s_%s_cpu".format(enc.name(), codec)
        val usertname = "%s_%s_user".format(enc.name(), codec)
        val dtname = dt._1.name()

        val result = em.createNativeQuery(scan_time_sql).setParameter(1, fsname).setParameter(2, wcname)
          .setParameter(3, cputname).setParameter(4, usertname).setParameter(5, dtname)
          .getResultList.asScala

        val output = new PrintWriter(new FileOutputStream("SCAN_%s_%s_%s.csv".format(dt._1.name(), enc.name(), codec)))

        result.foreach(record => {
          output.println(record.asInstanceOf[Array[AnyRef]].mkString(","))
        })

        output.close
      }
    }
  }

  def genTimeData: Unit = {
    val dts = Array((DataType.STRING, classOf[StringEncoding]),
      (DataType.INTEGER, classOf[IntEncoding]),
      (DataType.DOUBLE, classOf[FloatEncoding]),
      (DataType.LONG, classOf[LongEncoding]))
    val codecs = Array("GZIP", "LZO")

    val em = new JPAPersistence().em

    for (dt <- dts; codec <- codecs) {
      val encodings = dt._2.getEnumConstants
      for (enc <- encodings) {

        val fsname = "%s_%s_file_size".format(enc.name(), codec)
        val wcname = "%s_%s_wctime".format(enc.name(), codec)
        val cputname = "%s_%s_cputime".format(enc.name(), codec)
        val usertname = "%s_%s_usertime".format(enc.name(), codec)
        val dtname = dt._1.name()

        val result = em.createNativeQuery(gen_time_sql).setParameter(1, fsname).setParameter(2, wcname)
          .setParameter(3, cputname).setParameter(4, usertname).setParameter(5, dtname)
          .getResultList.asScala

        val output = new PrintWriter(new FileOutputStream("GEN_%s_%s_%s.csv".format(dt._1.name(), enc.name(), codec)))

        result.foreach(record => {
          output.println(record.asInstanceOf[Array[AnyRef]].mkString(","))
        })

        output.close
      }
    }
  }
}
