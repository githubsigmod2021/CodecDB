package codecdb.app.encoding

import java.io.File
import java.util

import codecdb.classify.NNPredictor
import codecdb.dataset.column.ColumnReaderFactory
import codecdb.dataset.feature.{Features, Filter}
import codecdb.dataset.schema.Schema
import codecdb.model.{DataType, IntEncoding, StringEncoding}
import codecdb.parquet.{EncContext, ParquetWriterHelper}
import edu.uchicago.cs.encsel.dataset.feature.Features
import edu.uchicago.cs.encsel.parquet.ParquetWriterHelper
import edu.uchicago.cs.encsel.model.IntEncoding
import org.apache.parquet.column.Encoding
import org.apache.parquet.schema.PrimitiveType.PrimitiveTypeName

import scala.collection.JavaConversions._

object ParquetEncoder extends App {

  val inputFile = new File(args(0)).toURI
  val schemaFile = new File(args(1)).toURI
  val outputFile = new File(args(2)).toURI

  val intModel = args(3)
  val stringModel = args(4)

  val sizeLimit = args.length match {
    case ge6 if ge6 >= 6 => args(5).toInt
    case _ => 1000000 // Default 1M
  }

  val schema = Schema.fromParquetFile(schemaFile)
  val parquetSchema = SchemaParser.toParquetSchema(schema)

  // Split file into columns
  val columnReader = ColumnReaderFactory.getColumnReader(inputFile)
  val columns = columnReader.readColumn(inputFile, schema)

  // See <code>edu.uchicago.cs.encsel.dataset.feature.Features
  val validFeatureIndex = Array(2, 3, 4, 5, 6, 8, 9, 10,
    11, 13, 15, 16, 18, 20,
    21, 23, 25, 26, 28)

  // Initialize predictor
  val intPredictor = new NNPredictor(intModel, validFeatureIndex.length)
  val stringPredictor = new NNPredictor(stringModel, validFeatureIndex.length)

  // For each column, extract features and run encoding selector
  val colEncodings = columns.map(col => {
    col.dataType match {
      case DataType.INTEGER => {
        val allFeatures = Features.extract(col, Filter.sizeFilter(sizeLimit), "temp_")
          .map(_.value).toArray
        val features = validFeatureIndex.map(allFeatures(_))
        intPredictor.predict(features)
      }
      case DataType.STRING => {
        val allFeatures = Features.extract(col, Filter.sizeFilter(sizeLimit), "temp_")
          .map(_.value).toArray
        val features = validFeatureIndex.map(allFeatures(_))
        stringPredictor.predict(features)
      }
      case _ => {
        -1
      }
    }
  })
  // Setup encoding parameters
  val encodingMap = new util.HashMap[String, Encoding]()
  EncContext.encoding.set(encodingMap)
  val contextMap = new util.HashMap[String, Array[AnyRef]]()
  EncContext.context.set(contextMap)

  colEncodings.zip(parquetSchema.getColumns).foreach(pair => {
    val encoding = pair._1
    val coldesc = pair._2

    coldesc.getType match {
      case PrimitiveTypeName.INT32 => {
        encodingMap.put(coldesc.toString, parquetIntEncoding(encoding))
        // TODO Determine bit size for integer, here hard code as a sample
        contextMap.put(coldesc.toString, Array[AnyRef]("16", "1024"))
      }
      case PrimitiveTypeName.BINARY => {
        encodingMap.put(coldesc.toString, parquetStringEncoding(encoding))
      }
      case _ => {}
    }
  })

  // Invoke Parquet Writer
  // TODO use CSV parser to parse file
  ParquetWriterHelper.write(inputFile, parquetSchema, outputFile, ",", false)

  def parquetStringEncoding(enc: Int): Encoding = {
    IntEncoding.values()(enc) match {
      case IntEncoding.PLAIN => {
        Encoding.PLAIN
      }
      case IntEncoding.BP => {
        Encoding.BIT_PACKED
      }
      case IntEncoding.RLE => {
        Encoding.RLE
      }
      case IntEncoding.DELTABP => {
        Encoding.DELTA_BINARY_PACKED
      }
      case IntEncoding.DICT => {
        Encoding.PLAIN_DICTIONARY
      }
      case _ => {
        throw new IllegalArgumentException
      }
    }
  }

  def parquetIntEncoding(enc: Int): Encoding = {
    StringEncoding.values()(enc) match {
      case StringEncoding.PLAIN => Encoding.PLAIN
      case StringEncoding.DELTA => Encoding.DELTA_BYTE_ARRAY
      case StringEncoding.DELTAL => Encoding.DELTA_LENGTH_BYTE_ARRAY
      case StringEncoding.DICT => Encoding.PLAIN_DICTIONARY
      case _ => throw new IllegalArgumentException
    }
  }
}
