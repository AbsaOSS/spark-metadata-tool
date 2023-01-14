package za.co.absa.spark_metadata_tool

import org.scalamock.scalatest.MockFactory
import org.scalatest.{EitherValues, OptionValues}
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

class DataToolSpec extends AnyFlatSpec with Matchers with OptionValues with EitherValues with MockFactory {

  "listDatafiles" should "list all data files in directory" in {

  }

  it should "list only properly formatted files" in {

  }

  it should "list files in ascending order" in {

  }

  "listDatafilesUpToPart" should "list only datafiles up to part number" in {

  }

  it should "list data files in ascending order" in {

  }

  it should "fail when some of required files are missing" in {

  }

  it should "fail when present with part duplicities" in {

  }
}
