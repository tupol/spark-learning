package spark

/**
 * Created by olivertupran on 03/11/15.
 */
object Main extends App {

  val test = """a,   1  , 2  """
  val test2 = """a,1,2, {d, e, f}"""

  val pattern = """\s*(\S*),?[\s]*(\{(.*)\})?""".r
  val pattern1 = """(?:\s*)(\S*)(?:\s*,?\s*)(\{(.*)\})?""".r

  val rez = pattern1.findAllIn(test).toList

  println(s"[${rez(1)}]")

//  implicit object ResultOrdering extends Ordering[Option[Int]] {
//    def compare(a:Option[Int], b:Option[Int]) = {
//      val x = a.getOrElse(0)
//      val y = b.getOrElse(0)
//      x compareTo y
//    }
//  }
//
//  implicit object ResultOrdering extends Ordering[Option[Int]] {
//    def compare(a:Option[Int], b:Option[Int]) = {
//      a.getOrElse(0).toInt compareTo b.getOrElse(0).toInt
//    }
//  }
//
//  val op1: Option[Int] = Some(1)
//  val op2: Option[Int] = None
//
//
//  println(op1.getOrElse(0) - op2.getOrElse(0))
//
//  val arr = Array(op1, op2)
//  Sorting.quickSort(arr)(ResultOrdering)
//  println(arr.toList)



}