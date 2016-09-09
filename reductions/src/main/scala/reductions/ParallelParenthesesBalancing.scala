package reductions

import scala.annotation._
import org.scalameter._
import common._

object ParallelParenthesesBalancingRunner {

  @volatile var seqResult = false

  @volatile var parResult = false

  val standardConfig = config(
    Key.exec.minWarmupRuns -> 40,
    Key.exec.maxWarmupRuns -> 80,
    Key.exec.benchRuns -> 120,
    Key.verbose -> true
  ) withWarmer(new Warmer.Default)

  def main(args: Array[String]): Unit = {
    val length = 10000000
    val chars = new Array[Char](length)
    val threshold = 100000
    val seqtime = standardConfig measure {
      seqResult = ParallelParenthesesBalancing.balance(chars)
    }
    println(s"sequential result = $seqResult")
    println(s"sequential balancing time: $seqtime ms")

    val fjtime = standardConfig measure {
      parResult = ParallelParenthesesBalancing.parBalance(chars, threshold)
    }
    println(s"parallel result = $parResult")
    println(s"parallel balancing time: $fjtime ms")
    println(s"speedup: ${seqtime / fjtime}")
  }
}

object ParallelParenthesesBalancing {

  /** Returns `true` iff the parentheses in the input `chars` are balanced.
   */
  def count(char:Char):Int ={
    if (char == '(') 1
    else if (char == ')') -1
    else 0
  }

  def balance(chars: Array[Char]): Boolean = {
    /*def balanceIter(sum:Int,chars:Array[Char]):Boolean={
      if (chars.isEmpty) sum==0
      else {
        val total=sum+count(chars.head)
        if(total<0) false
        //chars.tail a new array
        else balanceIter(total,chars.tail)
      }
    }*/
    def balanceIter(sum:Int,from:Int,to:Int):Boolean={
      if (from==to) sum==0
      else {
        val total=sum+count(chars(from))
        if(total<0) false
        else balanceIter(total,from+1,to)
      }
    }
    balanceIter(0,0,chars.length)
  }

  /** Returns `true` iff the parentheses in the input `chars` are balanced.
   */
  def parBalance(chars: Array[Char], threshold: Int): Boolean = {

    def traverse(idx: Int, until: Int, arg1: Int, arg2: Int):(Int,Int)= {
      if(idx==until) (arg1,arg2)
      else if (chars(idx) == '(') traverse(idx+1,until,arg1,arg2+1)
         else if (chars(idx) == ')') {
        if (arg2-1<0) traverse(idx+1,until,arg1-1,arg2)
        else traverse(idx+1,until,arg1,arg2-1)
      }
             else traverse(idx+1,until,arg1,arg2)
    }

    def reduce(from: Int, until: Int):(Int,Int) = {
      if(until-from<=threshold) traverse(from,until,0,0)
      else {
        val mid=(until+from)/2
        val ((a,b),(c,d))=parallel(reduce(from,mid),reduce(mid,until))
        val merge=b+c
        if(merge<0) (a+merge,d) else (a,d+merge)
      }
    }

    reduce(0, chars.length) == (0,0)
  }

  // For those who want more:
  // Prove that your reduction operator is associative!

}
