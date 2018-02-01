
//
//  OOP
//

// class for Rational numbers

/*
Primary vs. Auxiliary constructors
Operator overloading
Singleton objects
Inheritance through Traits

 */
class Rational(n: Int, d: Int){
  require(d != 0)

  def this(n: Int) = this(n, 1)

  override def toString = numer + "/" + denom

  private val g = gcd(n.abs, d.abs)

  private def gcd(a: Int, b: Int): Int =
    if (b == 0) a else gcd(b, a % b)

  val numer = n / g
  val denom = d / g

  def + (that: Rational): Rational =
    new Rational(
      numer * that.denom + that.numer * denom,
      denom * that.denom
    )

  def + (i: Int): Rational =
    new Rational(numer + i * denom, denom)

  def / (that: Rational): Rational =
    new Rational(numer * that.denom, denom * that.numer)

  def / (i: Int): Rational =
    new Rational(numer, denom * i)

}

val q1 = new Rational(2,3)
q1.toString

val q2 = new Rational(2)

val s1 = q1.+(q2)
val s2 = q1 + q2

// Singleton object
// exact the same name as class Rational
object Rational{

  val inf = "infinity"

  // use these methods/variables as if they are
  // static members of Rational class
  def divideByZero(x: Int): Unit = {
    println("Not defined " + x)
  }
}

println(Rational.inf)
val infT = Rational.inf

Rational.divideByZero(4)

// Inheritance
abstract class Animal {
  def speak
}

trait FourLeggedAnimal{
  def walk
  def run
}

trait WaggingTail {
  def startTail { println("tail started")}
  def stopTail { println("tail stopped")}
}

class Dog extends Animal with WaggingTail with FourLeggedAnimal {
  def speak { println("Dog says Bark")}
  def walk { println("Dog is walking")}
  def run { println("Dog is running")}
}

val obj_Dog = new Dog()
obj_Dog.speak
obj_Dog.walk
obj_Dog.run
obj_Dog.startTail
obj_Dog.stopTail


//
// Functions (function literals) and methods
//
(x: Int)=>x+1

val fun1 = (x: Int)=>x+1

fun1(2)

//
// Collections in Scala
//

// Array
val fiveInts = new Array[Int](5)
val fiveToOne = Array(5,4,3,2,1)
fiveToOne(2)

// Tuple
val tpl = (12, "This")
tpl._1

// Map
val words = List("the", "quick", "brown", "fox")
words.map(_.length)

// flatMap, returns a top level list
words.map(_.toList)
words.flatMap(_.toList)

// filter
List(1,2,3,4,5).filter(_%2 == 0)

//foreach
var sum = 0
List(1,2,3,4,5).foreach(sum += _)
sum

// First class functions

val increase = (x: Int) => x+1
increase(10)

val increase2 = (x: Int) => {
  println("Hi")
  x+2
}
increase2(2)


val maxFun = (x: Int, y: Int) => Math.max(x,y)
println(maxFun(3,4))

val maxFun2 = Math.max(_: Int, _: Int)
maxFun2(3,5)

// Partially applied functions
def sum(a: Int, b: Int, c: Int) = a + b + c
sum(1,2,3)

val b = sum(1, _: Int, 3)
b(3)

// a higher order method
def greeting(lang: String) = {
  lang match {
    case "Eng" => (x: String) => println ("Hello " + x)
    case "Hindi" => (x: String) => println ("Namaste " + x)
    case "French" => (x: String) => println ("Bonjour " + x)
    case "Spanish" => (x: String) => println ("Ola " + x)
  }
}

val greetEng = greeting("Eng")
greetEng("Melanie")


//
// https://docs.scala-lang.org/cheatsheets/index.html
//


// anonymous functions: to pass in multiple blocks,
// need outer parens.
def compose(g:Int=>Int, h:Int=>Int) = (x:Int) => g(h(x))
val f = compose({_*2}, {_-1})
println(f)
println(f(10))


def compose2(g:Int=>Int, h:Int=>Int) = (x:Int) => h(x)*g(x)
val f2 = compose2({_*2}, {_-1})
println(f2)
println(f2(10))


val zscore = (mean:Double, sd:Double) => (x:Double) => (x-mean)/sd
val normer = zscore(1, 1)

normer(10)

val list1 = List(1,2,3)

list1.map(normer(_))

// https://www.safaribooksonline.com/library/view/programming-scala-2nd/9781491950135/ch06.html#FunctionalProgramming

// Partially Applied Functions Versus Partial Functions
/*
A partially applied function is an expression with some,
but not all of a function’s argument lists applied (or provided),
returning a new function that takes the remaining argument lists.

A partial function is a single-argument function that is not
defined for all values of the type of its argument.
The literal syntax for a partial function is one or
more case match clauses enclosed in curly braces.
 */
def cat11(s1: String)(s2: String) = s1 + s2

val hello = cat11("Hello ") _
hello("World !")
cat11("Hello ")("World !")

val inverse: PartialFunction[Double,Double] = {
   case d if d != 0.0 => 1.0 / d
  }

inverse(2.0)
//inverse(0.0)

// Currying functions

def cat1(s1: String)(s2: String) = s1 + s2

def cat2(s1: String) = (s2: String) => s1 + s2

val cat2hello = cat2("Hello ") // No _
cat2hello("World !")

def cat3(s1: String, s2: String) = s1 + s2
cat3("Hello", "world")

val cat3Curried = (cat3 _).curried
cat3Curried("Hello")("world")



def mult(d1: Double, d2: Double, d3: Double) = d1 * d2 * d3
val d3 = (2.2, 3.3, 4.4)
mult(d3._1, d3._2, d3._3)

val multTupled = Function.tupled(mult _)
multTupled(d3)












