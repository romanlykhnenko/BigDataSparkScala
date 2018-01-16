
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










