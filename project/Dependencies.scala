import sbt._

object Dependencies {

  def dep_compile   (deps: ModuleID*): Seq[ModuleID] = deps map (_ % "compile")
  def dep_test      (deps: ModuleID*): Seq[ModuleID] = deps map (_ % "test")  // ttest vs test so as not to confuse w/sbt 'test'

  def AKKA = "2.4.7"
  val KAFKA = "0.10.0.0"

  //val akafka       = "com.typesafe.akka"   % "akka-stream-kafka"     % "0.11-M1"
  val akka_stream  = "com.typesafe.akka"   %% "akka-stream"          % AKKA
  val akka_slf4j   = "com.typesafe.akka"   %% "akka-slf4j"           % AKKA
  val zkclient     = "com.101tec"          %  "zkclient"             % "0.5"
  val kafka_client = "org.apache.kafka"    %  "kafka-clients"        % KAFKA excludeAll(
    ExclusionRule(organization = "com.sun.jdmk"),
    ExclusionRule(organization = "com.sun.jmx")
  )
  val kafka        = "org.apache.kafka"    %% "kafka"                % KAFKA excludeAll(
    ExclusionRule(organization = "com.sun.jdmk"),
    ExclusionRule(organization = "com.sun.jmx")
  )
  val scalatest    = "org.scalatest"       %% "scalatest"            % "2.2.4"
}
