import sbt._

object Dependencies {

  def dep_compile   (deps: ModuleID*): Seq[ModuleID] = deps map (_ % "compile")
  def dep_test      (deps: ModuleID*): Seq[ModuleID] = deps map (_ % "test")  // ttest vs test so as not to confuse w/sbt 'test'

  def AKKA = "2.4.3"
  val KAFKA = "0.10.1.0-SNAPSHOT"
  // val KAFKA = "0.9.0.1"

  val akafka       = "com.typesafe.akka"     % "akka-stream-kafka"              % "0.11-M1"
  val akka_stream  = "com.typesafe.akka"   %% "akka-stream"          % AKKA
  val akka_slf4j   = "com.typesafe.akka"   %% "akka-slf4j"           % AKKA
  val zkclient     = "com.101tec"          %  "zkclient"             % "0.5"
  val kafka        = "org.apache.kafka"    %  "kafka-clients"        % KAFKA excludeAll(
    ExclusionRule(organization = "com.sun.jdmk"),
    ExclusionRule(organization = "com.sun.jmx")
  )
  val kafka_core   = "org.apache.kafka"    %% "kafka"                % KAFKA excludeAll(
    ExclusionRule(organization = "com.sun.jdmk"),
    ExclusionRule(organization = "com.sun.jmx")
  )
  val slf4j        = "org.slf4j"           %  "slf4j-simple"         % "1.7.12"
  val scalatest    = "org.scalatest"       %% "scalatest"            % "2.2.4"
}