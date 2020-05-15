import org.jetbrains.kotlin.gradle.tasks.KotlinCompile

plugins {
    java
    application
    kotlin("jvm") version "1.3.60"
}

val kafkaClientsVersion = "2.5.0"
val immutablesVersion = "2.8.2"
val junitVersion = "4.12"
val rxJavaVersion = "3.0.3"

group = "it.stanislas"
version = "1.0-SNAPSHOT"

repositories {
    mavenCentral()
}

dependencies {
    implementation(kotlin("stdlib-jdk8"))

    implementation("org.apache.kafka:kafka-clients:$kafkaClientsVersion")
    implementation("org.apache.kafka:kafka-streams:$kafkaClientsVersion")
    implementation("io.reactivex.rxjava3:rxjava:$rxJavaVersion")

    annotationProcessor("org.immutables:value:$immutablesVersion")
    compileOnly("org.immutables:value:$immutablesVersion")

    testCompile("junit:junit:$junitVersion")
}

tasks.withType<KotlinCompile> {
    kotlinOptions.jvmTarget = JavaVersion.VERSION_11.toString()
}

configure<JavaPluginConvention> {
    sourceCompatibility = JavaVersion.VERSION_11
}

