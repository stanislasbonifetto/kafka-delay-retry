import org.jetbrains.kotlin.gradle.tasks.KotlinCompile

plugins {
    java
    application
    kotlin("jvm") version "1.3.60"
}

val kafkaClientsVersion = "2.5.0"
val immutablesVersion = "2.8.2"
val rxJavaVersion = "3.0.3"

val junitJupiterVersion = "5.6.2"
val testcontainersVersion = "1.14.1"
val awaitilityVersion = "4.0.2"

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

    testImplementation("org.junit.jupiter:junit-jupiter:$junitJupiterVersion")
    testRuntimeOnly("org.junit.jupiter:junit-jupiter-engine:$junitJupiterVersion")
    testImplementation("org.testcontainers:testcontainers:$testcontainersVersion")
    testImplementation("org.testcontainers:junit-jupiter:$testcontainersVersion")
    testImplementation("org.testcontainers:kafka:$testcontainersVersion")
    testImplementation("org.awaitility:awaitility:$awaitilityVersion")

}

tasks.withType<KotlinCompile> {
    kotlinOptions.jvmTarget = JavaVersion.VERSION_11.toString()
}

tasks.test {
    useJUnitPlatform()
}

configure<JavaPluginConvention> {
    sourceCompatibility = JavaVersion.VERSION_11
}

