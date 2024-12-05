plugins {
    kotlin("jvm") version "1.9.23"
}

group = "com.example"
version = "1.0-SNAPSHOT"

repositories {
    mavenCentral()
}

dependencies {
    //    kafka
    implementation ("org.apache.kafka:kafka-clients:3.7.1")
//    slf4j
    implementation ("org.slf4j:slf4j-api:1.7.36")
    implementation ("org.slf4j:slf4j-simple:1.7.36")
//    faker
    implementation ("com.github.javafaker:javafaker:1.0.2")

    //    postgres sql
    implementation ("org.postgresql:postgresql:42.7.3")

    testImplementation(kotlin("test"))
}

tasks.test {
    useJUnitPlatform()
}
kotlin {
    jvmToolchain(21)
}