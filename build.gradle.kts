import com.github.jengelman.gradle.plugins.shadow.tasks.ShadowJar

plugins {
  java
  id("com.github.johnrengelman.shadow") version "4.0.3"
}

repositories {
  mavenCentral()
  maven {
    url = uri("https://oss.sonatype.org/content/repositories/snapshots/")
  }
}

val vertxVersion = "3.9.0"
val junitVersion = "5.3.2"

dependencies {
  implementation("io.vertx:vertx-core:$vertxVersion")
  implementation("io.vertx:vertx-rx-java2:$vertxVersion")
  implementation("org.apache.commons:commons-lang3:3.9")
  testImplementation("io.vertx:vertx-junit5:$vertxVersion")
  testImplementation("io.vertx:vertx-web-client:$vertxVersion")
  testImplementation("org.junit.jupiter:junit-jupiter-api:$junitVersion")
  testRuntimeOnly("org.junit.jupiter:junit-jupiter-engine:$junitVersion")
}

java {
  sourceCompatibility = JavaVersion.VERSION_1_8
}

tasks {
  test {
    useJUnitPlatform()
  }
}
