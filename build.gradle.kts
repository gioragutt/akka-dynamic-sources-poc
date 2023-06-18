plugins {
    id("java")
}

group = "org.example"
version = "1.0-SNAPSHOT"

repositories {
    mavenCentral()
}

dependencies {
    implementation("org.projectlombok:lombok:1.18.20")
    annotationProcessor("org.projectlombok:lombok:1.18.20")

    implementation("com.typesafe.akka:akka-actor-typed_2.13:2.8.2")
    implementation("com.typesafe.akka:akka-stream_2.13:2.8.2")
    implementation("ch.qos.logback:logback-classic:1.2.9")
    implementation("com.typesafe.akka:akka-stream-contrib_2.13:0.11+4-91b2f9fa")
    implementation("org.scala-lang.modules:scala-java8-compat_2.13:1.0.2")

    testImplementation("com.typesafe.akka:akka-actor-testkit-typed_2.13:2.8.2")
    testImplementation(platform("org.junit:junit-bom:5.9.1"))
    testImplementation("org.junit.jupiter:junit-jupiter")
}

tasks.test {
    useJUnitPlatform()
}