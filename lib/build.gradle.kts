plugins {
  `java-library`
  `maven-publish`
  signing
}

group = "ar.emily"
version = "1.0-SNAPSHOT"

java {
  toolchain.languageVersion.set(JavaLanguageVersion.of(20))
  withSourcesJar()
}

tasks.compileJava {
  options.encoding = "UTF-8"
  options.compilerArgs = listOf("--enable-preview")
}

publishing {
  publications {
    register<MavenPublication>("java") {
      from(components["java"])
    }
  }
}

signing {
  sign(publishing.publications["java"])
}
