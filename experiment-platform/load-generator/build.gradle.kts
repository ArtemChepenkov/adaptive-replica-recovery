plugins {
    java
    alias(libs.plugins.spring.boot)
    alias(libs.plugins.spring.dependency.management)
}

dependencies {
    implementation(project(":common-model"))
    implementation(libs.spring.boot.starter.web)
    implementation(libs.kafka.clients)
    implementation(libs.hdrhistogram)

    testImplementation(libs.spring.boot.starter.test)
    testImplementation(libs.wiremock)
    testImplementation(platform(libs.testcontainers.bom))
    testImplementation(libs.testcontainers.junit.jupiter)
    testImplementation(libs.testcontainers.kafka)
    testRuntimeOnly("org.junit.platform:junit-platform-launcher")
}
