plugins {
    `java-library`
}

dependencies {
    testImplementation(platform("org.junit:junit-bom:5.12.2"))
    testImplementation("org.junit.jupiter:junit-jupiter")
    testImplementation(libs.assertj.core)
    testRuntimeOnly("org.junit.platform:junit-platform-launcher")
}
