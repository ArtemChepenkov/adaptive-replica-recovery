plugins {
    alias(libs.plugins.spring.boot) apply false
    alias(libs.plugins.spring.dependency.management) apply false
}

allprojects {
    group = "ru.nsu.replicarecovery"
    version = "0.1.0-SNAPSHOT"
}

subprojects {
    plugins.withType<JavaPlugin> {
        extensions.configure<JavaPluginExtension> {
            toolchain {
                languageVersion = JavaLanguageVersion.of(17)
            }
        }

        tasks.withType<Test>().configureEach {
            useJUnitPlatform()
        }
    }
}
