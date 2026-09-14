pluginManagement {
    repositories {
        maven("https://maven-central.storage-download.googleapis.com/maven2/")
        gradlePluginPortal()
    }
}

plugins {
    id("org.gradle.toolchains.foojay-resolver-convention") version "1.0.0"
}

dependencyResolutionManagement {
    repositoriesMode.set(RepositoriesMode.FAIL_ON_PROJECT_REPOS)
    repositories {
        maven("https://maven-central.storage-download.googleapis.com/maven2/")
    }
}

rootProject.name = "adaptive-replica-recovery"

include("common-model", "load-generator", "experiment-runner")
