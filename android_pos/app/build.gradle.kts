plugins {
    id("com.android.application")
    id("org.jetbrains.kotlin.android")
}

android {
    namespace = "cl.sucree.pos"
    compileSdk = 35

    defaultConfig {
        applicationId = "cl.sucree.pos"
        minSdk = 26
        targetSdk = 34
        versionCode = 4
        versionName = "1.3.0"
        buildConfigField("String", "POS_URL", "\"https://pasteleriasucree.cl/admin/pos/mobile\"")
    }

    buildFeatures {
        buildConfig = true
    }

    compileOptions {
        sourceCompatibility = JavaVersion.VERSION_17
        targetCompatibility = JavaVersion.VERSION_17
    }

    kotlinOptions {
        jvmTarget = "17"
    }

}

dependencies {
    implementation("com.github.iminsoftware:IminPrinterLibrary:V2.0.0.18")
}
