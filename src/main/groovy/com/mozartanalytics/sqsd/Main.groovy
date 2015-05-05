package com.mozartanalytics.sqsd

import static com.google.common.base.Preconditions.*
import static com.google.common.base.Strings.*
import static com.google.common.io.Resources.*

/**
 * <h1>SQSD</h1>
 * sqsd : A simple alternative to the Amazon SQS Daemon ("sqsd") used on AWS Beanstalk worker tier instances.
 * <p>
 * Copyright (c) 2014 Mozart Analytics
 *
 * @author <a href="mailto:abdiel.aviles@mozartanalytics.com">Abdiel Aviles</a>
 * @author <a href="mailto:ortiz.manuel@mozartanalytics.com">Manuel Ortiz</a>
 * @author <a href="mailto:acactown@gmail.com">Andrés Amado</a>
 */
final class Main {

    private static final String DEFAULT_CONFIG_FILE_LOCATION = "sqsd-default-config.groovy"

    static void main(String[] args) {
        SQSD sqsd = new SQSD(config: validateConfig())
        sqsd.run()
    }

    private static ConfigObject validateConfig() {
        ConfigObject config = getConfig()

        // Assert that all required properties are provided.
        checkArgument(!isNullOrEmpty(config.aws.access_key_id), "Required `aws.access_key_id` property not provided!!")
        checkArgument(!isNullOrEmpty(config.aws.secret_access_key), "Required `aws.secret_access_key` property not provided!!")

        checkArgument(!isNullOrEmpty(config.sqsd.queue.url) || !isNullOrEmpty(config.sqsd.queue.name), "Required `sqsd.queue.url` OR `sqsd.queue.name` property not provided!!")

        return config
    }

    private static ConfigObject getConfig() {
        // Retrieve configuration values from environment -> default-config -> config.
        ConfigObject config = new ConfigSlurper().parse(getResource(DEFAULT_CONFIG_FILE_LOCATION))
        File customConfigFile = new File(config.sqsd.config_file as String)

        // Merge default-config with provided config.
        if (customConfigFile.exists()) {
            ConfigObject customConfig = new ConfigSlurper().parse(customConfigFile.toURI().toURL())
            config = config.merge(customConfig)
        }

        return config
    }

}
