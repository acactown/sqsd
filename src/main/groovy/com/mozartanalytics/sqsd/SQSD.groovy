package com.mozartanalytics.sqsd

import com.amazonaws.AmazonClientException
import com.amazonaws.AmazonServiceException
import com.amazonaws.auth.BasicAWSCredentials
import com.amazonaws.regions.RegionUtils
import com.amazonaws.services.sqs.AmazonSQS
import com.amazonaws.services.sqs.AmazonSQSClient
import com.amazonaws.services.sqs.model.DeleteMessageRequest
import com.amazonaws.services.sqs.model.Message
import com.amazonaws.services.sqs.model.ReceiveMessageRequest
import groovy.json.JsonParserType
import groovy.json.JsonSlurper
import groovy.util.logging.Slf4j
import groovyx.net.http.HttpResponseException
import groovyx.net.http.RESTClient

import static com.google.common.base.Preconditions.*
import static java.util.concurrent.TimeUnit.*

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
@Slf4j
final class SQSD implements Runnable {

    ConfigObject config

    /**
     * AWS SQS required related configuration env-vars
     */
    private AmazonSQS sqs
    private String sqsQueueUrl

    /**
     * SQSD related configuration env-vars
     */
    private Integer maxMessagesPerRequest
    private boolean runDaemonized
    private long sleepSeconds
    private Integer waitTimeSeconds

    /**
     * HTTP service related configuration env-vars
     */
    private String workerHTTPHost
    private String workerHTTPPath
    private String workerHTTPRequestContentType

    @Override
    void run() {
        initConfig()

        try {
            log.info("Receiving messages from queue [{}]", sqsQueueUrl)

            // Configure sqs request
            ReceiveMessageRequest receiveMessageRequest = new ReceiveMessageRequest()
                    .withQueueUrl(sqsQueueUrl)
                    .withMaxNumberOfMessages(maxMessagesPerRequest)
                    .withWaitTimeSeconds(waitTimeSeconds)
            // Sets long-polling wait time seconds (long-polling has to be enabled on related SQS)

            // Consume queue until empty
            while (true) {
                log.info("Querying SQS for messages ...")
                // TODO: Limit the amount of messages to process using a property.
                List<Message> messages = sqs.receiveMessage(receiveMessageRequest).messages
                log.info("Received Messages [{}]", messages.size())

                // Break when empty if not running daemonized
                if (messages.size() <= 0) {
                    if (!runDaemonized) {
                        break
                    } else if (sleepSeconds) {
                        // don't want to hammer implementations that don't implement long-polling
                        SECONDS.sleep(sleepSeconds)
                    }
                }

                for (Message message : messages) {
                    // TODO: Make async.
                    if (handleMessage(message)) {
                        // If successful, delete the message
                        log.info("Deleting message...")
                        sqs.deleteMessage(new DeleteMessageRequest(sqsQueueUrl, message.receiptHandle))
                    } else {
                        // TODO we might validate that the long polling timeout is considerably longer than the visibility timeout,
                        // if true then we could skip the exception. Still this is a dangerous alternative.
                        throw new Exception("Local Service Failure. Message ID [{}]", message.messageId)
                        // We should stop consuming here
                    }
                }

                log.info("Done!!")
            }
        } catch (AmazonServiceException | AmazonClientException exception) {
            log.error("Error!", exception)
        }

        log.info("SQSD finished successfully!")
    }


    private boolean handleMessage(Message message) {
        def payload = new JsonSlurper(type: JsonParserType.LAX).parseText(message.body)

        int status
        try {
            def resp = new RESTClient(workerHTTPHost).post(
                    path: workerHTTPPath,
                    body: payload,
                    contentType: workerHTTPRequestContentType
            )
            status = resp.status
        } catch (HttpResponseException ex) {
            status = ex.response.status

            log.error("Worker response error!", ex)
        } catch (ConnectException ex) {
            status = 500

            log.error("Connection with worker error!", ex)
        }

        log.info("POST [{}]::[{}]", "${workerHTTPHost}${workerHTTPPath}", status)

        return status == 200
    }

    private void initConfig() {
        checkNotNull(config, "Required `config` not provided!!")

        // TODO: Determine if this is the correct approach when using Docker.
        sqs = new AmazonSQSClient(new BasicAWSCredentials(
                config.aws.access_key_id as String,
                config.aws.secret_access_key as String
        ))
        sqs.region = RegionUtils.getRegion(config.sqsd.queue.region_name as String)

        // Use provided queue url or name (url has priority)
        sqsQueueUrl = config.sqsd.queue.url ?: sqs.getQueueUrl(config.sqsd.queue.name as String).getQueueUrl()

        maxMessagesPerRequest = config.sqsd.max_messages_per_request as Integer

        waitTimeSeconds = config.sqsd.wait_time_seconds as Integer

        runDaemonized = (config.sqsd.run_daemonized < 1)

        sleepSeconds = config.sqsd.sleep_seconds

        workerHTTPHost = config.sqsd.worker.http.host as String
        workerHTTPPath = config.sqsd.worker.http.path as String
        workerHTTPRequestContentType = config.sqsd.worker.http.request.content_type as String
    }

}
