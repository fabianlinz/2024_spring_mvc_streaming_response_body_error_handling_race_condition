package com.example.streaming.response.body.error.handling.race.condition;

import org.apache.commons.io.IOUtils;
import org.awaitility.Durations;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.RepeatedTest;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.context.TestConfiguration;
import org.springframework.boot.test.web.server.LocalServerPort;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.ControllerAdvice;
import org.springframework.web.bind.annotation.ExceptionHandler;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;
import org.springframework.web.servlet.mvc.method.annotation.StreamingResponseBody;

import java.io.IOException;
import java.net.HttpURLConnection;
import java.net.SocketTimeoutException;
import java.net.URI;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

import static java.nio.charset.StandardCharsets.UTF_8;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.catchThrowable;
import static org.assertj.core.api.BDDAssertions.then;
import static org.awaitility.Awaitility.await;


@SpringBootTest(webEnvironment = SpringBootTest.WebEnvironment.RANDOM_PORT)
class StreamingResponseBodyErrorHandlingRaceConditionTest {

    private static final Logger log = LoggerFactory.getLogger(StreamingResponseBodyErrorHandlingRaceConditionTest.class);

    @LocalServerPort
    private int port;

    @Autowired
    private TestController testController;

    @Autowired
    private GlobalControllerExceptionHandler exceptionHandler;

    @BeforeEach
    void before() {
        exceptionHandler.handledExceptions.clear();
    }

    @Test
    void unexpectedBehaviour_raceConditionResultsInRootCauseBeingHandledInsteadOfWrappingException() {
        //  if a small delay is added before the StreamingResponseBody throws the exception, the error handling is called for the root cause exception which is unexpected
        triggerClientAbortExceptionBeingThrownOnTheServer("fails?threadSleepToInfluenceRaceCondition=true");

        await().await()
                .atMost(Durations.TWO_SECONDS)
                .untilAsserted(
                        () ->  assertThat(exceptionHandler.handledExceptions).containsAnyOf("Handling IOException: Broken pipe", "Handling IOException: Connection reset by peer")
                );
    }

    @RepeatedTest(20) // // this case is flaky. In most cases the desired dispatch for the wrapping exception is triggered, but sometimes the race condition kicks in.
    void expectedBehaviour_wrappingExceptionIsHandled_unlessTheRaceConditionAlsoKicksIn() {
        triggerClientAbortExceptionBeingThrownOnTheServer("fails?threadSleepToInfluenceRaceCondition=false");

        await().await()
                .atMost(Durations.TWO_SECONDS)
                .untilAsserted(
                        () ->  assertThat(exceptionHandler.handledExceptions).containsExactly("Handling SomethingWentWrongWhileStreamingException: org.springframework.web.context.request.async.AsyncRequestNotUsableException: ServletOutputStream failed to write: java.io.IOException: Broken pipe"));
    }

    @Test
    void shouldHaveNoImpactOnSuccessfulCalls() throws IOException {
        final HttpURLConnection connection = callWithTimeout("succeeds", 500);
        final String result = IOUtils.toString(connection.getInputStream(), UTF_8);
        then(result).isEqualTo("Hallo World!");
        connection.disconnect();
    }

    @TestConfiguration
    @ControllerAdvice
    static class GlobalControllerExceptionHandler {

        public final ConcurrentLinkedQueue<String> handledExceptions = new ConcurrentLinkedQueue<>();

        @ExceptionHandler
        public void handle(Throwable e) {
            var message = "Handling %s: %s".formatted(e.getClass().getSimpleName(), e.getMessage());
            handledExceptions.add(message);
            log.info(message, e);
        }
    }

    @RestController
    @TestConfiguration
    static class TestController {

        public static final byte[] SOMETHING_EXCEEDING_BUFFERS_BEFORE_THE_REAL_OUTPUT_STREAM = "x".repeat(99_999)
                .getBytes(UTF_8);

        final Lock connectionClosedLock = new ReentrantLock();

        @GetMapping("/fails")
        ResponseEntity<StreamingResponseBody> fails(@RequestParam final boolean threadSleepToInfluenceRaceCondition) {
            log.info("controller called");

            return ResponseEntity.ok().body(outputStream -> {

                waitForClientToCloseConnection();
                log.info("trying to write response...");

                try {
                    outputStream.write(SOMETHING_EXCEEDING_BUFFERS_BEFORE_THE_REAL_OUTPUT_STREAM); // this will fail with ClientAbortException as client closed the connection already
                    log.error("should have failed with client abort exception but did not...");
                } catch (final Exception e) {
                    log.info("ClientAbortException occurred...");
                    if (threadSleepToInfluenceRaceCondition) {
                        threadSleepToInfluenceRaceCondition();
                    }
                    throw new SomethingWentWrongWhileStreamingException(e);
                }
            });
        }

        private static void threadSleepToInfluenceRaceCondition() {
            try {
                Thread.sleep(200);
            } catch (InterruptedException e) {
                throw new RuntimeException(e);
            }
        }

        @GetMapping("/succeeds")
        ResponseEntity<StreamingResponseBody> succeeds() {
            return ResponseEntity.ok().body(outputStream -> {
                outputStream.write("Hallo World!".getBytes(UTF_8));
                log.info("finished writing to stream");
            });
        }

        private void waitForClientToCloseConnection() {
            log.info("wait for client to abort request...");
            boolean logAquired = false;
            try {
                logAquired = connectionClosedLock.tryLock(2, TimeUnit.SECONDS);
            } catch (InterruptedException e) {
                log.info("InterruptedException", e);
            } finally {
                if (logAquired) {
                    connectionClosedLock.unlock();
                }
            }
        }
    }

    private void triggerClientAbortExceptionBeingThrownOnTheServer(final String url) {
        try {
            testController.connectionClosedLock.lock();

            final Throwable throwable = catchThrowable(() -> {
                callWithTimeout(url, 500);
            });
            assertThat(throwable).isInstanceOf(SocketTimeoutException.class);
            log.info("closed connection from client side");
        } finally {
            testController.connectionClosedLock.unlock();
        }
    }

    private HttpURLConnection callWithTimeout(final String path, final int timeout) throws IOException {
        final String url = "http://localhost:%s/test-context-path/%s".formatted(port, path);
        final HttpURLConnection urlConnection = (HttpURLConnection) URI.create(url).toURL().openConnection();
        urlConnection.setReadTimeout(timeout);
        urlConnection.getResponseCode();
        return urlConnection;
    }

    public static class SomethingWentWrongWhileStreamingException extends RuntimeException {

        public SomethingWentWrongWhileStreamingException(final Exception e) {
            super(e);
        }
    }
}
