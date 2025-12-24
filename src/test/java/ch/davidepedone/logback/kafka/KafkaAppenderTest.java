package ch.davidepedone.logback.kafka;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.empty;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.hasItem;
import static org.hamcrest.Matchers.hasProperty;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.ArgumentMatchers.same;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoInteractions;
import static org.mockito.Mockito.when;

import ch.qos.logback.classic.Level;
import ch.qos.logback.classic.LoggerContext;
import ch.qos.logback.classic.spi.ILoggingEvent;
import ch.qos.logback.classic.spi.LoggingEvent;
import ch.qos.logback.core.BasicStatusManager;
import ch.qos.logback.core.encoder.Encoder;
import ch.davidepedone.logback.kafka.delivery.DeliveryStrategy;
import ch.davidepedone.logback.kafka.delivery.FailedDeliveryCallback;
import ch.davidepedone.logback.kafka.keying.KeyingStrategy;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;

import java.lang.reflect.Field;

public class KafkaAppenderTest {

	private final KafkaAppender<ILoggingEvent> unit = new KafkaAppender<>();

	private final LoggerContext ctx = new LoggerContext();

	@SuppressWarnings("unchecked")
	private final Encoder<ILoggingEvent> encoder = mock(Encoder.class);

	@SuppressWarnings("unchecked")
	private final KeyingStrategy<ILoggingEvent> keyingStrategy = mock(KeyingStrategy.class);

	private final DeliveryStrategy deliveryStrategy = mock(DeliveryStrategy.class);

	@BeforeEach
	public void before() {
		ctx.setName("testctx");
		ctx.setStatusManager(new BasicStatusManager());
		unit.setContext(ctx);
		unit.setName("kafkaAppenderBase");
		unit.setEncoder(encoder);
		unit.setTopic("topic");
		unit.addProducerConfig("bootstrap.servers=localhost:1234");
		unit.addProducerConfig("key.serializer=org.apache.kafka.common.serialization.ByteArraySerializer");
		unit.addProducerConfig("value.serializer=org.apache.kafka.common.serialization.ByteArraySerializer");
		unit.setKeyingStrategy(keyingStrategy);
		unit.setDeliveryStrategy(deliveryStrategy);
		ctx.start();
	}

	@AfterEach
	public void after() {
		ctx.stop();
		unit.stop();
	}

	@Test
	public void testPerfectStartAndStop() {
		unit.start();
		assertTrue(unit.isStarted(), "isStarted");
		unit.stop();
		assertFalse(unit.isStarted(), "isStopped");
		assertThat(ctx.getStatusManager().getCopyOfStatusList(), empty());
		verifyNoInteractions(encoder, keyingStrategy, deliveryStrategy);
	}

	@Test
	public void testDontStartWithoutTopic() {
		unit.setTopic(null);
		unit.start();
		assertFalse(unit.isStarted(), "isStarted");
		assertThat(ctx.getStatusManager().getCopyOfStatusList(), hasItem(
				hasProperty("message", equalTo("No topic set for the appender named [\"kafkaAppenderBase\"]."))));
	}

	@Test
	public void testDontStartWithoutBootstrapServers() {
		unit.getProducerConfig().clear();
		unit.start();
		assertFalse(unit.isStarted(), "isStarted");
		assertThat(ctx.getStatusManager().getCopyOfStatusList(), hasItem(hasProperty("message",
				equalTo("No \"bootstrap.servers\" set for the appender named [\"kafkaAppenderBase\"]."))));
	}

	@Test
	public void testDontStartWithoutEncoder() {
		unit.setEncoder(null);
		unit.start();
		assertFalse(unit.isStarted(), "isStarted");
		assertThat(ctx.getStatusManager().getCopyOfStatusList(), hasItem(
				hasProperty("message", equalTo("No encoder set for the appender named [\"kafkaAppenderBase\"]."))));
	}

	@Test
	public void testAppendUsesKeying() {
		when(encoder.encode(org.mockito.ArgumentMatchers.any(ILoggingEvent.class)))
			.thenReturn(new byte[] { 0x00, 0x00 });
		unit.start();
		final LoggingEvent evt = new LoggingEvent("fqcn", ctx.getLogger("logger"), Level.INFO, "message", null,
				new Object[0]);
		unit.append(evt);
		verify(deliveryStrategy).send(org.mockito.ArgumentMatchers.<KafkaProducer<byte[], byte[]>>any(),
				org.mockito.ArgumentMatchers.any(), eq(evt),
				org.mockito.ArgumentMatchers.<FailedDeliveryCallback<ILoggingEvent>>any());
		verify(keyingStrategy).createKey(same(evt));
		verify(deliveryStrategy).send(org.mockito.ArgumentMatchers.<KafkaProducer<byte[], byte[]>>any(),
				org.mockito.ArgumentMatchers.any(), eq(evt),
				org.mockito.ArgumentMatchers.<FailedDeliveryCallback<ILoggingEvent>>any());
	}

	@Test
	public void testAppendUsesPreConfiguredPartition() {
		when(encoder.encode(org.mockito.ArgumentMatchers.any(ILoggingEvent.class)))
			.thenReturn(new byte[] { 0x00, 0x00 });
		@SuppressWarnings("unchecked")
		ArgumentCaptor<ProducerRecord<byte[], byte[]>> producerRecordCaptor = ArgumentCaptor
			.forClass(ProducerRecord.class);
		unit.setPartition(1);
		unit.start();
		final LoggingEvent evt = new LoggingEvent("fqcn", ctx.getLogger("logger"), Level.INFO, "message", null,
				new Object[0]);
		unit.append(evt);
		verify(deliveryStrategy).send(org.mockito.ArgumentMatchers.<KafkaProducer<byte[], byte[]>>any(),
				producerRecordCaptor.capture(), eq(evt),
				org.mockito.ArgumentMatchers.<FailedDeliveryCallback<ILoggingEvent>>any());
		final ProducerRecord<byte[], byte[]> value = producerRecordCaptor.getValue();
		assertThat(value.partition(), equalTo(1));
	}

	@Test
	@Disabled
	public void testDeferredAppend() {
		when(encoder.encode(org.mockito.ArgumentMatchers.any(ILoggingEvent.class)))
			.thenReturn(new byte[] { 0x00, 0x00 });
		unit.start();
		final LoggingEvent deferredEvent = new LoggingEvent("fqcn", ctx.getLogger("org.apache.kafka.clients.logger"),
				Level.INFO, "deferred message", null, new Object[0]);
		unit.doAppend(deferredEvent);

		verify(deliveryStrategy, never()).send(org.mockito.ArgumentMatchers.<KafkaProducer<byte[], byte[]>>any(),
				org.mockito.ArgumentMatchers.any(), eq(deferredEvent),
				org.mockito.ArgumentMatchers.<FailedDeliveryCallback<ILoggingEvent>>any());

		final LoggingEvent evt = new LoggingEvent("fqcn", ctx.getLogger("logger"), Level.INFO, "message", null,
				new Object[0]);
		unit.doAppend(evt);
		verify(deliveryStrategy).send(org.mockito.ArgumentMatchers.<KafkaProducer<byte[], byte[]>>any(),
				org.mockito.ArgumentMatchers.any(), eq(deferredEvent),
				org.mockito.ArgumentMatchers.<FailedDeliveryCallback<ILoggingEvent>>any());
		verify(deliveryStrategy).send(org.mockito.ArgumentMatchers.<KafkaProducer<byte[], byte[]>>any(),
				org.mockito.ArgumentMatchers.any(), eq(evt),
				org.mockito.ArgumentMatchers.<FailedDeliveryCallback<ILoggingEvent>>any());
	}

	@Test
	public void testKafkaLoggerPrefix() throws ReflectiveOperationException {
		Field constField = KafkaAppender.class.getDeclaredField("KAFKA_LOGGER_PREFIX");
		if (!constField.canAccess(null)) {
			constField.setAccessible(true);
		}
		String constValue = (String) constField.get(null);
		assertThat(constValue, equalTo("org.apache.kafka"));
	}

}
