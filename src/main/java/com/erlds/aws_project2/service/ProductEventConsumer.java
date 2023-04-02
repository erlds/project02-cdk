package com.erlds.aws_project2.service;

import com.erlds.aws_project2.model.Envelop;
import com.erlds.aws_project2.model.ProductEvent;
import com.erlds.aws_project2.model.ProductEventLog;
import com.erlds.aws_project2.model.SnsMessage;
import com.erlds.aws_project2.repository.ProductEventLogRepository;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.jms.annotation.JmsListener;
import org.springframework.stereotype.Service;

import javax.jms.JMSException;
import javax.jms.TextMessage;
import java.io.IOException;
import java.time.Duration;
import java.time.Instant;

@Service
public class ProductEventConsumer {
    private static final Logger log = LoggerFactory.getLogger(ProductEventConsumer.class);
    private final ProductEventLogRepository productEventLogRepository;

    private ObjectMapper objectMapper;

    @Autowired
    public ProductEventConsumer (ObjectMapper objectMapper, ProductEventLogRepository productEventLogRepository) {
        this.objectMapper = objectMapper;
        this.productEventLogRepository = productEventLogRepository;
    }

    @JmsListener(destination = "${aws.sqs.queue.product.events.name}")
    public void receiveProductEvent(TextMessage textMessage)
            throws JMSException, IOException {

        SnsMessage snsMessage = objectMapper.readValue(textMessage.getText(),
                SnsMessage.class);

        Envelop envelop = objectMapper.readValue(snsMessage.getMessage(),
                Envelop.class);

        ProductEvent productEvent = objectMapper.readValue(
                envelop.getData(),ProductEvent.class);

        log.info("Product event received - Event: {} - ProductId: {} - MessageId: {}",
                envelop.getEventType(),
                productEvent.getProductId(), snsMessage.getMessageId());

        ProductEventLog productEventLog = buildProductEventLog(envelop, productEvent, snsMessage.getMessageId());

        productEventLogRepository.save(productEventLog);
    }

    ProductEventLog buildProductEventLog(Envelop envelop, ProductEvent productEvent, String messageId){
        long timestamp = Instant.now().toEpochMilli();
        ProductEventLog productEventLog = new ProductEventLog();
        productEventLog.setPk(productEvent.getCode());
        productEventLog.setSk(envelop.getEventType() + "_" + timestamp);
        productEventLog.setEventType(envelop.getEventType());
        productEventLog.setProductId(productEvent.getProductId());
        productEventLog.setUsername(productEvent.getUsername());
        productEventLog.setTimestamp(timestamp);
        productEventLog.setTtl(Instant.now().plus(
                Duration.ofMinutes(10)).getEpochSecond());
        productEventLog.setMessageId(messageId);

        return productEventLog;
    }
}
