package com.stock.management.kafka.event;

import com.fasterxml.jackson.annotation.JsonFormat;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.time.Instant;

@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
public class SkuCorrectedEvent {

    private String eventId;
    private String orderId;
    private String oldSku;
    private String correctedSku;
    private int quantity;
    private String correctedBy;
    private String correctionReason;

    @JsonFormat(shape = JsonFormat.Shape.STRING)
    private Instant occurredAt;
}