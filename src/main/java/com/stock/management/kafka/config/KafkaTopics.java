package com.stock.management.kafka.config;

public final class KafkaTopics {

    private KafkaTopics() {}

    public static final String ORDER_RECEIVED    = "order.received";
    public static final String STOCK_ALLOCATED   = "stock.allocated";
    public static final String ORDER_CANCELLED   = "order.cancelled";
    public static final String STOCK_RELEASED    = "stock.released";
    public static final String SKU_CORRECTED     = "sku.corrected";
    public static final String ALLOCATION_FAILED = "allocation.failed";
    public static final String SKU_SUBSTITUTED   = "sku.substituted";
    public static final String ORDER_DEALLOCATED = "order.deallocated";
	public static final String ORDER_INPROGRESS  = "order.inprogress";
}
