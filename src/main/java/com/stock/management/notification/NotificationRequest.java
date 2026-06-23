package com.stock.management.notification;

public record NotificationRequest(
	String name,
	String email,
    String phone,
	String subject,
	String message
) {
}
