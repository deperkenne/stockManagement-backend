package com.stock.management.notification.gateway;

import com.stock.management.notification.NotificationRequest;

public interface NotificationGateway {
	public  void sendMessage (NotificationRequest notificationRequest) throws Exception;
}
