package com.stock.management.notification;

import com.stock.management.notification.gateway.NotificationGateway;
import org.springframework.stereotype.Service;


@Service
public class NotificationService {

    private final NotificationGateway notificationGateway;

    public NotificationService(NotificationGateway notificationGateway) {
        this.notificationGateway = notificationGateway;
    }

    public void  sendNotification(NotificationRequest notificationRequest)  throws Exception{
        notificationGateway.sendMessage(notificationRequest);
    }
}
