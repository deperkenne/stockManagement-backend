package com.stock.management.notification;

import com.stock.management.notification.gateway.EmailNotificationGateway;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;
import lombok.extern.slf4j.Slf4j;


@RestController
@RequestMapping("/api/notification")
@Slf4j
public class NotificationController {
	private final NotificationService notificationService;

	public NotificationController(NotificationService notificationService){
		this.notificationService = notificationService;
	}

	@PostMapping
	public ResponseEntity<NotificationResponse> handleContactForm(@RequestBody NotificationRequest request) {

		try {
			log.info("call notification service");
			notificationService.sendNotification

				(request);

			// Retour en cas de succès (HTTP 200)
			return ResponseEntity.ok(
				new NotificationResponse(true, "your message was send with success !")
			);

		} catch (Exception e) {
			return ResponseEntity.status(HttpStatus.INTERNAL_SERVER_ERROR).body(
				new NotificationResponse(false, "Fail to send : " + e.getMessage())
			);
		}
	}
}
