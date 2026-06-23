package com.stock.management.notification.gateway;
import com.stock.management.notification.NotificationRequest;
import jakarta.mail.internet.MimeMessage;
import org.springframework.mail.javamail.JavaMailSender;
import org.springframework.mail.javamail.MimeMessageHelper;
import org.springframework.context.annotation.Primary;
import org.springframework.stereotype.Component;
import java.util.List;
import java.util.logging.Logger;

@Component
@Primary
public class EmailNotificationGateway implements NotificationGateway {

    private static final Logger log = Logger.getLogger(EmailNotificationGateway.class.getName());
    private final JavaMailSender mailSender;

    public EmailNotificationGateway(JavaMailSender mailSender) {
        this.mailSender = mailSender;
    }

    @Override
    public void sendMessage(NotificationRequest notificationRequest) throws Exception{
		    log.info("start send message");
            MimeMessage message = mailSender.createMimeMessage();

            MimeMessageHelper helper = new MimeMessageHelper(message, true, "UTF-8");

            helper.setFrom("ktdtchofo@gmail.com");
		    helper.setReplyTo(notificationRequest.email());
            helper.setTo(List.of("tchofodep@gmail.com", "ktdtchofo@gmail.com").toArray(new String[0]));
            helper.setSubject(notificationRequest.subject());

			// 4. On construit un corps de message HTML propre pour voir TOUTES les infos
			String htmlBody = String.format(
				"<h3>Nouveau message de contact</h3>" +
					"<p><b>Nom :</b> %s</p>" +
					"<p><b>Email :</b> %s</p>" +
					"<p><b>Téléphone :</b> %s</p>" +
					"<hr/>" +
					"<p><b>Message :</b></p>" +
					"<p style='white-space: pre-wrap;'>%s</p>",
				notificationRequest.name(),
				notificationRequest.email(),
				notificationRequest.phone() != null ? notificationRequest.phone() : "Non renseigné",
				notificationRequest.message()
			);


            helper.setText(htmlBody, true);

            mailSender.send(message);


    }
}
