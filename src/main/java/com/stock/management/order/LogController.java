package com.stock.management.order;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

	@RestController
	@RequestMapping("/api/logs")
	public class LogController {

		private static final Logger log = LoggerFactory.getLogger(LogController.class);

		@GetMapping("/test")
		public String testLogging() {
			log.info("Message d'information : Génération du rapport démarrée.");
			log.warn("Message d'avertissement : Attention au temps d'exécution.");
			log.error("Message d'erreur : Échec simulé pour le test.");

			return "Logs générés avec succès sur le lecteur Z:";
		}
	}

