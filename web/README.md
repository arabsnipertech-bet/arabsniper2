# Sito ArabSniperBet

Pagine pubbliche recuperate dalla distribuzione Cloudflare 694ba75d il 18 ottobre 2026. Homepage e Gold recuperate dal dominio; altre pagine e logo dalla distribuzione indicata dal pannello.

Gold principale aggiornata con le classifiche separate di v1.1.8. Le copie storiche nelle altre URL sono conservate senza modifiche. La pagina football-predictions-today.html era vuota (0 byte); /audit.html restituiva la homepage, quindi non viene ricreata come audit.

La homepage legge il vecchio repository arabsniper; Gold legge arabsniper2. Gold verifica gli accessi tramite stripe-checker.arabsnipertech.workers.dev. La homepage chiama /api/create-checkout-session: il relativo codice server non è incluso in questa copia e il checkout non è stato verificato.

La scheda Funzioni della distribuzione originale mostra il messaggio introduttivo, senza elenco di funzioni. Variabili Stripe presenti nelle impostazioni non dimostrano che il checkout sia operativo. Prima di distribuire verificare route Workers, redirect e pagamenti. Nessun deploy automatico attivato.

Output statico: web, dalla radice del repository, senza compilazione. Non usare la radice del repository come output. Lo script Cloudflare email-decode è conservato; la relativa pagina email-protection.html risponde 404 nella distribuzione originale.
