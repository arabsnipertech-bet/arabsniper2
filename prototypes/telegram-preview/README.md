# Publisher Telegram · collaudo isolato

La bozza appartiene ad arabsniper2. Il Publisher attivo usa il workflow di arabsniper-growth e importa il codice Python del motore. Il collaudo non trasferisce la proprietà della coda.

Il workflow di prova legge per nome cinque board Gold e il registro editoriale. I file restano sul runner effimero. Il test ricarica le copie in un bucket R2 locale e confronta i risultati del parser workerd con il lettore Python su tutte le partite prematch. Il confronto dei media usa il Worker Python locale, gli stessi moduli grafici e i font DejaVu del runner. I record con ricevuta danno testo, marker, orario e ID di Telegram. Il JPEG/GIF di riferimento viene rigenerato dal contenuto salvato. Telegram può ricomprimere o sostituire i media dopo un edit.

## Attivazione proposta

1. Verificare sul piano dell'account CPU, memoria, combaciamento dei board e riferimenti PT/FT, tutte le tipologie editoriali, JPEG, GIF e font. Provare i picchi di campionato e controllare il consumo del watchdog indipendente. Mantenere Growth operativo durante tutte le prove.
2. Introdurre in entrambi i trasporti una proprietà specifica per Telegram. Il campo corrente `runtime.publisher_repository` governa anche altri compiti editoriali e accetta solo i due repository. Il trasferimento di Telegram deve lasciare a Growth Facebook, Home e la pianificazione condivisa finché necessaria. Includere nel trasferimento il monitor Telegram Live, gli edit e tutti i percorsi di fallback.
3. Preparare il nuovo trasporto con invii disabilitati. Usare un solo registro di ricevute e le stesse chiavi per contenuto e canale. Per ogni tentativo verificare proprietario, versione e lease del ciclo, riservare con ETag, salvare `sending` prima della chiamata. Una perdita della risposta richiede una riconciliazione per marker, destinazione ed eventuale edit_id. Una consegna incerta resta sospesa.
4. Solo dopo approvazione, completare i tentativi in volo di Growth e acquisire il lease comune. Cambiare il proprietario Telegram con scrittura condizionale e verificare che il vecchio trasporto si astenga. Fornire il token Telegram solo al trasporto autorizzato. Convalidare una ricevuta di nuovo messaggio e una di aggiornamento, con lo stesso ID di destinazione previsto.
5. Per il rollback bloccare subito le nuove richieste del Worker. Attendere la fine di quelle già partite o il termine del lease, riconciliare le ricevute incerte, poi restituire la proprietà Telegram a Growth con ETag. I messaggi già confermati conservano chiave e ricevuta. Non si può garantire contemporaneamente una ripresa istantanea degli invii e l'assenza di duplicati per un invio già accettato da Telegram ma privo di risposta.

Il test può produrre costi una tantum di runner e letture R2. Non crea Cron, Worker di produzione, secret o abbonamenti.
