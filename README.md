# 06-Monitoring Global


Rappel du projet :

* Projet initié par Tarik Meksem.
* L'idée est de récupérer l'ensemble des logs de tous les projects en cron.
* Mettre en place un reporting pour suivre les alertes.

Solution :

* Récupération data depuis les projects directories de tous les logs.
* Compilation dans un dataframe raffraîchit tous les jours à minuit.

Visualisation :

le TDB est développé sur steamlit appelé par Cognos en fonction embdeed.