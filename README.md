# Dashbord_SGI - Guide d'utilisation

Dashbord_SGI est une application de visualisation sanitaire connectee a DHIS2 (source EZD), concue pour suivre les SGI:
- MPOX
- MVE
- CHOLERA

## A quoi sert l'application

L'application permet de:
- se connecter avec un compte DHIS2,
- choisir une maladie,
- analyser les donnees par section metier,
- filtrer par niveau geographique,
- suivre l'evolution des indicateurs dans le temps.

## Connexion

1. Ouvrir l'application.
2. Dans la barre laterale, saisir:
   - Nom d'utilisateur
   - Mot de passe
3. Cliquer sur `Se connecter`.

Une fois connecte, l'application charge automatiquement les donnees selon vos droits DHIS2.

## Navigation dans le tableau de bord

Apres connexion:

1. Choisir la `Maladie` (MPOX, MVE ou CHOLERA).
2. Parcourir les onglets:
   - `Vue d'ensemble`
   - `Surveillance`
   - `INRB`
   - `PEV`

Chaque onglet presente des graphiques et indicateurs adaptes au theme.

## Utilisation des filtres

Les filtres sont interactifs et se mettent a jour entre eux:

- `Province`
- `Zone de Sante`
- `Aire de Sante`
- `Date de collecte` (plage de temps)

Exemple:
- si vous choisissez une Province, la liste des Zones se limite a cette Province.
- si vous choisissez une Zone, la liste des Aires se limite a cette Zone.

## Comment lire les resultats

Le dashboard affiche notamment:
- la situation generale de l'epidemie,
- la situation hebdomadaire,
- les proportions par tranche d'age,
- l'evolution des cas, de la letalite et de la positivite,
- des repartitions territoriales,
- une table brute en bas de page.

Conseil:
- commencez par une plage de dates large,
- puis affinez progressivement par Province -> Zone -> Aire pour une analyse detaillee.

## Deconnexion

Dans la barre laterale, cliquer sur `Se deconnecter`.

## Notes importantes

- Les donnees affichees dependent de vos droits DHIS2.
- Certaines visualisations peuvent etre vides si aucune donnee n'existe pour les filtres choisis.
- La date de mise a jour affichee dans l'application correspond a la date du jour d'ouverture.
