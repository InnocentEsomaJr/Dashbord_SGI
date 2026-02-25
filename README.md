# Dashbord_SGI

Projet Streamlit dedie au dashboard SGI (MPOX, MVE, CHOLERA), connecte directement a DHIS2.

## Lancement

```powershell
python -m pip install -r requirements.txt
streamlit run dashbord_sgi.py
```

## Fonctionnalites

- Connexion DHIS2 avec identifiants utilisateur.
- Source DHIS2 unique (EZD) dans la sidebar.
- Lecture de `/api/me` pour adapter le dashboard au profil connecte.
- Filtre `Maladie` (MPOX, MVE, CHOLERA) selon les groupes utilisateur.
- Onglets de section:
  - `Vue d'ensemble`
  - `Surveillance`
  - `INRB`
  - `PEV`
- Filtres organisationnels en cascade: `Province` -> `Zone de Sante` -> `Aire de Sante`.
- Requetes analytiques sur `/api/analytics`.
- Portee organisationnelle configurable (defaut recommande):
  - `USER_ORGUNIT`
  - `USER_ORGUNIT_CHILDREN`
  - `USER_ORGUNIT_GRANDCHILDREN`
  - `USER_ORGUNIT_DESCENDANTS`

## Configuration

Creer `.streamlit/secrets.toml` a partir de `.streamlit/secrets.toml.example`.

Variables principales:
- `DHIS2_URL_EZD`
- `DHIS2_DATA_SOURCES` (optionnel, mapping JSON label->URL)
- `DHIS2_URL` (optionnel, repli)
- `DHIS2_TIMEOUT_CONNECT` (optionnel)
- `DHIS2_TIMEOUT_READ` (optionnel)
- `DHIS2_HTTP_RETRIES` (optionnel)
- `DHIS2_ALLOW_ALL_IF_NO_GROUP_MATCH` (optionnel)
- `DHIS2_PERIOD_YEARS_BACK` (optionnel)
- `DHIS2_FIXED_OU_SCOPE` (optionnel, ex: `USER_ORGUNIT_DESCENDANTS`)
- `DHIS2_ANALYTICS_OU_SCOPE_FALLBACKS` (optionnel, liste de fallback en cas de `409`)
- `DHIS2_SGI_GROUPS` (optionnel mais recommande)
- `DHIS2_SGI_METRICS` (recommande)

Note importante:
- si votre serveur est publie sous un contexte web, utilisez l'URL avec ce contexte (ex: `https://ezd.snisrdc.com/dhis`).
- le script tente aussi une resolution automatique (`/dhis`) en cas de `404` sur `/api/me`.
