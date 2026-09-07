"""
corrections.py

Table de correspondance des coquilles connues dans les données source
(type_sport, sport_pratique) -- corrigées ici uniquement, jamais dans
les données source elles-mêmes (immutabilité : referentiel_sport et
activites_sportives restent le reflet exact de ce qui a été déclaré,
y compris ses défauts -- traçabilité/audit, réversibilité si une
correction s'avère un jour fausse).

Module partagé entre silver_enrich.py et sync_mirror.py pour éviter
que deux dictionnaires divergent avec le temps -- un seul endroit à
mettre à jour si une nouvelle coquille apparaît dans les données réelles.
"""

CORRECTIONS_ORTHOGRAPHE = {"Runing": "Running"}
