from typing import Optional

PARLIAMENT_NAME_TO_CODE: dict[str, str] = {
    "Landtag von Baden-Württemberg": "BW",
    "Bayerischer Landtag": "BY",
    "Abgeordnetenhaus von Berlin": "BE",
    "Landtag Brandenburg": "BB",
    "Bremische Bürgerschaft": "HB",
    "Hamburgische Bürgerschaft": "HH",
    "Hessischer Landtag": "HE",
    "Landtag Mecklenburg-Vorpommern": "MV",
    "Niedersächsischer Landtag": "NI",
    "Landtag Nordrhein-Westfalen": "NW",
    "Landtag Rheinland-Pfalz": "RP",
    "Landtag des Saarlandes": "SL",
    "Sächsischer Landtag": "SN",
    "Landtag von Sachsen-Anhalt": "ST",
    "Schleswig-Holsteinischer Landtag": "SH",
    "Thüringer Landtag": "TH",
    "Deutscher Bundestag": "BT",
    "Bundestag": "BT",
    "Bundesrat": "BR",
}

CODE_TO_PARLIAMENT_NAME: dict[str, str] = {
    "BW": "Landtag von Baden-Württemberg",
    "BY": "Bayerischer Landtag",
    "BE": "Abgeordnetenhaus von Berlin",
    "BB": "Landtag Brandenburg",
    "HB": "Bremische Bürgerschaft",
    "HH": "Hamburgische Bürgerschaft",
    "HE": "Hessischer Landtag",
    "MV": "Landtag Mecklenburg-Vorpommern",
    "NI": "Niedersächsischer Landtag",
    "NW": "Landtag Nordrhein-Westfalen",
    "RP": "Landtag Rheinland-Pfalz",
    "SL": "Landtag des Saarlandes",
    "SN": "Sächsischer Landtag",
    "ST": "Landtag von Sachsen-Anhalt",
    "SH": "Schleswig-Holsteinischer Landtag",
    "TH": "Thüringer Landtag",
    "BT": "Deutscher Bundestag",
    "BR": "Bundesrat",
}


def get_parliament_code(parliament_name: str) -> Optional[str]:
    return PARLIAMENT_NAME_TO_CODE.get(parliament_name)


def get_parliament_name(parliament_code: str) -> Optional[str]:
    return CODE_TO_PARLIAMENT_NAME.get(parliament_code.upper())

