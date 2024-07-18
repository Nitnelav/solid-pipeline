import math

ST45_ST8 = [
    {
        "st8": 1,
        "st45": "1",
        "label": "Agriculture",
        "function": lambda e: 2.396 * math.log(e),
    },
    {
        "st8": 2,
        "st45": "2-2",
        "label": "Artisans (réparations)",
        "function": lambda e: 1.933 * math.log(e) + 14.307,
    },
    {
        "st8": 2,
        "st45": "2-3",
        "label": "Artisans (fabrication ou installation)",
        "function": lambda e: 3.132 * e + -2.2,
    },
    {
        "st8": 2,
        "st45": "2-4",
        "label": "Artisans (fabrication ou installation - petites réparations)",
        "function": lambda e: 0.789 * e + 2.492,
    },
    {
        "st8": 2,
        "st45": "26Ha",
        "label": "Tertiaire autre (services flux élevés)",
        "function": lambda e: 6.416 * math.log(e) + 0.195,
    },
    {
        "st8": 2,
        "st45": "26Mi",
        "label": "Tertiaire autre (services flux mixtes)",
        "function": lambda e: 0.101 * e + 1.628,
    },
    {
        "st8": 2,
        "st45": "26Mo",
        "label": "Tertiaire autre (services flux moyens)",
        "function": lambda e: 0.14 * e + 5.059,
    },
    {
        "st8": 3,
        "st45": "3",
        "label": "Industrie chimique",
        "function": lambda e: 1.536 * e + 3.077,
    },
    {
        "st8": 3,
        "st45": "4-2",
        "label": "Industrie de biens de production et intermédiaires (de base)",
        "function": lambda e: 1.172 * e + 6.893,
    },
    {
        "st8": 3,
        "st45": "5-2",
        "label": "Industrie de biens de consommation (produits alimentaires fragiles)",
        "function": lambda e: 1.045 * math.log(e) + 13.342,
    },
    {
        "st8": 3,
        "st45": "5-4",
        "label": "Industrie de biens de consommation (produits non alimentaires, équipement de la maison et de l’individu)",
        "function": lambda e: 6.246 * math.log(e),
    },
    {
        "st8": 3,
        "st45": "5-5",
        "label": "Industrie de biens de consommation (produits alimentaires non fragiles, équipement spécifique)",
        "function": lambda e: 1.8 * e + 0.719,
    },
    {
        "st8": 3,
        "st45": "4-6",
        "label": "Industrie de biens de production et intermédiaires (petits objets)",
        "function": lambda e: 2.59 * e,
    },
    {
        "st8": 3,
        "st45": "4-7",
        "label": "Industrie de biens de production et intermédiaires (objets volumineux)",
        "function": lambda e: 0.176 * e + 8.75,
    },
    {
        "st8": 3,
        "st45": "34-2",
        "label": "Industrie de la construction (réparations)",
        "function": lambda e: 0.614 * e + 19.720,
    },
    {
        "st8": 3,
        "st45": "34-3",
        "label": "Industrie de la construction (fabrication ou installation)",
        "function": lambda e: 0.352 * e + 7.574,
    },
    {
        "st8": 4,
        "st45": "7-2",
        "label": "Commerce de gros de produits intermédiaires fragiles",
        "function": lambda e: 15.086 * math.log(e) + 0.026,
    },
    {
        "st8": 4,
        "st45": "8-2",
        "label": "Commerce de gros de biens de consommation non alimentaires",
        "function": lambda e: 2.57 * e,
    },
    {
        "st8": 4,
        "st45": "9-2",
        "label": "Commerce de gros de biens de consommation alimentaires fragiles",
        "function": lambda e: 7.62 * e,
    },
    {
        "st8": 4,
        "st45": "7-3",
        "label": "Commerce de gros d’autres produits intermédiaires",
        "function": lambda e: 2.3 * e,
    },
    {
        "st8": 4,
        "st45": "8-3",
        "label": "Commerce de gros de biens de consommation non alimentaires",
        "function": lambda e: 2.52 * e,
    },
    {
        "st8": 4,
        "st45": "9-3",
        "label": "Commerce de gros d’autres biens de consommation alimentaires",
        "function": lambda e: 19.31 * math.log(e) + 10.01,
    },
    {
        "st8": 5,
        "st45": "10",
        "label": "Hypers et grands magasins polyvalents",
        "function": lambda e: 0.108 * e + 79.785,
    },
    {
        "st8": 5,
        "st45": "11",
        "label": "Supermarchés",
        "function": lambda e: 0.961 * e + 0.793,
    },
    {
        "st8": 5,
        "st45": "12",
        "label": "Grands magasins spécialisés",
        "function": lambda e: 10.626 * math.log(e),
    },
    {"st8": 6, "st45": "13", "label": "Supérettes", "function": lambda e: 1.57 * e},
    {
        "st8": 6,
        "st45": "14",
        "label": "Commerces de détail, habillement, chaussures, cuir",
        "function": lambda e: 3.029 * math.log(e),
    },
    {
        "st8": 6,
        "st45": "15",
        "label": "Boucheries",
        "function": lambda e: 5.124 * math.log(e) + 3.362,
    },
    {
        "st8": 6,
        "st45": "16",
        "label": "Epiceries, alimentation",
        "function": lambda e: 1.55 * e,
    },
    {
        "st8": 6,
        "st45": "17",
        "label": "Boulangeries - Pâtisseries",
        "function": lambda e: 1.057 * math.log(e) + 5.364,
    },
    {
        "st8": 6,
        "st45": "18",
        "label": "Cafés, hôtels, restaurants",
        "function": lambda e: 0.329 * e + 4.277,
    },
    {
        "st8": 6,
        "st45": "19",
        "label": "Pharmacies",
        "function": lambda e: 4.437 * math.log(e) + 12.43,
    },
    {
        "st8": 6,
        "st45": "20",
        "label": "Quincailleries",
        "function": lambda e: 0.107 * math.log(e) + 3.3347,
    },
    {
        "st8": 6,
        "st45": "21",
        "label": "Commerce d’ameublement",
        "function": lambda e: 1.413 * e + 0.685,
    },
    {
        "st8": 6,
        "st45": "22",
        "label": "Librairie papeterie",
        "function": lambda e: 4.998 * math.log(e) + 16.764,
    },
    {
        "st8": 6,
        "st45": "23",
        "label": "Autres commerces de détail",
        "function": lambda e: 3.304 * math.log(e) + 2.748,
    },
    {
        "st8": 6,
        "st45": "29",
        "label": "Commerces non sédentaires",
        "function": lambda e: -0.681 * math.log(e) + 5.015,
    },
    {
        "st8": 7,
        "st45": "6",
        "label": "Transport (sans entreposage)",
        "function": lambda e: 0.795 * math.log(e) + 1.053,
    },
    {
        "st8": 7,
        "st45": "25",
        "label": "Tertiaire pur",
        "function": lambda e: 0.074 * e + 1.801,
    },
    {
        "st8": 7,
        "st45": "27-2",
        "label": "Bureaux non tertiaires (agriculture, commerces de gros)",
        "function": lambda e: 0.64 * e,
    },
    {
        "st8": 7,
        "st45": "27-3",
        "label": "Bureaux non tertiaires (commerce de détail, industrie, transport,collectivités)",
        "function": lambda e: 4.657 * math.log(e),
    },
    {
        "st8": 7,
        "st45": "26Fa",
        "label": "Tertiaire autre",
        "function": lambda e: 0.157 * e + 1.941,
    },
    {"st8": 8, "st45": "30", "label": "Carrières", "function": lambda e: 12.12 * e},
    {
        "st8": 8,
        "st45": "28-2",
        "label": "Entrepôts (encombrants)",
        "function": lambda e: 4.37 * e,
    },
    {
        "st8": 8,
        "st45": "28-3",
        "label": "Entrepôts (dont transport)",
        "function": lambda e: 4.841 * e + 9.429,
    },
]

ALL_DISTRIBUTIONS = {
    "route_type": {"direct": 0.25, "round": 0.75},
}

# move_mode are french acronyms:
#   Compte Propre Destinataire (CPD)
#   Compte Propre Expéditeur (CPE)
#   Compte d'Autrui (CA)

ST8_DISTRIBUTIONS = {
    1: {
        "move_type": {"both": 0.26, "pickup": 0.34, "delivery": 0.40},
        "move_mode": {"CPD": 0.43, "CPE": 0.18, "CA": 0.09},
    },
    2: {
        "move_type": {"both": 0.15, "pickup": 0.22, "delivery": 0.63},
        "move_mode": {"CPD": 0.25, "CPE": 0.28, "CA": 0.46},
    },
    3: {
        "move_type": {"both": 0.08, "pickup": 0.42, "delivery": 0.50},
        "move_mode": {"CPD": 0.08, "CPE": 0.34, "CA": 0.58},
    },
    4: {
        "move_type": {"both": 0.04, "pickup": 0.63, "delivery": 0.33},
        "move_mode": {"CPD": 0.18, "CPE": 0.27, "CA": 0.56},
    },
    5: {
        "move_type": {"both": 0.24, "pickup": 0.17, "delivery": 0.60},
        "move_mode": {"CPD": 0.11, "CPE": 0.15, "CA": 0.74},
    },
    6: {
        "move_type": {"both": 0.14, "pickup": 0.13, "delivery": 0.73},
        "move_mode": {"CPD": 0.19, "CPE": 0.45, "CA": 0.37},
    },
    7: {
        "move_type": {"both": 0.14, "pickup": 0.28, "delivery": 0.58},
        "move_mode": {"CPD": 0.07, "CPE": 0.37, "CA": 0.56},
    },
    8: {
        "move_type": {"both": 0.04, "pickup": 0.59, "delivery": 0.27},
        "move_mode": {"CPD": 0.11, "CPE": 0.27, "CA": 0.62},
    },
}

# vehicle are french acronyms:
#   Poids Lourd (PL)
#   Véhicule Utilitaire Léger (VUL)

ST20_DISTRIBUTIONS = {
    1: {"vehicle": {"PL": 0.09, "VUL": 0.91}},
    2: {"vehicle": {"PL": 0.08, "VUL": 0.92}},
    3: {"vehicle": {"PL": 0.23, "VUL": 0.77}},
    4: {"vehicle": {"PL": 0.45, "VUL": 0.55}},
    5: {"vehicle": {"PL": 0.22, "VUL": 0.78}},
    6: {"vehicle": {"PL": 0.29, "VUL": 0.71}},
    7: {"vehicle": {"PL": 0.55, "VUL": 0.45}},
    8: {"vehicle": {"PL": 0.25, "VUL": 0.75}},
    9: {"vehicle": {"PL": 0.68, "VUL": 0.32}},
    10: {"vehicle": {"PL": 0.75, "VUL": 0.25}},
    11: {"vehicle": {"PL": 0.42, "VUL": 0.58}},
    12: {"vehicle": {"PL": 0.71, "VUL": 0.29}},
    13: {"vehicle": {"PL": 0.16, "VUL": 0.84}},
    14: {"vehicle": {"PL": 0.32, "VUL": 0.68}},
    15: {"vehicle": {"PL": 0.60, "VUL": 0.40}},
    16: {"vehicle": {"PL": 0.11, "VUL": 0.89}},
    17: {"vehicle": {"PL": 0.17, "VUL": 0.83}},
    18: {"vehicle": {"PL": 0.50, "VUL": 0.50}},
    19: {"vehicle": {"PL": 0.80, "VUL": 0.20}},
    20: {"vehicle": {"PL": 0.87, "VUL": 0.13}},
}


def get_st45_list(st8):
    return [
        {"st45": row["st45"], "function": row["function"]}
        for row in ST45_ST8
        if row["st8"] == st8
    ]
