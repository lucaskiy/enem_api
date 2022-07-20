query_test = """
SELECT * FROM `dataengproject-355818.enem.enem_materialized` limit %s
"""

query_year = """
SELECT * FROM `dataengproject-355818.enem.enem_materialized` where ANO = %s limit %s
"""

query_state = """
SELECT * FROM `dataengproject-355818.enem.enem_materialized` where UF_PROVA = '%s' limit %s
"""