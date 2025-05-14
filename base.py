from glob import glob

import pandas as pd
from pandasql import sqldf

base_path = "./data"
corpus = "MetaLing"
fpost = "judgments.csv"

exclude = [
    "character-Ray-124-20250414",
    "character-Ray-123-20250414"
]


jfiles = glob(f"{base_path}/{corpus}_*/*_judgments.csv")
ufiles = glob(f"{base_path}/{corpus}_*/*_uses.csv")

def get_df():
    usedf = None
    for filename in ufiles:
       with open(filename) as csvdata:
           if usedf is None:
               usedf = pd.read_csv(csvdata, delimiter="\t")
           else:
               tdf = pd.read_csv(csvdata, delimiter="\t")
               usedf = pd.concat([usedf,tdf])

    judgedf = None
    for filename in jfiles:
       with open(filename) as csvdata:
           if usedf is None:
               judgedf = pd.read_csv(csvdata, delimiter="\t")
           else:
               tdf = pd.read_csv(csvdata, delimiter="\t")
               judgedf = pd.concat([judgedf,tdf])

    
    newdf = pd.read_csv(f"{base_path}/{fpost}", delimiter="\t")
    startdf = pd.concat([judgedf,newdf]) if len(newdf) else judgedf.copy()

    # only disagreement
    testdf = sqldf(f"""
    SELECT
      u1.`context` AS context1, u1.indexes_target_token AS pos1, j.identifier1,
      u2.`context` AS context2, u2.indexes_target_token AS pos2, j.identifier2,
      j.lemma
      --count, delta, timestamp,
      --JULIANDAY() - JULIANDAY(timestamp) AS time
    FROM (
        SELECT
            *
        FROM (
            SELECT
              COUNT(*) AS count,
              MIN(judgment) AS min,
              MAX(judgment) AS max,
              AVG(judgment) AS avg,
              MAX(judgment)-MIN(judgment) AS delta,
              MAX(timestamp) AS timestamp,
              identifier1, identifier2,
              GROUP_CONCAT(DISTINCT lemma) AS lemma
            FROM startdf
            WHERE judgment != 0
            AND identifier1 NOT IN ('{"','".join(exclude)}')
            AND identifier2 NOT IN ('{"','".join(exclude)}')
            GROUP BY identifier1, identifier2
            ORDER BY delta DESC, count DESC
        ) AS aggdf
        WHERE delta > 0 AND count < 5
        AND JULIANDAY() - JULIANDAY(timestamp) > 0.007 -- 10 minutes in days
        -- WHERE (delta > 0 AND count < 5) OR count = 1
        ORDER BY delta DESC
        -- LIMIT 1
    ) AS j
    JOIN usedf AS u1 ON u1.`identifier`=j.identifier1
    JOIN usedf AS u2 ON u2.`identifier`=j.identifier2
        """, locals())

    # first unannotated, then disagreement
    """
    SELECT
      u1.`context` AS context1, u1.indexes_target_token AS pos1, j.identifier1,
      u2.`context` AS context2, u2.indexes_target_token AS pos2, j.identifier2,
      j.lemma
    FROM (
        SELECT
            *
        FROM (
            SELECT
              COUNT(*) AS count,
              MIN(judgment) AS min,
              MAX(judgment) AS max,
              AVG(judgment) AS avg,
              MAX(judgment)-MIN(judgment) AS delta,
              identifier1, identifier2,
              GROUP_CONCAT(DISTINCT lemma) AS lemma
            FROM startdf
            GROUP BY identifier1, identifier2
            ORDER BY delta DESC, count DESC
        ) AS aggdf
        WHERE (delta > 0 AND count < 5) OR count = 1
        ORDER BY count ASC, delta DESC
        LIMIT 1
    ) AS j
    JOIN usedf AS u1 ON u1.`identifier`=j.identifier1
    JOIN usedf AS u2 ON u2.`identifier`=j.identifier2
        """

    return testdf