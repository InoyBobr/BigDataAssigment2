#!/usr/bin/env python3
import sys
from cassandra.cluster import Cluster

cluster = Cluster(["cassandra-server"])
session = cluster.connect()

session.execute("""
    CREATE KEYSPACE IF NOT EXISTS search_engine
    WITH replication = {'class': 'SimpleStrategy', 'replication_factor': 1}
""")

session.set_keyspace("search_engine")

session.execute("""
CREATE TABLE IF NOT EXISTS vocabulary (
    term TEXT PRIMARY KEY,
    df INT
)""")

session.execute("""
CREATE TABLE IF NOT EXISTS doc_index (
    term TEXT,
    doc_id TEXT,
    tf INT,
    doc_len INT,
    PRIMARY KEY (term, doc_id)
)""")

session.execute("""
CREATE TABLE IF NOT EXISTS doc_stats (
    doc_id TEXT PRIMARY KEY,
    doc_len INT
)""")


current_term = None
postings = set()

for line in sys.stdin:
    term, doc_id, tf, doc_len = line.strip().split("\t")
    tf = int(tf)
    doc_len = int(doc_len)

    session.execute("""
        INSERT INTO doc_index (term, doc_id, tf, doc_len)
        VALUES (%s, %s, %s, %s)
    """, (term, doc_id, tf, doc_len))

    session.execute("""
        INSERT INTO doc_stats (doc_id, doc_len)
        VALUES (%s, %s)
    """, (doc_id, doc_len))

    if current_term != term:
        if current_term:
            session.execute(
                "INSERT INTO vocabulary (term, df) VALUES (%s, %s)",
                (current_term, len(postings))
            )
        current_term = term
        postings = set()
    postings.add(doc_id)

if current_term and postings:
    session.execute(
        "INSERT INTO vocabulary (term, df) VALUES (%s, %s)",
        (current_term, len(postings))
    )

print("Reducer finished!")
