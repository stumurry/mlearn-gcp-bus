from .neo import Neo
from .config import Config
from unittest.mock import patch
import pytest


@pytest.mark.skip(reason='Test not passing. Works locally, not in CICD.')
def test_create_session():
    ml_config = {
        'neo4j': {
            'uri': 'bolt://neo4j:7687',
            'username': 'neo4j',
            'password': 'dtm'
        }
    }
    with patch.object(Config, 'get_config', return_value=ml_config):
        with Neo().create_session() as session:
            # To Do: Move string queries into Pypher
            session.run("""
                MATCH (n:Person) DETACH DELETE n;
            """)
            session.run("""
                CREATE (Keanu:Person {name:'Keanu Reeves', born:1964})
            """)
            results = session.run("""
                MATCH (m:Person) WHERE m.name = "Keanu Reeves" RETURN m
            """)

            assert len(list(results)) == 1
