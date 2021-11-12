from .config import Config
import os
from neo4j import GraphDatabase


class Neo():

    def __init__(self):
        # uri = "neo4j://34.106.209.13:7687" # works
        # uri = "neo4j://neo4j:7687" # accessing local container via Neo4j doesn't work.
        # neo4j or bolt can be used here.  Differences are still unknown.
        # To do: find differences between them.
        # uri = "bolt://neo4j:7687"
        ml_config = Config.get_config(os.environ['ENV'])
        self.driver = GraphDatabase.driver(
            ml_config['neo4j']['uri'],
            auth=(ml_config['neo4j']['username'],
                  ml_config['neo4j']['password']))

    def create_session(self):
        return self.driver.session()
