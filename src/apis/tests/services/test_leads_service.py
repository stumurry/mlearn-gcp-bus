from services.leads_service import LeadsService
from models.leads_model import LeadsModel
from datetime import datetime
from libs.shared.test import skipif_dev
import pytest


def _seeds():
    return[
        ('wrench', [
            ('entities', [
                {
                    'icentris_client': 'bluesun',
                    'entity_id': '01169902-1ca5-430c-bb0a-39c275494e41',
                    'email': 'test@icentris.com',
                    'phone': '555-1212',
                    'twitter': 'hashtag_something',
                    'file': 'foo.csv',
                    'ingestion_timestamp': str(datetime.utcnow())
                },
                {
                    'icentris_client': 'plexus',
                    'entity_id': '12345-1ca5-430c-bb0a-39c275494e41',
                    'email': 'test@icentris.com',
                    'phone': '555-1212',
                    'twitter': 'hashtag_something',
                    'file': 'foo2.csv',
                    'ingestion_timestamp': str(datetime.utcnow())
                }
            ]),
            ('channel', [
                {
                    'entity_id': '01169902-1ca5-430c-bb0a-39c275494e41',
                    'preferences': ['Email', 'Web Chat'],
                    'ingestion_timestamp': str(datetime.utcnow())
                }
            ]),
            ('match_score', [
                {
                    'entity_id': '01169902-1ca5-430c-bb0a-39c275494e41',
                    'scores': [
                        {
                            'icentris_client': 'bluesun',
                            'score': 66
                        },
                        {
                            'icentris_client': 'worldventures',
                            'score': 28
                        }
                    ],
                    'ingestion_timestamp': str(datetime.utcnow())
                }
            ]),
            ('adopt_curve', [
                {
                    'entity_id': '01169902-1ca5-430c-bb0a-39c275494e41',
                    'scores': [
                        {
                            'corpus': 'direct_sales_2 Persona',
                            'score': 'late_adopter'
                        },
                        {
                            'corpus': 'monat_2 Persona',
                            'score': 'late_adopter'
                        }
                    ],
                    'ingestion_timestamp': str(datetime.utcnow())
                }
            ]),
            ('persuasion', [
                {
                    'entity_id': '01169902-1ca5-430c-bb0a-39c275494e41',
                    'persuasion': ['Personalization', 'Authority'],
                    'ingestion_timestamp': str(datetime.utcnow())
                }
            ]),
            ('relationship_fit', [
                {
                    'entity_id': '01169902-1ca5-430c-bb0a-39c275494e41',
                    'prediction': 'Market Partner',
                    'ingestion_timestamp': str(datetime.utcnow())
                }
            ]),
            ('archetypes', [
                {
                    'entity_id': '01169902-1ca5-430c-bb0a-39c275494e41',
                    'archetype': 'Quick Description:\nThis individual is managerial in their mannerisms and assertively...',
                    'ingestion_timestamp': str(datetime.utcnow())
                }
            ]),
            ('lcv_level', [
                {
                    'entity_id': '01169902-1ca5-430c-bb0a-39c275494e41',
                    'prediction': 'High',
                    'ingestion_timestamp': str(datetime.utcnow())
                }
            ]),
            ('days_to_first_sale_level', [
                {
                    'entity_id': '01169902-1ca5-430c-bb0a-39c275494e41',
                    'prediction': '\u003e 730',
                    'ingestion_timestamp': str(datetime.utcnow())
                }
            ]),
            ('lead_entities_lk', [
                {
                    'entity_id': '01169902-1ca5-430c-bb0a-39c275494e41',
                    'lead_id': 1
                },
                {
                    'entity_id': '12345-1ca5-430c-bb0a-39c275494e41',
                    'lead_id': 1
                }
            ])
        ]),
        ('lake', [
            ('zleads', [
                {
                    'id': 1,
                    'icentris_client': 'bluesun',
                    'first_name': 'John',
                    'last_name': 'Doe',
                    'email': 'test@icentris.com',
                    'phone': '555-1212',
                    'twitter': 'hashtag_something',
                    'purchased': 0,
                    'leo_eid': 'z/',
                    'ingestion_timestamp': str(datetime.utcnow())
                },
                {
                    'id': 1,
                    'icentris_client': 'plexus',
                    'first_name': 'Jim',
                    'last_name': 'Bob',
                    'email': 'test@icentris.com',
                    'phone': '555-1212',
                    'twitter': 'hashtag_something',
                    'purchased': 0,
                    'leo_eid': 'z/',
                    'ingestion_timestamp': str(datetime.utcnow())
                }
            ])
        ])]


@pytest.fixture
def seeds():
    return _seeds()


expected_leads = {
    'leads': [
        {
            'lead_id': 1,
            'entity': {
                'entity_id': '01169902-1ca5-430c-bb0a-39c275494e41',
                'email': 'test@icentris.com',
                'phone': '555-1212',
                'twitter': 'hashtag_something'
            },
            'match_score': [
                {
                    'icentris_client': 'bluesun',
                    'score': 66
                },
                {
                    'icentris_client': 'worldventures',
                    'score': 28
                }
            ],
            'adopt_curve': [
                {
                    'corpus': 'direct_sales_2 Persona',
                    'score': 'late_adopter'
                },
                {
                    'corpus': 'monat_2 Persona',
                    'score': 'late_adopter'
                }
            ],
            'persuasion_angle': {
                'persuasion': ['Personalization', 'Authority']
            },
            'channel_strategy': {
                'preferences': ['Email', 'Web Chat']
            },
            'relationship_fit': {
                'prediction': 'Market Partner'
            },
            'archetype': 'Quick Description:\nThis individual is managerial in their mannerisms and assertively...',
            'lcv_level': 'High',
            'days_to_first_sale_level': '\u003e 730'
        }
    ]
}


@skipif_dev
def test_get_leads(bigquery, env, seeds):
    bigquery.truncate(seeds)
    bigquery.seed(seeds)

    lead_ids = [1]
    expected_leads_model = LeadsModel(**expected_leads)
    leads_service = LeadsService(env=env['env'])
    leads = leads_service.get_leads(icentris_client='bluesun', lead_ids=lead_ids)
    assert leads == expected_leads_model


@skipif_dev
def test_get_leads_no_match_score(bigquery, env, seeds):
    bigquery.truncate(seeds)
    seeds[0][1][2] = ('match_score', [])
    bigquery.seed(seeds)

    expected_leads = {
        'leads': [
            {
                'lead_id': 1,
                'entity': {
                    'entity_id': '01169902-1ca5-430c-bb0a-39c275494e41',
                    'email': 'test@icentris.com',
                    'phone': '555-1212',
                    'twitter': 'hashtag_something'
                },
                'match_score': [],
                'adopt_curve': [
                    {
                        'corpus': 'direct_sales_2 Persona',
                        'score': 'late_adopter'
                    },
                    {
                        'corpus': 'monat_2 Persona',
                        'score': 'late_adopter'
                    }
                ],
                'persuasion_angle': {
                    'persuasion': ['Personalization', 'Authority']
                },
                'channel_strategy': {
                    'preferences': ['Email', 'Web Chat']
                },
                'relationship_fit': {
                    'prediction': 'Market Partner'
                },
                'archetype': 'Quick Description:\nThis individual is managerial in their mannerisms and assertively...',
                'lcv_level': 'High',
                'days_to_first_sale_level': '\u003e 730'
            }
        ]
    }

    lead_ids = [1]
    expected_leads_model = LeadsModel(**expected_leads)
    leads_service = LeadsService(env=env['env'])
    leads = leads_service.get_leads(icentris_client='bluesun', lead_ids=lead_ids)
    assert leads == expected_leads_model


@skipif_dev
def test_get_leads_no_persuasion_angle(bigquery, env, seeds):
    bigquery.truncate(seeds)
    seeds[0][1][4] = ('persuasion_angle', [])
    bigquery.seed(seeds)

    expected_leads = {
        'leads': [
            {
                'lead_id': 1,
                'entity': {
                    'entity_id': '01169902-1ca5-430c-bb0a-39c275494e41',
                    'email': 'test@icentris.com',
                    'phone': '555-1212',
                    'twitter': 'hashtag_something'
                },
                'match_score': [
                    {
                        'icentris_client': 'bluesun',
                        'score': 66
                    },
                    {
                        'icentris_client': 'worldventures',
                        'score': 28
                    }
                ],
                'adopt_curve': [
                    {
                        'corpus': 'direct_sales_2 Persona',
                        'score': 'late_adopter'
                    },
                    {
                        'corpus': 'monat_2 Persona',
                        'score': 'late_adopter'
                    }
                ],
                'persuasion_angle': {
                    'persuasion': []
                },
                'channel_strategy': {
                    'preferences': ['Email', 'Web Chat']
                },
                'relationship_fit': {
                    'prediction': 'Market Partner'
                },
                'archetype': 'Quick Description:\nThis individual is managerial in their mannerisms and assertively...',
                'lcv_level': 'High',
                'days_to_first_sale_level': '\u003e 730'
            }
        ]
    }

    lead_ids = [1]
    expected_leads_model = LeadsModel(**expected_leads)
    leads_service = LeadsService(env=env['env'])
    leads = leads_service.get_leads(icentris_client='bluesun', lead_ids=lead_ids)
    assert leads == expected_leads_model
