from services.contacts_service import ContactsService
from models.contacts_model import GetContactsResponse
from datetime import datetime
import pytest


def _seeds():
    return [
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
            ('days_to_first_sale_level', [
                {
                    'entity_id': '01169902-1ca5-430c-bb0a-39c275494e41',
                    'prediction': '\u003e 730',
                    'ingestion_timestamp': str(datetime.utcnow())
                }
            ]),
            ('lcv_level', [
                {
                    "entity_id": "01169902-1ca5-430c-bb0a-39c275494e41",
                    'prediction': 'High',
                    'ingestion_timestamp': str(datetime.utcnow())
                }
            ]),
            ('contact_entities_lk', [
                {
                    'entity_id': '01169902-1ca5-430c-bb0a-39c275494e41',
                    'contact_id': 1
                },
                {
                    'entity_id': '12345-1ca5-430c-bb0a-39c275494e41',
                    'contact_id': 1
                }
            ])
        ]),
        ('lake', [
            ('contacts', [
                {
                    'id': 1,
                    'icentris_client': 'bluesun',
                    'first_name': 'John',
                    'last_name': 'Doe',
                    'email': 'john.doe@test.com',
                    'phone': '111-111-1111',
                    'ingestion_timestamp': str(datetime.utcnow())
                },
                {
                    'id': 2,
                    'icentris_client': 'bluesun',
                    'first_name': 'Jane',
                    'last_name': 'Doe',
                    'email': 'jane.doe@test.com',
                    'phone': '222-222-2222',
                    'ingestion_timestamp': str(datetime.utcnow())
                },
                {
                    'id': 1,
                    'icentris_client': 'plexus',
                    'first_name': 'Jim',
                    'last_name': 'Bob',
                    'email': 'jim.bob@test.com',
                    'phone': '222-222-1111',
                    'ingestion_timestamp': str(datetime.utcnow())
                }
            ])
        ])]


@pytest.fixture
def seeds():
    return _seeds()


expected_contacts = {
    'contacts': [
        {
            'contact_id': 1,
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


def test_get_contacts(bigquery, env, seeds):
    bigquery.truncate(seeds)
    bigquery.seed(seeds)
    contact_ids = [1]
    expected_response = GetContactsResponse(**expected_contacts)
    contacts_service = ContactsService(env=env['env'])
    contacts = contacts_service.get_contacts(icentris_client='bluesun', contact_ids=contact_ids)
    assert contacts == expected_response


def test_post_contacts(bigquery, env, seeds):
    bigquery.truncate(seeds)
    contacts = seeds[1][1][0][1]
    contacts_service = ContactsService(env=env['env'])
    contacts_service.post_contacts(contacts)
