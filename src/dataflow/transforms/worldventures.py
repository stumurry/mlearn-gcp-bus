import apache_beam as beam
from apache_beam.transforms import PTransform


class WorldVenturesClientPayload(beam.DoFn):
    def process(self, payload):
        if payload['icentris_client'].lower() == 'worldventures':
            yield payload


class WorldVenturesNormalizeUserType(beam.DoFn):
    def __init__(self, in_name='client_type', out_name='type'):
        self._in_name = in_name
        self._out_name = out_name

    def process(self, payload):
        if (self._in_name in payload
                and payload[self._in_name] not in [None, False, '']):
            client_type = payload[self._in_name].lower()
            client_type_switcher = {
                'worldventures': 'Autoship',
                'employee': 'Autoship',
                'rcscompany': 'Autoship',
                'b2b': 'Distributor',
                'import': 'Distributor',
                'distributor': 'Distributor',
                'freemium': 'Distributor',
                'uk-trial': 'Distributor'
            }

            if 'transfer' in client_type:
                payload[self._out_name] = 'Distributor'
            elif 'customer' in client_type:
                payload[self._out_name] = 'Autoship'
            else:
                payload[self._out_name] = client_type_switcher.get(client_type)
            if payload[self._out_name] is None:
                raise Exception(
                    f'client type `{client_type}` not found in list')

            yield payload


class WorldVenturesNormalizeUserStatus(beam.DoFn):
    def process(self, payload):
        if payload['client_status'] is not None:
            client_status = payload['client_status'].lower()
            status_switcher = {
                'active': 'Active',
                'grace': 'Active',
                'hold': 'Active',
            }
            payload['status'] = status_switcher.get(client_status, 'Inactive')

        yield payload


class WorldVenturesStagingUsersTransform(PTransform):
    def expand(self, pcoll):
        return (pcoll
                | 'Filter WorldVentures Payloads' >> beam.ParDo(
                    WorldVenturesClientPayload())
                | 'Normalize WorldVentures User Type' >> beam.ParDo(
                    WorldVenturesNormalizeUserType())
                | 'Normalize WorldVentures User Status' >> beam.ParDo(
                    WorldVenturesNormalizeUserStatus()))


class WorldVenturesNormalizeOrderType(beam.DoFn):
    def process(self, payload):
        user_type = payload['type'].lower()

        if (user_type == 'distributor' and
                payload['created'] <= payload['order_date']):
            payload['type'] = 'Wholesale'
        elif (user_type == 'distributor'
                and payload['created'] > payload['order_date']):
            payload['type'] = 'Autoship'
        elif user_type == 'autoship':
            payload['type'] = 'Autoship'

        yield payload


class WorldVenturesNormalizeOrderStatus(beam.DoFn):
    def process(self, payload):
        if payload['client_status'] is not None:
            client_status = payload['client_status'].lower()
            status_switcher = {
                'ach declined': 'Inactive',
                'cancelled': 'Inactive',
                'cc declined': 'Inactive',
            }
            payload['status'] = status_switcher.get(client_status, 'Active')

        yield payload


class WorldVenturesEnrichOrderCommissionUserId(beam.DoFn):
    def process(self, payload):
        client_type = payload['type'].lower()
        if client_type == 'distributor':
            payload['commission_user_id'] = payload['tree_user_id']
        elif (client_type == 'distributor' and
                payload['created'] > payload['order_date']):
            payload['commission_user_id'] = payload['sponsor_id']
        elif client_type != 'distributor':
            payload['commission_user_id'] = payload['sponsor_id']
        else:
            payload['commission_user_id'] = 'Missing commission_user_id'

        yield payload


class WorldVenturesStagingContactsTransform(PTransform):
    def expand(self, pcoll):
        return (pcoll
                | 'Filter WorldVentures Payloads'
                >> beam.ParDo(WorldVenturesClientPayload()))


class Log(beam.DoFn):
    def __init__(self, clue):
        self._clue = clue

    def process(self, payload):
        print('*' * 100)
        print(self._clue)
        print(payload)
        yield payload


class WorldVenturesStagingOrdersTransform(PTransform):
    def expand(self, pcoll):
        return (pcoll
                | 'Filter WorldVentures Payloads'
                >> beam.ParDo(WorldVenturesClientPayload())
                | 'Normalize WorldVentures User Type' >> beam.ParDo(
                    WorldVenturesNormalizeUserType(in_name='client_user_type'))
                | 'Normalize WorldVentures Order Type'
                >> beam.ParDo(WorldVenturesNormalizeOrderType())
                | 'Normalize WorldVentures Order Status'
                >> beam.ParDo(WorldVenturesNormalizeOrderStatus())
                | 'Enrich WorldVentures Order CommissionUserId'
                >> beam.ParDo(WorldVenturesEnrichOrderCommissionUserId()))
# | 'Log' >> beam.ParDo(Log('Final Payload')))


class WorldVenturesFilterActiveDistributors(beam.DoFn):
    def process(self, payload):
        client_type = payload['client_type'].lower()
        client_status = payload['client_status'].lower()
        if client_type == 'distributor' and (client_status == 'active' or client_status == 'grace'):
            del payload['client_type']
            del payload['client_status']
            yield payload


class WorldVenturesWarehouseDistributedOrdersTransform(PTransform):
    def expand(self, pcoll):
        return (pcoll
                | 'Filter WorldVentures Payloads'
                >> beam.ParDo(WorldVenturesClientPayload())
                | 'Allow Only Active Distributors' >> beam.ParDo(
                    WorldVenturesFilterActiveDistributors()
                ))
