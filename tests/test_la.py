"""Offline LA ingestion regression tests; all records are synthetic."""
import json
from pathlib import Path
import unittest
from unittest.mock import Mock, patch
import dinescores_pipeline as dp

FIXTURES = Path(__file__).parent / 'fixtures' / 'la'
SERVICE = 'https://services.arcgis.com/TestOrg/arcgis/rest/services/Inventory/FeatureServer'


def response(data=None, content=b''):
    r = Mock(status_code=200, content=content)
    r.json.return_value = data
    return r


def feature(oid, fid='F1', lat=34.1, lng=-118.2):
    return {'attributes': dict(ObjectId=oid, FACILITY_ID=fid, LATITUDE=lat, LONGITUDE=lng)}


class LATests(unittest.TestCase):
    def setUp(self):
        self._notes = dict(dp._GROUP_NOTES)
        dp._GROUP_NOTES.clear()

    def tearDown(self):
        dp._GROUP_NOTES.clear()
        dp._GROUP_NOTES.update(self._notes)

    def discovery(self, extra=()):
        data = json.loads((FIXTURES / 'discovery.json').read_text())
        data['results'].extend(extra)
        with patch.object(dp.requests, 'get', return_value=response(data)) as get:
            items = dp._la_hub_items()
        self.assertIn('Feature Service', get.call_args.kwargs['params']['q'])
        return items

    def test_discovery_dates_publisher_and_supported_types(self):
        template = json.loads((FIXTURES / 'discovery.json').read_text())['results'][0]
        foreign = dict(template, id='foreign', owner='other', modified=999)
        unsupported = dict(template, id='wrong-type', type='Shapefile', modified=999)
        inspection_service = dict(template, id='wrong-inspection',
                                  title='Environmental Health Restaurant and Market Inspections 12/31/26')
        self.assertEqual(self.discovery([foreign, unsupported, inspection_service]), {
            'inventory': 'inventory-service', 'inspections': dp.LA_INSPECTIONS_ITEM,
            'violations': dp.LA_VIOLATIONS_ITEM})

    def test_discovery_modified_tiebreak_and_four_digit_end_date(self):
        item = dict(id='newer', owner=dp.LA_HUB_OWNER, type='CSV', modified=2,
                    title='Environmental Health Restaurant and Market Inventory 09/30/2026')
        self.assertEqual(self.discovery([item])['inventory'], 'newer')
        item['title'] = 'Environmental Health Restaurant and Market Inspections 10/01/2023 to 09/30/2026'
        self.assertEqual(self.discovery([item])['inspections'], 'newer')

    def test_discovery_failure_retains_known_ids(self):
        with patch.object(dp.requests, 'get', side_effect=RuntimeError('offline')):
            self.assertEqual(dp._la_hub_items()['inventory'], dp.LA_INVENTORY_ITEM)

    def test_discovery_pages_and_invalid_dates(self):
        data = json.loads((FIXTURES / 'discovery.json').read_text())
        bad = dict(data['results'][0], id='invalid',
                   title='Environmental Health Restaurant and Market Inventory 99/99/26')
        with patch.object(dp.requests, 'get', side_effect=[
                response({'results': [bad], 'nextStart': 51}), response(data)]) as get:
            self.assertEqual(dp._la_hub_items()['inventory'], 'inventory-service')
        self.assertEqual([c.kwargs['params']['start'] for c in get.call_args_list], [1, 51])

    def inventory_responses(self):
        return [response({'owner': dp.LA_HUB_OWNER, 'type': 'Feature Service', 'url': SERVICE}),
                response({'layers': [{'id': 0}]}),
                response({'objectIdField': 'ObjectId', 'fields': [{'name': f} for f in ('ObjectId', 'FACILITY_ID', 'LATITUDE', 'LONGITUDE')]}),
                response({'features': [feature(1)], 'exceededTransferLimit': True}),
                response({'features': [feature(2, fid='F2')], 'exceededTransferLimit': False})]

    def test_service_pagination_clamped_pages_and_field_mapping(self):
        with patch.object(dp.requests, 'get', side_effect=self.inventory_responses()) as get, \
                patch.object(dp.time, 'sleep'):
            rows = list(dp._la_inventory_rows('inventory-service'))
        self.assertEqual(rows[0], {'FACILITY ID': 'F1', 'FACILITY LATITUDE': 34.1,
                                   'FACILITY LONGITUDE': -118.2})
        queries = [c.kwargs['params'] for c in get.call_args_list if c.args[0].endswith('/query')]
        self.assertEqual([q['resultOffset'] for q in queries], [0, 1])
        self.assertTrue(all(q['orderByFields'] == 'ObjectId' for q in queries))
        self.assertEqual(len(rows), 2)

    def test_csv_inventory_compatibility(self):
        with patch.object(dp.requests, 'get', side_effect=[
                response({'owner': dp.LA_HUB_OWNER, 'type': 'CSV'}),
                response(content=b'FACILITY ID,FACILITY  LATITUDE,FACILITY LONGITUDE\nF1,34.1,-118.2\n')]):
            self.assertEqual(list(dp._la_inventory_rows('csv'))[0]['FACILITY LATITUDE'], '34.1')

    def test_service_failures_raise_instead_of_shipping_partial_inventory(self):
        for bad in ({'error': {'message': 'inaccessible'}}, {},
                    {'features': [], 'exceededTransferLimit': True},
                    {'features': [feature(1)]}):
            replies = self.inventory_responses()
            replies[-1] = response(bad)
            with self.subTest(bad=bad), patch.object(dp.requests, 'get', side_effect=replies), \
                    patch.object(dp.time, 'sleep'), self.assertRaises(RuntimeError):
                list(dp._la_inventory_rows('service'))

    def test_publisher_url_and_schema_failures(self):
        for index, bad in [(0, {'owner': 'other', 'type': 'CSV'}),
                           (0, {'owner': dp.LA_HUB_OWNER, 'type': 'Feature Service', 'url': 'http://example.com'}),
                           (0, {'error': {'message': 'item not found'}}),
                           (1, {'layers': [{'id': 0}, {'id': 1}]}),
                           (2, {'fields': []})]:
            replies = self.inventory_responses()
            replies[index] = response(bad)
            with self.subTest(index=index, bad=bad), \
                    patch.object(dp.requests, 'get', side_effect=replies), self.assertRaises(RuntimeError):
                list(dp._la_inventory_rows('service'))

    def test_fetch_joins_coordinates_and_retains_csv_inspections_violations(self):
        replies = [response(content=(FIXTURES / name).read_bytes())
                   for name in ('inspections.csv', 'violations.csv')]
        with patch.object(dp, '_la_hub_items', return_value={'inventory': 'i', 'inspections': 's', 'violations': 'v'}), \
                patch.object(dp, '_la_inventory_rows', return_value=[{'FACILITY ID': 'F1',
                    'FACILITY LATITUDE': 34.1, 'FACILITY LONGITUDE': -118.2}]), \
                patch.object(dp.requests, 'get', side_effect=replies):
            rows = dp.fetch_la(since_date='2026-09-01')
        self.assertEqual(len(rows), 1)
        self.assertEqual((rows[0]['latitude'], rows[0]['longitude']), (34.1, -118.2))
        self.assertEqual(rows[0]['inspection_date'], '2026-09-30')
        self.assertEqual(len(rows[0]['violations']), 1)

    def test_quarterly_lag_note_names_the_extracts_newest_row(self):
        # Between releases the lookback re-reads the extract's last days, so
        # the group ships records and only this note explains its STALE flag.
        csv = (b'FACILITY ID,FACILITY NAME,SERIAL NUMBER,ACTIVITY DATE,SCORE,PROGRAM STATUS\n'
               b'F2,Synthetic Deli,S2,06/29/2026,95,ACTIVE\n'
               b'F1,Synthetic Cafe,S1,06/30/2026,92,ACTIVE\n')

        def fetch(**kwargs):
            replies = [response(content=csv),
                       response(content=(FIXTURES / 'violations.csv').read_bytes())]
            with patch.object(dp, '_la_hub_items', return_value={'inventory': 'i', 'inspections': 's', 'violations': 'v'}), \
                    patch.object(dp, '_la_inventory_rows', return_value=[]), \
                    patch.object(dp.requests, 'get', side_effect=replies):
                return dp.fetch_la(since_date='2026-06-29', **kwargs)

        self.assertEqual(len(fetch()), 2)
        self.assertIn('2026-06-30', dp._GROUP_NOTES.get('LA County', ''))
        dp._GROUP_NOTES.clear()
        self.assertEqual(len(fetch(limit=1)), 1)
        self.assertNotIn('LA County', dp._GROUP_NOTES)   # a bounded read never saw the end


if __name__ == '__main__':
    unittest.main()
