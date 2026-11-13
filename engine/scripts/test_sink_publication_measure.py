import unittest
from sink_publication_measure import require_keyed_access


class KeyedPlans(unittest.TestCase):
    def plan(self, outer='Tid Scan', inner='Index Scan', rows=1):
        return {'generic-update-1': {'rows':1,'explain':[{'Plan':{'Node Type':'ModifyTable','Plans':[
            {'Node Type':'Hash Join','Actual Rows':rows,'Plans':[
                {'Node Type':outer,'Relation Name':'groups','Actual Rows':1,'Actual Loops':1},
                {'Node Type':'Nested Loop','Plans':[{'Node Type':inner,'Relation Name':'groups','Actual Rows':1,'Actual Loops':1}]}]}]}}]}}

    def test_requires_bounded_target_and_primary_key_probe(self):
        require_keyed_access(self.plan())
        for arguments in [('Seq Scan','Index Scan',1),('Tid Scan','Seq Scan',1),
                          ('Tid Scan','Function Scan',1),('Index Scan','Index Scan',1),
                          ('Tid Scan','Index Scan',0)]:
            with self.subTest(arguments=arguments), self.assertRaises(AssertionError):
                require_keyed_access(self.plan(*arguments))

    def test_unrelated_scan_does_not_mask_sink_access(self):
        value=self.plan();root=value['generic-update-1']['explain'][0]['Plan']
        root['Plans'][0]['Plans'].append({'Node Type':'Seq Scan','Relation Name':'unrelated'})
        require_keyed_access(value)
        root['Plans'][0]['Plans']=[{'Node Type':'Tid Scan','Relation Name':'unrelated'}]
        with self.assertRaises(AssertionError):
            require_keyed_access(value)

    def test_rejects_quadratic_batch_rescans_and_target_revisits(self):
        value=self.plan();root=value['generic-update-1']['explain'][0]['Plan']
        root['Plans'][0]['Plans'].append({'Node Type':'CTE Scan','Actual Rows':1,'Actual Loops':5})
        with self.assertRaises(AssertionError):
            require_keyed_access(value)
        value=self.plan();node=value['generic-update-1']['explain'][0]['Plan']['Plans'][0]['Plans'][0]
        node['Actual Loops']=2
        with self.assertRaises(AssertionError):
            require_keyed_access(value)
