import unittest
from speedrun_etl.forecast import scenarios


class ForecastTests(unittest.TestCase):
    def test_expansion_weights_population_instead_of_treating_equal_strata_as_equal_population(self):
        population={'small':1000,'large':10}
        rows=[{'band':'small','requests':2},{'band':'small','requests':4},
              {'band':'large','requests':100},{'band':'large','requests':200}]
        result=scenarios(population,rows,'requests')
        self.assertEqual(4500,result['mean_projection'])
        self.assertEqual(3000,result['observed_min_scenario'])
        self.assertEqual(6000,result['observed_max_scenario'])

    def test_missing_stratum_cannot_silently_be_expanded_as_zero(self):
        with self.assertRaisesRegex(ValueError,'No completed'):
            scenarios({'small':100,'large':10},[{'band':'small','requests':2}],'requests')
