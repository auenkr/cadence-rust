import unittest
from datetime import datetime,timezone
from suite import recovery_result
from compare_suite import aggregate


class SuiteTests(unittest.TestCase):
    def rows(self):
        return [{'scheduled':datetime.fromtimestamp(100+i/5,timezone.utc).isoformat(),
                 'correct':True,'completion_ms':50} for i in range(60)]

    def test_recovery_requires_full_healthy_window(self):
        rows=self.rows()
        result=recovery_result(rows,{'restored_unix_seconds':100},5)
        self.assertTrue(result['recovered'])
        self.assertEqual(result['seconds_after_restore'],5)
        rows[0]['completion_ms']=2000
        self.assertEqual(recovery_result(rows,{'restored_unix_seconds':100},5)['seconds_after_restore'],6)

    def test_incomplete_or_unrestored_recovery_is_not_success(self):
        self.assertFalse(recovery_result(self.rows()[:10],{'restored_unix_seconds':100},5)['recovered'])
        self.assertFalse(recovery_result(self.rows(),{},5)['recovered'])

    def test_partial_fixed_run_not_marked_complete(self):
        stage={'rate':5,'runs':{'00-rust':{'measured':150,'correct':150,'errors':{},'completions_per_second':5}}}
        self.assertFalse(aggregate(stage,'rust')['complete'])

if __name__=='__main__':unittest.main()
