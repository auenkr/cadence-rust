import copy
import unittest
from datetime import datetime, timezone
from report import summarize, percentiles
from run import deployments


class PipelineTests(unittest.TestCase):
    def setUp(self):
        now=datetime.now(timezone.utc).isoformat()
        self.row=dict(run_id='r1',expected_result_sha256='hash',observed_result_sha256='hash',activity_completion_unix_nano=[int(datetime.now(timezone.utc).timestamp()*1e9)],workflow_id='w1',phase='measure',correct=True,start_ok=True,
                      generator_dropped=False,scheduled=now,started=now,completed=now,
                      start_rpc_ms=1,completion_ms=2,generator_lag_ms=0,
                      activity_ms=[1],activity_schedule_to_start_ms=[1],
                      decision_schedule_to_start_ms=[1,1])
        self.manifest=dict(rate=1,duration=1)

    def test_matching_only(self):
        go,rust=deployments('go'),deployments('rust')
        self.assertEqual([k for k in go if go[k]!=rust[k]],['matching'])

    def test_missing_duplicate_or_wrong_results_rejected(self):
        for rows in ([],[self.row,self.row],[dict(self.row,correct=False)],
                     [dict(self.row,generator_dropped=True)],[dict(self.row,error='timeout')]):
            with self.assertRaises(ValueError):summarize(rows,self.manifest)

    def test_generator_lag_and_bad_metrics_rejected(self):
        for change in (dict(generator_lag_ms=2000),dict(start_rpc_ms=float('nan')),
                       dict(activity_ms=[]),dict(activity_schedule_to_start_ms=[-1])):
            with self.assertRaises(ValueError):summarize([dict(self.row,**change)],self.manifest)

    def test_percentiles_and_correct_count(self):
        self.assertEqual(percentiles(list(range(1,1001)))['p999'],999)
        self.assertEqual(summarize([self.row],self.manifest)['correct'],1)

    def test_warmup_failure_invalidates_measurement(self):
        bad=copy.deepcopy(self.row);bad.update(workflow_id='warm',phase='warmup',correct=False)
        with self.assertRaises(ValueError):summarize([bad,self.row],self.manifest)
