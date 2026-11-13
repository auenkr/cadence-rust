import json
from pathlib import Path
import tempfile
import unittest
from compact_evidence import export_compact


class CompactEvidenceTests(unittest.TestCase):
    def test_raw_data_excluded_and_provenance_preserved(self):
        with tempfile.TemporaryDirectory() as tmp:
            root=Path(tmp);source=root/'raw';source.mkdir();out=root/'out'
            for name,value in {'samples.jsonl':'raw', 'server.log':'verbose',
                               'binary.sha256':'abc binary', 'gate.exit':'0',
                               'command.txt':'exact command', 'config.yml':'setting: 1'}.items():
                (source/name).write_text(value)
            report={'runs':{'go':{'correct':150,'client_outstanding_timeline':[1],
                                 'resources':{'timeline':[2],'mysql_samples':[3],
                                              'containers':{'matching':{'cpu_seconds':1}}}}}}
            nested=source/'artifact';nested.mkdir()
            (nested/'report.json').write_text(json.dumps(report))
            export_compact(source,out)
            result=json.loads((out/'artifact/report.json').read_text())
            self.assertEqual(result['runs']['go']['correct'],150)
            self.assertNotIn('timeline',result['runs']['go']['resources'])
            self.assertNotIn('client_outstanding_timeline',result['runs']['go'])
            provenance=json.loads((out/'provenance.json').read_text())
            self.assertEqual(provenance['binary_hashes']['binary.sha256'],'abc binary')
            self.assertEqual(provenance['configurations']['config.yml'],'setting: 1')
            self.assertEqual(provenance['exit_codes']['gate.exit'],'0')
            self.assertEqual({p.name for p in out.rglob('*') if p.is_file()}, {'report.json','provenance.json'})
