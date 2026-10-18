#!/usr/bin/env python3
"""Run outbound Rust JSON calls against the pinned Go tchannel-go server."""
import socket, subprocess, time, tempfile
from pathlib import Path
root=Path(__file__).resolve().parents[1]
artifacts=Path(tempfile.mkdtemp(prefix="tchannel-json-client-"))
port_sock=socket.socket();port_sock.bind(("127.0.0.1",0));port=port_sock.getsockname()[1];port_sock.close()
go_bin=artifacts/"go-server"; probe=root/"rust/target/debug/examples/tchannel_json_client_probe"
subprocess.run(["go","build","-o",str(go_bin),"./cmd/tchannel-json-test-server"],cwd=root/"go",check=True)
subprocess.run(["cargo","build","-p","cadence-server","--example","tchannel_json_client_probe"],cwd=root/"rust",check=True)
with (artifacts/"server.log").open("w") as log:
 server=subprocess.Popen([str(go_bin),"--addr",f"127.0.0.1:{port}"],stdout=log,stderr=subprocess.STDOUT)
 try:
  end=time.monotonic()+20
  while time.monotonic()<end:
   try:
    with socket.create_connection(("127.0.0.1",port),.1): break
   except OSError: time.sleep(.1)
  else: raise RuntimeError("Go TChannel listener did not start")
  cases=[("echo","null","test-json"),("echo","empty","test-json"),("large","large","test-json"),("echo","large-request","test-json"),("app-error","null","test-json"),("bad-json","null","test-json"),("delayed","timeout","test-json"),("echo","null","missing-service"),("does-not-exist","null","test-json"),("delayed","cancel","test-json")]
  for method,mode,service in cases:
   result=subprocess.run([str(probe),f"127.0.0.1:{port}",method,mode,service],capture_output=True,text=True,timeout=10)
   if method=="bad-json":
    if result.returncode==0 or "malformed JSON response" not in result.stderr: raise RuntimeError("malformed JSON response was not rejected")
   elif service=="missing-service" or method=="does-not-exist":
    if result.returncode==0 or "TChannel error" not in result.stderr: raise RuntimeError("unknown service/method did not produce a system error")
   elif result.returncode: raise RuntimeError(f"{method}/{mode} failed: {result.stderr}")
   if method=="large" and len(result.stdout)<200000: raise RuntimeError("large fragmented response truncated")
   if mode=="large-request" and len(result.stdout)<200000: raise RuntimeError("large fragmented request/echo response truncated")
   if method=="echo" and service!="missing-service":
    import json
    headers=json.loads(result.stdout)["headers"]
    if mode=="null" and headers is not None: raise RuntimeError(f"expected null headers, got {headers!r}")
    if mode=="empty" and headers != {}: raise RuntimeError(f"expected empty headers, got {headers!r}")
   if method=="app-error" and not result.stdout.startswith("APPLICATION "): raise RuntimeError("application error flag/body was not decoded")
   if mode=="timeout" and not result.stdout.startswith("TIMEOUT "): raise RuntimeError("deadline was not enforced")
   print(f"{method}/{mode}: {len(result.stdout)} response bytes")
 finally:
  server.terminate();server.wait(timeout=5)
print(f"Go TChannel JSON interop passed; artifacts: {artifacts}")
