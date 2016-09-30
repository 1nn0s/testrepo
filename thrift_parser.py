from thrift.protocol import TBinaryProtocol
from thrift.transport import TTransport
import sys, os, re
sys.path.append("/home/ssp/lib/python")
sys.path.append("/home/ssp/sippy")
from ssp.thrift.ttypes import Calls, Cdrs, Surcharges, Commissions, CdrsCustomersDids, CallsSdp, CdrsConnectionsDids, CdrsDids, CdrsCustomers, CdrsConnections

files = []
for arg in sys.argv[1:]:
    files.append(arg)
name_path = os.path.basename(files[0])
names = re.findall('(\S.*?)-thrift.bin.*',name_path)
name = names[0]

if name == "calls":
    obj = Calls()
if name == "cdrs":
    obj = Cdrs()
if name == "calls_sdp":
    obj = CallsSdp()
if name == "cdrs_connections":
    obj = CdrsConnections()
if name == "cdrs_customers":
    obj = CdrsCustomers()
if name == "cdrs_dids":
    obj = CdrsDids()
if name == "cdrs_customers_dids":
    obj = CdrsCustomersDids()
if name == "cdrs_connections_dids":
    obj = CdrsConnectionsDids()
if name == "commissions":
    obj = Commissions()
if name == "surcharges":
    obj = Surcharges()

if __name__ == "__main__":
    if len (sys.argv) < 2:
        print "usage: thrift_parse.py full_path_to_the_[file|files_pattern_with_*|files_list])"
    else:
        for f in files:
            fd = open(f)
            t = TTransport.TFileObjectTransport(fd)
            p = TBinaryProtocol.TBinaryProtocolAccelerated(t)
            while True:
                try:
                    obj.read(p)
                    print obj
                except:
                    break
            fd.close()
