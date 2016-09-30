from thrift.protocol import TBinaryProtocol
from thrift.transport import TTransport
import sys, os, re, fileinput
sys.path.append("/home/ssp/lib/python")
sys.path.append("/home/ssp/sippy")
from ssp.thrift.ttypes import Calls, Cdrs, Surcharges, Commissions, CdrsCustomersDids, CallsSdp, CdrsConnectionsDids, CdrsDids, CdrsCustomers, CdrsConnections

if __name__ == "__main__":
    if len (sys.argv) < 2:
        print "usage: thrift_parse.py path_to_the_file(s)"
    else:
        files = []
        for arg in sys.argv[1:]:
            files.append(arg)
        name_path = os.path.basename(files[0])
        names = re.findall('(\S.*?)-thrift.bin.*',name_path)
        name = names[0]
        for f in files:
            for data in fileinput.input(f):
                while len(data) > 1:
                    fd = file(f,"r")
                    t = TTransport.TFileObjectTransport(fd)
                    p = TBinaryProtocol.TBinaryProtocolAccelerated(t)
                    try:
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
                        obj.read(p)
                        print obj
                    except EOFError:
                        break
                    fd.close()
