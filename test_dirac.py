#!/usr/bin/env python

from LHCbDIRAC.DataManagementSystem.Client.DataIntegrityClient import DataIntegrityClient

if __name__ == '__main__':
    print("Starting test")
    replicas = {
            '/lhcb/MC/2017/DST/00095594/0000/00095594_00000014_2.dst': ['RAL_MC-DST'],
            '/lhcb/MC/2017/DST/00171845/0000/00171845_00002880_2.dst': ['RAL_MC-DST'],
        }
    client = DataIntegrityClient()
    res = client.checkPhysicalFiles(replicas=replicas, catalogMetadata={})
    print(res)
