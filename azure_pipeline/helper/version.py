#!/usr/bin/env python3

import json
import sys
from urllib import request    
from pkg_resources import parse_version    

def versions(pkg_name):
    url = 'https://pypi.python.org/pypi/{}/json'.format(pkg_name)
    releases = json.loads(request.urlopen(url).read())['releases']
    return sorted(releases, key=parse_version, reverse=True)    

if __name__ == '__main__':
    # print(*versions(sys.argv[1]), sep='\n')
    libraryname = sys.argv[1]
    autoIncrementVersion = sys.argv[2]
    specifiedVersion = sys.argv[3]
    if autoIncrementVersion=='False':
        print(specifiedVersion)
    else:
        latestVersion = versions(libraryname)[0]
        position = latestVersion.rfind('.')
        if position!=-1:
            prefix = latestVersion[:position+1]
            subVersion = latestVersion[position+1:]
            subVersion = int(subVersion) + 1
            print(prefix + str(subVersion))
        else:
            print(int(latestVersion)+1)