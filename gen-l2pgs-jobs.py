#! /usr/bin/env python

import os
import sys
import glob
import json


def main():

    input_files = glob.glob('/data2/dilley/input-data/bridge-ops-gen-scenes/*')

    with open('l2pgs-template.json', 'r') as template_fd:
        template = template_fd.read()

    jobs = list()

    for input_file in input_files:
        template_json = json.loads(template)
        basename = os.path.basename(input_file)
        input_product_id = basename.replace('.tar.gz', '')
        input_url = 'file:///home/espa/input-data/bridge-ops-gen-scenes/{}'.format(basename)

        template_json['order']['input-product-id'] = input_product_id
        template_json['order']['input-url'] = input_url
        jobs.append(template_json)

    jobs_dict = {'jobs': jobs}
    jobs_pretty = json.dumps(jobs_dict, indent=4, sort_keys=True)

    with open('l2pgs-jobs.json', 'w') as jobs_fd:
           jobs_fd.write(jobs_pretty)

    return 0


if __name__ == '__main__':
    main()
