#!/usr/bin/python
# -*- coding: utf-8 -*-
#
########################################
##
# @author:          Amyth
# @email:           mail@amythsingh.com
# @website:         www.techstricks.com
# @created_date: 13-07-2017
# @last_modify: Tue Jul 25 15:38:26 2017
##
########################################


from __future__ import print_function

import argparse
import csv
import json
import random
import subprocess
import uuid

try:
    from faker import Factory as faker_factory
except ImportError:
    print ("Faker factory not installed. Installing dependencies.")
    subprocess.call(['pip', 'install', 'Faker'])


class GraphFaker(object):
    """ Create dummy data for a graph database
    """

    def __init__(self, schema=None, test=True, limit=10,
            result_file='fake_data.json', result_format='json'):

        self.test_mode = test
        self.limit = limit
        self.result_file = result_file
        self.result_format = result_format
        self.data = {'vertexes':[], 'edges':[]}
        self.csv_data = {'vertexes':{}, 'edges':{}}
        self.faker = faker_factory().create()
        self.email_domains = [
            'gmail.com',
            'yahoo.com',
            'rediffmail.com',
            'hotmail.com',
            'msn.com',
            'yahoo.co.in'
        ]

        # Define default schema
        if not schema:
            self.schema = {
                'vertexes': [
                    {'name': 'person', 'count': 10000, 'p': ['name'], 'csv_headers': ['label','uid','name']},
                    {'name': 'candidate', 'count': 1000000, 'p': ['name'], 'csv_headers': ['label','uid','name']},
                    {'name': 'employee', 'count': 100000, 'p': ['name'], 'csv_headers': ['label','uid','name']},
                    {'name': 'recruiter', 'count': 100000, 'p': ['name'], 'csv_headers': ['label','uid','name']},
                    {'name': 'phone', 'count': 1200000, 'p': ['number'], 'csv_headers': ['label','uid','name']},
                    {'name': 'email', 'count': 1200000, 'p': ['email'], 'csv_headers': ['label','uid','name']},
                    {'name': 'linkedin', 'count': 10000, 'p': ['linkedin_id', 'profile_url'], 'csv_headers': ['label','uid','linkedin_id','profile_url']},
                    {'name': 'job', 'count': 50000, 'p': ['job_id'], 'csv_headers': ['label','uid','job_id']},
                    {'name': 'company', 'count': 50000, 'p': ['company_name'], 'csv_headers': ['label','uid','company_name']},
                    {'name': 'institute', 'count': 50000, 'p': ['institute_name'], 'csv_headers': ['label','uid','institute_name']},
                ],
                'edges' : [
                    {
                        'name': 'knows',
                        'bw':[
                            {
                                'relation': ('person', 'candidate', 5000000),
                                'properties' : []
                            },
                            {
                                'relation':('candidate', 'candidate', 1000000),
                                'properties' : []
                            },
                            {
                                'relation': ('employee', 'person', 1000000),
                                'properties' : []
                            },
                            {
                                'relation': ('employee', 'candidate', 1000000),
                                'properties' : []
                            },
                            {
                                'relation': ('candidate', 'recruiter', 50000),
                                'properties' : []
                            }
                        ],
                        'csv_headers': ['label1','uid1','label2','uid2'],
                    },
                    {
                        'name': 'has',
                        'bw': [
                            {
                                'relation': ('candidate', 'phone', 1000000),
                                'properties' : []
                            },
                            {
                                'relation': ('candidate', 'email', 1000000),
                                'properties' : []
                            },
                            {
                                'relation': ('candidate', 'linkedin', 10000),
                                'properties' : []
                            },
                        ],
                        'csv_headers': ['label1','uid1','label2','uid2'],
                    },
                    {
                        'name': 'worked_with',
                        'bw': [
                            {
                                'relation': ('candidate', 'candidate', 100000),
                                'properties' : []
                            },
                        ],
                        'csv_headers': ['label1','uid1','label2','uid2'],
                    },
                    {
                        'name': 'studied_with',
                        'bw': [
                            {
                                'relation': ('candidate', 'candidate', 100000),
                                'properties' : []
                            }
                        ],
                        'csv_headers': ['label1','uid1','label2','uid2'],
                    },
                    {
                        'name': 'worked_at',
                        'bw': [
                            {
                                'relation': ('candidate', 'company', 100000),
                                'properties' : []
                            }
                        ],
                        'csv_headers': ['label1','uid1','label2','uid2'],
                    },
                    {
                        'name': 'studied_at',
                        'bw': [
                            {
                                'relation': ('candidate', 'company', 100000),
                                'properties' : []
                            }
                        ],
                        'csv_headers': ['label1','uid1','label2','uid2'],
                    },
                    {
                        'name': 'provided_by',
                        'bw': [
                            {
                                'relation': ('phone', 'candidate', 100000),
                                'properties' : ['name']
                            },
                            {
                                'relation': ('email', 'candidate', 100000),
                                'properties' : ['name']
                            },
                            {
                                'relation': ('linkedin', 'candidate', 100000),
                                'properties' : ['name']
                            }
                        ],
                        'csv_headers': ['label1','uid1','label2','uid2','name'],
                    },
                    {
                        'name': 'posted',
                        'bw': [
                            {
                                'relation': ('recruiter', 'job', 100000),
                                'properties' : []
                            }
                        ],
                        'csv_headers': ['label1','uid1','label2','uid2'],
                    },
                    {
                        'name': 'liked',
                        'bw': [
                            {
                                'relation': ('recruiter', 'candidate', 100000),
                                'properties' : []
                            }
                        ],
                        'csv_headers': ['label1','uid1','label2','uid2'],
                    },
                    {
                        'name': 'is_a_match_for',
                        'bw': [
                            {
                                'relation': ('candidate', 'job', 100000),
                                'properties': ['score']
                            }
                        ],
                        'csv_headers': ['label1','uid1','label2','uid2','score'],
                    },
                ]
            }

    def create_vertexes_json(self):

	print ("\n\nCreating vertex data. Please wait.\n")
        for vertex in self.schema.get('vertexes'):
            print ("generating data for vertex {}".format(vertex.get('name', 'unknown')))
            prog = 0
            labels = [vertex.get('name')]
            count = self.limit if self.test_mode else vertex.get('count', 0)
            p = vertex.get('p')

            while prog < count:
                uid = uuid.uuid4().hex
                properties = {'uid': uid}
                for prop in p:
                    try:
                        prop_method = getattr(self, 'fake_%s' % prop)
                        properties[prop] = prop_method()
                    except (AttributeError, TypeError) as e:
                        print ('GraphFaker does not have any method named fake_%s' % (prop))
                        raise

                record = {
                    'properties': properties,
                    'labels': labels
                }
                self.data['vertexes'].append(record)
                prog += 1
                progress(prog, count)

    def create_vertexes_csv(self):
	print ("\n\nCreating vertex data. Please wait.\n")
        for vertex in self.schema.get('vertexes'):
            print ("generating data for vertex {}".format(vertex.get('name', 'unknown')))
            prog = 0
            label = [vertex.get('name')][0]
            count = self.limit if self.test_mode else vertex.get('count', 0)
            p = vertex.get('p')
            filename = '{}.csv'.format(label)
            self.csv_data['vertexes'][filename] = []
            self.csv_data['vertexes'][filename].append(vertex.get('csv_headers'))

            while prog < count:
                uid = uuid.uuid4().hex
                record = [label,uid]
                for prop in p:
                    try:
                        prop_method = getattr(self, 'fake_%s' % prop)
                        record.append(prop_method())
                    except (AttributeError, TypeError) as e:
                        print ('GraphFaker does not have any method named fake_%s' % (prop))
                        raise

                self.csv_data['vertexes'][filename].append(record)
                prog += 1
                progress(prog, count)

    def create_edges_json(self):

	print ("\n\nCreating vertex data. Please wait.\n")
        for edge in self.schema.get('edges',[]):
            edge_name = edge.get('name')
            bw_data = edge.get('bw')
            for relation in bw_data:
                rel = relation.get('relation')
                properties = relation.get('properties')
                print ("generating data for edge between {} <-> {}".format(rel[0], rel[1]))
                vertex_data = self.data.get('vertexes')
                rel1_data = [x for x in vertex_data if rel[0] in x.get('labels')]
                rel2_data = [x for x in vertex_data if rel[1] in x.get('labels')]
                count = self.limit if self.test_mode else rel[2]
                prog = 0

                while prog < count:
                    r1 = random.choice(rel1_data).get("properties").get("uid")
                    r2 = random.choice(rel2_data).get("properties").get("uid")
                    props = {}
                    for prop in properties:
                        try:
                            prop_method = getattr(self, 'fake_%s' % prop)
                            props[prop] = prop_method()
                        except (AttributeError, TypeError) as e:
                            print ('GraphFaker does not have any method named fake_%s' % (prop))
                            raise

                    record = {
                        'edge': edge_name,
                        'nodes': [r1, r2],
                    }
                    if props:
                        record['properties'] = props

                    self.data['edges'].append(record)
                    prog += 1
                    progress(prog, count)

    def create_edges_csv(self):

	print ("\n\nCreating edge data. Please wait.\n")
        for edge in self.schema.get('edges',[]):
            edge_name = edge.get('name')
            csv_headers = edge.get('csv_headers')
            filename = '{}.csv'.format(edge_name)
            self.csv_data['edges'][filename] = []
            self.csv_data['edges'][filename].append(csv_headers)
            bw_data = edge.get('bw')
            for relation in bw_data:
                rel = relation.get('relation')
                properties = relation.get('properties')
                print ("generating data for edge between {} <-> {}".format(rel[0], rel[1]))
                vertex_data = self.csv_data.get('vertexes')
                rel1_data = [x for x in vertex_data['{}.csv'.format(rel[0])] if 'label' not in x]
                rel2_data = [x for x in vertex_data['{}.csv'.format(rel[1])] if 'label' not in x]

                ## remove headers
                count = self.limit if self.test_mode else rel[2]
                prog = 0

                while prog < count:
                    r1 = random.choice(rel1_data)[1]
                    r2 = random.choice(rel2_data)[1]
                    record = [rel[0], r1, edge_name, rel[1], r2]
                    for prop in properties:
                        try:
                            prop_method = getattr(self, 'fake_%s' % prop)
                            record.append(prop_method())
                        except (AttributeError, TypeError) as e:
                            print ('GraphFaker does not have any method named fake_%s' % (prop))
                            raise

                    self.csv_data['edges'][filename].append(record)
                    prog += 1
                    progress(prog, count)

    def create_vertexes(self):
        try:
            create_method = getattr(self, 'create_vertexes_{}'.format(self.result_format))
            create_method()
        except NameError as exc:
            print ("Format Not Supported: {} : {}".format(self.result_format, str(exc)))

    def create_edges(self):
        try:
            create_method = getattr(self, 'create_edges_{}'.format(self.result_format))
            create_method()
        except NameError as exc:
            print ("Format Not Supported: {} : {}".format(self.result_format, str(exc)))

    def generate(self):
        self.create_vertexes()
        self.create_edges()

        try:
            write_method = getattr(self, 'write_to_{}'.format(self.result_format))
            write_method()
        except NameError as exc:
            print ("Format Not Supported: {} : {}".format(self.result_format, str(exc)))


    def write_to_json(self):
        json_data = json.dumps(self.data)
        with open(self.result_file, 'w') as write_file:
            write_file.write(json_data)

        print ("Successfully created data in %s" % self.result_file)

    def write_to_csv(self):
        vertexes = self.csv_data.get('vertexes')
        for filename, data in vertexes.iteritems():
            with open (filename, 'w') as csv_file:
                writer = csv.writer(csv_file)
                for entry in data:
                    writer.writerow(entry)

        edges = self.csv_data.get('edges')
        for filename, data in edges.iteritems():
            with open (filename, 'w') as csv_file:
                writer = csv.writer(csv_file)
                for entry in data:
                    writer.writerow(entry)

    def fake_name(self):
        """ Returns a dummy name.
        """
        return self.faker.name()

    def fake_company_name(self):
        """ Returns a dummy name.
        """
        fields = [
            'Logistics',
            'Infrastructure',
            'Technologies',
            'Communications',
            'Media',
            'Realty'
        ]
        return "%s %s pvt ltd." % (self.faker.name(), random.choice(fields))

    def fake_institute_name(self):
        """ Returns a dummy name.
        """
        fields = [
            'Technology',
            'Business Administration',
            'Computer Science',
            'Mathematics'
        ]
        return "%s institute of %s" % (self.faker.name(), random.choice(fields))

    def fake_number(self):
        """ Returns a dummy phone number.
        """
        return '+91{}'.format(''.join(
            ["%s" % random.randint(0, 9) for num in range(0, 10)]))

    def fake_email(self):
        """ Return a dummy email.
        """
        repl_chr = random.choice(['', '_', '.'])
        base_name = self.fake_name().replace(' ', repl_chr).lower()
        suffix = ''.join(["%s" % random.randint(0,9) for num in range(0,random.randint(0,4))])
        suffix = random.choice([suffix, suffix, ""])
        domain = random.choice(self.email_domains)

        return "{}{}@{}".format(base_name, suffix, domain)

    def fake_id(self, max_length=10):
        """ Returns a dummy linkedin id.
        """
        return ''.join(["%s" % random.randint(1, 9) for num in range(0, random.randint(0,9))])

    def fake_job_id(self):
        """ Returns a dummy linkedin id.
        """
        return self.fake_id(max_length=8)

    def fake_linkedin_id(self):
        """ Returns a dummy linkedin id.
        """
        return self.fake_id()

    def fake_profile_url(self):
        """ Returns a dummy profile url.
        """

        return "https://linkedin.com/in/{}".format(self.fake_name().replace(' ','').lower())

    def fake_score(self):
        return random.random()



def progress(iteration, total, prefix = '', suffix = '', decimals = 1, length = 100, fill = '█'):
    """ Updates the progress bar
    """
    percent = ("{0:." + str(decimals) + "f}").format(100 * (iteration / float(total)))
    filledLength = int(length * iteration // total)
    bar = fill * filledLength + '-' * (length - filledLength)
    print('\r%s |%s| %s%% %s' % (prefix, bar, percent, suffix), end = '\r')
    # Print New Line on Complete
    if iteration == total:
        print("\n")



if __name__ == '__main__':

    parser = argparse.ArgumentParser()
    parser.add_argument('--output', help="Absolute path of the file to output data to. Make sure the directory exists and have required permissions to write a file.")
    parser.add_argument('--limit', help="Limit of records to create. Example 100. This will create 100 records for each type of node and relation defined in schema.")
    parser.add_argument('--test_mode', help="If provided the script will run in test mode and the limit (or default limit) will apply")
    parser.add_argument('--result_format', help="Result format. Options available: `json`, `csv`")

    args = parser.parse_args()
    kwargs = {}
    for key, value in {
        'result_file': args.output,
        'limit': args.limit,
        'test': bool(args.test_mode),
        'result_format': args.result_format}.iteritems():
        if value:
            kwargs[key] = value

    gf = GraphFaker(**kwargs)
    gf.generate()
