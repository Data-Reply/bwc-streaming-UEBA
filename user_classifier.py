#!/usr/bin/env python
# -*- coding: utf-8 -*-
#
# Copyright 2020 Confluent Inc.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
# http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

from argparse import ArgumentParser, FileType
from configparser import ConfigParser
import sys
sys.path.append("utils")

from utils.user_classifier_utils import *
import logging

if __name__ == '__main__':
    
    # Parse the command line.
    parser = ArgumentParser(description="This script allows labeling users based on an aggregated input data stream using an unsupervised ML algorithm.")
    parser.add_argument('config_file', type=FileType('r'), help="Configuration file --> specify the correct path of the configuration.ini file")
    parser.add_argument('-t', dest="topic", default="transactions_aggregate",
                        help="Input topic name --> default: transactions_aggregate")
    parser.add_argument('-g', dest="group", default="example_avro",
                        help="Consumer group --> default: example_avro")
    parser.add_argument('-k', dest="n_cluster", default=3,
                        help="Number of cluster of the Kmeans algorithm (default = 3)")
    args = parser.parse_args()
    
    config_parser = ConfigParser()
    
    # file of input config
    config_parser.read_file(args.config_file)
    
    # save the configuration of schema registru client, consumer and producer
    sr_conf = dict(config_parser['schema_registry'])
    consumer_conf = dict(config_parser['consumer'])
    producer_conf = dict(config_parser['producer'])
    
    #istantiate the logging 
    logging.basicConfig(filename='streaming-UEBA.log', level=logging.INFO)
    logging.info('Started')
    
    logging.info(sr_conf)
    logging.info(consumer_conf)
    logging.info(producer_conf)
   
    consumer(args, sr_conf, consumer_conf, producer_conf)
