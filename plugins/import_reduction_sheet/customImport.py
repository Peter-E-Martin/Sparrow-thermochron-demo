# -*- coding: utf-8 -*-
"""
Created on Mon Aug 23 11:37:56 2021

@author: Peter
"""

from sparrow.import_helpers import BaseImporter

class TRaILpartial(BaseImporter):
    def __init__(self, db, data_dir, **kwargs):
        super().__init__(db)
        self.build_sample()
    
    def build_sample(self):
        sample = {
            'name':'Test_sample'
        }
        self.basic_import(sample)
        
    def basic_import(self, sample):
        self.db.load_data("sample", sample)